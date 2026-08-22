"""Redis Streams: ``XADD`` to publish, ``XREADGROUP`` to take, ``XACK`` to settle.

The first transport here that names messages the way RabbitMQ and Kafka do — by id, not by
value — so it is the one that proves the seam rather than describing it. Everything
transport-specific stays inside this module; nothing above it learns that an entry has an id.

Two things 3.1.0 measured and rejected Streams for, as a *replacement* for the list, are
answered here rather than argued with:

* ``XGROUP CREATE … MKSTREAM`` leaves server-side state. As an opt-in transport that state is
  the project asking for it, which is the difference from the keyspace consumer it killed.
* ``MAXLEN`` trims exactly the entries a refused send deliberately leaves unacknowledged.
  Measured: trim past a pending entry and ``XPENDING`` still reports it while ``XAUTOCLAIM``
  hands the id back in its *deleted* list — the message is gone and no consumer can replay
  it. So this broker never trims by length; :meth:`trim` stops at the oldest unacknowledged
  entry, and unsafe trimming done outside it is reported rather than absorbed.
"""

import logging
from collections.abc import Mapping
from collections.abc import Sequence as Seq
from typing import Any, ClassVar

from django_aiogram.broker.base import REQUIRED, Broker
from django_aiogram.broker.models import Taken
from django_aiogram.broker.redis_streams.exceptions import (
    StreamLagUnknownError,
    StreamServerTooOldError,
)
from django_aiogram.redis import aget_redis, get_redis, heartbeat_ttl

__all__ = ('RedisStreamsBroker',)

logger = logging.getLogger('django_aiogram')

#: the single field each entry carries. A stream entry is a hash, and this package has
#: exactly one thing to put in it: the same envelope bytes the list stored on its own
_FIELD = b'payload'

#: the field `depth()` is answered from. Absent below Redis 7.0, and nil once entries have
#: been deleted — two different conditions, which is why the probe distinguishes them
_LAG = 'lag'


class RedisStreamsBroker(Broker):
    """A consumer group over one stream, with the pending list as the in-flight record."""

    #: importable module, and the extra that installs it — the same driver as the list
    REQUIRES: ClassVar[tuple[str, str] | None] = ('redis', 'redis')

    #: this transport's own settings. `REDIS_STREAM_KEY` has no default on purpose: see
    #: `_key`. The two timeouts are the package-wide ones, declared here because this
    #: broker reads them and `option` refuses a default that disagrees with that table
    OPTIONS: ClassVar[Mapping[str, Any]] = {
        'REDIS_URL': '',
        'REDIS_STREAM_KEY': REQUIRED,
        'REDIS_STREAM_GROUP': 'django-aiogram',
        'REDIS_TIMEOUT': 10,
        'BLPOP_TIMEOUT': 5,
    }

    def __init__(self) -> None:
        """Nothing reaches the server yet; the group is created on first use."""
        #: whether the server has been checked and the group created, once per process
        self._ready = False
        #: whether this consumer may be holding entries a `reclaim` handed it. `take` reads
        #: its own pending list while this is set, because `XREADGROUP … >` never returns an
        #: entry that was already delivered — a claimed entry would otherwise sit for ever
        self._recovering = True

    def _key(self) -> str:
        """Name the stream, from this broker's own required setting.

        Required rather than defaulted, and the reason is one keystroke wide: a default would
        sit next to ``REDIS_MESSAGES_KEY``, and ``XADD`` against a key holding a list answers
        ``WRONGTYPE`` on the first send rather than at startup. Naming it makes the two
        transports impossible to point at each other's data by accident, and `E047` asks for
        it before anything runs.
        """
        return str(self.option('REDIS_STREAM_KEY'))

    def _group(self) -> str:
        """Name the consumer group every worker joins.

        One group, so workers share the stream instead of each reading all of it — which the
        list gave for free and a stream does not. Defaulted, because unlike the key there is
        nothing another transport could collide with.
        """
        return str(self.option('REDIS_STREAM_GROUP'))

    def _consumer(self) -> str:
        """Name this process inside the group.

        Any name will do, and that is worth stating: unlike the list's in-flight key, the
        pending list belongs to the *group*, so a container that comes back under a fresh
        Docker hostname loses nothing. Measured — `XAUTOCLAIM` under a name that never
        existed before claims every entry a dead consumer held. See :attr:`needs_identity`.
        """
        from django_aiogram.eventlog.events import worker_identity  # noqa: PLC0415 - see below

        # imported here rather than at module scope: `events` reaches the recorder, and a
        # transport is imported from the checks, which may not import the event log
        return worker_identity()

    def _ensure(self) -> None:
        """Create the group, then prove the server can answer `depth()`. Once per process.

        ``id='0'`` and not ``'$'``: a group starting at the end would skip whatever was
        published before it existed, which for a queue means dropping messages nobody was
        told about. ``MKSTREAM`` so the first publisher does not have to be the one that
        creates the stream.

        The capability is probed rather than derived from a version string, for the same
        reason the list probes ``LMOVE``: what matters is whether this server answers
        ``XINFO GROUPS`` with a ``lag`` field, and asking it is one round trip that cannot be
        wrong. Reading ``INFO`` instead would also have been the more fragile choice —
        fakeredis, which the conformance suite runs on, implements streams including ``lag``
        and answers ``unknown command 'info'``.
        """
        if self._ready:
            return
        connection = get_redis()
        try:
            connection.xgroup_create(self._key(), self._group(), id='0', mkstream=True)
        except Exception as error:
            # narrowed by message rather than by class: the driver is imported lazily here,
            # so naming `redis.exceptions.ResponseError` would put the import back
            if 'BUSYGROUP' not in str(error):
                raise
        if not self._reports_lag(connection.xinfo_groups(self._key())):
            raise StreamServerTooOldError
        self._ready = True

    def _reports_lag(self, groups: object) -> bool:
        """Whether this server has the field at all, which is the 7.0 question.

        Absent means a server too old to count what is waiting. Present and nil means
        something deleted entries — a different fault, raised where it is read, so a group
        that has been trimmed unsafely still gets a message about *that* rather than about
        its Redis version.
        """
        for row in groups if isinstance(groups, list) else []:
            if isinstance(row, dict) and (_LAG in row or _LAG.encode() in row):
                return True
        return False

    # ------------------------------------------------------------------ producer

    def publish(self, payloads: Seq[bytes]) -> None:
        """One ``XADD`` per payload, pipelined so a chunk is one round trip.

        Nothing to publish is a return rather than an empty pipeline, for the same reason the
        list checks: a caller holding a list that turned out empty should not have to know
        which transport it is talking to.
        """
        if not payloads:
            return
        self._ensure()
        key = self._key()
        pipe = get_redis().pipeline(transaction=False)
        for payload in payloads:
            pipe.xadd(key, {_FIELD: payload})
        pipe.execute()

    async def apublish(self, payloads: Seq[bytes]) -> None:
        """Queue the same writes on the client belonging to the loop the caller is on."""
        if not payloads:
            return
        self._ensure()
        key = self._key()
        client = await aget_redis()
        pipe = client.pipeline(transaction=False)
        for payload in payloads:
            pipe.xadd(key, {_FIELD: payload})
        await pipe.execute()

    # ------------------------------------------------------------------ consumer

    def take(self, timeout: float) -> Taken | None:
        """Read what this consumer already holds first, then wait for something new.

        The two-phase read is not tidiness. ``XREADGROUP … >`` delivers only entries nobody
        has seen, so an entry a :meth:`reclaim` moved into this consumer's pending list would
        never come back through it. Reading id ``0`` returns exactly those — measured, the
        same ids that were delivered, and an empty list once they are acknowledged, which is
        what lets this stop asking.

        ``BLOCK`` is milliseconds and zero means *block for ever*, so a sub-second wait is
        floored at one millisecond rather than truncated to nothing: the consumer checks for
        shutdown between takes.
        """
        self._ensure()
        connection = get_redis()
        if self._recovering:
            taken = self._first(connection.xreadgroup(self._group(), self._consumer(), {self._key(): '0'}, count=1))
            if taken is not None:
                return taken
            # nothing of ours is outstanding, so stop paying for the extra read until the
            # next reclaim puts something there
            self._recovering = False
        block = max(1, int(timeout * 1000))
        return self._first(
            connection.xreadgroup(self._group(), self._consumer(), {self._key(): '>'}, count=1, block=block)
        )

    def take_nowait(self) -> Taken | None:
        """Read the same two phases without waiting, for a drain with no thread to block."""
        self._ensure()
        connection = get_redis()
        if self._recovering:
            taken = self._first(connection.xreadgroup(self._group(), self._consumer(), {self._key(): '0'}, count=1))
            if taken is not None:
                return taken
            self._recovering = False
        return self._first(connection.xreadgroup(self._group(), self._consumer(), {self._key(): '>'}, count=1))

    @staticmethod
    def _first(response: object) -> Taken | None:
        """Unwrap one entry from what ``XREADGROUP`` answers, or ``None`` for nothing.

        The shape is ``[[stream, [(id, {field: value}), …]], …]`` and an expired block is an
        empty list rather than nil, so both are handled here instead of at three call sites.
        The handle is the entry id: opaque above this module, and the only name a stream has
        for an entry.
        """
        if not isinstance(response, list) or not response:
            return None
        entries = response[0][1] if isinstance(response[0], (list, tuple)) and len(response[0]) > 1 else []
        if not entries:
            return None
        identifier, fields = entries[0]
        payload = fields.get(_FIELD) if isinstance(fields, dict) else None
        if payload is None:
            # an entry written by something else, with fields this package did not put there.
            # Acknowledging it would be a guess about someone else's data, so it is left
            # pending and reported: a stream shared with another producer is a configuration
            # mistake `_key`'s docstring exists to prevent
            logger.warning(
                'a stream entry carries no payload field and was left pending',
                extra={'tg_entry': identifier},
            )
            return None
        return Taken(payload, identifier)

    def ack(self, handle: object) -> None:
        """``XACK`` the entry this handle names, which drops it from the pending list."""
        get_redis().xack(self._key(), self._group(), handle)  # type: ignore[arg-type]

    def release(self, handle: object) -> None:
        """Make a refused entry reclaimable now, instead of after the idle threshold.

        The list implements this as a documented no-op because leaving a payload in its
        in-flight list already means "redeliver it". Copying that here would be wrong in a
        way that is easy to miss: the entry does stay pending, but :meth:`reclaim` only takes
        entries idle beyond the liveness TTL, so a message refused a second after it was
        taken would sit unsent for that long with nothing saying so.

        ``XCLAIM … IDLE`` sets the idle counter directly — measured, 0 to 90 000 ms in one
        call — so the next reclaim, this worker's or another's, picks it up straight away.
        Claiming it *for this consumer* rather than another keeps ownership honest: it has not
        been handed on, it has been given up.

        The idle it is set to is exactly :meth:`reclaim`'s threshold, and that boundary is
        load-bearing, so it was measured on both a real server and fakeredis: an entry whose
        idle *equals* ``min_idle_time`` is claimed, one a millisecond short is not. Idle only
        grows from there, so a release is reclaimable from the moment it happens.
        """
        get_redis().xclaim(
            self._key(),
            self._group(),
            self._consumer(),
            min_idle_time=0,
            message_ids=[handle],  # type: ignore[list-item]
            idle=heartbeat_ttl() * 1000,
        )
        self._recovering = True

    # ---------------------------------------------------------------- operations

    def reclaim(self) -> int | None:
        """Claim every entry idle longer than the liveness TTL, and say how many.

        ``XAUTOCLAIM`` from ``0`` in pages, following its cursor until it answers ``0-0``.
        The threshold is the same TTL a heartbeat would have expired at, so a consumer that
        is merely slow keeps what it holds while a dead one's work moves on.

        The third element of the answer is the reason this logs. It carries ids that were in
        the pending list and no longer exist in the stream — the fingerprint of a ``MAXLEN``
        trim or an ``XDEL`` reaching in-flight work. This broker cannot cause it and cannot
        undo it, so the only useful thing is to say so with the count.
        """
        self._ensure()
        connection = get_redis()
        key, group, consumer = self._key(), self._group(), self._consumer()
        idle = heartbeat_ttl() * 1000
        # '0' to start at the beginning; '0-0' is what XAUTOCLAIM answers when it is done,
        # so the two are deliberately not the same literal even though Redis reads them alike
        cursor: str | bytes = '0'
        claimed = lost = 0
        while True:
            answer = connection.xautoclaim(key, group, consumer, min_idle_time=idle, start_id=cursor, count=100)
            # three elements since 7.0, two on the 6.2 that `_ensure` has already refused —
            # unpacked defensively so a driver returning the older shape says so here
            following, entries, *rest = answer
            deleted = rest[0] if rest else []
            claimed += len(entries)
            lost += len(deleted)
            # Redis answers `0-0` when the scan is done. Nothing may: fakeredis returns the
            # last id it looked at and keeps returning it, claiming nothing — measured, and
            # it span this loop for ever. So a cursor that stops moving ends the scan too,
            # which is also what bounds this loop: it either advances or it is over
            if not following or following in ('0-0', b'0-0') or following == cursor:
                break
            cursor = following
        if lost:
            logger.warning(
                'entries were pending but no longer exist in the stream, so that work is lost; '
                'something trimmed or deleted unacknowledged entries',
                extra={'tg_lost': lost, 'tg_key': key},
            )
        if claimed:
            self._recovering = True
        return claimed

    def trim(self) -> int:
        """Drop acknowledged history, and stop at the oldest entry still unacknowledged.

        Not part of the contract and not called by the package: an operator's tool, exposed
        because the alternative a reader reaches for is ``MAXLEN``, which destroys in-flight
        work. ``XTRIM MINID`` at the oldest pending id, exactly, and nothing when there is
        no pending entry to stop at — a stream with nothing in flight is trimmed to its own
        tail, which would drop messages never delivered.
        """
        self._ensure()
        connection = get_redis()
        pending = connection.xpending(self._key(), self._group())
        oldest = pending.get('min') if isinstance(pending, dict) else None
        if not oldest:
            return 0
        return int(connection.xtrim(self._key(), minid=oldest, approximate=False) or 0)

    def depth(self) -> int:
        """How many entries the group has not been delivered, from ``XINFO GROUPS``.

        ``lag`` and not ``XLEN``: the length counts acknowledged history that has not been
        trimmed away, which would report a busy queue on an idle bot for ever.
        """
        self._ensure()
        return self._lag(get_redis().xinfo_groups(self._key()))

    def inflight_depth(self) -> int:
        """Count the group's pending entries, which is every consumer's in-flight work.

        The group's, not this consumer's, and deliberately: the number exists to answer
        "how much is unsettled", and after a crash the answer belongs to whoever picks it
        up. The list could only report its own because that was all it could see.
        """
        self._ensure()
        return self._count(get_redis().xpending(self._key(), self._group()))

    async def adepth(self) -> int:
        """Read the same count on the client belonging to the loop the caller is on."""
        self._ensure()
        client = await aget_redis()
        return self._lag(await client.xinfo_groups(self._key()))

    async def ainflight_depth(self) -> int:
        """Count the group's pending entries without blocking the loop."""
        self._ensure()
        client = await aget_redis()
        return self._count(await client.xpending(self._key(), self._group()))

    def _lag(self, groups: object) -> int:
        """Pull this group's ``lag`` out of ``XINFO GROUPS``, or refuse to guess.

        The field names come back as ``str`` even from a client handing everything else back
        as bytes — measured, and worth pinning here rather than at two call sites.

        ``lag`` is nil once entries have been removed from the middle of the stream, and that
        is a refusal rather than a zero: see :class:`StreamLagUnknownError`.
        """
        group = self._group()
        for row in groups if isinstance(groups, list) else []:
            if not isinstance(row, dict):
                continue
            name = row.get('name') or row.get(b'name')
            if isinstance(name, bytes):
                name = name.decode()
            if name != group:
                continue
            lag = row.get(_LAG, row.get(_LAG.encode()))
            if lag is None:
                raise StreamLagUnknownError(self._key(), group)
            return int(lag)
        # the group is gone from under us — dropped by hand, or the stream was deleted. Zero
        # is the honest count of what is waiting for a consumer that no longer has a group
        return 0

    @staticmethod
    def _count(pending: object) -> int:
        """Pull the ``pending`` total out of an ``XPENDING`` summary."""
        if isinstance(pending, dict):
            return int(pending.get('pending', pending.get(b'pending', 0)) or 0)
        return 0

    @property
    def crash_safe(self) -> bool:
        """True, and answered by the mechanism rather than probed for.

        ``XREADGROUP`` records every delivery in the group's pending list before the consumer
        sees it, so a worker killed mid-send leaves the entry there for the next
        :meth:`reclaim`. There is no version of this transport where that is untrue, which is
        the difference from the list — where it depended on whether the server had ``LMOVE``.
        """
        return True

    @property
    def needs_identity(self) -> bool:
        """False: the pending list belongs to the group, so any name can recover it.

        Measured — ``XAUTOCLAIM`` run under a consumer name that has never existed before
        claims every entry a dead consumer held. So ``WORKER_NAME`` buys nothing here, and a
        container coming back with a fresh Docker hostname strands nothing, which is the one
        thing `I001` exists to warn about for the list.
        """
        return False
