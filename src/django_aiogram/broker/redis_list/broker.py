"""The transport 3.x was, behind the contract every transport now answers.

Two Redis lists per worker: one queue everybody reads, and one in-flight list named after
the worker that took the message. The pair exists because a Redis list cannot say which
consumer holds an entry — which is why this is the one broker that answers True to
:attr:`~django_aiogram.broker.Broker.needs_identity`.
"""

import logging
import math
import time
from collections.abc import Mapping, Sequence
from typing import Any, ClassVar

from django_aiogram.broker.base import Broker
from django_aiogram.broker.models import Liveness, Taken
from django_aiogram.eventlog.events import worker_identity
from django_aiogram.redis import aget_redis, as_bytes, get_redis, heartbeat_key, heartbeat_ttl

logger = logging.getLogger('django_aiogram')


def _response_error() -> type[Exception]:
    """Fetch the driver's own error class when it is needed, and not before.

    `except _response_error() as error:` rather than a module-scope import, because
    importing this module must not fail on a machine without redis installed — that is what
    lets `Broker.verify` name the missing extra instead of a bare `ImportError` reaching a
    reader. `except` takes an expression, so the lazy fetch costs one attribute lookup on a
    path that is already handling an error.
    """
    from redis.exceptions import ResponseError  # noqa: PLC0415 - the whole point of this function

    return ResponseError


class RedisListBroker(Broker):
    """``RPUSH`` to publish, ``BLMOVE`` to take, ``LREM`` to settle."""

    #: this broker's own keys, which stopped being everyone's in 4.0
    CALL_TIMEOUT_OPTION: ClassVar[str] = 'REDIS_TIMEOUT'

    OPTIONS: ClassVar[Mapping[str, Any]] = {
        'REDIS_URL': '',
        'REDIS_MESSAGES_KEY': 'TELEGRAM_BOT_MESSAGE',
        'REDIS_TIMEOUT': 10,
        'BLPOP_TIMEOUT': 5,
    }

    def _queue(self) -> str:
        """Name the list this broker writes to and reads from, from its own declared option.

        Through :meth:`option` rather than the package-wide settings, because the key belongs
        to this transport: a stream has a name and a group, a topic has partitions, and none
        of them is a Redis list key. Each broker declares what it needs and reads it here.
        """
        return str(self.option('REDIS_MESSAGES_KEY'))

    def _inflight(self, worker: str | None = None) -> str:
        """Where one worker keeps what it is sending, derived from the queue's own name.

        Per worker, so a restarting one reclaims only its own — a shared list would let a
        starting worker pull a message out from under another still sending it. Takes a name
        so `tgbot_reclaim` can address a worker that is gone.
        """
        return f'{self._queue()}:processing:{worker or worker_identity()}'

    def __init__(self) -> None:
        """Assume crash safety until a server proves it does not have ``LMOVE``."""
        # discovered rather than configured: the first `LMOVE` against a pre-6.2 server
        # fails with "unknown command", and that is the only reliable probe there is
        self._reliable = True

    # ------------------------------------------------------------------ producer

    def publish(self, payloads: Sequence[bytes]) -> None:
        """One variadic ``RPUSH``, so a chunk is one round trip.

        Nothing to publish is a return, not a round trip: ``RPUSH key`` with no values is
        `wrong number of arguments for 'rpush' command` — measured — where a batching
        transport would have accepted it quietly.
        """
        if not payloads:
            return
        get_redis().rpush(self._queue(), *payloads)

    async def apublish(self, payloads: Sequence[bytes]) -> None:
        """Queue the same write, on the loop the caller is already on."""
        if not payloads:
            return
        client = await aget_redis()
        await client.rpush(self._queue(), *payloads)

    # ------------------------------------------------------------------ consumer

    def take(self, timeout: float) -> Taken | None:
        """``BLMOVE`` where the server has it, ``BLPOP`` where it does not.

        Rounded up to whole seconds, and never to zero: Redis reads a zero timeout as
        *block for ever*, so a sub-second wait truncated to an integer would swallow
        `stop()` and let the liveness marker expire under a consumer that is fine.
        """
        waiting = max(1, math.ceil(timeout))
        connection = get_redis()
        if self._reliable:
            try:
                raw = connection.blmove(self._queue(), self._inflight(), waiting, 'LEFT', 'RIGHT')
            except _response_error() as error:
                # the same downgrade `take_nowait` and `reclaim` do. `run` calls `reclaim`
                # before the first take, so the ordinary pre-6.2 server downgrades there —
                # but a connection that reaches a server without LMOVE *after* a reclaim
                # succeeded, a failover for one, left this raising on every iteration with
                # `run` logging and retrying for ever
                if not self._downgrade_without_lmove(error):
                    raise
                return self.take(timeout)
        if not self._reliable:
            item = connection.blpop([self._queue()], timeout=waiting)
            raw = None if item is None else item[1]
        return None if raw is None else Taken(as_bytes(raw), raw)

    def take_nowait(self) -> Taken | None:
        """Move the same way without waiting, for a drain that has no thread to block."""
        connection = get_redis()
        raw: bytes | str | None
        if self._reliable:
            try:
                raw = connection.lmove(self._queue(), self._inflight(), 'LEFT', 'RIGHT')
            except _response_error() as error:
                # a caller draining by hand never ran `reclaim`, so this is where it can
                # first meet a server without LMOVE; without the downgrade the raw error
                # would come out of a documented helper
                if not self._downgrade_without_lmove(error):
                    raise
                return self.take_nowait()
        else:
            # lpop only widens to a list when given a count
            raw = connection.lpop(self._queue())  # type: ignore[assignment]
        return None if raw is None else Taken(as_bytes(raw), raw)

    def ack(self, handle: object) -> None:
        """``LREM`` the one entry whose value is this handle.

        The handle is the payload, because a Redis list has no other name for an entry —
        so a handle of any other shape came from a different broker, and saying that is
        better than letting redis-py complain about a type it was handed.
        """
        if not isinstance(handle, bytes | str):
            msg = f'this broker settles by value, so a handle must be bytes or str, not {type(handle).__name__}'
            raise TypeError(msg)
        if not self._reliable:
            # nothing was moved, so there is nothing to remove: a plain pop already
            # took the message off the queue and the in-flight list stayed empty
            return
        try:
            # redis-py's stubs say str, but bytes round-trip identically
            # redis-py's stubs say str, but bytes round-trip identically
            get_redis().lrem(self._inflight(), 1, handle)  # type: ignore[arg-type]
        except Exception:
            # worst case the message is redelivered on the next start
            logger.exception('failed to acknowledge a delivered message', extra={'tg_key': self._inflight()})

    def release(self, handle: object) -> None:
        """Nothing, and that is the whole implementation.

        A message this broker has taken is *already* sitting in the in-flight list, so
        leaving it there is what makes it redeliverable — either by this worker's next
        `reclaim` or by `tgbot_reclaim` naming the worker by hand. There is no nack to
        send, and inventing one would mean pushing the payload back and creating a second
        copy of a message that never left.
        """

    # ---------------------------------------------------------------- operations

    def reclaim(self) -> int | None:
        """Move everything in flight back to the front of the queue, oldest first.

        Also the probe for crash safety: on a server without ``LMOVE`` the very first call
        fails, and this broker downgrades to plain pops for the rest of the process.

        Raises so the caller can retry — a Redis that was unreachable at startup left
        messages stranded, and reporting zero would look like a settled list.
        """
        connection = get_redis()
        count = 0
        try:
            # RIGHT->LEFT keeps the original order at the front of the queue
            while connection.lmove(self._inflight(), self._queue(), 'RIGHT', 'LEFT'):
                count += 1
        except _response_error() as error:
            # WRONGTYPE, NOPERM and friends say nothing about LMOVE support
            if not self._downgrade_without_lmove(error):
                raise
            return 0
        return count

    def depth(self) -> int:
        """One ``LLEN`` on the queue."""
        return int(get_redis().llen(self._queue()) or 0)

    def inflight_depth(self, worker: str | None = None) -> int:
        """One ``LLEN`` on an in-flight list -- this worker's, or the one named.

        Answering for somebody else is what this transport is *for*: the list is per worker and
        survives the process that owned it, so a monitor asking what a container that never came
        back was holding is asking a question the key can answer. `tgbot_reclaim` addresses the
        same key by the same name.
        """
        return int(get_redis().llen(self._inflight(worker)) or 0)

    async def adepth(self) -> int:
        """Count the same way, on the client belonging to the loop the caller is on."""
        client = await aget_redis()
        return int(await client.llen(self._queue()) or 0)

    async def ainflight_depth(self, worker: str | None = None) -> int:
        """Count the same way, for this worker's in-flight list or the one named."""
        client = await aget_redis()
        return int(await client.llen(self._inflight(worker)) or 0)

    def alive(self) -> None:
        """Write the key the healthcheck reads, with a TTL a stalled loop cannot renew."""
        get_redis().set(heartbeat_key(), str(int(time.time())), ex=heartbeat_ttl())

    def liveness(self) -> Liveness:
        """How old the heartbeat is, or that there is none."""
        raw = get_redis().get(heartbeat_key())
        if raw is None:
            return Liveness(reported=True, age=None, detail='no heartbeat has been written')
        try:
            written = int(as_bytes(raw))
        except (TypeError, ValueError):
            return Liveness(reported=True, age=None, detail='the heartbeat is not a timestamp')
        return Liveness(reported=True, age=max(0.0, time.time() - written))

    @property
    def call_ceiling(self) -> float:
        """``REDIS_TIMEOUT``, the deadline every call this transport makes carries.

        The socket deadline rather than ``BLPOP_TIMEOUT``: the pop is asked to wait for less
        than this on purpose, so the longest a call can take is the deadline, not the wait.
        """
        return float(max(1, int(str(self.option('REDIS_TIMEOUT')))))

    @property
    def crash_safe(self) -> bool:
        """False on a Redis without ``LMOVE``, where the pop and the send are two steps."""
        return self._reliable

    @property
    def needs_identity(self) -> bool:
        """True: the in-flight list is keyed on the worker's name and nothing else is."""
        return True

    def _downgrade_without_lmove(self, error: Exception) -> bool:
        """Fall back to plain pops when the server has no ``LMOVE``, and say whether it did."""
        if 'unknown command' not in str(error).lower():
            return False
        if self._reliable:
            self._reliable = False
            logger.warning(
                'crash-safe delivery unavailable: this Redis predates LMOVE (6.2); '
                'a worker killed mid-send may lose that one message',
                extra={'tg_key': self._queue()},
            )
        return True
