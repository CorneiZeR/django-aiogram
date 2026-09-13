"""The transport 3.x was, behind the contract every transport now answers.

Two Redis lists per worker: one queue everybody reads, and one in-flight list named after
the worker that took the message. The pair exists because a Redis list cannot say which
consumer holds an entry — which is why this is the one broker that answers True to
:attr:`~django_aiogram.broker.base.Broker.needs_identity`.
"""

import logging
import math
import time
from collections.abc import Mapping, Sequence
from collections.abc import Sequence as Seq
from typing import TYPE_CHECKING, Any, ClassVar

from django_aiogram.broker.base import Broker
from django_aiogram.broker.models import Liveness, Taken
from django_aiogram.eventlog.events import worker_identity
from django_aiogram.redis import (
    _escaped,
    aget_redis,
    as_bytes,
    as_command_argument,
    get_redis,
    heartbeat_key,
    heartbeat_ttl,
)

if TYPE_CHECKING:
    from redis import Redis
    from redis.asyncio import Redis as AsyncRedis

logger = logging.getLogger('django_aiogram')


def _watch_error() -> type[Exception]:
    """Return the driver's class for a watched key that changed, fetched when it is needed."""
    from redis import WatchError  # noqa: PLC0415 - the driver is an extra; see the note above

    return WatchError


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

    #: ``BLMOVE`` takes one source, and the move *is* the crash safety: reading several keys
    #: would mean ``BLPOP``, which loses the message between the pop and the send. So a
    #: container serving three queues on this transport runs three consumers and holds three
    #: connections -- stated rather than worked around, and `Deployment.md` says to spread the
    #: queues across containers by pool where that cost matters
    MULTIPLEXES: ClassVar[bool] = False

    #: this broker's own keys, which stopped being everyone's in 4.0
    QUEUE_OPTION: ClassVar[str] = 'REDIS_MESSAGES_KEY'
    CALL_TIMEOUT_OPTION: ClassVar[str] = 'REDIS_TIMEOUT'

    OPTIONS: ClassVar[Mapping[str, Any]] = {
        'REDIS_URL': '',
        'REDIS_MESSAGES_KEY': 'TELEGRAM_BOT_MESSAGE',
        'REDIS_TIMEOUT': 10,
    }

    def _queue(self) -> str:
        """Name the list this broker writes to and reads from, from its own declared option.

        Through :meth:`option` rather than the package-wide settings, because the key belongs
        to this transport: a stream has a name and a group, a topic has partitions, and none
        of them is a Redis list key. Each broker declares what it needs and reads it here.
        """
        return self.addressed()

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

    def _redis(self) -> 'Redis':
        """Return this instance's client, for the server its own settings name.

        Through here rather than `get_redis()` at each call site: ``REDIS_URL`` is a bot's
        setting, and a client asked for without them is the process's -- so a bot on its own
        Redis would address its own queue key on everybody else's server.
        """
        # the zero-argument call where this instance has no settings of its own, so a project
        # that swaps the accessor -- and every case in this suite that does -- keeps working:
        # `None` already means "the process's client", and asking for it by name is the same
        # question with one more argument
        return get_redis() if self.settings is None else get_redis(self.settings)

    async def _aredis(self) -> 'AsyncRedis':
        """Return the same client for a caller already on a loop, for the same reason."""
        if self.settings is None:
            return await aget_redis()
        return await aget_redis(self.settings)

    def publish(self, payloads: Sequence[bytes]) -> None:
        """One variadic ``RPUSH``, so a chunk is one round trip.

        Nothing to publish is a return, not a round trip: ``RPUSH key`` with no values is
        `wrong number of arguments for 'rpush' command` — measured — where a batching
        transport would have accepted it quietly.
        """
        if not payloads:
            return
        self._redis().rpush(self._queue(), *payloads)

    async def apublish(self, payloads: Sequence[bytes]) -> None:
        """Queue the same write, on the loop the caller is already on."""
        if not payloads:
            return
        client = await self._aredis()
        await client.rpush(self._queue(), *payloads)

    # ------------------------------------------------------------------ consumer

    def take(self, timeout: float, queues: 'Seq[str] | None' = None) -> Taken | None:
        """``BLMOVE`` where the server has it, ``BLPOP`` where it does not.

        Rounded up to whole seconds, and never to zero: Redis reads a zero timeout as
        *block for ever*, so a sub-second wait truncated to an integer would swallow
        `stop()` and let the liveness marker expire under a consumer that is fine.

        One queue, whatever the caller holds: see :attr:`MULTIPLEXES`.
        """
        self.one_queue(queues)
        waiting = max(1, math.ceil(timeout))
        connection = self._redis()
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

    def take_nowait(self, queues: 'Seq[str] | None' = None) -> Taken | None:
        """Move the same way without waiting, for a drain that has no thread to block."""
        self.one_queue(queues)
        connection = self._redis()
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
            raw = connection.lpop(self._queue())  # type: ignore[assignment]  # the widening above
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
            self._redis().lrem(self._inflight(), 1, as_command_argument(handle))
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

    @property
    def removes_queues(self) -> bool:
        """A list and its derived keys are this package's own, so it can remove them."""
        return True

    def discard(self, *, if_empty: bool = False) -> bool:
        """Delete this queue, every worker's in-flight list for it, and their heartbeats.

        All of them, because each is derived from the queue's own name: leaving the in-flight
        lists behind would leave the messages a dead worker held, which is the state
        `tgbot_reclaim` exists for and nothing will ever reclaim for a queue nobody serves.

        **The two derived shapes by name, not everything under the prefix.** A queue name may
        contain a colon, so `client` and `client:archive` can both be declared -- and Redis
        reads a colon as ordinary text, so a scan for `client:*` matches the *other queue's
        own list*. Deleting that would destroy a live client's messages, which is why this
        asks for `:processing:*` and `:heartbeat:*` and nothing wider.

        Under ``if_empty`` the read and the delete are one step: ``WATCH`` on every key, the
        lengths read inside it, and the delete in a transaction that fails if anything
        changed -- so a message published or taken in between costs a retry rather than the
        message.
        """
        connection = self._redis()
        queue = self._queue()
        pattern = _escaped(queue)
        derived = [
            *connection.scan_iter(match=f'{pattern}:processing:*', count=100),
            *connection.scan_iter(match=f'{pattern}:heartbeat:*', count=100),
        ]
        if not if_empty:
            connection.delete(queue, *derived)
            return True
        return self._discarded_if_empty(connection, queue, derived)

    @staticmethod
    def _discarded_if_empty(connection: 'Redis', queue: str, derived: list[Any]) -> bool:
        """Delete the queue and its derived keys only while every one of them is empty.

        The lists are watched, so a publish or a take between the read and the delete aborts
        the transaction: the answer is then "not empty", which is the truth by the time it is
        given. A heartbeat is not a message and is not counted -- it is a key a live consumer
        keeps warm, and a queue nothing publishes to has none for long.
        """
        holding = [queue, *[key for key in derived if b':processing:' in as_bytes(key)]]
        with connection.pipeline() as pipe:
            try:
                # untyped in redis-py's stubs, and the call is the whole mechanism here
                pipe.watch(*holding)  # type: ignore[no-untyped-call]
                if any(pipe.llen(key) for key in holding):
                    return False
                pipe.multi()
                pipe.delete(queue, *derived)
                pipe.execute()
            except _watch_error():
                # somebody published or took a message while this was deciding, so the queue
                # was not empty after all -- which is what the caller is told
                return False
            return True

    # ---------------------------------------------------------------- operations

    def reclaim(self, queues: 'Seq[str] | None' = None) -> int | None:
        """Move everything in flight back to the front of the queue, oldest first.

        Also the probe for crash safety: on a server without ``LMOVE`` the very first call
        fails, and this broker downgrades to plain pops for the rest of the process.

        Raises so the caller can retry — a Redis that was unreachable at startup left
        messages stranded, and reporting zero would look like a settled list.

        One queue, as :meth:`take` is.
        """
        self.one_queue(queues)
        connection = self._redis()
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
        return int(self._redis().llen(self._queue()) or 0)

    def inflight_depth(self, worker: str | None = None) -> int:
        """One ``LLEN`` on an in-flight list -- this worker's, or the one named.

        Answering for somebody else is what this transport is *for*: the list is per worker and
        survives the process that owned it, so a monitor asking what a container that never came
        back was holding is asking a question the key can answer. `tgbot_reclaim` addresses the
        same key by the same name.
        """
        return int(self._redis().llen(self._inflight(worker)) or 0)

    async def adepth(self) -> int:
        """Count the same way, on the client belonging to the loop the caller is on."""
        client = await self._aredis()
        return int(await client.llen(self._queue()) or 0)

    async def ainflight_depth(self, worker: str | None = None) -> int:
        """Count the same way, for this worker's in-flight list or the one named."""
        client = await self._aredis()
        return int(await client.llen(self._inflight(worker)) or 0)

    def alive(self) -> None:
        """Write the key the healthcheck reads, with a TTL a stalled loop cannot renew."""
        self._redis().set(heartbeat_key(queue=self._queue()), str(int(time.time())), ex=heartbeat_ttl())

    def liveness(self) -> Liveness:
        """How old the heartbeat is, or that there is none."""
        raw = self._redis().get(heartbeat_key(queue=self._queue()))
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
        return self.deadline()

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
