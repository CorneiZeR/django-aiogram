"""Kafka: ``produce`` to send, ``poll`` to take, and a committed offset to settle.

The fourth transport, and the one whose model differs most. The other three settle a *message*:
a value removed from a list, an entry acknowledged in a group, a delivery tag acked. Kafka
settles a *position* — committing offset N means every message below N has been dealt with —
and that difference decides almost everything in this module.

The consequence that matters: a consumer holding several messages at once, which is what
``MAX_IN_FLIGHT`` allows, cannot settle them in whatever order their sends happen to finish.
Committing the offset of the second while the first is still in flight would claim the first as
done. So this broker commits the highest **contiguous** prefix and nothing beyond it, which is
the only reading of an offset that keeps at-least-once true.
"""

import asyncio
import logging
import threading
import time
from collections.abc import Mapping
from collections.abc import Sequence as Seq
from typing import TYPE_CHECKING, Any, ClassVar, cast

from django_aiogram.broker.base import REQUIRED, Broker
from django_aiogram.broker.kafka.client import (
    close_clients,
    consumer_for_thread,
    metadata_client,
    shared_producer,
)
from django_aiogram.broker.kafka.exceptions import ProduceRefusedError
from django_aiogram.broker.models import Taken

if TYPE_CHECKING:
    from confluent_kafka import Consumer

__all__ = ('KafkaBroker',)

logger = logging.getLogger('django_aiogram')

#: what `committed()` answers for a partition nothing has committed yet
_NO_OFFSET = -1001

#: how long each poll waits while a consumer is still joining its group. Long enough for
#: librdkafka to do something with it, short enough to notice the join the moment it lands
_JOIN_SLICE = 0.2

#: and how long a publish waits per turn for its own delivery callbacks. Shorter, because the
#: acknowledgement is the thing being waited for rather than a handshake
_CALLBACK_SLICE = 0.05

#: what "is one there?" costs on this transport. Asking Kafka is a fetch round trip, not a
#: lookup: measured, a message re-delivered after a seek arrived 0.504s later, so a poll of a
#: millisecond answers "nothing" about a topic that has something in it. `take_nowait` is
#: therefore bounded rather than instant, and this is the bound
_FETCH_BUDGET = 1.5


class KafkaBroker(Broker):
    """One topic, one consumer group, and offsets committed only where they are contiguous."""

    #: importable module, and the extra that installs it
    REQUIRES: ClassVar[tuple[str, str] | None] = ('confluent_kafka', 'kafka')

    #: this transport's own settings. The servers and the topic are required for the same
    #: reason the AMQP url and queue are: neither has a default worth baking in
    OPTIONS: ClassVar[Mapping[str, Any]] = {
        'KAFKA_BOOTSTRAP': REQUIRED,
        'KAFKA_TOPIC': REQUIRED,
        'KAFKA_GROUP': 'django-aiogram',
        'KAFKA_TIMEOUT': 10,
    }

    def __init__(self) -> None:
        """Hold nothing open; the first publish or take builds what it needs."""
        #: offsets taken and not yet settled, and offsets settled out of order, per partition.
        #: Both are needed to know what may be committed: the lowest unsettled offset is the
        #: ceiling, and anything settled above it waits for it
        self._unsettled: dict[int, set[int]] = {}
        self._settled: dict[int, set[int]] = {}
        #: where each partition has been rewound to, in the order it happened. A handle carries
        #: how many rewinds its partition had seen when it was issued, so settling can ask
        #: whether any rewind *since* then reached its offset — see `_settleable`.
        #:
        #: Positions rather than a count, and that distinction is the whole of it: a rewind to
        #: offset 1 invalidates the handles at 1 and above and leaves the one at 0 alone, which
        #: `release` relies on when it keeps the lower offsets outstanding. A count would refuse
        #: them all, and the lowest of them would then block this partition's commits for ever.
        #:
        #: Never pruned, and that is a decision rather than an oversight. Clearing it when
        #: nothing is outstanding was tried and it costs the diagnosis: a handle arriving after
        #: that is refused as unknown rather than as rewound, so the log stops saying a message
        #: is coming back. It grows by one integer per *refused* message, and a consumer
        #: refusing often enough for that to matter has a louder problem than this list
        self._rewinds: dict[int, list[int]] = {}
        self._lock = threading.Lock()

    def _topic(self) -> str:
        """Name the topic this broker produces to and consumes from."""
        return str(self.option('KAFKA_TOPIC'))

    def _bootstrap(self) -> str:
        """Name the servers to reach, as librdkafka spells them."""
        return str(self.option('KAFKA_BOOTSTRAP'))

    def _timeout(self) -> float:
        """How long any single call may take before the broker is unreachable."""
        return float(str(self.option('KAFKA_TIMEOUT') or 10))

    def _consumer(self) -> 'Consumer':
        """Reach this thread's consumer, subscribed on first use."""
        return consumer_for_thread(self._bootstrap(), self._topic(), str(self.option('KAFKA_GROUP')), self._timeout())

    # ------------------------------------------------------------------ producer

    def publish(self, payloads: Seq[bytes]) -> None:
        """Produce each payload and wait for the broker to acknowledge **these**.

        ``produce`` answers locally — measured at 0.2 microseconds, because librdkafka's own
        thread does the I/O — and returning there would be a weaker promise than the rest of
        this package makes. So the delivery callbacks are waited for, at 166 to 237 microseconds
        for one message across repeated runs — the second most expensive publish of the four,
        behind RabbitMQ's confirmed and persistent one at 323 to 393.

        Waited for by counting *this call's* callbacks rather than by flushing. The producer is
        one per process, and `flush` waits for everything in it: a caller would then wait on
        another thread's records, and — worse — could be told its own batch failed because a
        record produced by somebody else during the wait was still outstanding. Retrying that
        would send this batch twice.

        Waited for once at the end rather than per message: the acknowledgements come back
        concurrently, so a batch of ten costs about what one does rather than ten times it.
        """
        if not payloads:
            return
        producer, topic = shared_producer(self._bootstrap()), self._topic()
        failures: list[str] = []
        outstanding = [len(payloads)]
        # a lock of its own, not `self._lock`: the callbacks run on whichever thread is
        # polling, and `poll` on a process-wide producer serves other threads' callbacks too.
        # `outstanding[0] -= 1` is a read, a subtract and a store, so two callbacks can lose a
        # decrement between them — and a lost decrement means this call reports a batch the
        # broker accepted as unanswered, which a retry then sends twice
        counted = threading.Lock()

        def delivered(error: object, _message: object) -> None:
            """Count this record as answered, and note a refusal."""
            with counted:
                if error is not None:
                    failures.append(str(error))
                outstanding[0] -= 1

        for payload in payloads:
            producer.produce(topic, payload, on_delivery=delivered)
        deadline = time.monotonic() + self._timeout()
        while time.monotonic() < deadline:
            with counted:
                if not outstanding[0]:
                    break
            # `poll` is what serves callbacks, including other threads' — which is how
            # librdkafka works and is safe: whoever polls serves whatever is ready
            producer.poll(_CALLBACK_SLICE)
        with counted:
            left, refused = outstanding[0], list(failures)
        if left:
            raise ProduceRefusedError(topic, f'{left} message(s) of this batch went unanswered')
        if refused:
            raise ProduceRefusedError(topic, refused[0])

    async def apublish(self, payloads: Seq[bytes]) -> None:
        """Make the same publishes, off the loop's thread.

        The driver is synchronous, and the hand-off costs about 100 microseconds — which is
        most of what an awaited publish adds here, because the broker's acknowledgement costs
        roughly twice that. Measured across five runs, awaiting `aiokafka` natively is 354 to
        390 microseconds against 166 to 237 for this.
        """
        if not payloads:
            return
        await asyncio.to_thread(self.publish, payloads)

    # ------------------------------------------------------------------ consumer

    def take(self, timeout: float) -> Taken | None:
        """``poll`` for one message, waiting up to ``timeout`` seconds.

        The handle is ``(partition, offset)``: Kafka names a message by where it sits, which is
        also why settling one is not the same as settling the ones before it.
        """
        return self._wrap(self._polled(max(0.001, timeout)))

    def take_nowait(self) -> Taken | None:
        """Take one if one is there — after waiting for the group to say what "there" is.

        This is the one place Kafka cannot answer the contract's question immediately, and
        pretending otherwise would make the answer wrong rather than late. Joining a group is a
        round trip: until the coordinator has assigned partitions, a poll returns nothing
        because the client does not yet know where to look, so a freshly built consumer would
        report an empty topic that has messages in it.

        So this waits for the *assignment* and not for a message: once partitions are known, a
        bounded poll decides it. The join is paid once per consumer — measured at 3.18 seconds
        against a local broker — and the poll every time, because asking Kafka whether a
        message is there is a fetch rather than a lookup: measured, one re-delivered after a
        seek arrived 0.504 seconds later, so a poll of a millisecond answers "nothing" about a
        topic that has something in it.

        Worth knowing before putting this on a request path. `take` is the method the consumer
        loop uses, and it has a timeout of its own.
        """
        return self._wrap(self._polled(_FETCH_BUDGET))

    def _polled(self, timeout: float) -> object:
        """Poll, and let a consumer that has not joined its group finish joining first.

        Any message that arrives while waiting is returned rather than dropped — a poll is how
        librdkafka makes progress, so the join and the first delivery can land in the same call.
        """
        consumer = self._consumer()
        if not consumer.assignment():
            # joining, in slices of its own rather than the caller's: librdkafka makes progress
            # inside `poll`, and the millisecond a `take_nowait` asks for is not enough of it.
            # Measured on a local broker, the assignment took 3.05 seconds
            deadline = time.monotonic() + self._timeout()
            while not consumer.assignment() and time.monotonic() < deadline:
                message = consumer.poll(_JOIN_SLICE)
                if message is not None:
                    return message
        # then the caller's own wait, which starts *after* the join rather than being spent by
        # it. Being assigned is not the same as having a message in hand: measured, the fetch
        # that follows the join took another 0.5 seconds, and an earlier version of this
        # returned "nothing" the moment the assignment landed
        return consumer.poll(timeout)

    def _wrap(self, message: object) -> Taken | None:
        """Turn a polled message into a :class:`Taken`, or ``None`` for nothing and for errors.

        A partition boundary arrives as a message carrying an error rather than as data, so
        `error()` has to be asked before `value()` — otherwise the consumer would hand the
        broker's bookkeeping to a handler as if it were a message.
        """
        if message is None:
            return None
        error = message.error()  # type: ignore[attr-defined]
        if error is not None:
            logger.debug('kafka reported an event rather than a message', extra={'tg_error': str(error)})
            return None
        payload = message.value()  # type: ignore[attr-defined]
        if payload is None:
            return None
        partition, offset = message.partition(), message.offset()  # type: ignore[attr-defined]
        with self._lock:
            self._unsettled.setdefault(partition, set()).add(offset)
            epoch = len(self._rewinds.get(partition, ()))
        return Taken(payload, (partition, offset, epoch))

    def ack(self, handle: object) -> None:
        """Settle this message, and commit as far as the settled offsets reach.

        The rule that makes an offset honest: commit the highest offset whose predecessors are
        all settled, and nothing above it. A message settled out of order waits — recorded, not
        committed — because committing it would claim everything below it too, including sends
        that are still in flight.

        Nothing is committed when the lowest unsettled offset is the one just settled's
        neighbour; the next `ack` that closes the gap commits both.
        """
        partition, offset, epoch = _position(handle)
        with self._lock:
            if not self._settleable(partition, offset, epoch):
                return
            self._unsettled.get(partition, set()).discard(offset)
            self._settled.setdefault(partition, set()).add(offset)
            # the ceiling is the lowest offset still outstanding, and *nothing* outstanding
            # means there is no ceiling — every settled offset may be committed. Taking the
            # just-settled offset as the ceiling instead, which this did at first, leaves the
            # last message of a batch uncommitted for ever: settle 1 then 0 and the commit
            # stops at 0, so 1 is redelivered on the next restart
            outstanding = self._unsettled.get(partition) or set()
            settled = self._settled[partition]
            candidates = settled if not outstanding else {value for value in settled if value < min(outstanding)}
            highest = max(candidates, default=None)
            if highest is None:
                return
            self._settled[partition] = {value for value in settled if value > highest}

            from confluent_kafka import TopicPartition  # noqa: PLC0415 - the driver is an extra

            # committed inside the lock, deliberately. The epoch was checked above and a
            # `release` on this partition takes the same lock, so nothing can rewind between
            # the check and the commit — which is exactly the window that loses both messages.
            # It holds the lock across a round trip, and that is affordable: every settle
            # happens on the consumer thread today, so there is nobody to block
            #
            # `highest + 1`: a committed offset is the *next* one to read, measured —
            # committing message 0 makes `committed()` answer 1
            self._consumer().commit(offsets=[TopicPartition(self._topic(), partition, highest + 1)], asynchronous=False)

    def release(self, handle: object) -> None:
        """Rewind to this message, so it and everything after it are delivered again.

        Kafka has no per-message nack, and this is the honest consequence rather than a
        pretence: a position is all there is to give back, so seeking to it redelivers the
        messages after it too. On this transport more than any other, **build idempotency on
        your own business key** — the advice in `Delivery` is not decoration here.

        Not a no-op, for the reason the Redis list's is: leaving the offset uncommitted would
        redeliver it, but only when this consumer's partitions move to somebody else.
        """
        from confluent_kafka import TopicPartition  # noqa: PLC0415 - the driver is an extra

        partition, offset, epoch = _position(handle)
        with self._lock:
            if not self._settleable(partition, offset, epoch):
                return
            # the seek first, and the bookkeeping only if it worked. Rewinding is the part
            # that can fail — a consumer without this partition assigned answers
            # `_UNKNOWN_PARTITION`, measured — and doing it second left the offsets dropped
            # from tracking and the epoch bumped for a rewind that never happened, which
            # makes the message unsettleable by the thread that actually holds it.
            #
            # Inside the lock as well, so an `ack` cannot commit between the two
            self._consumer().seek(TopicPartition(self._topic(), partition, offset))
            for tracked in (self._unsettled, self._settled):
                held = tracked.get(partition)
                if held is not None:
                    tracked[partition] = {value for value in held if value < offset}
            # recorded so that settling can tell a handle this rewind invalidated from one it
            # left alone. Everything at or above `offset` will be delivered again; everything
            # below it is still in flight and still this worker's to settle
            self._rewinds.setdefault(partition, []).append(offset)

    def _settleable(self, partition: int, offset: int, epoch: int) -> bool:
        """Say whether this handle may settle anything at all; call with the lock held.

        Two questions, and a handle has to pass both. The first is whether any rewind since this
        handle was issued reached its offset — a rewind to a *higher* offset says nothing about
        it, which is what lets a refused message be given up without stranding the ones taken
        before it. The second is whether this broker is actually
        holding that offset — `_position` checks the *shape* of a handle and nothing else, so a
        duplicate or a hand-made tuple with the right epoch would otherwise reach `commit` or
        `seek`: a second `ack` would put an already-committed offset back into the settled set,
        and a `release` for an offset nobody took would move the live consumer to it.

        A handle this broker did not hand out, or handed out and has already been given back,
        settles nothing and is not an error — a retry that acknowledges twice is a caller doing
        its job twice, not a caller doing something wrong.

        A `release` puts a whole partition back to an offset — Kafka has no per-message nack —
        so every message taken before it will be delivered again, and a handle from before it
        names a delivery that no longer exists. Settling with one is not merely stale, it is
        wrong in the losing direction: with nothing outstanding, an `ack` for the *higher*
        offset of a released pair would commit past both, and a restart would skip them.

        Checked inside the caller's critical section rather than before it, because a check that
        releases the lock and then commits leaves the same window it exists to close. Every
        settle happens on the consumer thread today, so the window cannot open — but that is a
        property of the call graph rather than of this code, and the lock makes it one of this
        code.

        The rewind case is reported, because it means a send finished after its position was
        given up. An unknown offset is not: it is either a duplicate settle, which is harmless,
        or a handle from somewhere else, which `_position` already refuses when it is not even
        the right shape.
        """
        since = self._rewinds.get(partition, ())[epoch:]
        if any(position <= offset for position in since):
            logger.warning(
                'a message finished after its partition was rewound, so it will be redelivered',
                extra={'tg_key': self._topic()},
            )
            return False
        return offset in self._unsettled.get(partition, set())

    # ---------------------------------------------------------------- operations

    def reclaim(self) -> int | None:
        """``None``: an uncommitted offset is redelivered by the group, not by this package.

        A consumer that dies stops sending heartbeats, the group rebalances, and its partitions
        go to another member from the last committed offset — so everything it had taken and
        not settled is delivered again without anybody reclaiming anything.
        """
        return None

    def depth(self) -> int:
        """How many messages are past the committed offset, across the assigned partitions.

        The end of the log minus where this group has committed to. A partition nothing has
        committed on reads as the whole log, which is what a group that has never run sees.

        Every partition of the topic, read from metadata rather than from this consumer's
        assignment, and with this process's unsettled messages taken off.

        From metadata because a producer asking how deep the queue is — which is what
        `queue_depth()` and the healthcheck do — is usually a web process that consumes
        nothing, and answering from the assignment would report an empty queue to every one of
        them. Measured, both `get_watermark_offsets` and `committed` answer for a partition
        this consumer was never given.

        And through a client that never subscribes, which is the part that matters more: a
        subscription is group membership, so asking the depth from a web process used to make
        it a member and hand it partitions nobody would poll.
        """
        from confluent_kafka import TopicPartition  # noqa: PLC0415 - the driver is an extra

        # the *reader*, not this thread's consumer: subscribing is what makes a consumer a
        # group member, and a process that only publishes must not become one — it would be
        # given partitions it never polls, and on a single-partition topic the real worker
        # then gets nothing until that member's session times out. A healthcheck could starve
        # the consumer it was checking on
        consumer = metadata_client(self._bootstrap(), str(self.option('KAFKA_GROUP')), self._timeout())
        topic = self._topic()
        described = consumer.list_topics(topic, timeout=self._timeout()).topics.get(topic)
        if described is None or described.error is not None:
            # the topic does not exist yet, so nothing is waiting in it. A publish would
            # create it or refuse, and either way that is `publish`'s report to make
            return 0
        parts = [TopicPartition(topic, number) for number in sorted(described.partitions)]
        committed = {part.partition: part.offset for part in consumer.committed(parts, timeout=self._timeout())}
        waiting = 0
        for part in parts:
            low, high = consumer.get_watermark_offsets(part, timeout=self._timeout(), cached=False)
            position = committed.get(part.partition, _NO_OFFSET)
            waiting += max(0, high - (low if position == _NO_OFFSET else position))
        # minus what this process is holding, because taking a message on Kafka moves nothing:
        # the only position the server knows is the committed offset, so a message taken and
        # not settled is still "past the commit". The other three transports move a message out
        # of the queue when it is taken, and this is what makes the pair add up the same way
        return max(0, waiting - self.inflight_depth())

    def inflight_depth(self) -> int:
        """How many this worker has taken and not settled, counted here.

        Kafka has nothing to ask: an offset is either committed or not, and "taken but not
        settled" exists only in this process. Which is what the contract asks for.
        """
        with self._lock:
            return sum(len(offsets) for offsets in self._unsettled.values())

    async def adepth(self) -> int:
        """Read the same count off the loop's thread; see :meth:`apublish`."""
        return await asyncio.to_thread(self.depth)

    async def ainflight_depth(self) -> int:
        """Answer from this process, so no thread and no round trip."""
        return self.inflight_depth()

    @property
    def crash_safe(self) -> bool:
        """True, and the group is what makes it true.

        An offset that was never committed is delivered again to whoever takes the partition
        over. A worker killed mid-send committed nothing for that message, so it comes back.
        """
        return True

    def close(self) -> None:
        """Flush the producer and leave the group."""
        close_clients()


def _position(handle: object) -> tuple[int, int, int]:
    """Read a partition, offset and rewind count out of an opaque handle.

    Kafka names a message by where it sits, and the rewind count says whether that position
    still means what it did — so a handle of any other shape belongs to another broker, and the
    same refusal the Redis list and RabbitMQ make applies here.
    """
    triple = 3  # where the message sits, and which rewind of that partition it belongs to
    wrong = not isinstance(handle, tuple) or len(handle) != triple or not all(isinstance(p, int) for p in handle)
    if wrong:
        msg = (
            'this broker settles by position, so a handle must be the (partition, offset, epoch) '
            f'triple it handed out, not {type(handle).__name__}'
        )
        raise TypeError(msg)
    partition, offset, epoch = cast('tuple[int, int, int]', handle)
    return partition, offset, epoch
