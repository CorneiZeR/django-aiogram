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
from django_aiogram.broker.kafka.client import close_clients, consumer_for_thread, shared_producer
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
        """Produce each payload and wait for the broker to acknowledge it.

        ``produce`` answers locally — measured at 0.2 microseconds, because librdkafka's own
        thread does the I/O — and returning there would be a weaker promise than the rest of
        this package makes. So the delivery callbacks are waited for, at about 479 microseconds
        for one message, which makes this the most expensive publish of the four transports.

        Waited for once at the end rather than per message: the acknowledgements come back
        concurrently, so a batch of ten costs about what one does rather than ten times it.
        """
        if not payloads:
            return
        producer, topic = shared_producer(self._bootstrap()), self._topic()
        failures: list[str] = []

        def delivered(error: object, _message: object) -> None:
            """Note a refusal; success needs nothing recorded."""
            if error is not None:
                failures.append(str(error))

        for payload in payloads:
            producer.produce(topic, payload, on_delivery=delivered)
        remaining = producer.flush(self._timeout())
        if remaining:
            raise ProduceRefusedError(topic, f'{remaining} message(s) were still unsent after flushing')
        if failures:
            raise ProduceRefusedError(topic, failures[0])

    async def apublish(self, payloads: Seq[bytes]) -> None:
        """Make the same publishes, off the loop's thread.

        The driver is synchronous, and the hand-off costs about 100 microseconds — which is
        invisible here, because waiting for the broker costs five times that. Measured,
        awaiting `aiokafka` natively is 502 microseconds against 479 for this.
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
        return Taken(payload, (partition, offset))

    def ack(self, handle: object) -> None:
        """Settle this message, and commit as far as the settled offsets reach.

        The rule that makes an offset honest: commit the highest offset whose predecessors are
        all settled, and nothing above it. A message settled out of order waits — recorded, not
        committed — because committing it would claim everything below it too, including sends
        that are still in flight.

        Nothing is committed when the lowest unsettled offset is the one just settled's
        neighbour; the next `ack` that closes the gap commits both.
        """
        partition, offset = _position(handle)
        commit_to = None
        with self._lock:
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
            if highest is not None:
                commit_to = highest
                self._settled[partition] = {value for value in settled if value > highest}
        if commit_to is not None:
            from confluent_kafka import TopicPartition  # noqa: PLC0415 - the driver is an extra

            # `commit_to + 1`: a committed offset is the *next* one to read, measured —
            # committing message 0 makes `committed()` answer 1
            self._consumer().commit(
                offsets=[TopicPartition(self._topic(), partition, commit_to + 1)], asynchronous=False
            )

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

        partition, offset = _position(handle)
        with self._lock:
            for tracked in (self._unsettled, self._settled):
                held = tracked.get(partition)
                if held is not None:
                    tracked[partition] = {value for value in held if value < offset}
        self._consumer().seek(TopicPartition(self._topic(), partition, offset))

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
        """
        from confluent_kafka import TopicPartition  # noqa: PLC0415 - the driver is an extra

        consumer, topic = self._consumer(), self._topic()
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


def _position(handle: object) -> tuple[int, int]:
    """Read a partition and offset out of an opaque handle, or say where it came from.

    Kafka names a message by where it sits, so a handle of any other shape belongs to another
    broker — the same refusal the Redis list and RabbitMQ make, for the same reason.
    """
    pair = 2  # a partition and an offset, which is all Kafka knows about where a message is
    wrong = not isinstance(handle, tuple) or len(handle) != pair or not all(isinstance(part, int) for part in handle)
    if wrong:
        msg = (
            'this broker settles by position, so a handle must be a (partition, offset) pair, '
            f'not {type(handle).__name__}'
        )
        raise TypeError(msg)
    partition, offset = cast('tuple[int, int]', handle)
    return partition, offset
