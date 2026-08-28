"""RabbitMQ: ``basic_publish`` to send, ``basic_get``/``consume`` to take, a delivery tag to settle.

The third transport, and the one that needs the least of this package. An unacknowledged
message returns to the queue when the channel drops, so there is no worker name to keep, no
in-flight list to reclaim and nothing to write down about liveness — three things the Redis
list needs machinery for.

``pika`` rather than ``aio-pika``, decided by measurement: a coroutine reaching a thread costs
67 to 85 microseconds against 121 to 131 for a synchronous caller reaching a loop, so the driver
that needs no crossing on the synchronous path — which is where ``bot.send()`` is called from —
is also the cheaper one where it does have to cross. See
:mod:`django_aiogram.broker.rabbitmq.client`.
"""

import asyncio
import logging
from collections.abc import Mapping
from collections.abc import Sequence as Seq
from typing import TYPE_CHECKING, Any, ClassVar

from django_aiogram.broker.base import REQUIRED, Broker
from django_aiogram.broker.exceptions import WorkerDepthUnavailableError
from django_aiogram.broker.models import Taken
from django_aiogram.broker.rabbitmq.client import (
    channel_for_thread,
    channel_generation,
    close_connections,
)
from django_aiogram.broker.rabbitmq.exceptions import QueueRefusedError

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel

__all__ = ('RabbitMQBroker',)

logger = logging.getLogger('django_aiogram')


def _position(handle: object) -> tuple[int, int]:
    """Read the channel generation and delivery tag out of an opaque handle.

    AMQP names a delivery by an integer the channel assigned, and the generation says *which*
    channel — so a handle of any other shape belongs to a different broker, and saying that is
    better than letting the driver complain about a type it was handed. The Redis list makes
    the same refusal for the same reason.
    """
    pair = 2  # the channel it came from, and the tag that channel gave it
    if not isinstance(handle, tuple) or len(handle) != pair or not all(isinstance(p, int) for p in handle):
        msg = (
            'this broker settles by delivery tag, so a handle must be the (channel, tag) pair '
            f'it handed out, not {type(handle).__name__}'
        )
        raise TypeError(msg)
    return handle


class RabbitMQBroker(Broker):
    """One durable queue, consumed with acknowledgements, settled by delivery tag."""

    #: importable module, and the extra that installs it
    REQUIRES: ClassVar[tuple[str, str] | None] = ('pika', 'rabbitmq')

    #: this transport's own settings. Both names are required: a URL carries credentials and
    #: a host, and neither has a default worth baking in, while the queue is where messages
    #: go — the same reason the stream key has none
    CALL_TIMEOUT_OPTION: ClassVar[str] = 'RABBITMQ_TIMEOUT'

    OPTIONS: ClassVar[Mapping[str, Any]] = {
        'RABBITMQ_URL': REQUIRED,
        'RABBITMQ_QUEUE': REQUIRED,
        'RABBITMQ_PREFETCH': 0,
        'RABBITMQ_TIMEOUT': 10,
    }

    def __init__(self) -> None:
        """Hold nothing open; the first publish or take opens this thread's channel."""
        #: the open consumer, the timeout it was opened with, and the channel it belongs to.
        #: `consume` fixes its inactivity timeout when the generator is made, so a different
        #: one needs a new generator — and so does a different *channel*: a connection replaced
        #: under this broker leaves a generator whose channel is dead, and advancing that is a
        #: failure where opening a new consumer was the whole intent
        self._consumer: Any = None
        self._consumer_timeout: float | None = None
        self._consumer_channel: Any = None
        #: delivery tags handed out and not yet settled. AMQP does not report an unacked count
        #: — `message_count` counts only what is ready, measured — and the contract asks what
        #: *this worker* holds, which is exactly what this knows
        self._unsettled: set[object] = set()

    def _queue(self) -> str:
        """Name the queue this broker publishes to and consumes from."""
        return str(self.option('RABBITMQ_QUEUE'))

    def _channel(self) -> 'BlockingChannel':
        """Reach this thread's channel, declaring the queue on first use.

        The deadline comes from :meth:`call_timeout`, which is the only reader of
        ``RABBITMQ_TIMEOUT``. It used to be read again here with an `or 10` on it, and the two
        disagreed on every value a project wrote and `or` treats as unset: a configured `0` gave
        pika 10 while :attr:`call_ceiling` said 0, so `W004` and the consumer's cap were computed
        from a deadline no publish, get or confirm on this channel ever carried.
        """
        return channel_for_thread(
            str(self.option('RABBITMQ_URL')),
            self._queue(),
            # the same `or` idiom the deadline no longer uses, and kept on purpose: 0 *is* this
            # option's declared default and its meaning, so nothing a project writes changes hands
            # here except a value `int` would refuse outright. Refusing it by name needs a rule per
            # transport option, which is #23 rather than this line
            int(str(self.option('RABBITMQ_PREFETCH') or 0)),
            type(self).call_timeout(),
        )

    # ------------------------------------------------------------------ producer

    def publish(self, payloads: Seq[bytes]) -> None:
        """One confirmed, mandatory, persistent publish per payload.

        ``mandatory`` so a queue that is not there is an error rather than a message dropped
        by the exchange, and confirms so the broker has answered before this returns. Measured
        at 323 to 393 microseconds against 15 to 20 with only the confirm taken off —
        the difference is the promise the rest of this package already makes, where ``RPUSH``
        answers with a length. Most of it is the disk: without persistence, 135 to 173.
        """
        if not payloads:
            return
        from pika import BasicProperties, DeliveryMode  # noqa: PLC0415 - the driver is an extra
        from pika.exceptions import NackError, UnroutableError  # noqa: PLC0415 - as above

        channel, queue = self._channel(), self._queue()
        # the driver's own enum rather than the number 2: AMQP spells persistence as a
        # delivery mode, and its stubs will not accept a bare int for it
        properties = BasicProperties(delivery_mode=DeliveryMode.Persistent)
        # one try around the loop, not one per payload: a refusal ends the batch either way,
        # and the caller is told which queue refused rather than which message it stopped at
        try:
            for payload in payloads:
                channel.basic_publish('', queue, payload, properties=properties, mandatory=True)
        except (NackError, UnroutableError) as refusal:
            raise QueueRefusedError(queue, type(refusal).__name__) from refusal

    async def apublish(self, payloads: Seq[bytes]) -> None:
        """Make the same publishes, off the loop's thread.

        The driver is synchronous, so this is where the hand-off is paid — measured at 67 to 85
        microseconds, against the 121 to 131 the *other* driver would have charged the
        synchronous caller instead. A thread rather than a second connection library: one way of talking
        to RabbitMQ is enough.
        """
        if not payloads:
            return
        await asyncio.to_thread(self.publish, payloads)

    # ------------------------------------------------------------------ consumer

    def take(self, timeout: float) -> Taken | None:
        """Wait up to ``timeout`` for one message, or answer ``None``.

        ``consume`` with an inactivity timeout, which yields ``(None, None, None)`` when
        nothing arrived — measured — so the consumer gets its turn back and can check whether
        it is shutting down.
        """
        channel = self._channel()
        self._notice_a_new_channel(channel)
        if self._consumer is None or self._consumer_timeout != timeout:
            self._cancel(channel)
            self._consumer = channel.consume(self._queue(), inactivity_timeout=max(0.001, timeout))
            self._consumer_timeout = timeout
            self._consumer_channel = channel
        method, _properties, body = next(self._consumer)
        if method is None or body is None:
            return None
        return self._issued(method, body)

    def take_nowait(self) -> Taken | None:
        """Take one message if one is ready, without waiting.

        ``basic_get`` rather than the consumer above, and the open consumer is cancelled
        first: a queue being consumed hands messages to that consumer, so a `basic_get`
        beside it would be racing the drain it is meant to be doing. Cancelling costs a
        round trip on a path that runs at shutdown, not in the loop.
        """
        channel = self._channel()
        self._notice_a_new_channel(channel)
        self._cancel(channel)
        method, _properties, body = channel.basic_get(self._queue(), auto_ack=False)
        return self._issued(method, body)

    def _issued(self, method: object, body: bytes | None) -> Taken | None:
        """Hand out a message, remembering which channel's tag this is.

        A delivery tag is an integer the *channel* assigned, and it means nothing on another
        one — settling with a tag from a replaced channel would acknowledge whichever delivery
        now holds that number, or draw `PRECONDITION_FAILED - unknown delivery tag`, which
        closes the channel. So the handle carries the generation it came from, and settling
        checks it.
        """
        if method is None or body is None:
            return None
        tag = method.delivery_tag  # type: ignore[attr-defined]
        generation = channel_generation()
        self._unsettled.add((generation, tag))
        return Taken(body, (generation, tag))

    def _notice_a_new_channel(self, channel: 'BlockingChannel') -> None:
        """Forget what belonged to the channel this broker was using before.

        Two things belong to a channel, and both are wrong to keep once it is replaced.

        The **consumer** is dropped rather than cancelled: the channel it was opened on is
        gone, so there is nothing to cancel and asking would be the failure this avoids.

        The **unsettled handles** go too, because they are no longer this worker's work.
        RabbitMQ requeues an unacknowledged delivery when the channel that held it closes — so
        those messages are back on the queue, and anybody may take them. Keeping the handles
        made `inflight_depth()` count deliveries this process no longer holds, growing by one
        per reconnect, and the same message taken again would be counted twice.

        A connection is replaced whenever the settings behind it move or it was closed, which
        is a live path rather than a theoretical one.
        """
        if self._consumer is not None and self._consumer_channel is not channel:
            self._consumer = None
            self._consumer_timeout = None
            self._consumer_channel = None
        current = channel_generation()
        self._unsettled = {held for held in self._unsettled if _position(held)[0] == current}

    def _cancel(self, channel: 'BlockingChannel') -> None:
        """Close the open consumer, if there is one, and forget it."""
        if self._consumer is None:
            return
        self._consumer = None
        self._consumer_timeout = None
        self._consumer_channel = None
        channel.cancel()

    def ack(self, handle: object) -> None:
        """``basic_ack`` the delivery tag, on the channel that issued it or not at all."""
        channel = self._channel()
        tag = self._settleable(handle)
        if tag is None:
            return
        channel.basic_ack(tag)
        self._unsettled.discard(handle)

    def release(self, handle: object) -> None:
        """``basic_nack`` with requeue, which is a real nack rather than a documented no-op.

        The Redis list has nothing to say here — leaving a payload in its in-flight list
        already means "redeliver it" — and a stream has to move an idle counter. AMQP has the
        operation, so this is the one transport where giving a message up is one command and
        takes effect at once. Measured: the message is back in the queue and the next take
        returns it.
        """
        channel = self._channel()
        tag = self._settleable(handle)
        if tag is None:
            return
        channel.basic_nack(tag, requeue=True)
        self._unsettled.discard(handle)

    def _settleable(self, handle: object) -> int | None:
        """Find the tag to settle with, or ``None`` when the channel that issued it is gone.

        ``None`` rather than an error, and nothing sent: the channel that owed the
        acknowledgement has dropped, so RabbitMQ has already put the message back on the queue.
        Sending the tag anyway would settle whichever delivery now holds that number on the new
        channel. Doing nothing is what leaves the message where the broker has already put it.

        Reported once per occurrence, because it means a send finished across a reconnect and
        that message will be delivered again.
        """
        generation, tag = _position(handle)
        current = channel_generation()
        if generation != current:
            self._unsettled.discard(handle)
            logger.warning(
                'a message finished after its channel was replaced, so it will be redelivered',
                extra={'tg_key': self._queue()},
            )
            return None
        return tag

    # ---------------------------------------------------------------- operations

    def reclaim(self) -> int | None:
        """``None``: the broker does this itself, so there is nothing for a restart to do.

        An unacknowledged message returns to the queue when the channel that held it drops,
        which is exactly what a worker being killed does to it. No in-flight list to walk, no
        worker name to name, and nothing for `tgbot_reclaim` to be pointed at — which is why
        the contract has a ``None`` for this rather than a zero.
        """
        return None

    def depth(self) -> int:
        """How many messages are ready, from a passive declare.

        Ready, not unacknowledged: measured, ``message_count`` reads 0 while a message is out
        with a consumer. That is the right number for a queue depth — work waiting for
        somebody — and :meth:`inflight_depth` answers the other half.
        """
        declared = self._channel().queue_declare(queue=self._queue(), durable=True, passive=True)
        return int(declared.method.message_count)

    def inflight_depth(self, worker: str | None = None) -> int:
        """How many this worker holds, counted here because AMQP will not say.

        The broker tracks unacknowledged deliveries per *channel*, and a client sees its own;
        asking about another channel's means the management HTTP API, which would be a second
        way of talking to the broker for a number the contract defines as *this worker's*. So
        this counts what it handed out and has not settled, which is that number exactly.

        Which is also why a *named* worker is refused: what this process holds is a list of
        delivery tags on its own channel, and another worker's are on a channel this one cannot
        see or ask about. The broker knows them as a channel rather than as a name.
        """
        if worker is not None:
            raise WorkerDepthUnavailableError(type(self).__name__, worker)
        return len(self._unsettled)

    async def adepth(self) -> int:
        """Read the same count off the loop's thread; see :meth:`apublish`."""
        return await asyncio.to_thread(self.depth)

    async def ainflight_depth(self, worker: str | None = None) -> int:
        """Answer from this process, so no thread and no round trip."""
        return self.inflight_depth(worker)

    @property
    def call_ceiling(self) -> float:
        """``RABBITMQ_TIMEOUT``, which bounds a publish, a get and the confirm it waits for."""
        return type(self).call_timeout()

    @property
    def crash_safe(self) -> bool:
        """True, and the broker is what makes it true rather than anything here.

        A message delivered and not acknowledged goes back on the queue when the channel
        drops. A worker killed mid-send drops its channel by dying, so the message is
        redeliverable without this package doing anything about it.
        """
        return True

    def close(self) -> None:
        """Close every connection this process opened."""
        close_connections()
