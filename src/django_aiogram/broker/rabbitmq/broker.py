"""RabbitMQ: ``basic_publish`` to send, ``basic_get``/``consume`` to take, a delivery tag to settle.

The third transport, and the one that needs the least of this package. An unacknowledged
message returns to the queue when the channel drops, so there is no worker name to keep, no
in-flight list to reclaim and nothing to write down about liveness — three things the Redis
list needs machinery for.

``pika`` rather than ``aio-pika``, decided by measurement: crossing the thread boundary costs
about 100 microseconds whichever driver is used, so the one that needs no crossing on the
synchronous path — which is where ``bot.send()`` is called from — wins. See
:mod:`django_aiogram.broker.rabbitmq.client`.
"""

import asyncio
import logging
from collections.abc import Mapping
from collections.abc import Sequence as Seq
from typing import TYPE_CHECKING, Any, ClassVar

from django_aiogram.broker.base import REQUIRED, Broker
from django_aiogram.broker.models import Taken
from django_aiogram.broker.rabbitmq.client import channel_for_thread, close_connections
from django_aiogram.broker.rabbitmq.exceptions import QueueRefusedError

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel

__all__ = ('RabbitMQBroker',)

logger = logging.getLogger('django_aiogram')


def _tag(handle: object) -> int:
    """Read a delivery tag out of an opaque handle, or say where it must have come from.

    AMQP names a delivery by an integer the channel assigned, so a handle of any other shape
    belongs to a different broker — and saying that is better than letting the driver complain
    about a type it was handed. The Redis list makes the same refusal for the same reason.
    """
    if not isinstance(handle, int) or isinstance(handle, bool):
        msg = f'this broker settles by delivery tag, so a handle must be an int, not {type(handle).__name__}'
        raise TypeError(msg)
    return handle


class RabbitMQBroker(Broker):
    """One durable queue, consumed with acknowledgements, settled by delivery tag."""

    #: importable module, and the extra that installs it
    REQUIRES: ClassVar[tuple[str, str] | None] = ('pika', 'rabbitmq')

    #: this transport's own settings. Both names are required: a URL carries credentials and
    #: a host, and neither has a default worth baking in, while the queue is where messages
    #: go — the same reason the stream key has none
    OPTIONS: ClassVar[Mapping[str, Any]] = {
        'RABBITMQ_URL': REQUIRED,
        'RABBITMQ_QUEUE': REQUIRED,
        'RABBITMQ_PREFETCH': 0,
        'RABBITMQ_TIMEOUT': 10,
    }

    def __init__(self) -> None:
        """Hold nothing open; the first publish or take opens this thread's channel."""
        #: the open consumer and the timeout it was opened with. `consume` fixes its
        #: inactivity timeout when the generator is made, so a different one needs a new one
        self._consumer: Any = None
        self._consumer_timeout: float | None = None
        #: delivery tags handed out and not yet settled. AMQP does not report an unacked count
        #: — `message_count` counts only what is ready, measured — and the contract asks what
        #: *this worker* holds, which is exactly what this knows
        self._unsettled: set[object] = set()

    def _queue(self) -> str:
        """Name the queue this broker publishes to and consumes from."""
        return str(self.option('RABBITMQ_QUEUE'))

    def _channel(self) -> 'BlockingChannel':
        """Reach this thread's channel, declaring the queue on first use."""
        return channel_for_thread(
            str(self.option('RABBITMQ_URL')),
            self._queue(),
            int(str(self.option('RABBITMQ_PREFETCH') or 0)),
            float(str(self.option('RABBITMQ_TIMEOUT') or 10)),
        )

    # ------------------------------------------------------------------ producer

    def publish(self, payloads: Seq[bytes]) -> None:
        """One confirmed, mandatory, persistent publish per payload.

        ``mandatory`` so a queue that is not there is an error rather than a message dropped
        by the exchange, and confirms so the broker has answered before this returns. Measured
        at 170.7 microseconds against 18.9 unconfirmed — the difference is the promise the
        rest of this package already makes, where ``RPUSH`` answers with a length.
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

        The driver is synchronous, so this is where the hand-off is paid — measured at about
        100 microseconds, which is what the *other* driver would have charged the synchronous
        caller instead. A thread rather than a second connection library: one way of talking
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
        if self._consumer is None or self._consumer_timeout != timeout:
            self._cancel(channel)
            self._consumer = channel.consume(self._queue(), inactivity_timeout=max(0.001, timeout))
            self._consumer_timeout = timeout
        method, _properties, body = next(self._consumer)
        if method is None or body is None:
            return None
        self._unsettled.add(method.delivery_tag)
        return Taken(body, method.delivery_tag)

    def take_nowait(self) -> Taken | None:
        """Take one message if one is ready, without waiting.

        ``basic_get`` rather than the consumer above, and the open consumer is cancelled
        first: a queue being consumed hands messages to that consumer, so a `basic_get`
        beside it would be racing the drain it is meant to be doing. Cancelling costs a
        round trip on a path that runs at shutdown, not in the loop.
        """
        channel = self._channel()
        self._cancel(channel)
        method, _properties, body = channel.basic_get(self._queue(), auto_ack=False)
        if method is None or body is None:
            return None
        self._unsettled.add(method.delivery_tag)
        return Taken(body, method.delivery_tag)

    def _cancel(self, channel: 'BlockingChannel') -> None:
        """Close the open consumer, if there is one, and forget it."""
        if self._consumer is None:
            return
        self._consumer = None
        self._consumer_timeout = None
        channel.cancel()

    def ack(self, handle: object) -> None:
        """``basic_ack`` the delivery tag, which is what a handle is here."""
        self._channel().basic_ack(_tag(handle))
        self._unsettled.discard(handle)

    def release(self, handle: object) -> None:
        """``basic_nack`` with requeue, which is a real nack rather than a documented no-op.

        The Redis list has nothing to say here — leaving a payload in its in-flight list
        already means "redeliver it" — and a stream has to move an idle counter. AMQP has the
        operation, so this is the one transport where giving a message up is one command and
        takes effect at once. Measured: the message is back in the queue and the next take
        returns it.
        """
        self._channel().basic_nack(_tag(handle), requeue=True)
        self._unsettled.discard(handle)

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

    def inflight_depth(self) -> int:
        """How many this worker holds, counted here because AMQP will not say.

        There is no unacknowledged count in the protocol — the management HTTP API has one,
        and reaching for it would mean a second way of talking to the broker for a number the
        contract defines as *this worker's*. So the broker counts what it handed out and has
        not settled, which is that number exactly.
        """
        return len(self._unsettled)

    async def adepth(self) -> int:
        """Read the same count off the loop's thread; see :meth:`apublish`."""
        return await asyncio.to_thread(self.depth)

    async def ainflight_depth(self) -> int:
        """Answer from this process, so no thread and no round trip."""
        return len(self._unsettled)

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
