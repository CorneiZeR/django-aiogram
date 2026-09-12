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
from collections import deque
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
    from collections.abc import Callable

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

    #: several ``basic_consume`` on one channel, which is all AMQP asks for -- so a container
    #: serving twenty client queues holds the connection it already had
    MULTIPLEXES: ClassVar[bool] = True

    #: this transport's own settings. Both names are required: a URL carries credentials and
    #: a host, and neither has a default worth baking in, while the queue is where messages
    #: go — the same reason the stream key has none
    QUEUE_OPTION: ClassVar[str] = 'RABBITMQ_QUEUE'
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
        #: the queues a multiplexed consumer is subscribed to on the current channel, and the
        #: messages those subscriptions have handed over. pika delivers through callbacks, so
        #: the deque is where a `take` reads from after giving the connection its turn
        self._subscribed: tuple[str, ...] = ()
        #: the channel those subscriptions were registered on. Its own rather than the
        #: generator consumer's: the two are separate paths, and reading one for the other
        #: dropped every subscription -- and everything already delivered through it -- on the
        #: take after the one that registered them. Measured against a real broker: the second
        #: queue's message was delivered, cleared and never seen again
        self._subscribed_channel: Any = None
        #: the consumer tag each subscribed queue was registered under, so one can be cancelled
        #: without touching the others: `channel.cancel()` cancels every consumer on the
        #: channel, which on a set that merely lost one queue would stop reading for the rest
        self._tags: dict[str, str] = {}
        #: the queues declared on the channel this broker is using. Declaring is idempotent and
        #: cheap, and doing it here rather than when the channel is opened is what keeps the
        #: channel's identity free of the queue set -- see `channel_for_thread`
        self._declared: set[str] = set()
        #: which channel generation those declarations were made on
        self._declared_on: int | None = None
        #: the queues this instance reads, as the last take asked for them. Kept because every
        #: other call needs the same channel: settling by delivery tag on a channel opened for
        #: a *different* set is settling on a different channel, which AMQP requeues the whole
        #: set over -- measured against a real broker, as a take that never saw its second
        #: queue again
        self._serving: tuple[str, ...] = ()
        self._delivered: deque[tuple[str, Any, bytes]] = deque()

    def _queue(self) -> str:
        """Name the queue this broker publishes to and consumes from."""
        return self.addressed()

    def _queues(self, queues: 'Seq[str] | None') -> tuple[str, ...]:
        """Name the queues a read is for: those asked for, or the one this broker addresses."""
        if not queues:
            return (self._queue(),)
        return tuple(dict.fromkeys(str(one) for one in queues))

    def _declare(self, channel: 'BlockingChannel', queues: 'Seq[str]') -> None:
        """Declare each of these queues on this channel, once per channel.

        Durable, so a broker restart does not lose the queue itself -- the messages in it are
        marked persistent by the publisher. Remembered per channel rather than per process,
        because a replaced channel is a replaced connection and the server may have been
        restarted under it.
        """
        for queue in queues:
            if queue in self._declared:
                continue
            channel.queue_declare(queue=queue, durable=True)
            self._declared.add(queue)
            self._declared_on = channel_generation()

    def _channel(self, queues: 'Seq[str] | None' = None) -> 'BlockingChannel':
        """Reach this thread's channel, declaring the queues it is for on first use.

        The deadline comes from :meth:`call_timeout`, which is the only reader of
        ``RABBITMQ_TIMEOUT``. It used to be read again here with an `or 10` on it, and the two
        disagreed on every value a project wrote and `or` treats as unset: a configured `0` gave
        pika 10 while :attr:`call_ceiling` said 0, so `W004` and the consumer's cap were computed
        from a deadline no publish, get or confirm on this channel ever carried.
        """
        channel = channel_for_thread(
            str(self.opt('RABBITMQ_URL')),
            # the same `or` idiom the deadline no longer uses, and kept on purpose: 0 *is* this
            # option's declared default and its meaning, so nothing a project writes changes hands
            # here except a value `int` would refuse outright. Refusing it by name needs a rule per
            # transport option, which is #23 rather than this line
            int(str(self.opt('RABBITMQ_PREFETCH') or 0)),
            self.deadline(),
        )
        self._notice_a_new_channel(channel)
        self._declare(channel, self._queues(queues) if queues else (self._queue(),))
        return channel

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

    def take(self, timeout: float, queues: 'Seq[str] | None' = None) -> Taken | None:
        """Wait up to ``timeout`` for one message, or answer ``None``.

        ``consume`` with an inactivity timeout, which yields ``(None, None, None)`` when
        nothing arrived — measured — so the consumer gets its turn back and can check whether
        it is shutting down.

        A caller that **names** its queues takes :meth:`_multiplexed` instead, whether it names
        one or five, because ``consume`` is a generator over one queue: AMQP's way of reading
        several is a ``basic_consume`` each and the connection's own turn, which is the same
        channel and the same socket.

        Named rather than counted, and the difference is a live path: a lane of three whose
        other two queues are at their budget asks for one, and coming down here for it would
        leave those two subscribed and delivering while a second consumer opened on this one.
        ``None`` is the only thing that reaches the generator, which is every caller that
        predates queue sets.
        """
        if queues is not None:
            return self._multiplexed(timeout, self._queues(queues))
        self._serving = (self._queue(),)
        channel = self._channel()
        if self._consumer is None or self._consumer_timeout != timeout:
            self._cancel(channel)
            self._consumer = channel.consume(self._queue(), inactivity_timeout=max(0.001, timeout))
            self._consumer_timeout = timeout
            self._consumer_channel = channel
        method, _properties, body = next(self._consumer)
        if method is None or body is None:
            return None
        return self._issued(method, body)

    def take_nowait(self, queues: 'Seq[str] | None' = None) -> Taken | None:
        """Take one message if one is ready, without waiting.

        ``basic_get`` rather than the consumer above, and the open consumer is cancelled
        first: a queue being consumed hands messages to that consumer, so a `basic_get`
        beside it would be racing the drain it is meant to be doing. Cancelling costs a
        round trip on a path that runs at shutdown, not in the loop.

        Several queues are asked in turn, which is what a drain wants: the first that has
        anything answers, and nothing is left waiting behind a queue that is empty.
        """
        asked = self._queues(queues)
        self._serving = asked
        channel = self._channel(asked)
        self._cancel(channel)
        # what the subscriptions already handed over, before they are cancelled: a drain that
        # cancelled first would report an empty queue while this channel still held those
        # messages unacknowledged, and they would come back only when it closed
        held = self._buffered(channel, asked)
        if held is not None:
            return held
        self._unsubscribe(channel)
        for queue in asked:
            method, _properties, body = channel.basic_get(queue, auto_ack=False)
            taken = self._issued(method, body, queue)
            if taken is not None:
                return taken
        return None

    def _multiplexed(self, timeout: float, queues: tuple[str, ...]) -> Taken | None:
        """Read whichever of these queues has something, over the one channel.

        A ``basic_consume`` per queue and then ``process_data_events``, which is how pika
        hands a blocking connection its turn: the callbacks fill :attr:`_delivered` and this
        takes the oldest. One socket, one turn, however many queues -- and a queue with a
        backlog cannot starve a quiet one of the *read*, because the server pushes from all of
        them into the same channel.

        The subscriptions are kept between calls and rebuilt only when the set moves, since a
        consumer that resubscribed per take would cancel and re-register on every loop.
        """
        self._serving = queues
        channel = self._channel(queues)
        self._cancel(channel)
        self._subscribe(channel, queues)
        held = self._buffered(channel, queues)
        if held is not None:
            return held
        # pika's own name for "give the connection its turn": callbacks run inside it, and it
        # returns when one has been served or the time is up
        channel.connection.process_data_events(time_limit=max(0.001, timeout))
        return self._buffered(channel, queues)

    def _buffered(self, channel: 'BlockingChannel', queues: tuple[str, ...]) -> Taken | None:
        """Hand back the oldest delivery for one of these queues, giving the rest back.

        The callbacks fill one deque for every queue subscribed, and the set may have moved
        since: a container whose client went away calls `Delivery.serve` while a read is
        blocked, and what arrived for that queue is no longer this consumer's to deliver --
        counted under another queue's budget, it would be counted wrong. Those are nacked with
        requeue, which puts them back where a consumer that *is* serving them will find them.

        Order is kept for everything else: what is not handed back now stays in front of
        whatever the next turn delivers.
        """
        kept: deque[tuple[str, Any, bytes]] = deque()
        found: tuple[str, Any, bytes] | None = None
        while self._delivered:
            entry = self._delivered.popleft()
            queue, method, body = entry
            if queue not in queues:
                self._give_back(channel, method, queue)
                continue
            if found is None:
                found = entry
            else:
                kept.append(entry)
        self._delivered = kept
        if found is None:
            return None
        queue, method, body = found
        return self._issued(method, body, queue)

    def _give_back(self, channel: 'BlockingChannel', method: object, queue: str) -> None:
        """Nack one delivery for a queue this consumer no longer serves, so somebody else can.

        Never counted and never settled here: it was delivered by a subscription and nothing
        above this module was told about it, so there is no slot to return and no handle in
        flight -- only a message to put back.
        """
        try:
            channel.basic_nack(method.delivery_tag, requeue=True)  # type: ignore[attr-defined]  # pika is an extra, see `_issued`
        except Exception:
            # it goes back when this channel closes either way; saying so is the whole of what
            # can be done about a nack that did not land
            logger.exception('could not give back a delivery for a queue this consumer left', extra={'tg_key': queue})

    def _subscribe(self, channel: 'BlockingChannel', queues: tuple[str, ...]) -> None:
        """Consume exactly these queues on this channel, touching only what changed.

        One consumer cancelled and one registered, rather than ``channel.cancel()`` and a fresh
        set: cancelling every consumer to add a queue stops reading for the clients that were
        already being served, and this is the path a container takes whenever one of them
        arrives or goes away.
        """
        if self._subscribed == queues:
            return
        for queue in [held for held in self._subscribed if held not in queues]:
            tag = self._tags.pop(queue, '')
            if tag:
                channel.basic_cancel(tag)
        for queue in queues:
            if queue in self._tags:
                continue
            self._tags[queue] = channel.basic_consume(queue, self._delivery_into(queue), auto_ack=False)
        self._subscribed = queues
        self._subscribed_channel = channel

    def _delivery_into(self, queue: str) -> 'Callable[[object, object, object, bytes], None]':
        """Make the callback that files a delivery under the queue it came from.

        A closure over the name rather than reading ``method.routing_key``: what a message was
        published *with* is not what it was consumed *from* once an exchange is involved, and
        the consumer's budget is kept under the queue it is reading.
        """

        def arrived(_channel: object, method: object, _properties: object, body: bytes) -> None:
            """File one delivery under its queue, for the take that asked for the turn."""
            self._delivered.append((queue, method, body))

        return arrived

    def _unsubscribe(self, channel: 'BlockingChannel') -> None:
        """Cancel the multiplexed subscriptions, for a drain that uses ``basic_get`` instead."""
        if not self._subscribed:
            return
        self._subscribed = ()
        self._subscribed_channel = None
        for tag in self._tags.values():
            channel.basic_cancel(tag)
        self._tags.clear()

    def _issued(self, method: object, body: bytes | None, queue: str = '') -> Taken | None:
        """Hand out a message, remembering which channel's tag this is.

        A delivery tag is an integer the *channel* assigned, and it means nothing on another
        one — settling with a tag from a replaced channel would acknowledge whichever delivery
        now holds that number, or draw `PRECONDITION_FAILED - unknown delivery tag`, which
        closes the channel. So the handle carries the generation it came from, and settling
        checks it.
        """
        if method is None or body is None:
            return None
        # `method` is typed `object` for the same reason the Kafka broker types its message that
        # way: pika is an extra, and this signature does not name a class from one
        tag = method.delivery_tag  # type: ignore[attr-defined]  # the parameter is `object`, see above
        generation = channel_generation()
        self._unsettled.add((generation, tag))
        return Taken(body, (generation, tag), queue)

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
        if self._subscribed_channel is not None and self._subscribed_channel is not channel:
            # the subscriptions belonged to the channel that is gone, and so did anything it
            # had handed over: RabbitMQ requeues an unacknowledged delivery when its channel
            # drops, so those messages are back on their queues for whoever takes them next
            self._subscribed = ()
            self._subscribed_channel = None
            self._tags.clear()
            self._delivered.clear()
        if self._declared and channel_generation() != self._declared_on:
            # a new channel is a new connection, and the server may have been restarted under
            # it: what this instance declared belonged to the one that is gone
            self._declared.clear()
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

    @property
    def removes_queues(self) -> bool:
        """An AMQP queue is a server object this package declared, so it can delete it."""
        return True

    def discard(self, *, if_empty: bool = False) -> bool:
        """Delete this queue on the server, with whatever is still in it.

        Unconditionally under `drop`, because the caller has already decided:
        `tgbot_prune_queues` names the policy that got here, and a queue nothing publishes to
        is not made safer by keeping the messages nobody will read.

        **Refused under ``if_empty``**, and that refusal is the honest answer rather than a
        gap. AMQP's own ``if_empty`` counts *ready* messages, so a queue whose only message is
        an unacknowledged delivery in another container reads as empty and is deleted -- the
        message going with it. Nothing here can see that delivery: it belongs to another
        connection, and the broker reports no unacked count per queue. So `hold` reports the
        queue as still held and an operator uses `drop` when they know what is sending.
        """
        if if_empty:
            return False
        self._channel().queue_delete(queue=self._queue())
        return True

    def reclaim(self, queues: 'Seq[str] | None' = None) -> int | None:  # noqa: ARG002 - the contract's, and there is nothing here to reclaim on any queue
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
        return self.deadline()

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
