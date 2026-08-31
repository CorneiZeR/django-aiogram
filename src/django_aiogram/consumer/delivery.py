"""The consumer that moves queued messages to Telegram, and the seam a project may replace.

:class:`BlpopDelivery` is the one this package ships and the value ``DELIVERY`` defaults to: it
takes from the broker in a blocking read, which needs no server configuration, delivers as soon
as a message arrives and leaves messages where they are while the worker is down. The keyspace
consumer 1.x used was removed in 3.0 — it needed ``CONFIG SET notify-keyspace-events``, which
managed Redis providers usually refuse, and it could not deliver before the TTL elapsed.

Since 4.0 ``DELIVERY`` is a **dotted path**, so a project can name a :class:`Delivery` of its
own; until then it accepted the single string ``'blpop'``, the name of a Redis command that three
of the four transports never issue. What a subclass must do is on the **Delivery** page, with the
six rules that are each a defect this module has already had.

It consumes crash-safely where the server allows it: a message is moved to a
processing list while it is being sent and removed once the send has actually
finished, so a worker killed mid-send leaves it behind to be reclaimed on the
next start. That makes delivery at-least-once — after a crash a message may be
sent twice. Servers older than Redis 6.2 lack ``LMOVE``; there the consumer
falls back to plain pops, which is the 1.x at-most-once behavior, and says so
in the log.

"Once the send has finished" is doing real work in that sentence. Until 3.1.0 the
message was acknowledged when the handler *returned*, and ``send_raw`` returns as
soon as the coroutine is scheduled — so in polling mode the message left the
in-flight list before Telegram had seen anything, and the guarantee above was
false. A handler that takes an ``on_complete`` keyword is now handed one and the
message waits for it; one that does not keeps the old semantics exactly.
"""

import asyncio
import hashlib
import inspect
import logging
import queue
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

from django.utils.module_loading import import_string

from django_aiogram.api import check_function
from django_aiogram.broker.registry import get_broker
from django_aiogram.config.enums import EventKind
from django_aiogram.config.settings import conf, take_ceiling
from django_aiogram.eventlog.events import new_correlation_id, worker_identity
from django_aiogram.eventlog.recorder import recorder
from django_aiogram.eventlog.records import Event, as_identifier
from django_aiogram.exceptions import DeliveryNotConfiguredError
from django_aiogram.redis import (
    heartbeat_interval,
    heartbeat_key,
    processing_key,
    queue_key,
)
from django_aiogram.wire.envelope import Envelope, UnknownEnvelopeVersionError, unpack
from django_aiogram.wire.serializers import PickleReadRefusedError, SerializationError, loads

logger = logging.getLogger('django_aiogram')

Handler = Callable[..., Any]


def defers_completion(handler: Handler) -> bool:
    """Whether ``handler`` will take the callback that says a send has finished.

    An explicit parameter only. Every documented recipe takes ``**kwargs`` — and
    so does ``TelegramBot.send_raw`` — so treating that as acceptance would hand
    the callback to handlers that never call it, and their messages would sit in
    the in-flight list until a restart reclaimed them.

    It also has to be a parameter the keyword call can reach. A positional-only
    ``on_complete`` reads as acceptance but refuses ``on_complete=...`` with a
    ``TypeError``, and that lands in the handler-failed branch — acknowledging a
    message nothing ever sent.
    """
    return accepts_keyword(handler, 'on_complete')


def accepts_keyword(handler: Handler, name: str) -> bool:
    """Whether ``handler`` has a parameter of that name a keyword call can reach.

    Asked once per callback rather than for the pair together: a handler written to the
    documented recipe takes ``on_complete`` and nothing else, and handing it
    ``on_refused`` would be an unexpected keyword — a ``TypeError`` landing in the
    handler-failed branch, acknowledging a message nothing sent. ``send_raw`` takes both,
    so the consumer's real handler gets both.
    """
    takes_keyword = (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
    try:
        parameter = inspect.signature(handler).parameters.get(name)
    except (TypeError, ValueError):
        # a callable signature cannot always be read; the old semantics are safe
        return False
    return parameter is not None and parameter.kind in takes_keyword


class Delivery(ABC):
    """Consumes whichever transport `BROKER` names, until stopped."""

    def __init__(self, handler: Handler) -> None:
        """Take what each decoded message is handed to once it arrives."""
        self.handler = handler
        self._stop = threading.Event()
        # the one transport this consumer talks to, resolved once: everything below asks it
        # rather than a Redis client, which is what lets a second transport exist at all
        self.broker = get_broker()
        self._beat_at = 0.0
        #: messages whose send is over, and whether each may leave the in-flight list.
        #: `(handle, True)` is a finished send and gets acknowledged; `(handle, False)` is
        #: one the producer refused outright, which releases the slot and leaves the
        #: message for a redelivery. Filled from the bot's event loop, drained on this
        #: thread, because every Redis call in this class belongs to the consumer
        self._finished: queue.SimpleQueue[tuple[object, bool]] = queue.SimpleQueue()
        self._in_flight = 0
        # asked once: a handler that cannot take the callback is acknowledged the
        # moment it returns, which is the behavior every existing caller has
        self._defers = defers_completion(handler)
        # asked separately: a handler may take the completion callback and not its pair
        self._releases = accepts_keyword(handler, 'on_refused')
        # read here rather than per message: `at_capacity` runs inside `run`'s loop, where
        # an unreadable value would raise out of the consumer thread and end delivery for
        # the life of the container. `run` resolves `BLPOP_TIMEOUT` once for the same reason
        self._limit = max(0, int(conf['MAX_IN_FLIGHT']))

    @property
    def crash_safe(self) -> bool:
        """Whether a message survives this worker being killed mid-send.

        Each transport answers for itself — a Redis list on a server without ``LMOVE``
        cannot make the pop and the send one step, and says so. ``REQUIRE_CRASH_SAFE`` is
        how a deployment refuses to run that way whatever the reason.
        """
        return self.broker.crash_safe

    @property
    def queue_key(self) -> str:
        """The list queued messages are written to and read from."""
        return queue_key()

    @property
    def processing_key(self) -> str:
        """Per-worker, so a restarting worker reclaims only its own messages.

        A shared list would let a starting worker pull a message back out from
        under another worker that is still sending it.
        """
        return processing_key()

    @abstractmethod
    def run(self) -> None:
        """Block, consuming messages, until :meth:`stop` is called."""

    def stop(self) -> None:
        """Ask :meth:`run` to return after its current read."""
        self._stop.set()

    @property
    def read_timeout(self) -> int:
        """How long a blocking take may ask for, in seconds.

        ``BLPOP_TIMEOUT`` capped by what the transport and the heartbeat allow: a read asked to
        wait longer than the socket deadline raises inside the read instead of returning, and one
        that outlasts ``HEARTBEAT_INTERVAL`` lets the heartbeat expire under a consumer that is
        doing fine. `W004` reports on the same helper, so a check cannot describe a cap the
        consumer does not use.

        Public for the reason :attr:`stopping` is: a subclass that has to redo this arithmetic
        will get it wrong, and the page that documents writing one would have to teach it.

        The transport term is the configured broker's own deadline, asked of the broker rather
        than read from `REDIS_TIMEOUT`: until #41 it was the Redis setting whichever transport was
        running, so a Kafka deployment had its poll shortened by a setting it never reads --
        measured, `REDIS_TIMEOUT: 2` capped a 30-second `KAFKA_TIMEOUT` at one second -- and a read
        could outlast `Broker.call_ceiling` on a transport whose own timeout was lower, while the
        join in `start_tgbot` is derived from that ceiling.
        """
        # asked of the object rather than of its class: a `Delivery` may hold anything that
        # answers like a broker -- a test double, a wrapper a project wrote -- and
        # `type(...).CALL_TIMEOUT_OPTION` reads the wrapper's class, which does not have it
        ceiling = take_ceiling(self.broker.CALL_TIMEOUT_OPTION, int(self.broker.call_ceiling))
        return max(1, min(int(conf['BLPOP_TIMEOUT']), ceiling.seconds))

    @property
    def stopping(self) -> bool:
        """Whether :meth:`stop` has been called, which is what ``run`` loops until.

        Public because ``DELIVERY`` names a class a project may write, and a subclass that has
        to read ``self._stop`` to know when to return is not being offered an extension point.
        The shipped consumer reads this same property, so the two cannot describe different
        conditions.
        """
        return self._stop.is_set()

    def start_thread(self) -> threading.Thread:
        """Run the consumer on a daemon thread and return it."""
        thread = threading.Thread(target=self.run, name='tgbot-delivery', daemon=True)
        thread.start()
        return thread

    def reclaim(self) -> bool:
        """Requeue messages a crashed worker left in the processing list.

        Returns whether the list is settled; False means the caller should try again,
        because a transport that was unreachable at startup left messages stranded. The
        broker says how many it moved, or ``None`` where the question does not apply —
        a transport that returns an unsettled message to its group needs no reclaiming.
        """
        try:
            count = self.broker.reclaim()
        except Exception:
            # run() is the thread target, so anything escaping here — a Redis
            # that is not up yet, for one — would end the consumer for good
            logger.exception(
                'could not reclaim previous messages, will retry',
                extra={'tg_key': self.processing_key},
            )
            return False
        if count:
            logger.info(
                'reclaimed messages from a previous run',
                extra={'tg_key': self.queue_key, 'tg_count': count},
            )
        return True

    @property
    def heartbeat_key(self) -> str:
        """Per worker, like the in-flight list: each one answers for itself."""
        return heartbeat_key()

    def heartbeat(self) -> None:
        """Say the loop is still turning, at most once per HEARTBEAT_INTERVAL.

        A container cannot see a thread in another process. This key is what the
        healthcheck reads — ``python -m django_aiogram.healthcheck`` in a container,
        ``tgbot_healthcheck`` by hand — and refreshing it per message would be a write per
        message, so it is paced.
        """
        now = time.monotonic()
        if now - self._beat_at < heartbeat_interval():
            return
        self._beat_at = now
        try:
            # the pace is policy and stays here; whether anything has to be written down,
            # and where, is the transport's business — for two of the four it is nothing
            self.broker.alive()
        except Exception:
            # the loop must keep consuming even when it cannot say so
            logger.exception('could not write the heartbeat', extra={'tg_key': self.heartbeat_key})

    def collect(self) -> None:
        """Take every finished send off the in-flight list.

        Called between reads rather than inside one, so every Redis call this
        class makes still happens on this thread.
        """
        while True:
            try:
                raw, delivered = self._finished.get_nowait()
            except queue.Empty:
                return
            self._in_flight -= 1
            if delivered:
                self.acknowledge(raw)

    def _release_for(self, handle: object) -> Callable[[], None]:
        """Give back the slot a refused send took, without acknowledging the message.

        The slot has to come back — `_hand_over` took one before the handler ran — but
        the message must **not** be acknowledged: nothing sent it, so leaving it in the
        in-flight list is what lets the next start pick it up. Without this a refusal
        held its slot for the life of the process, and under ``MAX_IN_FLIGHT`` the
        consumer stopped taking messages entirely once enough had piled up.

        Latched like its pair, and for the same reason: two reports would take another
        message's place in the count.
        """
        latch = threading.Lock()

        def once() -> None:
            """Give the slot back, once."""
            if latch.acquire(blocking=False):
                self._finished.put((handle, False))

        return once

    def _completion_for(self, handle: object) -> Callable[[], None]:
        """One report per message, however many times the send says it finished.

        A latch rather than a flag: two threads can both read an unset flag and
        both report. A second report is not harmless — it takes another message's
        place in the in-flight count, drives it below zero and quietly widens the
        bound ``MAX_IN_FLIGHT`` exists to hold.
        """
        latch = threading.Lock()

        def once() -> None:
            """Report the first finish and drop every later one.

            The acquire is never released: the lock is a one-way latch here, not a
            critical section, and the first caller through it is the only one that
            should reach the in-flight count.
            """
            if latch.acquire(blocking=False):
                self._finished.put((handle, True))

        return once

    def at_capacity(self) -> bool:
        """Whether this consumer is already holding as many sends as it may."""
        return bool(self._limit) and self._in_flight >= self._limit

    def hold_for_capacity(self) -> None:
        """Stop taking messages while too many are still in flight.

        The bound is on the in-flight list as much as on memory: acknowledging is
        an ``LREM``, which scans that list, so letting a backlog accumulate there
        turns draining it into quadratic work. Zero, the default, is the
        behavior that shipped before deferred acknowledgement existed.

        The wait keeps writing the heartbeat, for the same reason ``run()`` caps
        the blocking pop at ``HEARTBEAT_INTERVAL``: a worker at its limit is busy,
        not dead. Held silently past the key's :func:`heartbeat_ttl` it would be
        restarted while healthy, and the messages it was still sending reclaimed
        and sent again.
        """
        while self.at_capacity() and not self._stop.is_set():
            self.heartbeat()
            try:
                raw, delivered = self._finished.get(timeout=1)
            except queue.Empty:
                continue
            self._in_flight -= 1
            if delivered:
                self.acknowledge(raw)

    def acknowledge(self, handle: object) -> None:
        """Settle a delivered message, however this transport spells that.

        The handle goes back unread: what it names is the broker's business — a payload for
        a Redis list, an entry id for a stream, a delivery tag, an offset.
        """
        try:
            self.broker.ack(handle)
        except Exception:
            # worst case the message is redelivered on the next start
            logger.exception(
                'failed to acknowledge a delivered message',
                extra={'tg_key': self.processing_key},
            )

    def _read(self, raw: bytes) -> tuple['Envelope | None', bool]:
        """Turn one message off the queue into an envelope, or into a verdict.

        Everything here is untrusted input, so no failure may escape: what comes
        back is either the envelope or `None` plus whether to acknowledge the
        message that never became one.
        """
        try:
            payload = loads(raw)
        except PickleReadRefusedError:
            logger.exception(
                'leaving a refused pickle message in flight; set ALLOW_PICKLE to deliver it',
                extra={'tg_key': self.processing_key},
            )
            return None, False
        except SerializationError:
            self._record_undecodable(raw, 'serialization')
            logger.exception('dropping undecodable queued message')
            return None, True
        except Exception:
            self._record_undecodable(raw, 'unknown')
            logger.exception('dropping queued message that failed to decode')
            return None, True
        try:
            return unpack(payload), True
        except UnknownEnvelopeVersionError:
            # written by a newer producer than this consumer understands, so
            # leaving it in flight is what lets an upgrade deliver it
            logger.exception('leaving a message from a newer version in flight')
            return None, False
        except Exception:
            # MalformedEnvelopeError and whatever else a hostile payload can
            # provoke: nothing will ever make sense of it, so it is
            # acknowledged rather than left to come back for ever — and this
            # reader is on the far side of a trust boundary, where an escaping
            # exception would end the consumer for the life of the container
            self._record_undecodable(raw, 'envelope')
            logger.exception('dropping a queued message whose envelope cannot be read')
            return None, True

    def dispatch(self, raw: bytes, handle: object | None = None) -> bool:
        """Decode one message and hand it to the handler.

        A bad payload is one message's problem, so everything short of a kill is
        logged and dropped: the consumer has to survive it to deliver the rest.

        Returns whether the message should be acknowledged. Four paths say no, in
        two kinds. Three are refusals that leave a valid payload for somebody else:
        a pickle the configuration refuses, an envelope from a newer version, and a
        handler raising ``CancelledError``, whose outcome is *unknown* rather than
        nothing — a send can be cancelled after Telegram has taken the request — at
        shutdown usually, but the ``except`` is unqualified, so any cancellation counts.
        Acknowledging any of the three would destroy a message over a setting, a deploy
        order or a restart. The fourth is
        :meth:`_hand_over` returning ``not deferring``, which is not a refusal: a handler
        that took ``on_complete`` *signals* completion through it, the handle goes into a
        queue, and :meth:`collect` takes the message off the in-flight list on the
        consumer's next turn. That is what makes at-least-once true — **where there is an
        in-flight list**. Without ``LMOVE`` the plain pop has already removed the message
        and :meth:`acknowledge` is a no-op, so deferring the acknowledgement defers
        nothing: that server is at-most-once whatever the handler does.

        Those three refusals save the message only where there *is* an in-flight list.
        Against a server without ``LMOVE`` the consumer falls back to a plain pop, so the
        message is gone before the refusal happens and ``False`` buys nothing: what they
        avoid there is a second delete, not a loss.
        """
        if handle is None:
            handle = raw
        envelope, acknowledge = self._read(raw)
        if envelope is None:
            return acknowledge
        try:
            check_function(envelope.function)
        except ValueError:
            self._record(
                EventKind.QUEUE_REJECTED,
                envelope,
                error='not a Telegram API method',
            )
            logger.exception(
                'dropping queued message naming a method that is not Telegram API',
                extra={'tg_function': envelope.function},
            )
            return True
        self._record(EventKind.OUTBOUND_CONSUMED, envelope)
        # by keyword, the way 2.x splatted it: a handler taking **kwargs
        # only — which every documented recipe does — refuses a positional.
        #
        # The envelope's own fields go in *after* the payload, for the reason
        # `_hand_over` gives about `on_complete`: the queue is a trust boundary, and
        # spreading last let a payload carrying `function` replace the name
        # `check_function` had just validated. `send_raw` validates again and so refuses
        # an unknown one, but a handler taking only `**kwargs` does not — and
        # `correlation_id` and `queued_at` were replaceable either way, which is the
        # event log's correlation and its queue latency
        call: dict[str, Any] = {
            **envelope.kwargs,
            'function': envelope.function,
            'correlation_id': envelope.correlation_id,
            'queued_at': envelope.queued_at,
        }
        return self._hand_over(envelope, call, handle)

    def _hand_over(self, envelope: Envelope, call: dict[str, Any], handle: object) -> bool:
        """Call the handler, and say whether the message may be acknowledged.

        Cancellation is the reason this is not one ``except``: it is a
        ``BaseException``, so letting it through would leave :meth:`run` and end
        the consumer for the life of the container. The message stays in flight because
        the outcome is *unknown*: a send can be cancelled after Telegram has taken the
        request, so leaving it risks a duplicate rather than a loss. This worker has to
        keep reading either way.
        """
        deferring = self._defers
        if deferring:
            # into the dict, never alongside it as a second keyword. The queue is
            # a trust boundary and send() forwards whatever it was given, so a
            # payload can carry this name — as a keyword that is "got multiple
            # values", a TypeError landing in the failure branch below, which
            # acknowledges a message nothing sent. Assigning simply wins
            call['on_complete'] = self._completion_for(handle)
            if self._releases:
                # its pair, so a producer that refuses the send gives the slot back
                call['on_refused'] = self._release_for(handle)
            self._in_flight += 1
        try:
            self.handler(**call)
        except asyncio.CancelledError:
            if deferring:
                self._in_flight -= 1
            logger.warning(
                'a queued send was cancelled; leaving it in flight',
                extra={'tg_function': envelope.function},
            )
            return False
        except Exception:
            if deferring:
                self._in_flight -= 1
            logger.exception(
                'handler failed for queued message',
                extra={'tg_function': envelope.function},
            )
            return True
        # a deferring handler decides when this message is done. Returning True
        # here is what made the at-least-once promise false: send_raw returns as
        # soon as the coroutine is scheduled, long before Telegram has seen it
        return not deferring

    def _record(self, kind: EventKind, envelope: Envelope, error: str = '') -> None:
        """Record what the consumer did with one message."""
        chat_id = envelope.kwargs.get('chat_id')
        recorder.record(
            Event(
                kind=kind.value,
                correlation_id=envelope.correlation_id or new_correlation_id(),
                function=envelope.function,
                chat_id=as_identifier(chat_id),
                worker=worker_identity(),
                error=error,
                detail=self._queue_latency(envelope),
            )
        )

    @staticmethod
    def _queue_latency(envelope: Envelope) -> dict[str, Any]:
        """How long the message waited, when the producer said when it was queued."""
        if not envelope.queued_at:
            return {}
        return {'queue_ms': int((time.time() - envelope.queued_at) * 1000)}

    def _record_undecodable(self, raw: bytes, reason: str) -> None:
        """Record a payload nothing could read.

        A fingerprint, never the bytes: an undecodable payload is by definition
        untrusted input and may be a pickle, so putting it in a JSON column
        would spread it into every log shipper and admin page downstream.
        """
        recorder.record(
            Event(
                kind=EventKind.QUEUE_UNDECODABLE.value,
                worker=worker_identity(),
                error=reason,
                detail={'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()[:16]},
            )
        )

    def consume_pending(self) -> None:
        """Drain the queue without blocking, acknowledging each message."""
        while not self.stopping:
            self.collect()
            if self.at_capacity():
                # the blocking loop waits here; a drain has no thread to wait on,
                # so it stops instead of scheduling past the bound
                return
            taken = self.broker.take_nowait()
            if taken is None:
                self.collect()
                return
            if self.dispatch(taken.payload, taken.handle):
                self.acknowledge(taken.handle)
            self.collect()


class BlpopDelivery(Delivery):
    """Blocks on the queue itself, so a message is delivered as it arrives."""

    def run(self) -> None:
        """Block on the queue until :meth:`stop` is called."""
        # read once and reused below: `read_timeout` is a property over `blpop_ceiling()`,
        # which the subclass a project writes needs as much as this one does -- see the
        # property for what the three terms are and why `bound_by` never names
        # `BLPOP_TIMEOUT`
        timeout = self.read_timeout
        reclaimed = self.reclaim()
        logger.info(
            'delivery started',
            extra={
                'tg_delivery': type(self).__name__,
                'tg_key': self.queue_key,
                'tg_timeout': timeout,
                'tg_crash_safe': self.crash_safe,
            },
        )
        while not self.stopping:
            self.heartbeat()
            self.collect()
            self.hold_for_capacity()
            if self._stop.is_set():
                # the gate above releases on shutdown as well as on capacity, and
                # without this the loop would go on to take one more message it
                # has no intention of sending
                break
            if not reclaimed:
                reclaimed = self.reclaim()
            try:
                taken = self.broker.take(timeout)
            except Exception:
                # a dropped connection must not kill the worker thread
                logger.exception('blocking pop failed, retrying', extra={'tg_key': self.queue_key})
                self._stop.wait(timeout)
                continue
            if taken is None:
                continue
            if self.dispatch(taken.payload, taken.handle):
                self.acknowledge(taken.handle)
        # sends that finished while the last read was blocking still have to
        # leave the in-flight list, or every stop redelivers them
        self.collect()


#: what 3.x accepted, against the path that does the same thing now. Kept because a project
#: upgrading has the old word in its settings and deserves to be told where it went rather than
#: `'blpop' is not a dotted path`. `keyspace` was removed in 3.0 and is named for the same reason:
#: the reader wants to know what to write, not what their value is not
THREE_X_DELIVERIES = {
    'blpop': 'django_aiogram.consumer.delivery.BlpopDelivery',
    'keyspace': '',
}


def delivery_class() -> type[Delivery]:
    """Resolve ``DELIVERY`` to a class, and refuse anything that is not a delivery.

    A dotted path, the way ``BROKER`` is one, because a consumer somebody else wrote is a
    reasonable thing to want and the setting is where a reader looks for it. Until 4.0 this
    accepted exactly one string -- ``'blpop'``, the name of a Redis command that three of the
    four transports never issue -- so the setting documented one transport's mechanism while
    offering no choice at all.

    Separate from :func:`get_delivery` for the reason `broker_class` is separate from
    `get_broker`: the checks want the class without building one, and a check must not be the
    thing that starts a consumer.
    """
    path = str(conf['DELIVERY'] or '').strip()
    if not path:
        raise DeliveryNotConfiguredError(conf['DELIVERY'], 'so no consumer is chosen.')
    if path in THREE_X_DELIVERIES:
        replacement = THREE_X_DELIVERIES[path]
        instead = f'write {replacement!r}' if replacement else 'that consumer was removed in 3.0'
        raise DeliveryNotConfiguredError(path, f'a name 4.0 replaced with a dotted path -- {instead}.')
    try:
        resolved = import_string(path)
    # `ValueError` for a path with an empty module part, as in `producer.from_settings` and the
    # registry
    except (ImportError, ValueError) as error:
        raise DeliveryNotConfiguredError(path, f'which cannot be imported: {error}') from error
    if not (isinstance(resolved, type) and issubclass(resolved, Delivery)):
        raise DeliveryNotConfiguredError(path, 'which is not a Delivery subclass.')
    if inspect.isabstract(resolved):
        # `Delivery` itself, or a subclass that left `run` abstract. Building one raises
        # `TypeError: Can't instantiate abstract class`, which names the class and not the
        # setting -- and this is the one refusal a reader is most likely to earn, since the base
        # class is the name they have just read on the page
        raise DeliveryNotConfiguredError(path, 'which is abstract: implement run() or name a subclass that does.')
    return resolved


def get_delivery(handler: Handler) -> Delivery:
    """Build the consumer ``DELIVERY`` names, with the handler it delivers through."""
    return delivery_class()(handler)
