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
from collections import deque
from collections.abc import Callable, Mapping
from collections.abc import Sequence as Seq
from typing import TYPE_CHECKING, Any, NamedTuple

from django.utils.module_loading import import_string

from django_aiogram.api import check_function
from django_aiogram.broker.exceptions import QueueMultiplexingUnavailableError
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
from django_aiogram.runtime.control import apply_control, is_control
from django_aiogram.wire.envelope import Envelope, UnknownEnvelopeVersionError, unpack
from django_aiogram.wire.serializers import PickleReadRefusedError, SerializationError, loads

if TYPE_CHECKING:
    from django_aiogram.broker.models import Taken

logger = logging.getLogger('django_aiogram')

Handler = Callable[..., Any]


class _Pending(NamedTuple):
    """One decoded message on its way to a handler, and everything settling it needs.

    Held together because it travels together: a message may be handed over now, or parked
    until its bot has room and handed over later, and the four things that go to the handler
    plus the queue its send is counted against move as one either way.
    """

    envelope: 'Envelope'
    call: dict[str, Any]
    handle: object
    handler: Handler
    #: which of the consumer's queues it came off, which is the budget its send is bounded by
    on_queue: str


#: answers with the handler for one bot's identity. Raising is how it says this process serves
#: no such bot, which `Delivery._handler_for` turns into a message left in flight
Route = Callable[[int], Handler]


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

    def __init__(
        self,
        handler: Handler,
        route: 'Route | None' = None,
        settings: 'Mapping[str, Any] | None' = None,
        queues: 'Seq[str] | None' = None,
    ) -> None:
        """Take what each decoded message is handed to once it arrives.

        ``route`` is how a message reaches the bot it was queued for: it answers with the
        handler for one identity, and ``handler`` is what a payload naming no bot gets --
        every 4.x payload, and every send by a bot whose token has no identity in it.

        Both, rather than the route alone, because the shape of the handler is read once here
        rather than per message: whether it takes ``on_complete``, and whether it takes
        ``on_refused``. Every routed handler is some bot's ``send_raw`` and they are the same
        shape, so asking one answers for all -- and a route handing back something else would
        be answered as though it were `send_raw`, which is why the contract says so out loud.

        ``settings`` is which queue this consumer is *for*, and ``None`` is the process's own.

        ``queues`` is the rest of them, where the transport can read several over one
        connection: a container serving twenty client queues then holds one connection and one
        thread rather than twenty of each. The budget stays **per queue** whichever shape it
        runs in -- a backlog on one queue is a backlog on one queue -- and a transport that
        cannot multiplex is refused here rather than at the first read, so a container is never
        wrong about how many backlogs it is serving.
        """
        self.handler = handler
        self.route = route
        #: the resolved settings this consumer serves, or ``None`` for the process's own
        self.settings = settings
        self._stop = threading.Event()
        # the one transport this consumer talks to, resolved once: everything below asks it
        # rather than a Redis client, which is what lets a second transport exist at all
        self.broker = get_broker() if settings is None else get_broker(settings)
        self._beat_at = 0.0
        #: messages whose send is over, and whether each may leave the in-flight list.
        #: `(handle, True)` is a finished send and gets acknowledged; `(handle, False)` is
        #: one the producer refused outright, which releases the slot and leaves the
        #: message for a redelivery. Filled from the bot's event loop, drained on this
        #: thread, because every Redis call in this class belongs to the consumer
        self._finished: queue.SimpleQueue[tuple[object, bool, int | None, str]] = queue.SimpleQueue()
        #: which queues this consumer serves. One of them for every deployment before 5.0 and
        #: for every transport that reads one queue per connection
        self.queues: tuple[str, ...] = tuple(queues) if queues else (self._own_queue(),)
        if len(self.queues) > 1 and not type(self.broker).MULTIPLEXES:
            raise QueueMultiplexingUnavailableError(type(self.broker).__name__, self.queues)
        #: what a read asks for, and ``None`` only where the one queue served is the one this
        #: broker addresses anyway -- which is the call every transport has always been given,
        #: and what keeps a `Broker` somebody else wrote out of a signature it never agreed to.
        #: A single queue that is *not* the broker's own is named, or the read would take from
        #: whatever the transport was configured with and the queue asked for would go unread
        self._asked: tuple[str, ...] | None = None if self.queues == (self._own_queue(),) else self.queues
        #: sends in flight **per queue**, because the bound is per queue: one client's backlog
        #: must not stop the container reading for everybody else
        self._in_flight: dict[str, int] = dict.fromkeys(self.queues, 0)
        #: over the books above, and over which queues are served. Everything that counts a
        #: send runs on the consumer's own thread, but :meth:`serve` does not -- a queue
        #: arriving while the container runs reaches this from the pass that noticed it, and a
        #: dict growing under a reader is what that would otherwise be
        self._books = threading.Lock()
        self._tell_the_broker()
        # asked once: a handler that cannot take the callback is acknowledged the
        # moment it returns, which is the behavior every existing caller has
        self._defers = defers_completion(handler)
        # asked separately: a handler may take the completion callback and not its pair
        self._releases = accepts_keyword(handler, 'on_refused')
        # read here rather than per message: `at_capacity` runs inside `run`'s loop, where
        # an unreadable value would raise out of the consumer thread and end delivery for
        # the life of the container. `run` resolves `BLPOP_TIMEOUT` once for the same reason
        resolved = conf if settings is None else settings
        self._limit = max(0, int(resolved['MAX_IN_FLIGHT']))
        # read here for the reason above, and separately from the queue's own bound: this one
        # is per bot, so one client saturating its own budget cannot fill the queue's
        self._per_bot_limit = max(0, int(resolved['MAX_IN_FLIGHT_PER_BOT']))
        #: how many sends each bot has **with the handler right now**, by identity. `None` is
        #: a payload that named no bot, which is every 4.x one and every send by a bot whose
        #: token has no identity.
        #:
        #: Active sends only, and that is the whole distinction: a parked message is *waiting*
        #: for one of these to end, so counting it here would be a reservation nothing can
        #: release. With a budget of two, two active sends and two parked messages would leave
        #: this at two once both sends finished -- at the budget, with nothing running to take
        #: it below again, and that bot stalled for the life of the process
        self._sending: dict[int | None, int] = {}
        #: messages taken for a bot that was already at its budget: kept rather than released,
        #: because a release is a documented no-op on the transport that has an in-flight list
        #: -- there the message would sit until a restart reclaimed it. Each holds one of the
        #: queue's slots, so `MAX_IN_FLIGHT` bounds this as well
        self._parked: deque[_Pending] = deque()

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
        """The list queued messages are written to and read from.

        The transport's own answer where it has one, because this consumer may be serving a
        queue that is not the process's -- and the module-level reader answers for the
        process. A ``DELIVERY`` a project wrote may hold a broker that predates
        :meth:`~django_aiogram.broker.base.Broker.addressed`, and that one falls back.
        """
        addressed = getattr(self.broker, 'addressed', None)
        return addressed() if callable(addressed) else queue_key()

    def _own_queue(self) -> str:
        """Name the one queue this consumer serves when nothing named a set.

        The transport's own answer, which is what a read without a queue set reaches anyway --
        so the budget is kept under the name the messages actually came off.
        """
        return self.queue_key

    def _queue_of(self, taken: 'Taken') -> str:
        """Name the queue a message came off, as this consumer keeps its books.

        A transport reading several fills :attr:`Taken.queue`; one reading its own leaves it
        empty, and there is exactly one queue it can have come from.

        Against the books rather than against the queues served *now*: a read begun before
        :meth:`serve` took a queue away can still answer from it, and that message's slot
        belongs to the count it will be given back to. A name in neither -- which nothing
        shipped produces -- is counted under the first queue rather than growing the books a
        message at a time.
        """
        return taken.queue if self._counted_under(taken.queue) else self.queues[0]

    def _counted_under(self, on_queue: str) -> bool:
        """Whether this consumer keeps a count for that queue, served now or still settling."""
        with self._books:
            return on_queue in self._in_flight

    def _forget_retired(self) -> None:
        """Drop the books of a queue that is no longer served and has nothing left in flight.

        Called from the read loop and nowhere else, which is what makes it safe: the consumer
        thread is the only reader, so between one take and the next there is no read that could
        still answer from a queue this removes -- and a read that *began* before `serve` took a
        queue away is still counted under it, because that entry is only dropped once its sends
        have settled.

        Without it a long-lived container whose clients come and go keeps one zero-count entry
        per queue that ever existed, which is a dict that grows for the life of the process.
        """
        with self._books:
            retired = [queue for queue, held in self._in_flight.items() if queue not in self.queues and held <= 0]
            for queue in retired:
                del self._in_flight[queue]

    def readable(self) -> 'tuple[str, ...] | None':
        """Which of this consumer's queues a read may ask for, dropping those at their budget.

        Public for the reason :attr:`stopping` is: a `Delivery` a project wrote has a ``run``
        of its own, and this is what it hands the broker.

        A queue at its bound is simply not read from until a send finishes, which is how the
        per-queue budget survives one connection: the alternative -- taking the message and
        giving it back -- spins against a saturated queue's backlog, and parking it would put
        the bound past itself one message per read.

        ``None`` where there is one queue, and the callers pass **nothing at all** then rather
        than passing it: a `Broker` somebody wrote before 5.0 declares ``take(self, timeout)``,
        and a `None` handed to that is a `TypeError` out of the consumer's own loop. The
        argument exists for the transports that were asked for a set.

        **Empty** is not the same as either, and it is the answer where every queue this
        consumer serves is at its budget: `hold_for_capacity` usually stops the loop before
        that, but it returns as soon as *one* queue has room and `serve` can take that queue
        away in between. A caller reads the empty answer as "wait", never as a set to hand
        down -- an empty one means *the queue I address* to every transport here, which would
        be a read against a queue at its budget.
        """
        if self._asked is None:
            return None
        return tuple(one for one in self._asked if not self.at_capacity(one))

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
            count = self.broker.reclaim(self._asked) if self._asked else self.broker.reclaim()
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
                raw, delivered, bot_id, on_queue = self._finished.get_nowait()
            except queue.Empty:
                self._hand_over_parked()
                return
            self._took_a_slot_back(bot_id, on_queue)
            if delivered:
                self.acknowledge(raw)

    @staticmethod
    def _settlement() -> 'Callable[[], bool]':
        """Return the claim on settling one message, which exactly one caller may win.

        Three callers race for it and each does something different with the win: the
        handler's ``on_complete``, its ``on_refused``, and the exception paths in
        :meth:`_hand_over`. Each has to be able to give the slots back and none of them may
        do it twice -- a second report takes another message's place in the count, drives it
        below zero and quietly widens the bound ``MAX_IN_FLIGHT`` exists to hold.

        **One claim for all three**, not one per callback, and the reason is a handler that
        reports and *then* raises: with a latch each, the completion queued a settlement and
        the exception path returned the slots again -- the per-bot budget was then over by one
        for every such send, and `_in_flight` went negative.

        A latch rather than a flag: two threads can both read an unset flag and both report.
        The acquire is never released; the lock is a one-way latch here, not a critical
        section.
        """
        latch = threading.Lock()

        def claim() -> bool:
            """Whether this caller is the one that settles the message."""
            return latch.acquire(blocking=False)

        return claim

    def _release_for(
        self, handle: object, bot_id: 'int | None', claim: 'Callable[[], bool]', on_queue: str
    ) -> Callable[[], None]:
        """Give back the slot a refused send took, without acknowledging the message.

        The slot has to come back — `_hand_over` took one before the handler ran — but
        the message must **not** be acknowledged: nothing sent it, so leaving it in the
        in-flight list is what lets the next start pick it up. Without this a refusal
        held its slot for the life of the process, and under ``MAX_IN_FLIGHT`` the
        consumer stopped taking messages entirely once enough had piled up.
        """

        def once() -> None:
            """Give the slot back, once, if nothing else has settled this message."""
            if claim():
                self._finished.put((handle, False, bot_id, on_queue))

        return once

    def _completion_for(
        self,
        handle: object,
        bot_id: 'int | None',
        claim: 'Callable[[], bool]',
        on_queue: str,
    ) -> Callable[[], None]:
        """One report per message, however many times the send says it finished."""

        def once() -> None:
            """Report the first finish and drop every later one."""
            if claim():
                self._finished.put((handle, True, bot_id, on_queue))

        return once

    def _took_a_slot_back(self, bot_id: 'int | None', on_queue: str) -> None:
        """One send is over: give its slot back to the queue it came off and to its bot."""
        with self._books:
            self._in_flight[on_queue] = self._in_flight.get(on_queue, 1) - 1
        self._one_less_sending(bot_id)

    def _one_less_sending(self, bot_id: 'int | None') -> None:
        """Say one of this bot's sends has ended, without touching the queue's own count."""
        held = self._sending.get(bot_id, 0) - 1
        if held > 0:
            self._sending[bot_id] = held
        else:
            # dropped rather than left at zero: a container serving a client per bot would
            # otherwise grow this dict by one entry per bot for ever
            self._sending.pop(bot_id, None)

    def _at_its_own_capacity(self, bot_id: 'int | None') -> bool:
        """Whether this bot already has as many sends in flight as it may.

        The second of the two budgets, and the reason there are two: one bound over the whole
        queue means a client whose sends are slow -- rate limited, or simply chatty -- fills
        it and every other bot on that queue waits behind them. That is the mistake a single
        prefetch multiplier makes, and this is what makes a shared queue survivable.
        """
        return bool(self._per_bot_limit) and self._sending.get(bot_id, 0) >= self._per_bot_limit

    def _waited_for_room(self, bot_id: 'int | None') -> bool:
        """Wait until this bot has a send slot, and say whether it got one.

        The degraded half of the per-bot budget, for a deployment that set no queue bound:
        holding the message would be unbounded, so the consumer waits for one of that bot's
        own sends to end. That is head-of-line blocking -- the messages behind this one are
        for other bots -- which is why `W012` asks for `MAX_IN_FLIGHT` to be set.

        The wait keeps writing the heartbeat and settling what finishes, for the reason
        :meth:`hold_for_capacity` gives: a worker at its limit is busy, not dead, and held
        silently past the key's TTL it would be restarted while healthy.

        ``False`` where the shutdown arrived first, which leaves the message in flight.
        """
        while self._at_its_own_capacity(bot_id) and not self._stop.is_set():
            self.heartbeat()
            try:
                raw, delivered, settled, on_queue = self._finished.get(timeout=1)
            except queue.Empty:
                continue
            self._took_a_slot_back(settled, on_queue)
            if delivered:
                self.acknowledge(raw)
        return not self._stop.is_set()

    def _park(self, pending: _Pending) -> None:
        """Keep a message whose bot is at its budget, and hold a queue slot for it.

        Kept rather than released, because `release` is a documented no-op on the transport
        that has an in-flight list: the message would sit there until a restart reclaimed it.
        Kept rather than waited on, because waiting is the head-of-line blocking the per-bot
        budget exists to prevent -- the messages behind it are for other bots.

        It holds one of the queue's own slots while it waits, so ``MAX_IN_FLIGHT`` bounds how
        many can be parked; a queue whose budget is entirely parked stops being read until a
        send finishes, which is the backpressure it was always going to be.
        """
        # the queue's slot, and only that: what the message is waiting for is one of *this
        # bot's* sends to end, so a reservation here would be one nothing can release
        with self._books:
            self._in_flight[pending.on_queue] = self._in_flight.get(pending.on_queue, 0) + 1
        self._parked.append(pending)
        logger.debug(
            'holding a message for a bot at its own in-flight budget',
            extra={'tg_bot_id': pending.envelope.bot_id, 'tg_queue': pending.on_queue},
        )

    def _hand_over_parked(self) -> None:
        """Hand over every parked message whose bot now has room, oldest first.

        Called wherever a slot comes back, on the consumer's own thread like every other
        transport call this class makes. A message whose bot is *still* at its budget stays
        parked and the walk carries on: the queue is one deque, and stopping at the first
        saturated bot would be the blocking this exists to avoid.
        """
        if not self._parked:
            return
        waiting, self._parked = self._parked, deque()
        while waiting:
            pending = waiting.popleft()
            if self._at_its_own_capacity(pending.envelope.bot_id):
                self._parked.append(pending)
                continue
            # the queue's slot it has been holding is the one the send will use, so that count
            # is not touched again -- see `_hand_over`'s `counted`
            if self._hand_over(pending, counted=True):
                self.acknowledge(pending.handle)

    def serve(self, queues: 'Seq[str]') -> None:
        """Read exactly these queues from the next read on, without stopping.

        What a container calls when a client's queue arrives or goes away while it runs: the
        alternative is stopping the consumer and starting another, which on a multiplexing
        transport would pause every *other* queue in the group for the length of a join.

        A queue that goes away keeps its count until its sends finish -- they are still in
        flight, and the transport still has to be told about them -- but nothing new is read
        from it. One that arrives starts at zero.
        """
        asked = tuple(dict.fromkeys(queues))
        if not asked:
            return
        if len(asked) > 1 and not type(self.broker).MULTIPLEXES:
            raise QueueMultiplexingUnavailableError(type(self.broker).__name__, asked)
        with self._books:
            for queue in asked:
                self._in_flight.setdefault(queue, 0)
            self.queues = asked
            self._asked = None if asked == (self._own_queue(),) else asked
        self._tell_the_broker()

    def _tell_the_broker(self) -> None:
        """Say which queues this consumer is *for*, as against which it is reading right now.

        The two are not the same question, and one transport pays for the difference: a `take`
        naming fewer queues is one of them at its budget, and on Kafka a narrower subscription
        is a group rebalance. So the set moves here -- when a queue is added to this container
        or taken away from it -- and a narrower read pauses rather than resubscribes.

        Through `getattr`, like every other optional thing a `DELIVERY` may hold: `self.broker`
        is whatever the registry handed over, and a double standing in for one need not have
        heard of a method the contract gained.
        """
        told = getattr(self.broker, 'serving', None)
        if callable(told):
            # including ``None``, which means *the queue this broker addresses* -- a lane that
            # shrank to one queue has to say so, or a transport whose subscription follows this
            # keeps holding a queue somebody else is now serving
            told(self._asked)

    def in_flight(self, on_queue: str = '') -> int:
        """How many sends this consumer is holding, on one queue or across all of them.

        Public because the bound is: a project's own `Delivery` reads it to decide anything it
        paces itself, and a case asserting the count comes back to nothing should not have to
        reach into the books to do it.
        """
        with self._books:
            if on_queue:
                return self._in_flight.get(on_queue, 0)
            return sum(self._in_flight.values())

    def at_capacity(self, on_queue: str = '') -> bool:
        """Whether this consumer is already holding as many sends as it may.

        Per queue, and ``''`` asks about the consumer as a whole -- which is every queue it
        serves being at its own bound, since one that is not can still be read from. With one
        queue the two questions are the same, which is what every caller before 5.0 asked.
        """
        if not self._limit:
            return False
        with self._books:
            if on_queue:
                return self._in_flight.get(on_queue, 0) >= self._limit
            # every queue it is *serving*: one it no longer reads may still be settling sends,
            # and waiting for those to end before reading anything would be a stop by another
            # name
            return all(self._in_flight.get(queue, 0) >= self._limit for queue in self.queues)

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
                raw, delivered, bot_id, on_queue = self._finished.get(timeout=1)
            except queue.Empty:
                continue
            self._took_a_slot_back(bot_id, on_queue)
            if delivered:
                self.acknowledge(raw)
            self._hand_over_parked()

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

    def _decoded(self, raw: bytes) -> tuple[object, bool]:
        """Turn the bytes into whatever they hold, or into a verdict about them.

        The three ways decoding fails, and they do not get the same answer. A pickle the
        configuration refuses is left in flight, because a setting is what stands between it
        and delivery. The other two are acknowledged: nothing will ever make sense of them, and
        this reader is on the far side of a trust boundary where an escaping exception would
        end the consumer for the life of the container.
        """
        try:
            return loads(raw), True
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

    def _read(self, raw: bytes) -> tuple['Envelope | None', bool]:
        """Turn one message off the queue into an envelope, or into a verdict.

        Everything here is untrusted input, so no failure may escape: what comes
        back is either the envelope or `None` plus whether to acknowledge the
        message that never became one.
        """
        payload, readable = self._decoded(raw)
        if payload is None:
            return None, readable
        if is_control(payload):
            return None, self._acted_on(payload)
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

    @staticmethod
    def _acted_on(payload: object) -> bool:
        """Act on a notice sharing the queue with the calls, and acknowledge it.

        Acknowledged rather than left in flight: it is not a message anybody is waiting for,
        and keeping it would have every restart reconcile from a payload nothing needs twice.
        """
        apply_control(payload)
        return True

    def dispatch(self, raw: bytes, handle: object | None = None, on_queue: str = '') -> bool:
        """Decode one message and hand it to the handler.

        ``on_queue`` is which of this consumer's queues the message came off, and it is what
        its send is counted against: the budget is per queue, and a consumer reading several
        over one connection has to say which one rather than have it inferred. Left out -- as
        every caller before 5.0 left it out -- it is the one queue this consumer serves.

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
        # against the books, so a message read from a queue `serve` has since taken away is
        # still counted where its slot will be given back -- see `_queue_of`
        on_queue = on_queue if self._counted_under(on_queue) else self.queues[0]
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
                on_queue=on_queue,
            )
            logger.exception(
                'dropping queued message naming a method that is not Telegram API',
                extra={'tg_function': envelope.function},
            )
            return True
        try:
            handler = self._handler_for(envelope)
        except Exception:
            # left in flight, not acknowledged: this process is not configured for that bot
            # and another one may be. `tgbot_reclaim` is what puts it back once one is
            logger.exception(
                'leaving a message for a bot this process does not serve in flight',
                extra={'tg_bot_id': envelope.bot_id, 'tg_key': self.processing_key},
            )
            return False
        self._record(EventKind.OUTBOUND_CONSUMED, envelope, on_queue=on_queue)
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
        pending = _Pending(envelope, call, handle, handler, on_queue)
        if self._at_its_own_capacity(envelope.bot_id):
            if not self._limit:
                # nothing bounds the queue, so nothing would bound the holding either: a
                # saturated bot would grow the held list, and the transport's in-flight state
                # with it, until the process ran out of memory -- and on a Redis list every
                # acknowledgement scans that state. So this waits for the bot instead, which
                # costs the head-of-line blocking the holding avoids and costs nothing
                # unbounded. `W012` is what tells an operator to set `MAX_IN_FLIGHT` and get
                # the better behaviour
                if not self._waited_for_room(envelope.bot_id):
                    return False
            else:
                # not acknowledged and not released: it is held here until this bot has room,
                # and a crash in between leaves it where every other in-flight message is
                self._park(pending)
                return False
        return self._hand_over(pending)

    def _handler_for(self, envelope: Envelope) -> Handler:
        """Return the handler for the bot this message names, or the one for no bot at all.

        Raises where the identity names a bot this process does not serve. `dispatch` leaves
        such a message *in flight* rather than acknowledging it, which is the same answer it
        gives an envelope from a newer version and for the same reason: the message is
        perfectly deliverable by a process that is configured for it, and acknowledging would
        destroy it over a deployment that has not caught up.

        **A consumer with no route still checks the identity**, and that is not belt and
        braces. A process serving one bot is handed no route -- there is nothing to choose
        between -- and two such processes can share a queue, so a message naming the *other*
        one would otherwise be delivered by this bot and acknowledged: the wrong token, into a
        chat it may not be in, and the message gone. Only a payload naming this process's own
        bot, or naming none, is its to deliver.
        """
        if envelope.bot_id is None:
            return self.handler
        if self.route is not None:
            return self.route(envelope.bot_id)
        # deferred: this reads Django settings and the module is imported by the checks
        from django_aiogram.config.bots import records  # noqa: PLC0415 - as above

        if envelope.bot_id not in {found.bot_id for found in records()}:
            msg = f'this process serves no bot with the identity {envelope.bot_id}'
            raise LookupError(msg)
        return self.handler

    def _hand_over(self, pending: _Pending, *, counted: bool = False) -> bool:
        """Call the handler, and say whether the message may be acknowledged.

        Cancellation is the reason this is not one ``except``: it is a
        ``BaseException``, so letting it through would leave :meth:`run` and end
        the consumer for the life of the container. The message stays in flight because
        the outcome is *unknown*: a send can be cancelled after Telegram has taken the
        request, so leaving it risks a duplicate rather than a loss. This worker has to
        keep reading either way.
        """
        envelope, call, handle, on_queue = pending.envelope, pending.call, pending.handle, pending.on_queue
        deferring = self._defers
        # one claim for the callbacks and for the two exception paths below: whoever settles
        # this message first is the only one that may give its slots back
        settling = self._settlement()
        if deferring:
            # into the dict, never alongside it as a second keyword. The queue is
            # a trust boundary and send() forwards whatever it was given, so a
            # payload can carry this name — as a keyword that is "got multiple
            # values", a TypeError landing in the failure branch below, which
            # acknowledges a message nothing sent. Assigning simply wins
            call['on_complete'] = self._completion_for(handle, envelope.bot_id, settling, on_queue)
            if self._releases:
                # its pair, so a producer that refuses the send gives the slot back
                call['on_refused'] = self._release_for(handle, envelope.bot_id, settling, on_queue)
            if not counted:
                # a message handed over from the park is already holding the queue's slot --
                # the one `_park` took -- and counting it twice would leave that budget short
                # by one for the life of the process
                with self._books:
                    self._in_flight[on_queue] = self._in_flight.get(on_queue, 0) + 1
            # this bot's count moves either way: a parked message was *waiting* for a send to
            # end, and now it is one
            self._sending[envelope.bot_id] = self._sending.get(envelope.bot_id, 0) + 1
        try:
            pending.handler(**call)
        except asyncio.CancelledError:
            if deferring and settling():
                # only where nothing has settled it: a handler that reported and *then* was
                # cancelled has already put its settlement on the queue
                self._took_a_slot_back(envelope.bot_id, on_queue)
                self._hand_over_parked()
            logger.warning(
                'a queued send was cancelled; leaving it in flight',
                extra={'tg_function': envelope.function},
            )
            return False
        except Exception:
            # as above: a handler that reported and then raised has settled this message
            # already, and returning the slots a second time is what makes the count drift
            ours = not deferring or settling()
            if deferring and ours:
                self._took_a_slot_back(envelope.bot_id, on_queue)
                self._hand_over_parked()
            logger.exception(
                'handler failed for queued message',
                extra={'tg_bot_id': envelope.bot_id, 'tg_function': envelope.function},
            )
            # and the acknowledgement belongs to whoever settled it. A handler that reported
            # through `on_complete` before raising has a settlement on the queue and `collect`
            # will acknowledge from it, so saying yes here acknowledges the same message
            # twice; one that reported through `on_refused` said *nothing sent this*, and
            # acknowledging it would destroy a message a later start could deliver
            return ours
        # a deferring handler decides when this message is done. Returning True
        # here is what made the at-least-once promise false: send_raw returns as
        # soon as the coroutine is scheduled, long before Telegram has seen it
        return not deferring

    def _record(self, kind: EventKind, envelope: Envelope, error: str = '', on_queue: str = '') -> None:
        """Record what the consumer did with one message, and which queue it came off."""
        chat_id = envelope.kwargs.get('chat_id')
        recorder.record(
            Event(
                kind=kind.value,
                correlation_id=envelope.correlation_id or new_correlation_id(),
                # through `as_identifier` like every other number off the wire: an envelope
                # is untrusted input, a Python integer has no width, and one wider than the
                # column fails the whole batch it travelled in rather than its own row
                bot_id=as_identifier(envelope.bot_id),
                function=envelope.function,
                chat_id=as_identifier(chat_id),
                worker=worker_identity(),
                error=error,
                detail=self._queue_latency(envelope, on_queue),
            )
        )

    def _queue_latency(self, envelope: Envelope, on_queue: str = '') -> dict[str, Any]:
        """Where the message was and how long it waited, as far as either is known.

        The queue by name, because a container serving several is the normal shape since 5.0
        and a row that does not say which one leaves an operator guessing. In `detail` rather
        than in a column of its own: a column on this table is a migration on the one table
        whose size is set by traffic, and nothing queries the feed *by* queue -- the bot is
        the dimension a client's messages are found under, and that has a column and an index.

        The queue the *message* came off, not the one this consumer was built from: one
        consumer reads several since it learnt to multiplex, and recording the first of them
        for all of them would say every client's messages came off one queue.
        """
        came_off = on_queue or self.queue_key
        said: dict[str, Any] = {'queue': came_off} if came_off else {}
        if envelope.queued_at:
            said['queue_ms'] = int((time.time() - envelope.queued_at) * 1000)
        return said

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
            self._forget_retired()
            asked = self.readable()
            if asked == ():
                # every queue this consumer serves is at its budget, and a drain has no thread
                # to wait on -- the blocking loop goes round again instead
                return
            taken = self.broker.take_nowait(asked) if asked else self.broker.take_nowait()
            if taken is None:
                self.collect()
                return
            if self.dispatch(taken.payload, taken.handle, self._queue_of(taken)):
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
                self._forget_retired()
                asked = self.readable()
                if asked == ():
                    # `hold_for_capacity` returns when *one* queue has room, and `serve` can
                    # have taken that queue away since. Nothing to read from, so round again
                    continue
                taken = self.broker.take(timeout, asked) if asked else self.broker.take(timeout)
            except Exception:
                # a dropped connection must not kill the worker thread
                logger.exception('blocking pop failed, retrying', extra={'tg_key': self.queue_key})
                self._stop.wait(timeout)
                continue
            if taken is None:
                continue
            if self.dispatch(taken.payload, taken.handle, self._queue_of(taken)):
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


def get_delivery(
    handler: Handler,
    route: 'Route | None' = None,
    settings: 'Mapping[str, Any] | None' = None,
    queues: 'Seq[str] | None' = None,
) -> Delivery:
    """Build the consumer ``DELIVERY`` names, with the handlers it delivers through.

    ``DELIVERY`` is a documented seam, and what it promised was a subclass implementing
    ``run()`` -- so a project's own ``__init__(self, handler)`` predates the route and must keep
    working. It is passed only to a class that says it takes one.

    A class that does not, in a process serving **several** bots, is refused rather than built:
    without a route every addressed message would be delivered through the process's own bot,
    under a token the producer did not name, silently. With one bot there is nothing to route
    and nothing to say.

    ``settings`` is the same contract one release later, for the queue rather than the bot: a
    consumer that cannot be told which queue it serves is refused where a queue other than the
    process's own was asked for, and left alone otherwise.

    ``queues`` is the same again, for a lane of several read over one connection -- and a class
    that does not take it is refused rather than built for one of them, since a container
    believing it serves three queues and serving one leaves two backlogs with nobody on them.
    """
    resolved = delivery_class()
    named = f'{resolved.__module__}.{resolved.__qualname__}'
    # each argument judged on its own, and passed on its own: a class may take one and not the
    # other, and a consumer built without the queue it was asked for would read the process's
    # instead -- silently, while the container believed it was serving another
    takes_route = accepts_keyword(resolved.__init__, 'route')
    takes_settings = accepts_keyword(resolved.__init__, 'settings')
    takes_queues = accepts_keyword(resolved.__init__, 'queues')
    if route is not None and not takes_route:
        raise DeliveryNotConfiguredError(
            named,
            'whose __init__ takes no `route`, and this process serves more than one bot: '
            'every addressed message would be delivered through the wrong one. Add '
            '`route=None` to its __init__ and hand it to `Delivery.__init__`.',
        )
    if settings is not None and not takes_settings:
        raise DeliveryNotConfiguredError(
            named,
            'whose __init__ takes no `settings`, and this process serves a queue that is not '
            'its own: every message would be taken from the process-wide queue instead. Add '
            '`settings=None` to its __init__ and hand it to `Delivery.__init__`.',
        )
    if queues is not None and not takes_queues:
        raise DeliveryNotConfiguredError(
            named,
            'whose __init__ takes no `queues`, and this container serves several of them over '
            'one connection: every message would be taken from one queue while the rest went '
            'unread. Add `queues=None` to its __init__ and hand it to `Delivery.__init__`.',
        )
    given: dict[str, Any] = {}
    if takes_route:
        given['route'] = route
    if takes_settings:
        given['settings'] = settings
    if takes_queues and queues is not None:
        given['queues'] = queues
    return resolved(handler, **given)
