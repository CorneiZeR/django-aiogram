"""The bot object Django code talks to.

One facade over an aiogram ``Bot``, ``Dispatcher`` and ``Router``, built lazily
so that importing the package costs nothing in the processes — web workers, cron
jobs, the test suite — that only ever queue a message.

What is here is the object and its lifetime: the lazy aiogram parts, the loop it drives,
every route a message can take out of this process, and the shutdown that has to hold the
at-least-once guarantee across all of them. That guarantee is why the class is not cut
further into mixins — it is a property of state that ``send``, ``_schedule``, ``_drain``
and ``close`` share, and spreading that state across files would leave no file answerable
for it.

Six modules carry the parts that are *not* about that state, and each says what belongs
in it:

* :mod:`~django_aiogram.producer.outbound` — what names one send in flight, and what says
  it is finished.
* :mod:`~django_aiogram.producer.looping` — who may drive the loop, and for how long.
* :mod:`~django_aiogram.producer.queueing` — everything a queue write does except the
  write, shared by the synchronous producer and the awaiting one.
* :mod:`~django_aiogram.producer.committing` — whether that write waits for the caller's
  transaction to commit.
* :mod:`~django_aiogram.producer.from_settings` — the aiogram objects a project's settings
  describe, and the refusals they earn.
* :mod:`~django_aiogram.producer.routing` — the handler decorators, which read the router
  and nothing else.
"""

import asyncio
import contextlib
import datetime
import logging
import threading
import time
import uuid
from asyncio import AbstractEventLoop
from collections.abc import Callable, Coroutine, Iterable
from concurrent import futures
from typing import TYPE_CHECKING, Any

from aiogram import Bot, Dispatcher, Router, exceptions
from aiogram.types import Update
from django.core.exceptions import ImproperlyConfigured

from django_aiogram.api import check_function
from django_aiogram.broker.registry import close_broker, get_broker
from django_aiogram.config.enums import EventKind
from django_aiogram.config.settings import SETTINGS_NAME, coerce_bool, conf
from django_aiogram.db import DatabaseConnectionMiddleware
from django_aiogram.eventlog.events import short_id
from django_aiogram.eventlog.instrumentation import install_instrumentation
from django_aiogram.eventlog.recorder import recorder
from django_aiogram.eventlog.records import Event, as_identifier
from django_aiogram.exceptions import LoopThreadNotStartedError, ShuttingDownError
from django_aiogram.producer.committing import defer
from django_aiogram.producer.from_settings import build_default_properties, build_storage
from django_aiogram.producer.looping import LOOP_THREAD, RUNNER_TIMEOUT, drain_budget, loop_lock, mention_asend
from django_aiogram.producer.outbound import TASK_PREFIX, Outbound, completion, resolve_correlation_id, settle
from django_aiogram.producer.queueing import Queueing, chunks, publishing, serialise
from django_aiogram.producer.routing import RouterShortcuts
from django_aiogram.producer.scheduling import aschedule, cancel, due_moment, schedule
from django_aiogram.producer.throttling import RateLimiter, get_rate_limiter
from django_aiogram.redis import aclose_redis

if TYPE_CHECKING:
    # `outcomes` reaches the ORM through `writer`, and this module is imported by
    # `_singleton` on the first touch of `bot` — earlier than the app registry in a process
    # that only ever queues. The two methods below import it when they are called
    from django_aiogram.eventlog.outcomes import Outcome

logger = logging.getLogger('django_aiogram')


def _message_ids(result: object) -> list[int] | None:
    """Return the ids in a result that is a collection of messages, or ``None`` otherwise.

    Only ``send_media_group`` and its siblings answer with a list; everything else has its id
    in the ``message_id`` column, and a key that repeated it would be a second place to read
    the same thing from.
    """
    if not isinstance(result, (list, tuple)):
        return None
    found = [getattr(message, 'message_id', None) for message in result]
    return [one for one in found if isinstance(one, int)] or None


class TelegramBot(RouterShortcuts):
    """Facade over an aiogram bot, dispatcher and router.

    Everything expensive — the aiogram ``Bot``, the ``Dispatcher`` and the event
    loop — is built on first use. Instantiating this class must stay cheap and
    must not require a token, otherwise merely importing the package would break
    projects that never talk to Telegram.
    """

    def __init__(
        self,
        max_retries: int | None = None,
        loop: AbstractEventLoop | None = None,
    ) -> None:
        """Record the overrides; nothing aiogram or Redis owns is built here."""
        self._max_retries = max_retries
        self._loop = loop
        self._bot: Bot | None = None
        self._dispatcher: Dispatcher | None = None
        self._router = Router()
        #: sends this bot scheduled, so shutdown drains its own work only
        # the call behind each task, so shutdown can say what it canceled
        self._sends: dict[asyncio.Task[None], Outbound] = {}
        self._polling = False
        self._closing = False
        #: the thread a web process gives the loop, so updates do not serialize
        self._runner: threading.Thread | None = None
        #: set once that thread is actually running the loop. Every caller waits
        #: on it, not only the one that started the thread
        self._runner_ready = threading.Event()
        #: updates a request thread is blocked on. Stopping the loop under one of
        #: these would leave that thread waiting on a future nothing will finish
        self._updates: set[futures.Future[None]] = set()
        # only true while close() is flushing the loop, so the refusal below can tell
        # a hand-off queued before shutdown from one queued during it
        self._draining = False
        # reentrant: _attach_router holds it while reading self.dispatcher
        self._build_guard = threading.RLock()

    @property
    def enabled(self) -> bool:
        """Whether this process should **send** — reach Telegram, or write to the broker.

        Not "reach nothing": the depth reads reach the broker regardless of this flag,
        deliberately, because a tier kept from sending is exactly the one an operator asks how
        deep the queue is. `queue_depth` says so too, and `tests/test_enabled_flag.py` fails if either reader
        grows a gate.
        """
        return coerce_bool(conf['ENABLED'], f"{SETTINGS_NAME}['ENABLED']")

    @property
    def _raises_send_failures(self) -> bool:
        """Whether a failed send reaches the caller instead of only the log.

        Read through ``coerce_bool``, like every other boolean here. A bare ``if`` on the
        raw value is how ``DJANGO_AIOGRAM_RAISE_EXCEPTION=false`` — the string
        ``'false'``, which is truthy — used to re-raise the exception the project had asked
        to have swallowed, on a path that only runs once a send has exhausted its retries.
        """
        return coerce_bool(conf['RAISE_EXCEPTION'], f"{SETTINGS_NAME}['RAISE_EXCEPTION']")

    @property
    def rate_limiter(self) -> RateLimiter | None:
        """Paced per token: Telegram meters the bot, not this object.

        Two instances holding the same token therefore share one budget; a
        different token gets its own.
        """
        # no instance cache: the registry already caches per token, and holding
        # a second copy here is what kept a bot on stale RATE_LIMIT settings
        # after the registry was reset
        return get_rate_limiter(str(conf['TOKEN'] or ''))

    @property
    def max_retries(self) -> int:
        """How many rate-limited attempts a send gets before it is given up on."""
        if self._max_retries is not None:
            return self._max_retries
        return int(conf['MAX_RETRIES'])

    @property
    def loop(self) -> AbstractEventLoop:
        """The event loop every send and the dispatcher run on."""
        if self._loop is None:
            # two first sends from different web threads would otherwise each
            # build one, and loop_lock would then serialize nothing: the two
            # senders would hold locks belonging to different loops
            with self._build_guard:
                if self._loop is None:
                    self._loop = asyncio.new_event_loop()
        return self._loop

    @property
    def bot(self) -> Bot:
        """The aiogram ``Bot``, which is the first thing that needs a token."""
        if self._bot is None:
            token = conf['TOKEN']
            if not token:
                msg = f"{SETTINGS_NAME}['TOKEN'] is required to talk to Telegram."
                raise ImproperlyConfigured(msg)
            with self._build_guard:
                if self._bot is None:
                    self._bot = Bot(token=token, default=build_default_properties())
        return self._bot

    @property
    def dispatcher(self) -> Dispatcher:
        """The aiogram ``Dispatcher``: the FSM storage, the connection reset, the event log.

        Built here and only here, so polling and webhook get the same middleware chain -- one
        update middleware sees every update exactly once, whichever way updates arrive.

        The order of the two registrations is the contract, not a detail.
        :class:`~django_aiogram.db.DatabaseConnectionMiddleware` goes on first and so runs
        outermost, which is what makes the connection reset the first thing that happens to an
        update and the last: a recording middleware that wrote its row through a dead connection
        would be the same outage one frame further in.

        And it is unconditional, where `install_instrumentation` returns before building anything
        if nothing reads events. The event log is optional; a live database connection is not.
        """
        if self._dispatcher is None:
            # two concurrent first requests would otherwise build one each, and
            # the router would attach to whichever was discarded
            with self._build_guard:
                if self._dispatcher is None:
                    self._dispatcher = Dispatcher(storage=build_storage())
                    self._dispatcher.update.outer_middleware.register(DatabaseConnectionMiddleware())
                    install_instrumentation(self._dispatcher)
        return self._dispatcher

    @property
    def router(self) -> Router:
        """Router holding every handler registered through the decorators."""
        return self._router

    @property
    def is_worker(self) -> bool:
        """True only inside the process that runs the bot itself."""
        return self._polling

    def _attach_router(self) -> None:
        """Attach the router once; aiogram refuses a second attachment.

        Under the build lock: two concurrent first requests would both see no
        parent and the second would raise.
        """
        with self._build_guard:
            if self._router.parent_router is None:
                self.dispatcher.include_router(self._router)

    def start_polling(self) -> None:
        """Attach the router and block on Telegram long polling."""
        self._attach_router()

        async def poll() -> None:
            """Long-poll Telegram, with ``_polling`` true only while this is running.

            A coroutine rather than a straight call, so the flag is raised and lowered
            on the loop itself — see the comment below for what setting it earlier cost.
            """
            # marked from inside the loop: setting it before run_until_complete
            # left a window where send() chose send_raw while the loop was not
            # running yet, and a consumer thread would then drive it from the
            # wrong thread
            self._polling = True
            try:
                await self.dispatcher.start_polling(self.bot)
            finally:
                self._polling = False

        self.loop.run_until_complete(poll())

    def feed_update(self, update: Update) -> None:
        """Hand one update to the dispatcher and wait for the handlers.

        Webhook mode calls this from a request thread. It waits rather than
        scheduling: the response must not be sent before the handlers have run,
        or a failure would go unreported and the request would look successful.
        """
        self._attach_router()
        owned = self._ensure_loop_runs()

        coroutine = self.dispatcher.feed_update(self.bot, update)
        loop = self.loop
        with loop_lock(loop):
            if self._closing or loop.is_closed():
                # decided under the same lock the shutdown snapshot is taken
                # under, or an update submitted just after it would be neither
                # waited for nor canceled, and its request would never return.
                #
                # `is_closed()` as well, because `close()` puts `_closing` back to
                # False in its finally: a request that captured the loop before the
                # teardown and reached this lock after it saw a loop that was neither
                # closing nor running, drove `run_until_complete` on a closed one, and
                # the view answered 200 to an update nothing had handled — so Telegram
                # never redelivered it. `_schedule` has always checked both
                coroutine.close()
                raise ShuttingDownError
            if not loop.is_running():
                if owned:
                    # our own thread owns this loop and was slow to start.
                    # Driving it here would put two threads on one loop
                    coroutine.close()
                    raise LoopThreadNotStartedError(RUNNER_TIMEOUT)
                # nothing could be started to run it, so drive it here — which is
                # what every update did before, one at a time under this lock
                loop.run_until_complete(coroutine)
                return
            # something runs this loop — polling, or the thread started above —
            # so hand the update over. Decided under the lock: a loop another
            # request is driving looks running until it stops, and the update
            # would then wait for ever
            future = asyncio.run_coroutine_threadsafe(coroutine, loop)
            self._updates.add(future)

        try:
            # waiting outside the lock, so the next request is not held up by ours
            future.result()
        except (futures.CancelledError, asyncio.CancelledError) as cancelled:
            # `_stop_runner` canceled this one: no handler finished, so it is the
            # same refusal a request arriving mid-shutdown gets, and the view has
            # to answer it the same way. Left as a cancellation it reads as a
            # handler that failed — a 200 telling Telegram to forget an update
            # nothing handled. Both classes: they are one object on some versions
            # and, where they are not, only one of them is an `Exception`
            raise ShuttingDownError from cancelled
        finally:
            self._forget_update(future)

    def _forget_update(self, future: 'futures.Future[None]') -> None:
        """Drop a finished update from the set the shutdown snapshot reads.

        Under the same lock it was added under. `_stop_runner` takes `list()` over
        this set while holding that lock, and a `discard` from a request thread
        mid-iteration raises `RuntimeError: Set changed size during iteration`
        inside `close()` — aborting the shutdown before anything is torn down.
        """
        loop = self._loop
        if loop is None:
            self._updates.discard(future)
            return
        with loop_lock(loop):
            self._updates.discard(future)

    def _ensure_loop_runs(self) -> bool:
        """Give this process's loop a thread of its own, once.

        A web process serving the webhook drives nothing: every `feed_update`
        took `run_until_complete` **under `loop_lock`**, so updates in one process
        handled strictly one at a time, and a send a handler scheduled was not
        stepped until the next update arrived — or until `close()`, or never.
        Measured on four concurrent updates with a 200 ms handler: 0.81 s
        serialized against 0.21 s with the loop running.

        Not started in the polling process: `start_polling` runs the loop itself,
        and `loop.is_running()` below is what says so.

        Returns whether a thread of ours owns this loop — ready or not. A caller
        that owns it must never fall back to driving the loop itself, even when
        the thread was slow to start: the two would be running the same loop.
        """
        if self._closing:
            return False
        # judged under the guard the thread is also created under. `is_alive()` is
        # false *before* `start()` too, so a check outside it can read a runner
        # registered a moment ago as dead and start a second one — two threads on
        # one loop, which is the collision this method exists to prevent
        with self._build_guard:
            if self._closing:
                return False
            runner = self._runner
            if runner is not None and not runner.is_alive():
                # a thread that died before it ran the loop would otherwise be
                # kept for the life of the process: every later update would wait
                # out the timeout, log the warning and be refused, and no
                # redelivery can recover a condition that never clears
                logger.warning('the event loop thread is gone; starting another')
                self._runner = runner = None
                self._runner_ready.clear()
            if runner is None:
                loop = self.loop
                if loop.is_running():
                    # polling drives it; there is nothing to start
                    return False
                if loop.is_closed():
                    # starting a thread on it would raise "Event loop is closed" inside
                    # that thread, where nothing catches it, and every caller would then
                    # wait out RUNNER_TIMEOUT for a readiness event no one can set.
                    # Refusing is the caller's cue to answer that it did not run
                    return False
                self._runner_ready.clear()

                def run() -> None:
                    """Own this loop on this thread, and say so before blocking in it.

                    The readiness signal is queued *on the loop* rather than set here:
                    scheduled with ``call_soon`` it can only fire once ``run_forever``
                    is actually turning, so a caller that waits for it cannot find a
                    loop that exists but is not running yet.
                    """
                    asyncio.set_event_loop(loop)
                    loop.call_soon(self._runner_ready.set)
                    loop.run_forever()

                self._runner = threading.Thread(target=run, name=LOOP_THREAD, daemon=True)
                self._runner.start()
        # every caller waits, not only the one that started the thread: a second
        # request that returned as soon as the thread existed would find
        # `is_running()` still false below and drive the update with
        # `run_until_complete`, while the thread it saw called `run_forever` on
        # the same loop. That kills the thread, leaves `_runner` pointing at a
        # dead one, and quietly returns the process to handling updates one at a
        # time — the thing this method exists to stop
        if self._runner is None:
            return False
        if not self._runner_ready.wait(RUNNER_TIMEOUT):
            # slow, not absent. Saying so is the whole point of the return value:
            # a caller that drove the update here would collide with the thread
            # the moment it did start
            logger.warning('the event loop thread did not start in time', extra={'tg_timeout': RUNNER_TIMEOUT})
        return True

    def _stop_runner(self, drain_timeout: float) -> None:
        """Stop the thread this process gave the loop, if it started one.

        Before the teardown, not after: `close()` refuses outright on a running
        loop, so a bot that started a runner could never be closed.

        Updates in flight are waited for first, and canceled if they outlast the
        drain. A request thread blocks on `future.result()` with no deadline, so
        stopping the loop under one would leave that thread waiting on a future
        nothing will ever finish — a web worker held for the life of the process.
        Canceling before the loop stops is what turns that into an exception the
        request can answer with.
        """
        # under the guard `_ensure_loop_runs` registers a thread beneath, and
        # released before `loop_lock` below rather than held across it: a send
        # driven under `loop_lock` reaches the `bot` property, which takes this
        # guard, so guard-inside-lock is the order that already exists.
        #
        # Without it a request that passed the `_closing` check could register a
        # runner *after* this snapshot read None, and that thread would call
        # `run_forever` on the loop the teardown below is closing
        with self._build_guard:
            runner, self._runner = self._runner, None
            self._runner_ready.clear()
        if runner is None:
            return
        loop = self._loop
        if loop is not None:
            # under the lock `feed_update` submits beneath, so an update cannot
            # slip in between this snapshot and the loop stopping. The waiting
            # stays outside it: holding it would block nothing useful and delay
            # the refusal above
            with loop_lock(loop):
                pending = list(self._updates)
        else:
            pending = list(self._updates)
        if pending:
            logger.info('waiting for updates in flight', extra={'tg_pending': len(pending)})
            futures.wait(pending, timeout=max(0.0, drain_timeout))
            unfinished = [future for future in pending if not future.done()]
            if unfinished:
                logger.warning('cancelling updates still in flight', extra={'tg_pending': len(unfinished)})
                for future in unfinished:
                    future.cancel()
        if loop is not None and not loop.is_closed() and runner.is_alive():
            # only while the thread is there to consume it: queued at a loop nobody is
            # turning, the `stop` sits in the ready queue and fires inside the *drain's*
            # `run_until_complete` instead, which then raises "Event loop stopped before
            # Future completed" and takes the teardown down with it
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(loop.stop)
        runner.join(timeout=RUNNER_TIMEOUT)
        if runner.is_alive():
            logger.warning('the event loop thread did not stop in time', extra={'tg_timeout': RUNNER_TIMEOUT})
            # put it back. `close()` refuses on a running loop and returns, so this
            # orphan is still driving it — and with `_runner` left as None every later
            # `close()` returned in no time without asking it to stop again, leaving the
            # loop, the aiogram session and the FSM client open for the life of the
            # process. `_runner_ready` goes back with it: the thread that outlived the
            # join is the one running the loop, so waiting on a cleared event would
            # refuse every update for `RUNNER_TIMEOUT` and never clear
            with self._build_guard:
                if self._runner is None:
                    self._runner = runner
                    self._runner_ready.set()

    def send(
        self,
        function: str = 'send_message',
        *,
        correlation_id: uuid.UUID | str | None = None,
        eta: datetime.datetime | None = None,
        **kwargs: Any,
    ) -> uuid.UUID:
        """Deliver a message the way this process can.

        Inside the bot container that means calling Telegram directly; anywhere
        else it means handing the call to the queue for the bot to pick up. It
        saves every caller from having to know which process it is running in.

        ``eta`` puts it in the schedule instead, for a mover to publish when the time comes.
        See :mod:`django_aiogram.producer.scheduling` for why the wait cannot live in the
        transport, and ``manage.py tgbot_dispatch_scheduled`` for what publishes it.

        Returns the correlation id every event about this message carries, so a
        project can store it next to its own model and join the two.
        """
        # resolved here so both routes agree on the id, and so a caller reading
        # the return value gets the same one the rows carry
        identifier = resolve_correlation_id(correlation_id)
        # before the worker shortcut, not after: an `eta` inside the bot container has to be
        # written down like anywhere else, and sending it now is the one reading of `eta`
        # that cannot be what the caller meant
        if eta is not None:
            return self.enqueue(function, correlation_id=identifier, eta=eta, **kwargs)
        if self.is_worker:
            return self.send_raw(function, correlation_id=identifier, **kwargs)
        # named here as well as in `enqueue`, and before delegating, because the twin a
        # caller should hear about is the twin of the method they called: `send` pairs with
        # `asend`, and `enqueue` — which this is about to call — pairs with `aenqueue`.
        # The latch means whichever entry point the caller used is the one that speaks.
        # Behind `enabled`, because `enqueue` refuses before writing anything when the
        # bot is off — advice about the async twin of a call that does nothing is noise,
        # and it would burn the once-per-process latch for whoever does write later
        if self.enabled:
            # validated here as well as in `enqueue`, and before the mention: the latch
            # fires once per process, so a call that is about to raise `check_function`
            # would otherwise emit the line and take it from the first caller who could
            # have acted on it. Two membership tests on the happy path is the whole cost
            check_function(function)
            # and the broker for the same reason, one failure mode further along: `enqueue`
            # resolves it below, so a misconfigured `BROKER` used to raise *after* the latch
            # was spent. Cheap to do twice -- the registry caches per process, so the call
            # inside `enqueue` is a hit. `aenqueue` was fixed first and this is its twin
            get_broker()
            mention_asend('asend')
        return self.enqueue(function, correlation_id=identifier, **kwargs)

    async def asend(
        self,
        function: str = 'send_message',
        *,
        correlation_id: uuid.UUID | str | None = None,
        eta: datetime.datetime | None = None,
        **kwargs: Any,
    ) -> uuid.UUID:
        """Deliver a message from code that is already on an event loop.

        The same routing as :meth:`send`, without the blocking socket write that
        one does on the calling thread — which under ASGI is the thread serving
        requests, and on a first call includes a connect bounded by whatever
        timeout the configured transport declares.

        In the bot container it still calls Telegram directly, and that path was
        never blocking: :meth:`send_raw` schedules onto the loop and returns.
        """
        # before the first await, so a handler's correlation_scope is still the
        # one in effect: after an await the caller's context may have moved on
        identifier = resolve_correlation_id(correlation_id)
        if eta is not None:
            # as `send` does, and before the worker shortcut for the same reason
            return await self.aenqueue(function, correlation_id=identifier, eta=eta, **kwargs)
        if self.is_worker:
            return self.send_raw(function, correlation_id=identifier, **kwargs)
        return await self.aenqueue(function, correlation_id=identifier, **kwargs)

    def close(self, drain_timeout: float | None = None) -> None:
        """Finish what is in flight, then release everything this bot owns.

        A send waiting in the rate limiter is an ordinary state — pacing means
        waiting — so closing the loop without draining silently dropped those
        messages on every `docker stop`.

        ``drain_timeout`` defaults to ``DRAIN_TIMEOUT``. It used to be a hardcoded
        five seconds that `start_tgbot` never passed, so a deployment could raise
        `stop_grace_period` all it liked and never buy the drain a second more.
        """
        if drain_timeout is None:
            drain_timeout = drain_budget()
        # draining first, then closing, and the order is the whole guard. `start()` runs on
        # the loop thread and refuses on `_closing and not _draining`; two assignments
        # cannot be made atomic without a lock every send would have to take, but they can
        # be ordered so the state *between* them is harmless — with `_draining` already set
        # a callback landing mid-transition sees a bot that is not closing yet, and one
        # landing after sees a drain in progress. Neither refuses.
        #
        # The window is real: the runner is still stepping the loop while it is being
        # stopped, so a hand-off queued a moment before this call becomes a task there.
        # Setting `_closing` first dropped it, and only sometimes — pause between the send
        # and the close and the callback has already run
        self._draining = True
        self._closing = True
        # set only once the teardown has actually finished, and read in the `finally` to decide
        # whether the transport may be released. The transport is process-global, so releasing
        # it while this bot is half torn down closes a queue a live consumer may still be
        # taking from -- on Kafka the rebuild then joins the group a second time while the
        # first member still holds the partitions, which is the stall this change exists to fix.
        #
        # One flag rather than two: it is false on the skipped-teardown path *and* on every
        # exception out of the block below, which are the same question asked twice
        torn_down = False
        try:
            # inside the try, so a join that raises still reaches the finally below: with
            # both flags left set the bot could never send again. Before the teardown
            # because close() refuses on a running loop, so a process that gave the loop a
            # thread could otherwise never close its bot
            self._stop_runner(drain_timeout)
            if self._loop is not None or self._bot is not None or self._dispatcher is not None:
                loop = self.loop
                if loop.is_running():
                    # run_until_complete and loop.close() both raise on a running
                    # loop; leaving everything in place keeps close() retryable
                    logger.warning('skipping close: stop polling, or the loop thread, before closing the bot')
                    return
                # a send from another thread may be driving this loop; the lock
                # keeps the teardown from interleaving with it
                with loop_lock(loop):
                    self._drain(drain_timeout)
                    # RedisStorage owns a second, async Redis client nothing else closes
                    if self._dispatcher is not None:
                        loop.run_until_complete(self._dispatcher.storage.close())
                        self._dispatcher = None
                    if self._bot is not None:
                        loop.run_until_complete(self._bot.session.close())
                        self._bot = None
                    if not loop.is_closed():
                        loop.close()
            self._loop = None
            torn_down = True
        finally:
            # the transport too, and here rather than only at exit: `start_tgbot` joins the
            # consumer thread before calling this, so by now nothing is taking from the broker
            # and a flush cannot race a read. The `atexit` hook stays armed for the processes
            # that never call `close` -- a web tier that only queues -- and `close_broker` is
            # idempotent, so being reached twice costs nothing.
            #
            # Only once the teardown finished: a skipped one expects to be called again, and a
            # failed one leaves this bot half apart, so in both cases the queue has to survive
            # -- `atexit` still releases it if nobody manages a clean close. Nested, so a
            # transport that raises on the way out cannot leave the flags set: `close_broker`
            # propagates on purpose, and an exception escaping a `finally` skips whatever
            # follows it in the same block, which here is the pair below that must never stick
            try:
                if torn_down:
                    close_broker()
            finally:
                # a closed bot can be built again, so neither of these may stick, and they are
                # cleared in the mirror order: `_closing` first, so nothing sees
                # closing-without-draining on the way out either
                self._closing = False
                self._draining = False

    def send_raw(
        self,
        function: str = 'send_message',
        *,
        correlation_id: uuid.UUID | str | None = None,
        queued_at: float = 0.0,
        on_complete: Callable[[], None] | None = None,
        on_refused: Callable[[], None] | None = None,
        **kwargs: Any,
    ) -> uuid.UUID:
        """Call an aiogram bot method, retrying on Telegram rate limits.

        Returns the correlation id every event about this message carries.
        ``queued_at`` is set by the consumer when the call came off the queue,
        which is what makes time-in-queue measurable and tells the two apart.

        ``on_complete`` is called once the send has actually finished — sent,
        refused or given up on — and **not** called when the send is refused
        before it starts or canceled at shutdown. The consumer passes one so it
        can acknowledge the message then rather than now: this method returns as
        soon as the coroutine is *scheduled*, which is long before Telegram has
        seen anything.

        ``on_refused`` is its pair, for exactly the cases ``on_complete`` skips: called
        when this method refuses the send outright, so a caller holding a slot for the
        message can take the slot back **without** acknowledging it. The consumer passes
        both; nothing else needs to. Without it a refusal held its slot for the life of
        the process, and under ``MAX_IN_FLIGHT`` that stops the consumer taking messages
        at all.
        """
        check_function(function)
        identifier = resolve_correlation_id(correlation_id)
        if not self.enabled:
            logger.debug('send skipped: bot disabled', extra={'tg_function': function})
            # the same slot-return as the refusals below: `ENABLED` is read live, so a
            # consumer that took a slot can reach this branch after the setting changed,
            # and without this the bound closes one message at a time until a restart
            settle(on_refused)
            return identifier

        async def send() -> None:
            """Make the call, and keep making it while Telegram asks for a wait.

            The loop owns three things the caller cannot see: the rate limiter's wait,
            which is per attempt rather than per message; the ``retry_after`` sleeps,
            which are Telegram's number and not ours; and the decision that a refusal is
            final. Returning is therefore the only definition of *finished* this package
            has — sent, refused or out of retries alike — which is what the done-callback
            on the task is watching for.
            """
            last_error: exceptions.TelegramRetryAfter | None = None
            retries = 0
            started = time.monotonic()
            while retries <= self.max_retries:
                try:
                    # per attempt, not since the first: measured from `started`
                    # this would fold in the earlier attempts and the sleeps
                    # Telegram asked for, and stop being the limiter's wait
                    attempted = time.monotonic()
                    limiter = self.rate_limiter
                    if limiter is not None:
                        await limiter.acquire(call_kwargs.get('chat_id'))
                    paced = time.monotonic()
                    result = await getattr(self.bot, function)(**call_kwargs)
                except exceptions.TelegramRetryAfter as error:  # noqa: PERF203 - retrying is what the loop is for
                    last_error = error
                    self._record_send(
                        EventKind.OUTBOUND_RETRIED,
                        outbound,
                        attempt=retries,
                        detail={'retry_after': error.retry_after},
                    )
                    logger.warning(
                        'rate limited by telegram',
                        extra={
                            'tg_function': function,
                            'tg_retry_after': error.retry_after,
                            'tg_retries': retries,
                        },
                    )
                    retries += 1
                    await asyncio.sleep(error.retry_after)
                except Exception as error:
                    self._record_send(
                        EventKind.OUTBOUND_FAILED,
                        outbound,
                        attempt=retries,
                        error=error,
                    )
                    logger.exception('send failed', extra={'tg_function': function})
                    if self._raises_send_failures:
                        raise
                    return
                else:
                    self._record_send(
                        EventKind.OUTBOUND_SENT,
                        outbound,
                        attempt=retries,
                        # the return value used to be thrown away, and it carries
                        # the only id Telegram will ever give for this message.
                        # `eventlog.outcomes` is what reads it back out, in the process
                        # that queued the send and never saw the reply
                        message_id=getattr(result, 'message_id', None),
                        duration_ms=int((time.monotonic() - started) * 1000),
                        detail={
                            'paced_ms': int((paced - attempted) * 1000),
                            'queue_ms': int((time.time() - queued_at) * 1000) if queued_at else None,
                            # `send_media_group` answers with a *list* of messages, so the
                            # column above is None for it and every id would be lost. One row
                            # still, because one call happened and a receiver counting sends
                            # must not see ten for an album -- the ids go here instead
                            'message_ids': _message_ids(result),
                        },
                    )
                    logger.info('message sent', extra={'tg_function': function})
                    return

            # exhausting the retries used to return silently
            self._record_send(
                EventKind.OUTBOUND_DROPPED,
                outbound,
                attempt=retries,
                error=last_error,
                detail={'max_retries': self.max_retries},
            )
            logger.error(
                'giving up on message',
                extra={'tg_function': function, 'tg_max_retries': self.max_retries},
            )
            if self._raises_send_failures and last_error is not None:
                raise last_error

        call_kwargs = {**conf['DEFAULT_KWARGS'](function), **kwargs}
        outbound = Outbound(identifier, function, call_kwargs)
        self._schedule(send(), outbound, on_complete, on_refused)
        return identifier

    @staticmethod
    def _record_send(kind: EventKind, outbound: 'Outbound', **fields: Any) -> None:
        """Record one stage of an outbound message.

        Called from inside the send coroutine, which is the only place that
        knows the outcome: _schedule returns once the task is *created*, so the
        consumer acknowledges the message before Telegram has seen it.
        """
        # `active`, not `enabled`: this one guard gates every `outbound.sent`,
        # `failed`, `retried` and `dropped` row there is — the whole of what a
        # metrics receiver was connected for. Reading the table flag here is how
        # the advertised set comes out empty for anyone who left the table off
        if not recorder.active:
            return
        error = fields.pop('error', None)
        detail = fields.pop('detail', None) or {}
        recorder.record(
            Event(
                kind=kind.value,
                correlation_id=outbound.correlation_id,
                function=outbound.function,
                chat_id=as_identifier(outbound.call_kwargs.get('chat_id')),
                error_code=type(error).__name__ if error is not None else '',
                error=str(error) if error is not None else '',
                # None means 'not measured here', and a column is a better place
                # for it than a JSON key that reads as a measurement of zero
                detail={key: value for key, value in detail.items() if value is not None},
                **fields,
            )
        )

    def _register(
        self,
        task: 'asyncio.Task[None]',
        outbound: 'Outbound',
        on_complete: Callable[[], None] | None = None,
    ) -> None:
        """Track a send so :meth:`close` can wait for it.

        Registration happens when the task is created, not when it starts
        running: a task that has been scheduled but not yet stepped is exactly
        the one shutdown must not lose.
        """
        self._sends[task] = outbound
        task.add_done_callback(self._sends.pop)
        task.add_done_callback(self._log_task_failure)
        if on_complete is not None:
            task.add_done_callback(completion(on_complete))

    @staticmethod
    def _log_task_failure(task: 'asyncio.Task[None]') -> None:
        """Report what a finished send raised, since nobody awaits these tasks."""
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            logger.error('scheduled send failed', exc_info=error)

    def _schedule(
        self,
        coroutine: Coroutine[Any, Any, None],
        outbound: 'Outbound',
        on_complete: Callable[[], None] | None = None,
        on_refused: Callable[[], None] | None = None,
    ) -> None:
        """Run a coroutine on the bot loop from whichever thread we are on.

        The delivery consumer runs in its own thread while the loop belongs to
        the polling thread; calling create_task across that boundary is not
        thread safe and silently corrupts the loop's internals.

        Every path that refuses the coroutine leaves ``on_complete`` uncalled: the
        message was not sent, so the consumer has to keep it in flight.
        """
        if self._closing:
            # the loop is being torn down, so nothing would ever run this
            coroutine.close()
            self._record_drop(outbound, 'the bot is shutting down')
            logger.error('send refused: the bot is shutting down')
            # the slot back, not the acknowledgement: the message was not sent, so it
            # stays in flight for a redelivery, but the consumer must stop counting it
            settle(on_refused)
            return

        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None

        loop = self.loop
        if running is loop:
            if not self._polling and self._runner is None:
                # nothing in this process drives this loop — not polling, and no
                # runner — so the task is created and never stepped. With a
                # runner it *is* stepped, which is the normal webhook path and
                # must not warn: a warning on the healthy path is how people
                # learn to stop reading them
                logger.warning(
                    'scheduling a send on a loop nothing in this process runs',
                    extra={
                        'tg_function': outbound.function,
                        'tg_correlation_id': str(outbound.correlation_id),
                        'tg_short_id': short_id(outbound.correlation_id),
                    },
                )
            self._register(self._start(coroutine, loop, outbound), outbound, on_complete)
            return

        # several web threads may send at once, and run_until_complete is not
        # reentrant — the second caller would get "this event loop is already
        # running". The lock belongs to the loop, so two bots sharing one are
        # still serialized.
        with loop_lock(loop):
            # close() holds the same lock, so it may have finished the whole
            # teardown while this thread waited for it
            if self._closing or loop.is_closed():
                coroutine.close()
                self._record_drop(outbound, 'the event loop was closed')
                logger.error('send refused: the event loop was closed')
                settle(on_refused)
                return
            if loop.is_running():
                # decided under the lock: seen from outside it, a loop another
                # thread drives for one run_until_complete looks running right
                # up to the moment it stops, and the handoff would be lost
                self._hand_off(coroutine, loop, outbound, on_complete, on_refused)
                return
            with self._build_guard:
                # guard inside the lock, the order that already exists here
                runner = self._runner
            if runner is not None and runner.is_alive():
                # `is_running()` is False for the whole window between `Thread.start()`
                # and the runner reaching `run_forever`, so driving the loop here killed
                # our own thread with "this event loop is already running" — and set
                # `_runner_ready` on the way, because this call ran the `call_soon` the
                # dead thread had queued. `feed_update` consults `owned` for exactly
                # this; nothing here did
                self._hand_off(coroutine, loop, outbound, on_complete, on_refused)
                return
            try:
                loop.run_until_complete(coroutine)
            except RuntimeError:
                # polling started between the check above and this call
                if not loop.is_running():
                    raise
                self._hand_off(coroutine, loop, outbound, on_complete, on_refused)
                return
            # only a return settles. Cancellation is not completion — the same
            # rule the task path follows — and a failure RAISE_EXCEPTION let
            # through is already owned by the consumer's own except branch, so
            # settling here too would report one message finished twice and
            # drive the in-flight count below zero, quietly widening the bound
            # MAX_IN_FLIGHT exists to hold.
            #
            # Driven to completion right here, so there is no task to hang a
            # done-callback on — webhook mode takes this path for every send
            settle(on_complete)

    def _hand_off(
        self,
        coroutine: Coroutine[Any, Any, None],
        loop: AbstractEventLoop,
        outbound: 'Outbound',
        on_complete: Callable[[], None] | None = None,
        on_refused: Callable[[], None] | None = None,
    ) -> None:
        """Create the task on the loop thread, so it is registered before it runs."""

        def start() -> None:
            """Turn the coroutine into a registered task, or refuse it and say so.

            Runs on the loop thread, which is the point: creating the task here means it
            is in ``_sends`` before it can run, so a drain that comes next cannot miss
            it. The refusal below is what stops a coroutine queued a moment before
            ``close()`` from being garbage-collected unmentioned.
            """
            if self._closing and not self._draining:
                # close() began after this was queued; the loop will not run it.
                # _draining is the exception: close() runs one turn of the loop on
                # purpose, precisely so callbacks queued before it become tasks
                coroutine.close()
                self._record_drop(outbound, 'the bot started shutting down')
                logger.error('send dropped: the bot started shutting down')
                # the slot back, not the acknowledgement — as in `_schedule`'s refusals
                settle(on_refused)
                return
            self._register(self._start(coroutine, loop, outbound), outbound, on_complete)

        try:
            loop.call_soon_threadsafe(start)
        except RuntimeError:
            coroutine.close()
            self._record_drop(outbound, 'the event loop is closed')
            logger.exception('send dropped: the event loop is closed')
            # the same slot-return as `start`'s own refusal above: the loop closed between
            # the check under `loop_lock` and this call, so nothing will ever run it
            settle(on_refused)

    @staticmethod
    def _start(
        coroutine: Coroutine[Any, Any, None],
        loop: AbstractEventLoop,
        outbound: 'Outbound',
    ) -> 'asyncio.Task[None]':
        """Create the task, named so shutdown can say which message it canceled."""
        return loop.create_task(coroutine, name=f'{TASK_PREFIX}{outbound.correlation_id.hex}')

    @staticmethod
    def _record_drop(outbound: 'Outbound', reason: str) -> None:
        """Record a send that never reached Telegram, and used to leave only a log line.

        Carries the call, not just its id: a direct `send_raw` was never queued,
        so this row is the only one that will ever exist for that message.
        """
        recorder.record(
            Event(
                kind=EventKind.OUTBOUND_DROPPED.value,
                correlation_id=outbound.correlation_id,
                function=outbound.function,
                chat_id=as_identifier(outbound.call_kwargs.get('chat_id')),
                error_code='NotScheduled',
                error=reason,
            )
        )

    def _drain(self, timeout: float) -> None:
        """Let scheduled sends finish, canceling whatever outlasts the timeout."""
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        if loop.is_running():
            # cannot drive it from here; the caller is expected to stop polling first
            logger.warning('skipping drain: the event loop is still running')
            return

        # a hand-off is a call_soon_threadsafe callback until the loop steps, and
        # a callback is not in _sends — so without one turn here, a send queued
        # just before shutdown is invisible to the drain below and dies with the
        # loop. _register's own docstring says that is the one it must not lose
        self._draining = True
        try:
            loop.run_until_complete(asyncio.sleep(0))
        finally:
            self._draining = False

        # only this bot's sends: canceling unrelated tasks on the loop is not
        # ours to do, and aiogram keeps its own there
        pending = [task for task in self._sends if not task.done()]
        if not pending:
            return

        logger.info('draining in-flight sends', extra={'tg_pending': len(pending)})
        loop.run_until_complete(asyncio.wait(pending, timeout=timeout))

        dropped = [task for task in pending if not task.done()]
        if not dropped:
            return
        for task in dropped:
            self._record_drop(self._sends[task], 'cancelled at shutdown')
            task.cancel()
        loop.run_until_complete(asyncio.gather(*dropped, return_exceptions=True))
        logger.warning(
            'dropped in-flight sends at shutdown',
            extra={'tg_dropped': len(dropped), 'tg_drain_timeout': timeout},
        )

    def _accept(self, function: str, correlation_id: uuid.UUID | str | None) -> tuple[uuid.UUID, bool]:
        """Judge a queueing request, and name the message either way.

        Both producers ask the same three questions, and the third has a shape
        worth keeping in one place: a disabled bot returns the id rather than
        raising, so a caller storing it beside its own model gets the same value
        whether or not this deployment sends anything.
        """
        check_function(function)
        identifier = resolve_correlation_id(correlation_id)
        if not self.enabled:
            logger.debug('queueing skipped: bot disabled', extra={'tg_function': function})
            return identifier, False
        return identifier, True

    def _deferred(
        self,
        function: str,
        writes: list[Queueing],
        publish: Callable[[list[bytes]], None],
    ) -> bool:
        """Hand these writes to the caller's transaction, and say whether that happened.

        ``False`` means the caller publishes where it stands, which is what every send did
        before ``TRANSACTIONAL`` existed and what all of them still do with it off.

        **One hook for the call, however many writes it covers.** A fan-out is a list here
        rather than a hook per chunk, because the hooks are what decide what a failing chunk
        costs: registered one by one and swallowed one by one, chunk three would go out after
        chunk two was refused, where the immediate path raises and never reaches it. In one
        hook the loop below stops where that loop stops.

        ``writes`` is read when the hook runs, not when it is registered, which is what lets
        a fan-out hand this its list before filling it. See :meth:`send_many` for why it has
        to.

        The synchronous ``publish`` is what a hook runs: a commit hook is ordinary
        synchronous code on the thread that committed, with no loop to hand an ``await`` to.
        In practice the awaiting producers never reach one — a coroutine holds its own
        database connection, so there is no transaction of the caller's for them to see.

        A publish that fails here fails **after** the commit, so there is nothing left to
        roll back — the row the message was about is already there. ``publishing`` has
        recorded the drop by then, and ``RAISE_EXCEPTION`` decides whether the exception
        also leaves the ``atomic()`` block, where it would take every commit hook queued
        behind this one with it.
        """

        def publish_after_commit() -> None:
            """Write to the broker now that the caller's transaction has landed."""
            try:
                for write in writes:
                    with publishing(function, write) as ready:
                        publish(ready.payloads)
            except Exception:
                if self._raises_send_failures:
                    raise
                logger.exception(
                    'a deferred publish failed after its transaction committed',
                    extra={
                        'tg_function': function,
                        'tg_messages': sum(len(write.messages) for write in writes),
                    },
                )

        return defer(publish_after_commit)

    def enqueue(
        self,
        function: str = 'send_message',
        *,
        correlation_id: uuid.UUID | str | None = None,
        eta: datetime.datetime | None = None,
        **kwargs: Any,
    ) -> uuid.UUID:
        """Queue a message for the bot worker to deliver, whichever transport carries it.

        Named for what happens rather than for where it lands, because where it lands is a
        setting: the queue is a list, a stream, an AMQP queue or a Kafka topic depending on
        ``BROKER``. `send` decides between this and delivering directly; this one always
        queues — where the process is enabled. With ``ENABLED`` off it reaches neither the
        broker nor Telegram and returns the id anyway, so a caller storing it beside its own
        row gets the same value whether or not this deployment sends.

        With an ``eta`` it goes to the schedule rather than to the broker, and the id comes
        back the same way. Nothing is published until a mover finds the row due.

        Returns the correlation id the delivered row will carry too.
        """
        identifier, accepted = self._accept(function, correlation_id)
        if not accepted:
            return identifier
        if eta is not None:
            # no broker resolved and none needed: the row holds the bytes, and whichever
            # transport is configured when it comes due is the one that carries it
            due_at = due_moment(eta)
            schedule(function, serialise(function, [(identifier, kwargs)], due_at.timestamp()), due_at)
            return identifier

        # before the context manager, like the awaiting twin does: a `BROKER` that cannot be
        # resolved is a misconfiguration, and inside `queueing` it would be recorded as a
        # *queueing* drop — which the event log defines as a write that may still have been
        # applied, so re-sending may duplicate. Nothing was written, and the two producers
        # promise the same rows
        broker = get_broker()
        # after the broker resolves, not before: the mention fires once per process, so a call
        # that is about to raise on a misconfigured `BROKER` would otherwise spend it and leave
        # the first caller who could have acted on the advice hearing nothing. Same reasoning as
        # `check_function` in `send`, one failure mode further along
        mention_asend('aenqueue')
        write = serialise(function, [(identifier, kwargs)])
        if not self._deferred(function, [write], broker.publish):
            with publishing(function, write) as ready:
                broker.publish(ready.payloads)
        return identifier

    async def aenqueue(
        self,
        function: str = 'send_message',
        *,
        correlation_id: uuid.UUID | str | None = None,
        eta: datetime.datetime | None = None,
        **kwargs: Any,
    ) -> uuid.UUID:
        """Queue a message without blocking the loop this coroutine runs on.

        The synchronous twin writes to a socket on the calling thread, which under
        ASGI is the thread serving requests — including, on the first call, a connect
        bounded by whatever timeout the transport named by ``BROKER`` declares. Everything
        else about the message is identical: same payload, same event rows, the same
        destination — a list, a stream, an AMQP queue or a Kafka topic — and the same
        no-op when ``ENABLED`` is off, which returns the id without reaching the broker.
        """
        identifier, accepted = self._accept(function, correlation_id)
        if not accepted:
            return identifier
        if eta is not None:
            # `abulk_create`, because the synchronous one raises `SynchronousOnlyOperation`
            # from a coroutine -- measured. An earlier comment here claimed this path merely
            # blocked like its twin, and an `eta` on this method simply raised
            due_at = due_moment(eta)
            await aschedule(function, serialise(function, [(identifier, kwargs)], due_at.timestamp()), due_at)
            return identifier

        broker = get_broker()
        write = serialise(function, [(identifier, kwargs)])
        if not self._deferred(function, [write], broker.publish):
            with publishing(function, write) as ready:
                await broker.apublish(ready.payloads)
        return identifier

    def _accept_bulk(self, function: str) -> bool:
        """Whether this process should write, decided once for the whole batch.

        A disabled bot reaches neither Telegram nor the broker, and still returns an id
        per message — the same contract :meth:`enqueue` has, so a caller can
        store the ids beside its own rows whether or not this deployment sends.

        Validates first, for the same reason :meth:`_accept` does: `_chunks` is a
        generator, so the check inside it used to run on the first chat rather than
        on the call — after the batch had spent the once-per-process mention, and
        after `asend_many` had built a client a refused method never needed.
        """
        check_function(function)
        if self.enabled:
            return True
        logger.debug('queueing skipped: bot disabled', extra={'tg_function': function})
        return False

    def send_many(
        self,
        chat_ids: 'Iterable[int | str]',
        function: str = 'send_message',
        *,
        chunk_size: int = 100,
        eta: datetime.datetime | None = None,
        **kwargs: Any,
    ) -> list[uuid.UUID]:
        """Queue one message per chat, a chunk of them per round trip.

        Returns an id per message, in the order the chats were given — not a
        single receipt for the batch. A batch id would trade the indexed
        ``correlation_id__in`` lookup the event log is built for against a scan of
        the JSON column, and ``unpack`` drops keys it does not know, so the
        consumer's own rows could never carry it anyway.

        This speeds up **queueing**, not delivery: the rate limits still pace what
        leaves for Telegram, so fifty thousand chats is still about half an hour at
        the default thirty a second. It also removes the pacing that sequential
        round trips gave the event log — see **Event log** before broadcasting.

        A chunk that fails records a drop for its own messages and raises; earlier
        chunks are already queued, and their ids are lost with the exception, which
        is why the drops are recorded rather than left to the caller to infer.
        """
        writing = self._accept_bulk(function)
        due_at = None if eta is None else due_moment(eta)
        # resolved before the first chunk, as above: a broker that cannot be resolved is not
        # a chunk that failed to write. Not resolved at all for a scheduled batch, which
        # reaches no transport today and takes whichever one is configured when it comes due
        broker = get_broker() if writing and due_at is None else None
        if writing:
            mention_asend('asend_many')
        # the hook is registered before the first chunk and handed the list it will read at
        # commit time, still empty. Two things follow, and the second is why it is not done
        # after the loop: the decision is taken once for the batch, as it must be; and a
        # `serialise` that raises part way through leaves what it managed on a hook that
        # already exists, so a caller catching that inside its own block and committing
        # anyway sends the chunks the immediate path had already published rather than
        # silently none of them
        held: list[Queueing] = []
        waiting = broker is not None and self._deferred(function, held, broker.publish)
        identifiers: list[uuid.UUID] = []
        for chunk in chunks(chat_ids, chunk_size, kwargs):
            if writing and due_at is not None:
                # a scheduled fan-out needs no broker and no commit hook: the rows are the
                # caller's own write, so `atomic()` rolls them back on its own
                schedule(function, serialise(function, chunk, due_at.timestamp()), due_at)
            elif broker is not None:
                write = serialise(function, chunk)
                if waiting:
                    held.append(write)
                else:
                    with publishing(function, write) as ready:
                        broker.publish(ready.payloads)
            identifiers.extend(identifier for identifier, _ in chunk)
        return identifiers

    async def asend_many(
        self,
        chat_ids: 'Iterable[int | str]',
        function: str = 'send_message',
        *,
        chunk_size: int = 100,
        eta: datetime.datetime | None = None,
        **kwargs: Any,
    ) -> list[uuid.UUID]:
        """Queue one message per chat without blocking the loop.

        Everything :meth:`send_many` says applies, and the reason to reach for this
        one is stronger than for :meth:`asend`: a fan-out writes once per chunk and
        serializes every payload, so on a serving loop it blocks longer and more
        often than a single send does.
        """
        writing = self._accept_bulk(function)
        due_at = None if eta is None else due_moment(eta)
        # after the decision, not before: a disabled process may have no transport
        # configured at all, and resolving one would raise where the point is to do nothing.
        # A scheduled batch is the same case for a different reason -- see the twin above
        broker = get_broker() if writing and due_at is None else None
        # before the loop and handed an empty list, exactly as the synchronous twin does
        held: list[Queueing] = []
        waiting = broker is not None and self._deferred(function, held, broker.publish)
        identifiers: list[uuid.UUID] = []
        for chunk in chunks(chat_ids, chunk_size, kwargs):
            if writing and due_at is not None:
                await aschedule(function, serialise(function, chunk, due_at.timestamp()), due_at)
            elif broker is not None:
                write = serialise(function, chunk)
                if waiting:
                    held.append(write)
                else:
                    with publishing(function, write) as ready:
                        await broker.apublish(ready.payloads)
            identifiers.extend(identifier for identifier, _ in chunk)
        return identifiers

    async def aclose(self) -> None:
        """Release the async Redis client this loop was using.

        Not the mirror of :meth:`close`, and deliberately not named to suggest it:
        that one tears down the bot — loop, session, FSM storage — while this one
        closes the single thing an ASGI process opens lazily and Django gives no
        hook to close.

        This is the only path that closes the connection on the loop it belongs to,
        which is the only loop allowed to close it. Skipping it does not accumulate
        clients — the registry drops the ones whose loop has closed — but the
        connection stays open until its client is collected, and Python may say so
        with a ``ResourceWarning``. Call it from a lifespan shutdown if your server
        has one, and from anything that runs a loop per unit of work.
        """
        await aclose_redis()

    @staticmethod
    def cancel_scheduled(correlation_id: uuid.UUID | str) -> int:
        """Call off the sends this id still has waiting, and say how many there were.

        Unclaimed rows only: a mover that already owns one is on its way to the broker, and
        deleting the row here would neither stop the message nor be visible to it. So zero
        is the answer for an id that was never scheduled *and* for one whose message is
        already going out -- see :func:`~django_aiogram.producer.scheduling.cancel`.
        """
        return cancel(resolve_correlation_id(correlation_id))

    @staticmethod
    def outcome(correlation_id: uuid.UUID | str) -> 'Outcome':
        """Report what became of the message this id names, read back from the event log.

        The id :meth:`send` returned, and the answer carries the ``message_id`` Telegram
        gave — which is what an edit or a delete needs and what a process that only queued
        the send never had. See :mod:`django_aiogram.eventlog.outcomes` for what the four
        states mean and for why an absent row is not a failed send.

        A ``staticmethod`` because it reads a table and nothing this instance holds. It is a
        method anyway, and not only a function in that module, because ``bot.send()`` is
        where the id came from and this is the same object it should be handed back to.
        """
        from django_aiogram.eventlog.outcomes import outcome  # noqa: PLC0415 - django.db, not at import

        return outcome(correlation_id)

    @staticmethod
    async def aoutcome(correlation_id: uuid.UUID | str) -> 'Outcome':
        """:meth:`outcome` without blocking the loop this coroutine runs on."""
        from django_aiogram.eventlog.outcomes import aoutcome  # noqa: PLC0415 - as above

        return await aoutcome(correlation_id)

    def queue_depth(self) -> int:
        """How many messages are waiting for a worker to take them.

        One read, asked of whichever transport ``BROKER`` names — the length of a list
        or a stream, a queue's message count, a topic's lag. Named for what it measures
        rather than for any one of those commands, which is what let the name survive
        4.0 unchanged while everything under it became four implementations.

        Answers regardless of `ENABLED`: that setting gates sending, and a web tier kept
        from sending is exactly where someone asks how deep the queue is.

        Growing is not by itself a fault — producers can outpace delivery, and
        ``MAX_IN_FLIGHT`` holds intake back on purpose. See **Troubleshooting**.
        """
        return get_broker().depth()

    async def aqueue_depth(self) -> int:
        """:meth:`queue_depth` without blocking the loop this coroutine runs on."""
        return await get_broker().adepth()

    def inflight_depth(self, worker: str | None = None) -> int:
        """How many messages one worker is part-way through sending.

        Defaults to this process's own worker identity. Naming another is how a
        monitor reads a list left behind by a worker that is gone — the scheme
        those keys follow is this package's business, not an exporter's to
        reproduce.
        """
        return get_broker().inflight_depth(worker)

    async def ainflight_depth(self, worker: str | None = None) -> int:
        """:meth:`inflight_depth` without blocking the loop this coroutine runs on."""
        return await get_broker().ainflight_depth(worker)

    def __repr__(self) -> str:
        """Say whether the aiogram bot behind this facade has been built yet."""
        return f'<TelegramBot bot={"built" if self._bot else "lazy"}>'
