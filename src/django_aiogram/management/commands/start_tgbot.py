"""Run the bot: receive updates, and consume the queue Django writes to.

This is the long-running process a bot container is built around. It owns two
things at once — whatever brings updates in, and the consumer that drains the
queue ``BROKER`` names — and has to shut both down cleanly when the container stops.
"""

import contextlib
import logging
import signal
import threading
from argparse import ArgumentParser
from collections.abc import Callable
from types import FrameType
from typing import TYPE_CHECKING, Any

from django.core.exceptions import ImproperlyConfigured
from django.core.management import BaseCommand, CommandError

from django_aiogram import bot
from django_aiogram.broker.registry import get_broker
from django_aiogram.config.defaults import DEFAULTS
from django_aiogram.config.enums import UpdateMode
from django_aiogram.config.settings import SETTINGS_NAME, coerce_bool, conf
from django_aiogram.consumer.delivery import Delivery, get_delivery
from django_aiogram.consumer.serving import Consumers, settle
from django_aiogram.consumer.webhook import MODES, current_mode
from django_aiogram.eventlog.events import worker_identity
from django_aiogram.eventlog.recorder import recorder
from django_aiogram.runtime.queues import named, served_by, settings_for

if TYPE_CHECKING:
    from django_aiogram.broker.base import Broker

logger = logging.getLogger('django_aiogram')


def _routing() -> 'Callable[[int], Callable[..., Any]] | None':
    """Return the route to deliver by, or ``None`` where there is nothing to route.

    A process with one bot has every message addressed to it or to nobody, and a project's own
    `Delivery` written before 5.0 takes no route at all -- so asking for one where it cannot
    matter would refuse a consumer that works. `get_delivery` refuses such a class only where
    the addressing does matter, which is what this decides.
    """
    from django_aiogram.config.bots import records  # noqa: PLC0415 - after the enabled gate, like the rest

    return _send_raw_of if len(records()) > 1 else None


def _split(written: str) -> list[str]:
    """Read a comma-separated option the way Celery's `-Q` reads one."""
    return [part.strip() for part in written.split(',') if part.strip()]


def _built_for(serving: 'tuple[str, ...]', build: 'Callable[[str], Delivery]') -> dict[str, 'Delivery']:
    """Build the startup set, settling what was already built if one of them refuses.

    The whole set is built before anything is started, so a refusal -- `REQUIRE_CRASH_SAFE` on
    a transport that cannot promise crash safety -- reaches the operator as a command that
    would not start rather than a warning per queue from a thread.

    One at a time, and settled on the way out, because a consumer that was built has already
    reclaimed: a refusal on the third queue would otherwise strand what the first two took,
    and nothing in this process could acknowledge those messages again.
    """
    ready: dict[str, Delivery] = {}
    try:
        for queue in serving:
            ready[queue] = build(queue)
    except BaseException:
        for queue, built in ready.items():
            settle(built, queue)
        raise
    return ready


def _only_its_own_queue(serving: tuple[str, ...], pools: str) -> bool:
    """Whether this container serves exactly the queue its settings name, and only ever will.

    A container that does is every deployment before there were several queues, so it is
    handed no settings at all and a ``DELIVERY`` written then keeps working -- including where
    the operator named that same queue on the command line, which changes nothing about what
    is served.

    A pool is the exception, and not a cautious one: a pool holding one queue today holds two
    after the next pass, and a consumer built without settings could not be told which of them
    it is for.
    """
    return serving == (named(),) and not _split(pools)


def _consumer_for(queue: str, *, one_queue: bool) -> Delivery:
    """Build the consumer for one queue, telling it which queue only when that is news.

    A container serving exactly the queue its settings already name is every deployment
    before this, and it is handed no settings at all -- so a `DELIVERY` a project wrote
    before there were several queues keeps working, and is refused only where it is asked
    to serve a queue that is not its own.
    """
    if one_queue:
        return get_delivery(handler=bot.send_raw, route=_routing())
    return get_delivery(handler=bot.send_raw, route=_routing(), settings=settings_for(queue))


def _transport_for(queue: str) -> 'Broker':
    """Return the transport one queue is consumed through, from the registry."""
    if queue == named():
        return get_broker()
    return get_broker(settings_for(queue))


def _send_raw_of(bot_id: int) -> 'Callable[..., Any]':
    """Return the send of the bot a queued message names, or raise if this process has none.

    Raising is the answer rather than falling back to the default bot: a message queued for
    one bot and delivered by another would go out under the wrong token, to a chat that bot
    may not even be in. The consumer leaves such a message in flight, where a process
    configured for it can still take it.
    """
    from django_aiogram.runtime.registry import bots  # noqa: PLC0415 - after the enabled gate, like the rest

    return bots.by_id(bot_id).send_raw


#: what signal.signal returns: a handler, one of the SIG_* constants, or None
Handler = Callable[[int, FrameType | None], Any] | int | None


class Command(BaseCommand):
    """Start the bot and the queue consumer, and stop them together."""

    help = 'Start telegram bot'

    #: what --idle waits on; tests replace it so they can end the wait
    idle_event: threading.Event | None = None

    def add_arguments(self, parser: ArgumentParser) -> None:
        """Declare --mode and --idle."""
        parser.add_argument(
            '--mode',
            choices=sorted(MODES),
            default=None,
            help=(
                "how updates reach the bot for this run. Defaults to TELEGRAM_BOT_DEFAULTS['MODE'] "
                "(env: DJANGO_AIOGRAM_MODE), itself 'polling'. In webhook mode this "
                'process consumes the queue and never calls getUpdates, because the updates '
                'arrive over HTTP instead.'
            ),
        )
        parser.add_argument(
            '--queues',
            default='',
            help=(
                'comma-separated queues this container consumes, as `-Q` does in Celery. '
                "Defaults to the one TELEGRAM_BOT_DEFAULTS['QUEUE'] names, which is the "
                "transport's own where that is empty. A name that is not declared in "
                "TELEGRAM_BOT_DEFAULTS['QUEUES'] or in the TelegramQueue table is refused."
            ),
        )
        parser.add_argument(
            '--pools',
            default='',
            help=(
                'comma-separated pools whose queues this container consumes. A pool is the '
                'label on a TelegramQueue row, so a queue created after this container '
                'started is served without a redeploy -- which is what enumeration cannot do '
                'when clients arrive at run time. Combined with --queues as a union.'
            ),
        )
        parser.add_argument(
            '--no-updates',
            action='store_true',
            help=(
                'consume the queues and never ask Telegram for updates. The shape a webhook '
                'deployment wants, where updates arrive in the web tier and this process '
                'exists to send -- and the shape a sender pool wants at any scale.'
            ),
        )
        parser.add_argument(
            '--updates-only',
            action='store_true',
            help=(
                'receive updates and consume nothing. Pair it with '
                '`tgbot_healthcheck --no-consumer`, or the probe will call a container with '
                'no consumer in it unhealthy for not having one.'
            ),
        )
        parser.add_argument(
            '--idle',
            action='store_true',
            help=(
                'When the bot is disabled, block instead of exiting. Useful under '
                'restart policies that treat a clean exit as a crash loop.'
            ),
        )

    def handle(self, *args: Any, **options: Any) -> None:
        """Receive updates, drain the queue, and unwind both on a signal."""
        if not bot.enabled:
            self.stdout.write(
                self.style.WARNING(
                    'django-aiogram is disabled '
                    "(TELEGRAM_BOT_DEFAULTS['ENABLED'] or DJANGO_AIOGRAM_ENABLED); "
                    'not starting the bot.'
                )
            )
            if options['idle']:
                self._idle_until_signalled()
            return

        mode = self._mode_for_this_run(options)
        receives, consumes = self._roles(options, mode)

        serving = self._queues_to_serve(options) if consumes else ()
        one_queue = _only_its_own_queue(serving, options['pools'])

        def consumer_for(queue: str) -> Delivery:
            """Build the consumer for one queue and prove what it promises before it runs.

            A refusal from the preflight settles the consumer it was proving: `reclaim` has
            already run by then, so a consumer dropped here takes what it reclaimed with it --
            and nothing else in this process can acknowledge those messages.
            """
            built = _consumer_for(queue, one_queue=one_queue)
            try:
                self._preflight(built)
            except BaseException:
                settle(built, queue)
                raise
            return built

        ready = _built_for(serving, consumer_for)

        # the transport's own deadline, not `REDIS_TIMEOUT`: this bounds the thread being
        # joined below, and reading it from one transport's setting meant a consumer could be
        # inside a call the join had already given up on. Measured: at `KAFKA_TIMEOUT = 45`
        # the old arithmetic gave the join 11 seconds for a call that may take 45, and a
        # worker outliving its join acknowledges a message `close()` has already refused --
        # 3.1.0's B3 arriving through a different door.
        #
        # Still before a thread exists, for the reason it always was: reading a broken setting
        # raises, and raised from the `finally` below it would skip `close()`, `collect()` and
        # `recorder.stop()`, stranding the drain's own messages
        #
        # Asked of the registry rather than through `delivery.broker`. `DELIVERY` names a class
        # a project may write, so a `Delivery` here is not necessarily one of ours -- and the
        # attribute is set in `Delivery.__init__`, which a double standing in for a consumer need
        # not have run. The registry is asked once per queue with that queue's settings, which
        # is the same instance each delivery holds when it has one
        # 1 where nothing is consumed: there is no thread to join, and `max` of nothing raises
        join_timeout = max((_transport_for(name).call_ceiling for name in serving), default=0.0) + 1
        consumers = Consumers(build=consumer_for, join_timeout=join_timeout, ready=ready)

        # Both modes: starting the consumer before the loop runs would let a
        # backlog reach send_raw while loop.is_running() is still False, so the
        # coroutine would be driven from the consumer thread. Deferring the start
        # until the loop picks up this callback keeps the loop single-threaded.
        # Webhook mode used to start it directly because nothing ran the loop
        # there — something does now, which is what this change is about.
        #
        # Started through a callback on the loop, so it cannot begin before the loop is
        # turning, and refused once the shutdown starts. close() runs one turn of the loop
        # on purpose, so a callback still queued when we reach the finally would
        # start the consumer *after* stop() and after the joins — a thread nobody
        # waits for, doing Redis work, whose first act is reclaim()
        shutting_down = threading.Event()

        def start_consuming() -> None:
            """Start the consumer thread on the loop, unless the shutdown got there first.

            Queued with ``call_soon`` so the thread begins once the loop is turning, and
            gated because the callback can still be pending when the teardown runs — see
            the comment above for what an ungated one starts, and how late.
            """
            if shutting_down.is_set():
                logger.info('not starting the consumer: the shutdown had already begun')
                return
            consumers.reconcile(serving)
            if consumes:
                watch.start()

        # the queues are re-read while the container runs, and that is what selecting by pool
        # is *for*: a queue created for a client an hour after the deploy is served without one.
        # A pass that could not read the table leaves the set alone, like the bot providers'
        watch = threading.Thread(
            target=self._watch_the_queues,
            args=(consumers, options, shutting_down),
            name='django-aiogram-queues',
            daemon=True,
        )

        bot.loop.call_soon(start_consuming)
        previous = self._install_sigterm_handler()

        try:
            with contextlib.suppress(KeyboardInterrupt, SystemExit):
                if not receives:
                    # the consumers are on their own threads, so this process has to keep a
                    # loop turning for the sends they hand over -- exactly what webhook mode
                    # has always done, for exactly that reason
                    self.stdout.write('Consuming the queues; this process asks for no updates.')
                    self._idle_on_the_loop()
                elif mode == UpdateMode.WEBHOOK:
                    self.stdout.write('Consuming the queue; updates are expected over HTTP.')
                    self._idle_on_the_loop()
                else:
                    bot.start_polling()
        finally:
            self._unwind(consumers, shutting_down, previous)

    def _watch_the_queues(
        self,
        consumers: Consumers,
        options: dict[str, Any],
        shutting_down: threading.Event,
    ) -> None:
        """Re-read the queues this container serves until the shutdown begins.

        A daemon thread and a poll rather than a signal, for the reason the bot supervisor
        gives: the push arrives on one queue and the poll is what makes a change arrive at
        all. `BOT_REFRESH_INTERVAL` is the interval, because it is the same question about a
        different table.
        """
        asked = (_split(options['queues']), _split(options['pools']))
        if not any(asked):
            # nothing to re-read: the queue was the process's own and settings do not change
            # under a running container
            return
        while not shutting_down.wait(self._refresh_interval()):
            try:
                wanted = served_by(*asked)
            except Exception:
                # a table that could not be read has not said this container serves nothing
                logger.exception('could not re-read the queues to serve; keeping the ones running')
                continue
            consumers.reconcile(wanted)

    @staticmethod
    def _refresh_interval() -> float:
        """How long between re-reads, never below a second and never unreadable."""
        try:
            return max(1.0, float(conf['BOT_REFRESH_INTERVAL']))
        except (TypeError, ValueError, ImproperlyConfigured):
            # a container that refuses to re-read its queues because a number is unreadable
            # serves the startup set for ever, which is the trade `Supervisor.interval` makes
            logger.warning('BOT_REFRESH_INTERVAL is unreadable; falling back to the default')
            return float(DEFAULTS['BOT_REFRESH_INTERVAL'])

    def _unwind(
        self,
        consumers: Consumers,
        shutting_down: threading.Event,
        previous: 'Handler | None',
    ) -> None:
        """Stop the consumers, wait for their threads, close the bot, and settle the log.

        Its own method because the order in it is the whole of a clean shutdown, and every
        line carries the reason it is where it is. Extracted when a container gained several
        queues -- `handle` had grown past what one function should decide.
        """
        logger.info('shutting down')
        # before stop(), so the callback above cannot slip a consumer in
        # behind the joins below
        shutting_down.set()
        # stops each consumer and waits out its thread, on the bound that actually governs it
        # -- the transport's own call ceiling. `BLPOP_TIMEOUT + 1` was six seconds against a
        # worst case of ten, so a consumer that outlived the join went on to acknowledge a
        # message close() had already refused
        consumers.stop()
        try:
            bot.close()
        finally:
            # the sends close() just drained reported themselves finished into a
            # queue whose only reader is the consumer loop, and that returned before
            # the join above — so without this every message the drain delivered
            # stays in the in-flight list and the next start sends it again. A
            # graceful stop duplicated whatever the drain had time to finish, which
            # is the one thing `Delivery.md` says a *kill* is needed for
            try:
                consumers.collect()
            finally:
                # after close(), never before: closing drains in-flight sends,
                # and those are what produce the final rows. In its own finally
                # because a close() that raises must not also lose the rows
                recorder.stop()
        if previous is not None:
            # the command may be called in-process; leaving our handler
            # installed would turn a later SIGTERM into a stray interrupt
            with contextlib.suppress(ValueError):
                signal.signal(signal.SIGTERM, previous)

    def _roles(self, options: dict[str, Any], mode: str) -> tuple[bool, bool]:
        """Say which of the two jobs this run does, and refuse a run that does neither.

        `start_tgbot` has always done both, and at scale they do not scale together: a
        webhook deployment wants a process that only sends, a busy pool wants receivers
        without consumers, and a small installation wants what it has always had. Which is
        why both is the default and each flag takes one away.

        A container that does neither is refused rather than started: it would sit there
        looking alive, answering a probe, and doing nothing at all. That is two
        configurations, not one -- the flags together, and ``--updates-only`` in webhook
        mode, where this process receives nothing anyway because the updates arrive over HTTP
        in whatever serves them. Consuming is all a bot container *is* there, so taking it
        away leaves an idle loop.
        """
        receives = not options['no_updates']
        consumes = not options['updates_only']
        if not consumes and mode == UpdateMode.WEBHOOK:
            msg = (
                '--updates-only leaves nothing for this process to do in webhook mode: the '
                'updates arrive over HTTP in whatever serves the webhook, so consuming the '
                'queues is all this process was doing. Drop the flag, or run it where MODE '
                'is polling.'
            )
            raise CommandError(msg)
        if not receives and not consumes:
            msg = '--no-updates and --updates-only together leave nothing for this process to do.'
            raise CommandError(msg)
        if not consumes:
            self.stdout.write(
                self.style.WARNING(
                    'Consuming nothing: this process only receives updates. Give the probe '
                    '`--no-consumer`, or it will call this container unhealthy for having no '
                    'consumer in it.'
                )
            )
        return receives, consumes

    def _mode_for_this_run(self, options: dict[str, Any]) -> str:
        """Say how updates reach this process, and warn where the flag and the setting differ."""
        configured = current_mode()
        mode = options['mode'] or configured
        self.stdout.write(f'Updates arrive by {mode}.')
        if mode != configured:
            # the webhook view reads the setting, not this flag, so it would
            # refuse the updates this process is no longer polling for
            self.stdout.write(
                self.style.WARNING(
                    f"--mode {mode} disagrees with TELEGRAM_BOT_DEFAULTS['MODE'] ({configured}), and it "
                    'changes this process only: '
                    + (
                        'the webhook view still refuses updates while the setting says polling'
                        if mode == UpdateMode.WEBHOOK
                        else 'getUpdates fails while a webhook is registered'
                    )
                )
            )
        return str(mode)

    def _queues_to_serve(self, options: dict[str, Any]) -> tuple[str, ...]:
        """Say which queues this run consumes, and refuse a run that would consume none.

        Pools that exist and hold nothing are not the same as asking for nothing: a consumer
        with no queue at all idles while every probe reads it as healthy.
        """
        serving = served_by(_split(options['queues']), _split(options['pools']))
        if not serving:
            msg = 'No queues to consume: the pools asked for hold none.'
            raise CommandError(msg)
        self.stdout.write(f'Consuming {", ".join(name or "the transport default" for name in serving)}.')
        return serving

    def _idle_on_the_loop(self) -> None:
        """Wait on the bot's loop rather than on an Event.

        In webhook mode this process consumes the queue and nothing drove the
        loop, so every send the consumer scheduled sat there until something else
        happened to run it — the next update, or `close()`. `run_forever` is what
        makes a scheduled send run when it is scheduled, and it unwinds on
        SIGTERM exactly as `start_polling` does, so the teardown below is
        unchanged.
        """
        stop = self.idle_event or threading.Event()
        loop = bot.loop

        def wait_then_stop() -> None:
            """Wait for the idle event on this thread, then stop the loop from it.

            The main thread is inside ``run_forever`` and cannot wait for anything, so
            the wait lives here and reaches the loop through ``call_soon_threadsafe`` —
            the only safe way in from another thread. A loop already closed raises
            ``RuntimeError``, which is a race with the teardown and not a fault.
            """
            stop.wait()
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(loop.stop)

        threading.Thread(target=wait_then_stop, name='tgbot-idle', daemon=True).start()
        loop.run_forever()

    def _idle_until_signalled(self) -> None:
        """Hold a disabled container open, and unwind it the way the enabled path does.

        The same SIGTERM handler, so `docker stop` exits 0 rather than 143: without it the
        signal kills the process outright and a container idling on purpose looked like one
        that crashed. And `recorder.stop()`, because a disabled process with the log on
        still has a writer thread holding a database connection.
        """
        self.stdout.write('Idling. Send SIGINT or SIGTERM to stop.')
        previous = self._install_sigterm_handler()
        try:
            with contextlib.suppress(KeyboardInterrupt):
                (self.idle_event or threading.Event()).wait()
        finally:
            recorder.stop()
            if previous is not None:
                with contextlib.suppress(ValueError):
                    signal.signal(signal.SIGTERM, previous)

    def _preflight(self, delivery: Delivery) -> None:
        """Everything worth saying or refusing before a thread exists."""
        self._warn_about_an_unstable_worker_name()
        self._require_crash_safety(delivery)

    def _warn_about_an_unstable_worker_name(self) -> None:
        """Say it here, where being the consumer is known.

        The in-flight list is keyed on the worker's name, so a name that changes when the
        container is replaced strands whatever the old one was sending. As a system check
        this can only be information: `manage.py check` runs in every process, and a check
        cannot tell a consumer from a web tier — as a warning it failed
        `check --fail-level WARNING` in containers that own no in-flight list at all.

        This process is the consumer. The same rule, reused rather than restated, so the
        two cannot drift.
        """
        from django_aiogram.config.bots import defaults_record  # noqa: PLC0415 - no aiogram at import
        from django_aiogram.config.checks import worker_name_problems  # noqa: PLC0415 - as above

        for problem in worker_name_problems(defaults_record()):
            logger.warning(
                'the worker name will not survive a replacement container',
                extra={'tg_worker': worker_identity()},
            )
            self.stdout.write(self.style.WARNING(f'WORKER_NAME {problem.message}'))

    @staticmethod
    def _require_crash_safety(delivery: Delivery) -> None:
        """Refuse to start where a killed worker loses the message it was sending.

        Probed here rather than from inside ``run()``: that is a daemon thread, so
        a ``SystemExit`` raised there kills only the thread and leaves a process
        polling updates with a dead consumer. A ``CommandError`` gives a non-zero
        exit and a restart loop somebody can see.

        ``reclaim()`` is the probe, and it is the same call ``run()`` opens with.
        An unreachable Redis returns False with crash safety still intact, which
        must not be read as an old server — a blip is not a reason to refuse to
        start.
        """
        if not coerce_bool(conf['REQUIRE_CRASH_SAFE'], f"{SETTINGS_NAME}['REQUIRE_CRASH_SAFE']"):
            return
        settled = delivery.reclaim()
        if delivery.crash_safe:
            if not settled:
                # NOPERM and WRONGTYPE come back this way too, and unlike a blip
                # they do not clear. Refusing here would turn every restart into
                # a crash loop, so say plainly that the guarantee is unproven
                # rather than let silence read as a passed check
                logger.warning(
                    'could not verify crash-safe delivery: the probe did not settle',
                    extra={'tg_key': delivery.queue_key},
                )
            return
        msg = (
            'This Redis predates LMOVE (6.2), so a worker killed mid-send loses that message, '
            f"and {SETTINGS_NAME}['REQUIRE_CRASH_SAFE'] refuses to run that way. Upgrade the "
            'server, or set it to False to accept at-most-once delivery.'
        )
        raise CommandError(msg)

    @staticmethod
    def _install_sigterm_handler() -> Handler:
        """Turn SIGTERM into KeyboardInterrupt so `docker stop` unwinds cleanly.

        Returns the handler it replaced, or None when it could not install one —
        signal.signal only works on the main thread.
        """

        def raise_interrupt(_signum: int, _frame: FrameType | None) -> None:
            """Raise where the signal arrived, which is inside whatever was blocking.

            That is the whole trick: ``KeyboardInterrupt`` unwinds ``start_polling`` and
            ``run_forever`` through the same path a Ctrl-C takes, so one teardown covers
            both an operator and ``docker stop``.
            """
            raise KeyboardInterrupt

        try:
            return signal.signal(signal.SIGTERM, raise_interrupt)
        except ValueError:
            return None
