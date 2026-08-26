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
from typing import Any

from django.core.management import BaseCommand, CommandError

from django_aiogram import bot
from django_aiogram.broker.registry import get_broker
from django_aiogram.config.enums import UpdateMode
from django_aiogram.config.settings import SETTINGS_NAME, coerce_bool, conf
from django_aiogram.consumer.delivery import Delivery, get_delivery
from django_aiogram.consumer.webhook import MODES, current_mode
from django_aiogram.eventlog.events import worker_identity
from django_aiogram.eventlog.recorder import recorder

logger = logging.getLogger('django_aiogram')

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
                "how updates reach the bot for this run. Defaults to TELEGRAM_BOT['MODE'] "
                "(env: DJANGO_AIOGRAM_MODE), itself 'polling'. In webhook mode this "
                'process consumes the queue and never calls getUpdates, because the updates '
                'arrive over HTTP instead.'
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
                    "(TELEGRAM_BOT['ENABLED'] or DJANGO_AIOGRAM_ENABLED); "
                    'not starting the bot.'
                )
            )
            if options['idle']:
                self._idle_until_signalled()
            return

        configured = current_mode()
        mode = options['mode'] or configured
        self.stdout.write(f'Updates arrive by {mode}.')
        if mode != configured:
            # the webhook view reads the setting, not this flag, so it would
            # refuse the updates this process is no longer polling for
            self.stdout.write(
                self.style.WARNING(
                    f"--mode {mode} disagrees with TELEGRAM_BOT['MODE'] ({configured}), and it "
                    'changes this process only: '
                    + (
                        'the webhook view still refuses updates while the setting says polling'
                        if mode == UpdateMode.WEBHOOK
                        else 'getUpdates fails while a webhook is registered'
                    )
                )
            )

        delivery = get_delivery(handler=bot.send_raw)
        self._preflight(delivery)
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
        # asked of the registry rather than through `delivery.broker`: `DELIVERY` is a
        # documented extension point and `Testing.md` shows people writing their own, so
        # reaching into one for an attribute would make `.broker` a contract nobody declared.
        # The broker is process-global anyway -- the same instance the delivery holds
        join_timeout = get_broker().call_ceiling + 1
        threads: list[threading.Thread] = []

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
            threads.append(delivery.start_thread())

        bot.loop.call_soon(start_consuming)
        previous = self._install_sigterm_handler()

        try:
            with contextlib.suppress(KeyboardInterrupt, SystemExit):
                if mode == UpdateMode.WEBHOOK:
                    self.stdout.write('Consuming the queue; updates are expected over HTTP.')
                    self._idle_on_the_loop()
                else:
                    bot.start_polling()
        finally:
            logger.info('shutting down')
            # before stop(), so the callback above cannot slip a consumer in
            # behind the joins below
            shutting_down.set()
            delivery.stop()
            for thread in threads:
                # derived from the bound that actually governs the thread: every
                # call it makes is capped by REDIS_TIMEOUT, and its blocking pop
                # by one less than that. BLPOP_TIMEOUT + 1 was six seconds against
                # a worst case of ten, so a consumer that outlived the join went on
                # to acknowledge a message close() had already refused
                thread.join(timeout=join_timeout)
                if thread.is_alive():
                    logger.warning(
                        'the delivery consumer did not stop in time',
                        extra={'tg_timeout': join_timeout},
                    )
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
                    delivery.collect()
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
        from django_aiogram.config.checks import worker_name_problems  # noqa: PLC0415 - no aiogram at import

        for problem in worker_name_problems():
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
