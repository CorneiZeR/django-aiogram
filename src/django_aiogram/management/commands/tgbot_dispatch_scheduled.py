"""Publish the sends whose time has come.

Nothing on the write path publishes a scheduled send, so a row sits in the table until this
runs. Schedule it, or run it with ``--loop`` in a container of its own.

**A command and not a thread inside ``start_tgbot``.** The bot container is where messages
are delivered, and hanging the clock off it would make an outage there stop time as well:
scheduled sends would neither go out nor be visible as waiting. As a command it can run
beside the bot, on a different schedule, or several times over -- the claim below is what
makes the last of those safe.

**It needs no token and builds no bot.** A due row already holds the bytes the queue wants,
so this publishes them and deletes the row -- there is no ``Bot``, no dispatcher and nothing
that reads ``TOKEN``, which is what lets the mover run in a container configured for the
database and the broker alone. ``ENABLED`` is read through ``conf`` for the same reason:
``bot.enabled`` is the same answer, and reaching it would build the client.

It does still *import* aiogram, and not through anything here: ``producer.queueing`` reaches
``wire.serializers`` at module scope, and the serializer registry imports aiogram to know how
to encode its models. Measured -- importing ``producer.queueing`` alone pulls it in. Worth
knowing before sizing the container; not worth restructuring the send path over.
"""

import contextlib
import logging
import signal
import time
from argparse import ArgumentParser
from types import FrameType
from typing import TYPE_CHECKING, Any

from django.core.management import BaseCommand, CommandError
from django.utils import timezone

from django_aiogram.broker.registry import get_broker
from django_aiogram.config.enums import EventKind
from django_aiogram.config.settings import SETTINGS_NAME, coerce_bool, conf
from django_aiogram.eventlog.recorder import recorder
from django_aiogram.eventlog.records import Event
from django_aiogram.producer.queueing import Queueing, publishing
from django_aiogram.producer.scheduling import DEFAULT_LEASE, claim

if TYPE_CHECKING:
    from collections.abc import Callable

    from django_aiogram.broker.base import Broker
    from django_aiogram.models import TelegramScheduledSend

logger = logging.getLogger('django_aiogram')


class Command(BaseCommand):
    """Move due rows onto the broker, in bounded batches."""

    help = 'Publish scheduled Telegram sends that have come due'

    def add_arguments(self, parser: ArgumentParser) -> None:
        """Declare the batch bound, the loop and the two ways to refuse a late row."""
        parser.add_argument(
            '--limit',
            type=int,
            default=500,
            help='how many rows one pass may publish. A bound and not a target: a pass that '
            'fills it leaves the rest for the next one (default 500)',
        )
        parser.add_argument(
            '--interval',
            type=float,
            default=5.0,
            help='seconds between passes under --loop (default 5). Without --loop it is ignored',
        )
        parser.add_argument(
            '--loop',
            action='store_true',
            help='keep running instead of exiting after one pass. Use this in a container of '
            'its own; use the plain form from cron',
        )
        parser.add_argument(
            '--lease',
            type=int,
            default=DEFAULT_LEASE,
            help='seconds a claim is believed before another mover may take the row back. A '
            f'mover killed between publishing and deleting strands its rows until this expires '
            f'(default {DEFAULT_LEASE}); 0 trusts a claim for ever, which means a crash needs '
            'an operator',
        )
        parser.add_argument(
            '--grace',
            type=int,
            default=0,
            help='refuse a row more than this many seconds overdue, recording a drop instead '
            'of sending it late. 0, the default, sends everything however old it is',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='say what is due and publish nothing. Claims nothing either, so a real '
            'mover running beside this is unaffected',
        )

    def handle(self, *args: Any, **options: Any) -> None:
        """Run one pass, or keep running until a signal arrives."""
        if not coerce_bool(conf['ENABLED'], f"{SETTINGS_NAME}['ENABLED']"):
            # the same refusal every producer makes, said where an operator will read it:
            # a disabled process reaches neither the broker nor Telegram, so a mover here
            # would claim rows and publish nothing
            msg = (
                "django-aiogram is disabled (TELEGRAM_BOT['ENABLED'] or DJANGO_AIOGRAM_ENABLED), "
                'so a scheduled send has nowhere to go. Nothing was claimed.'
            )
            raise CommandError(msg)
        if options['dry_run']:
            self._report_due(options['limit'])
            return

        limit = max(1, int(options['limit']))
        grace = max(0, int(options['grace']))
        lease = max(0, int(options['lease']))
        previous = self._install_sigterm_handler()
        try:
            self._run(
                limit=limit,
                grace=grace,
                lease=lease,
                loop=options['loop'],
                interval=max(0.0, options['interval']),
            )
        finally:
            recorder.stop()
            if previous is not None:
                # the command may be called in-process; leaving our handler installed
                # would turn a later SIGTERM into a stray interrupt
                with contextlib.suppress(ValueError):
                    signal.signal(signal.SIGTERM, previous)

    def _run(self, *, limit: int, grace: int, lease: int, loop: bool, interval: float) -> None:
        """Pass after pass, or one, unwinding on the signal either way."""
        with contextlib.suppress(KeyboardInterrupt):
            while True:
                published = self._one_pass(limit=limit, grace=grace, lease=lease)
                if not loop:
                    return
                # a pass that filled its bound has more waiting, so it goes straight round
                # again rather than sleeping on a backlog it already knows about
                if published < limit:
                    time.sleep(interval)

    def _one_pass(self, *, limit: int, grace: int, lease: int) -> int:
        """Claim what is due, publish it, and delete what went out. Returns the count."""
        # before the claim, and this is the same rule `enqueue` follows: a `BROKER` that
        # cannot be resolved is a misconfiguration, not a message that failed to publish.
        # Claimed first, its rows would keep the claim with no drop row and no later pass
        # willing to look at them -- invisible until an operator cleared them by hand
        broker = get_broker()
        rows = claim(limit, lease=lease)
        if not rows:
            return 0
        published = 0
        for row in rows:
            if grace and (timezone.now() - row.due_at).total_seconds() > grace:
                self._drop_late(row, grace)
                continue
            if self._publish(broker, row):
                published += 1
        logger.info('published scheduled sends', extra={'tg_published': published, 'tg_claimed': len(rows)})
        self.stdout.write(f'Published {published} of {len(rows)} claimed.')
        return published

    def _publish(self, broker: 'Broker', row: 'TelegramScheduledSend') -> bool:
        """Put one row on the queue and delete it, or leave it claimed and say why.

        The row is deleted **after** the publish, which is what makes this at-least-once
        like everything else here: a mover killed in between leaves a claimed row whose
        message is already on the queue, and an operator clearing that claim gets the
        message twice. The other order would lose it silently, and this package has
        nowhere it prefers loss to duplication.
        """
        # every field `publishing` reads, and none of them defaulted. `details` is one
        # entry per message or the zip inside it refuses the pair, and `queued_at` is the
        # row's *due* time -- the message was waiting for a calendar, and its time in the
        # queue starts when it came due rather than when it was scheduled
        write = Queueing(
            payloads=[bytes(row.payload)],
            messages=[(row.correlation_id, {'chat_id': row.chat_id})],
            queued_at=row.due_at.timestamp(),
            details=[None],
        )
        try:
            with publishing(row.function, write):
                broker.publish(write.payloads)
        except Exception:
            # `publishing` has recorded the drop; the claim stays, so this row is not
            # retried by the next pass. An operator reading the drop decides
            logger.exception('a scheduled send could not be published', extra={'tg_function': row.function})
            return False
        row.delete()
        return True

    @staticmethod
    def _drop_late(row: 'TelegramScheduledSend', grace: int) -> None:
        """Record a row too old to send, and delete it.

        `--grace` exists because a mover that was down for a day would otherwise deliver a
        day of messages at once, all of them about a moment that has passed. Recorded rather
        than silently dropped: the row is gone, and the feed says which and why.
        """
        overdue = int((timezone.now() - row.due_at).total_seconds())
        recorder.record(
            Event(
                kind=EventKind.OUTBOUND_DROPPED.value,
                correlation_id=row.correlation_id,
                function=row.function,
                chat_id=row.chat_id,
                error_code='TooLate',
                error=f'{overdue}s overdue, past the {grace}s grace',
                detail={'stage': 'scheduling', 'overdue_s': overdue, 'grace_s': grace},
            )
        )
        logger.warning('dropped a scheduled send past its grace', extra={'tg_overdue': overdue, 'tg_grace': grace})
        row.delete()

    def _report_due(self, limit: int) -> None:
        """Say what a real pass would take, claiming nothing."""
        from django_aiogram.models import TelegramScheduledSend  # noqa: PLC0415 - django.db, not at import

        now = timezone.now()
        due = TelegramScheduledSend.objects.filter(claimed_at__isnull=True, due_at__lte=now)
        waiting = TelegramScheduledSend.objects.filter(claimed_at__isnull=True, due_at__gt=now).count()
        claimed = TelegramScheduledSend.objects.filter(claimed_at__isnull=False).count()
        self.stdout.write(f'{due.count()} due now, of which a pass would take {limit}.')
        self.stdout.write(f'{waiting} not due yet, {claimed} claimed by a mover.')
        for row in due.order_by('due_at', 'id')[:limit]:
            self.stdout.write(f'  {row.due_at:%Y-%m-%d %H:%M:%S}  {row.function}  chat={row.chat_id}')

    @staticmethod
    def _install_sigterm_handler() -> 'Callable[[int, FrameType | None], object] | int | None':
        """Turn SIGTERM into KeyboardInterrupt so `docker stop` unwinds a --loop cleanly."""

        def raise_interrupt(_signum: int, _frame: FrameType | None) -> None:
            """Raise where the signal arrived, which is inside the sleep or the pass."""
            raise KeyboardInterrupt

        try:
            return signal.signal(signal.SIGTERM, raise_interrupt)
        except ValueError:
            return None
