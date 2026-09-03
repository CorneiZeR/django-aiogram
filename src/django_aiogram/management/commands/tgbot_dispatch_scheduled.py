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
from dataclasses import dataclass
from types import FrameType
from typing import TYPE_CHECKING, Any

from django.core.management import BaseCommand, CommandError
from django.db import transaction
from django.db.models import F
from django.utils import timezone

from django_aiogram.broker.registry import get_broker
from django_aiogram.config.enums import EventKind
from django_aiogram.config.settings import SETTINGS_NAME, coerce_bool, conf
from django_aiogram.eventlog.recorder import recorder
from django_aiogram.eventlog.records import Event
from django_aiogram.producer.queueing import Queueing, publishing
from django_aiogram.producer.scheduling import DEFAULT_LEASE, claim, still_held_by, unheld

if TYPE_CHECKING:
    from collections.abc import Callable

    from django_aiogram.broker.base import Broker
    from django_aiogram.models import TelegramScheduledSend

logger = logging.getLogger('django_aiogram')


@dataclass(frozen=True)
class Bounds:
    """The four numbers one pass runs by, so the signatures below stop growing.

    Every one of them is a bound rather than a target, and each answers a different way for
    a pass to go wrong: too much work at once, a claim believed too long, a row too old to
    be worth sending, and a failure repeated too often.
    """

    limit: int
    lease: int
    grace: int
    attempts: int


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
            '--max-attempts',
            type=int,
            default=5,
            help='give up on a row after this many failed publishes, recording why and '
            'deleting it (default 5). A lease means a failure is retried every lease, so '
            'without a bound a payload the broker refuses for ever writes one more drop row '
            'per pass. 0 retries without end',
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
        bounds = Bounds(
            limit=max(1, int(options['limit'])),
            lease=max(0, int(options['lease'])),
            grace=max(0, int(options['grace'])),
            attempts=max(0, int(options['max_attempts'])),
        )
        if options['dry_run']:
            # the clamped limit, like the real pass: the raw one reached a queryset slice,
            # where Django refuses a negative index outright and 0 reported nothing at all
            self._report_due(bounds.limit)
            return
        previous = self._install_sigterm_handler()
        try:
            self._run(bounds, loop=options['loop'], interval=max(0.0, options['interval']))
        finally:
            recorder.stop()
            if previous is not None:
                # the command may be called in-process; leaving our handler installed
                # would turn a later SIGTERM into a stray interrupt
                with contextlib.suppress(ValueError):
                    signal.signal(signal.SIGTERM, previous)

    def _run(self, bounds: Bounds, *, loop: bool, interval: float) -> None:
        """Pass after pass, or one, unwinding on the signal either way."""
        with contextlib.suppress(KeyboardInterrupt):
            while True:
                claimed = self._one_pass(bounds)
                if not loop:
                    return
                # a pass that filled its bound has more waiting, so it goes straight round
                # again rather than sleeping on a backlog it already knows about. What was
                # *claimed* and not what was published: a full batch that was all dropped as
                # too late, or all refused by the broker, is still a full batch, and sleeping
                # on it delays every row behind it by an interval for no reason
                if claimed < bounds.limit:
                    time.sleep(interval)

    def _one_pass(self, bounds: Bounds) -> int:
        """Claim what is due, publish it, and delete what went out.

        Returns how many rows it **claimed**, which is what tells the loop whether there is
        more waiting. How many were published is what the log and the command's own output
        report, and the two differ by every row dropped as late or refused by the broker.
        """
        # before the claim, and this is the same rule `enqueue` follows: a `BROKER` that
        # cannot be resolved is a misconfiguration, not a message that failed to publish.
        # Claimed first, its rows would keep the claim with no drop row and no later pass
        # willing to look at them -- invisible until an operator cleared them by hand
        broker = get_broker()
        self._warn_about_a_lease_shorter_than_a_publish(broker, bounds.lease)
        rows = claim(bounds.limit, lease=bounds.lease)
        if not rows:
            return 0
        published = 0
        for row in rows:
            if bounds.grace and (timezone.now() - row.due_at).total_seconds() > bounds.grace:
                self._drop_late(row, bounds.grace)
                continue
            if self._publish(broker, row, bounds.attempts):
                published += 1
        logger.info('published scheduled sends', extra={'tg_published': published, 'tg_claimed': len(rows)})
        self.stdout.write(f'Published {published} of {len(rows)} claimed.')
        return len(rows)

    @staticmethod
    def _warn_about_a_lease_shorter_than_a_publish(broker: 'Broker', lease: int) -> None:
        """Say so where a publish may outlive the claim that protects it.

        A claim cannot be exclusive *through* a call into the broker -- nothing fences an
        external call, and a token checked before or after it does not change that. What can
        be checked is the arithmetic: the transport declares how long one call may take, and
        while the lease is comfortably longer than that the window does not open in practice.

        Warned rather than refused. The numbers come from two settings an operator sets
        independently, a short lease is a legitimate choice for a queue that never blocks,
        and this is a mover that should keep moving -- but nobody would guess the connection
        between `KAFKA_TIMEOUT` and a duplicate message without being told.
        """
        if not lease:
            return
        ceiling = broker.call_ceiling
        if lease > ceiling:
            return
        logger.warning(
            'the lease is not longer than a single publish, so a slow one may outlive its claim',
            extra={'tg_lease': lease, 'tg_call_ceiling': ceiling},
        )

    def _publish(self, broker: 'Broker', row: 'TelegramScheduledSend', attempts: int) -> bool:
        """Put one row on the queue and delete it, or count the failure and say why.

        The row is deleted **after** the publish, which is what makes this at-least-once
        like everything else here: a mover killed in between leaves a claimed row whose
        message is already on the queue, and the next mover to take the lapsed claim sends
        it twice. The other order would lose it silently, and this package has nowhere it
        prefers loss to duplication.

        A publish that *fails* leaves the row for its claim to lapse, so the lease paces the
        retries rather than the interval -- and ``attempts`` bounds them. Without the bound a
        payload the broker refuses permanently would be retried every lease for ever, writing
        one more drop row each time: an event log growing without end over one message, and
        an operator reading the same failure a hundred times.

        The claim is not exclusive *through* this call, and cannot be made so: nothing fences
        a request already in flight to another system, so a publish that outlives its lease
        can be joined by a second mover taking the row back. What guards that is arithmetic
        rather than a lock -- the lease is 300 seconds by default against a call the transport
        itself bounds at ten -- and :meth:`_warn_about_a_lease_shorter_than_a_publish` says so
        where the two are set the other way round.
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
            # `publishing` has already recorded the drop for this attempt
            logger.exception('a scheduled send could not be published', extra={'tg_function': row.function})
            self._count_failure(row, attempts)
            return False
        row.delete()
        return True

    @staticmethod
    def _count_failure(row: 'TelegramScheduledSend', attempts: int) -> None:
        """Record one failed publish against the row, and give up where the bound says to.

        The row is left claimed either way: its lease is what paces the next attempt, so a
        broker that is down for a minute costs a minute rather than a pass per interval.

        **Every write here is conditional on still holding the claim.** A lease that lapsed
        mid-publish means another mover owns the row now, and it may well be publishing it
        successfully -- so counting a failure against it, or recording that it was given up
        on, would put a ``TooManyAttempts`` drop in the feed about a message that went out.
        A publish this mover cannot account for is one it says nothing about.
        """
        from django_aiogram.models import TelegramScheduledSend  # noqa: PLC0415 - django.db, not at import

        # `F` and not `row.attempts + 1`: after a lease lapses two movers can be here for
        # the same row, and an absolute value written twice loses one failure -- so the bound
        # is reached later than it says, or never on a row that keeps being retried
        held = still_held_by(row)
        with transaction.atomic():
            counted = TelegramScheduledSend.objects.filter(held).update(attempts=F('attempts') + 1)
            # read back inside the same transaction, because the number this decides on is
            # the one the database now holds rather than the one this process last saw
            failed = TelegramScheduledSend.objects.filter(held).values_list('attempts', flat=True).first()
        if not counted or failed is None or not attempts or failed < attempts:
            return
        # and again for the delete, because the claim can lapse between the two. Recording the
        # drop only when this took the row is what keeps the feed from claiming a give-up
        # twice, or at all where another mover went on to publish the message
        if not TelegramScheduledSend.objects.filter(held).delete()[0]:
            return
        recorder.record(
            Event(
                kind=EventKind.OUTBOUND_DROPPED.value,
                correlation_id=row.correlation_id,
                function=row.function,
                chat_id=row.chat_id,
                attempt=failed,
                error_code='TooManyAttempts',
                error=f'given up on after {failed} failed publishes',
                detail={'stage': 'scheduling', 'attempts': failed},
            )
        )
        logger.error('giving up on a scheduled send', extra={'tg_attempts': failed, 'tg_function': row.function})

    @staticmethod
    def _drop_late(row: 'TelegramScheduledSend', grace: int) -> None:
        """Record a row too old to send, and delete it.

        `--grace` exists because a mover that was down for a day would otherwise deliver a
        day of messages at once, all of them about a moment that has passed. Recorded rather
        than silently dropped: the row is gone, and the feed says which and why.

        Deleted first and recorded second, conditionally, for the reason `_count_failure`
        gives: a row this mover's claim no longer covers is one another mover may be
        publishing, and a ``TooLate`` drop about a delivered message is worse than no row.
        """
        from django_aiogram.models import TelegramScheduledSend  # noqa: PLC0415 - django.db, not at import

        if not TelegramScheduledSend.objects.filter(still_held_by(row)).delete()[0]:
            return
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

    def _report_due(self, limit: int) -> None:
        """Say what a real pass would take, claiming nothing."""
        from django_aiogram.models import TelegramScheduledSend  # noqa: PLC0415 - django.db, not at import

        now = timezone.now()
        # `unheld()` and not `claimed_at__isnull`, or the two readers disagree: a row whose
        # claim has lapsed is one the next pass publishes, and reporting it as held would
        # understate what a real pass takes -- the one thing this output is for
        free = unheld(now)
        due = TelegramScheduledSend.objects.filter(free, due_at__lte=now)
        waiting = TelegramScheduledSend.objects.filter(free, due_at__gt=now).count()
        claimed = TelegramScheduledSend.objects.exclude(free).count()
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
