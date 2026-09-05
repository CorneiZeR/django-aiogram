"""Queue a failed send again, from the row that recorded it.

A send that exhausted its retries writes an ``outbound.failed`` row and stops existing. Until
this command, the answer to "Telegram was down for ten minutes, what did we lose" was a
changelist an operator read and then retyped by hand.

**The failure row does not carry the call.** It names the function, the chat, the correlation
id and the error; the *arguments* are on the ``outbound.queued`` row the producer wrote before
the message ever reached a worker -- or, for a send made with an ``eta``, on its
``outbound.scheduled`` row. So a replay reads two rows joined by the correlation id, and a
failure whose queued row has been pruned is not replayable however recent the failure is.

And a recorded argument is a *description* of an argument: ``wire.payloads`` summarizes,
redacts and caps, in that order, and says plainly that it is never lossless.
:func:`~django_aiogram.wire.payloads.lossy_reason` is what decides, per row, whether what was
written down is what was sent -- ``EVENT_LOG_PAYLOAD: 'full'`` is necessary and not sufficient.
Anything else is refused by name rather than sent truncated.

**That claim cost three markers to make true.** Before this command, three of the caps in
``payloads`` were invisible in their own output: a body over ``MAX_STRING`` became a prefix
and an ellipsis, a mapping over ``MAX_KEYS`` simply had fewer keys, and a sequence over
``MAX_ITEMS`` fewer items. A refusal built on the markers that *did* exist would have replayed
all three -- sending two thousand characters of a longer message to somebody, which is the
exact failure this command exists to avoid. They are marked where they happen now, and the
string case is caught by its length as well, because rows written before 4.1 are still in the
table and a replay reads history.
"""

import datetime
import logging
import uuid
from argparse import ArgumentParser
from collections import Counter
from typing import TYPE_CHECKING, Any, NamedTuple

from django.conf import settings as django_settings
from django.core.management import BaseCommand, CommandError
from django.db import IntegrityError, transaction
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from django_aiogram.config.enums import EventKind
from django_aiogram.config.settings import SETTINGS_NAME, coerce_bool, conf
from django_aiogram.eventlog.events import worker_identity
from django_aiogram.eventlog.recorder import recorder
from django_aiogram.eventlog.records import Event
from django_aiogram.eventlog.writer import log_alias, write_batch
from django_aiogram.models import TelegramEvent, TelegramReplayClaim
from django_aiogram.producer.scheduling import DUE_AT_DETAIL
from django_aiogram.wire.payloads import lossy_reason

if TYPE_CHECKING:
    from django.db.models import QuerySet

logger = logging.getLogger('django_aiogram')

#: the kinds a replay may select, and **both are the default**, which took a review to get
#: right. The list the default is built from, because the answer is not guessable from the
#: names -- every place this package records the end of a send, and what each means:
#:
#: * `producer/client.py`, `outbound.failed` -- the call raised. Lost.
#: * `producer/client.py`, `outbound.dropped` with `detail.max_retries` -- the rate-limit
#:   retries ran out. **Lost, and the case an operator means by "Telegram was down"** -- which
#:   a default of `outbound.failed` alone missed entirely.
#: * `producer/client.py`, `outbound.dropped` with `NotScheduled` -- the send never reached the
#:   loop, from either of two callers, and they do not mean the same thing. A direct `send_raw`
#:   was never queued and has no arguments recorded, so the arguments rule refuses it. A message
#:   the *consumer* took off the queue has an `outbound.queued` row and its arguments -- and the
#:   worker deliberately does not acknowledge it ("the slot back, not the acknowledgement"), so
#:   the transport redelivers it on restart. Replaying that is a duplicate, which is why
#:   `UNACKNOWLEDGED_DROPS` skips the code rather than the caller: from the feed the two are
#:   told apart only by whether a queued row exists, and the safe answer is the same for both.
#: * `producer/queueing.py`, `outbound.dropped` with `detail.stage` -- the queue write failed.
#: * `tgbot_dispatch_scheduled`, `outbound.dropped` with `TooManyAttempts` -- the mover gave up.
#:   Lost.
#: * `tgbot_dispatch_scheduled`, `outbound.dropped` with `TooLate` -- past `--grace`. **Not
#:   lost: the deployment decided not to send it**, so `DELIBERATE_DROPS` refuses it below.
#:
#: The two with no arguments recorded -- `NotScheduled` and the queueing drop, neither of which
#: has an `outbound.queued` row, because that row is written after the transport takes the
#: payload -- are refused by the arguments rule without needing to be named here.
#:
#: `outbound.retried` is not selectable at all: that message went on to succeed or fail under
#: the same id, and replaying it would duplicate whichever it was.
REPLAYABLE_KINDS = (EventKind.OUTBOUND_FAILED.value, EventKind.OUTBOUND_DROPPED.value)

#: error codes on an ending whose message the *transport* still has. Nothing is lost: the worker
#: refused the send without acknowledging it, so the queue hands it back when the container comes
#: up. A replay would be the second copy
UNACKNOWLEDGED_DROPS = frozenset({'NotScheduled'})

#: error codes on an ending that mean the deployment chose this, rather than losing it. Replaying
#: one is not a recovery, it is an override of the policy that discarded it -- `--grace` exists
#: to stop a mover delivering a day of stale messages at once, and a replay that sent them
#: anyway would be that outage twice
DELIBERATE_DROPS = frozenset({'TooLate'})

#: where the arguments are, in the order they are looked for. The queued row is the ordinary
#: case; the scheduled row is where an `eta` send's arguments live, since nothing queued it
#: until a mover did and the mover records no description of its own
ARGUMENT_KINDS = (EventKind.OUTBOUND_QUEUED.value, EventKind.OUTBOUND_SCHEDULED.value)

#: the three answers that mean "nothing to do here", written once because they are read in four
#: places -- the per-row line, the report, the log field's documentation and the page. Counted
#: apart from the refusals, since a refusal asks somebody to decide and these do not
SKIPPED_DELIVERED = 'it was sent in the end, so nothing was lost'
#: said by two paths -- a claim naming it in a live run, and this run's own memory in a dry one,
#: which is why the sentence names neither
SKIPPED_REPLAYED = 'it has been replayed already; one failure gets one replacement'
SKIPPED_DELIBERATE = 'the deployment discarded it on purpose ({code}), so this is not a loss'
SKIPPED_UNACKNOWLEDGED = 'the worker never acknowledged it ({code}), so the queue redelivers it on restart'
SKIPPED_CLAIMED = 'another run holds it ({by}, since {at:%H:%M:%S}); one run at a time reaches it'


class Verdict(NamedTuple):
    """What one row came to, and which column of the report it belongs in.

    ``reason`` empty means the message was queued. Otherwise it is the line the operator reads,
    and ``skipped`` says whether it is a *skip* -- nothing to do here -- or a **refusal**, which
    is something somebody has to decide about.

    A flag rather than a set of known strings, which is what this was until the claim landed: two
    of the reasons name a worker and a time, so membership of a frozenset classified them as
    refusals. Deciding at the point that knows is the fix; matching on the text was the bug.
    """

    reason: str
    skipped: bool = False


#: how long a claim is believed before another run may take it over. A claim outlives its run
#: whenever the queue write did not plainly answer -- the process died, or `publish` raised
#: after the bytes went -- and an hour is far past a command that spends milliseconds per row.
#: Retaking one can send a second copy, exactly as the mover's `--lease` can: the message may
#: have reached the queue in the instant before the answer stopped coming
DEFAULT_CLAIM_LEASE = 3600

#: how many messages one run may put back, unless an operator says otherwise. A default rather
#: than "all of them", because a slipped date range would otherwise empty a month of failures
#: into the queue at once and every one of them is a message somebody receives
DEFAULT_LIMIT = 100

#: how many rows are read at a time while walking towards that many replays. Bigger than the
#: default bound, because the common shape is a second run reading past the first run's work:
#: one query per window for the rows and two for the answers about them
WINDOW = 200

#: how much of one argument a dry run prints. Enough to recognise the message, short enough
#: that a hundred of them stay a report rather than a transcript
SHOWN_WIDTH = 40


class Command(BaseCommand):
    """Select failed sends and queue them again, or say why they cannot be.

    **Two runs at once are safe now, and they were not until 4.1.** Both could read that a
    failure had not been replayed, both queue it, and both write the row saying so afterwards --
    a recovered message delivered twice, which is the wrong side of the at-least-once trade for
    a command whose whole purpose is repairing an incident. The narrowing that came first, a
    second read immediately before the send, made the window small and left it open.

    `TelegramReplayClaim` closes it: one row per failure with a **unique** constraint, inserted
    before the queue write, so the second run is told by the database rather than by a read it
    would have to trust. That is the one claim that is atomic on all four databases this package
    supports -- PostgreSQL has advisory locks and MySQL has ``GET_LOCK``, SQLite has neither --
    and it is the same reasoning that made the mover's claim a compare-and-set update rather
    than ``SELECT ... FOR UPDATE SKIP LOCKED``.

    The event log stays insert-only, which is what lets a web process and a worker write one
    message's history with no coordination. The claim is not in it: it lives with the caller's
    own writes, because a claim on a database ``EVENT_LOG_DATABASE`` may point somewhere else
    entirely is not a claim at all.

    What remains, said plainly: a claim stands until its message is known to have reached the
    queue or ``--claim-lease`` runs out, because a queue write that raised is not proof the
    message stayed out -- the event log defines a queueing drop as a write that *may still have
    been applied*. So a refusal holds its row for the lease, and taking it over afterwards may
    send a second copy. The mover's ``--lease`` makes the same trade for the same reason. A
    replay made by something other than this command is still not known to it.
    """

    help = 'Queue failed sends again, from the arguments the event log recorded.'

    def add_arguments(self, parser: ArgumentParser) -> None:
        """Declare the selection, the bound and the two ways to look before leaping."""
        parser.add_argument(
            '--since',
            default=None,
            help='replay failures recorded at or after this moment, as ISO 8601. Required '
            'unless --correlation-id names the rows instead.',
        )
        parser.add_argument(
            '--until',
            default=None,
            help='and before this one, as ISO 8601. Defaults to the moment the run starts, so a '
            'row dated in the future is left alone rather than sent early.',
        )
        parser.add_argument(
            '--kind',
            action='append',
            default=None,
            choices=REPLAYABLE_KINDS,
            help='which endings to replay; repeatable (default: both, since exhausted retries are recorded as a drop).',
        )
        parser.add_argument('--chat', type=int, default=None, help='only failures for this chat id.')
        parser.add_argument(
            '--correlation-id',
            action='append',
            default=None,
            help='replay exactly these ids, whenever they failed; repeatable.',
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=DEFAULT_LIMIT,
            help=f'stop after this many messages (default {DEFAULT_LIMIT}). 0 means no bound, '
            'which is a deliberate thing to type.',
        )
        parser.add_argument(
            '--claim-lease',
            type=int,
            default=DEFAULT_CLAIM_LEASE,
            help=f'seconds before a claim whose queue write never answered -- the process died, '
            f'or publish raised -- is taken over (default {DEFAULT_CLAIM_LEASE}). Retaking one '
            f'may send a second copy, since the message may have reached the queue anyway.',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='say what would be replayed, and why anything is refused, without queueing.',
        )

    #: replays whose join row the feed would not take, counted so the report can say so
    _unrecorded = 0

    #: the moment the run began, which is the upper end of the window when `--until` is absent
    _started: datetime.datetime

    #: what a dry run has already said it would send, since it takes no claim to read back
    _simulated: set[uuid.UUID]

    #: seconds after which a claim whose run never queued anything is taken over
    _claim_lease: int

    def handle(self, *args: Any, **options: Any) -> None:
        """Walk the selection until the bound is spent on messages that actually went.

        **``--limit`` counts replays, not rows examined**, and that is the difference between
        a bound and a wall. Applied to the raw selection it was a wall: with a hundred and one
        failures and ``--limit 100``, the first run replayed the oldest hundred, and the next
        run selected those same hundred, skipped every one of them as already replayed, and
        never reached the hundred-and-first. Which made "run it again for the next hundred"
        false in the one place an operator would rely on it -- during an incident, with the
        rest of the failures still waiting.

        So the rows are walked in windows, the two "nothing to do here" answers are read for a
        whole window at a time, and the walk stops when the bound has been spent on messages
        that went. What it costs is reading past the rows a previous run already handled, which
        is what paging is.
        """
        self._unrecorded = 0
        self._simulated = set()
        self._claim_lease = max(0, options['claim_lease'])
        self._started = timezone.now()
        self._refuse_where_there_is_nothing_to_read(dry_run=options['dry_run'])
        refused: Counter[str] = Counter()
        skipped: Counter[str] = Counter()
        replayed = 0
        limit = self._bound(options['limit'])
        rows = self._selected(options)
        offset = 0
        while limit is None or replayed < limit:
            window = list(rows[offset : offset + WINDOW])
            if not window:
                break
            offset += len(window)
            done = self._nothing_to_do_for(window)
            for row in window:
                if limit is not None and replayed >= limit:
                    break
                nothing = done.get(row.correlation_id) or _deliberate(row)
                if nothing:
                    self.stdout.write(f'skipped {row.short_id or row.correlation_id} ({row.function}): {nothing}')
                    skipped[nothing] += 1
                    continue
                verdict = self._replay(row, dry_run=options['dry_run'])
                if verdict.skipped:
                    skipped[verdict.reason] += 1
                    continue
                if verdict.reason:
                    refused[verdict.reason] += 1
                    continue
                # nothing to record for the next ending of this message: the claim taken a
                # moment ago is what `_claim_on` reads on the way past
                replayed += 1
        self._report(replayed, refused, skipped, dry_run=options['dry_run'])

    def _refuse_where_there_is_nothing_to_read(self, *, dry_run: bool) -> None:
        """Stop before a query that cannot answer, and before a send that cannot happen.

        Two settings, two different failures, and neither is obvious from an empty report: with
        ``EVENT_LOG`` off there are no rows at all, and with ``ENABLED`` off every replay is a
        no-op that answers with an id -- so a run would report a hundred messages queued and
        queue none. The second is checked only when something would actually be sent.
        """
        if not coerce_bool(conf['EVENT_LOG'], f"{SETTINGS_NAME}['EVENT_LOG']"):
            msg = (
                f"{SETTINGS_NAME}['EVENT_LOG'] is off, so nothing was recorded to replay from. "
                f'A replay reads the feed; it has no other source.'
            )
            raise CommandError(msg)
        if dry_run:
            return
        if not coerce_bool(conf['ENABLED'], f"{SETTINGS_NAME}['ENABLED']"):
            msg = (
                f"{SETTINGS_NAME}['ENABLED'] is off, so a replay would queue nothing while "
                f'reporting that it had. Use --dry-run to read the selection instead.'
            )
            raise CommandError(msg)
        if not recorder.wants(EventKind.OUTBOUND_REPLAYED.value):
            msg = (
                f"{SETTINGS_NAME}['EVENT_LOG_KINDS'] excludes {EventKind.OUTBOUND_REPLAYED.value!r}, "
                f'so a replay would send the message and record nothing joining it to the '
                f'failure. The claim still stops a second run repeating it, but the feed would '
                f'show a fresh send that repaired nothing. Add the kind, or read the selection '
                f'with --dry-run.'
            )
            raise CommandError(msg)

    def _selected(self, options: dict[str, Any]) -> 'QuerySet[TelegramEvent]':
        """Build the selection, refusing a window nobody bounded."""
        identifiers = options['correlation_id']
        if not identifiers and not options['since']:
            msg = (
                '--since is required, or --correlation-id to name the rows instead: '
                'a replay of everything ever recorded is not a default.'
            )
            raise CommandError(msg)
        kinds = options['kind'] or list(REPLAYABLE_KINDS)
        # argparse checks `choices` when it parses a command line and not when `call_command`
        # is handed a list, so the same run through Python's API could have selected
        # `outbound.retried` -- a message that went on to succeed or fail under the same id.
        # Checked here, where both paths pass
        unknown = sorted(set(kinds) - set(REPLAYABLE_KINDS))
        if unknown:
            msg = f'--kind {unknown[0]!r} is not an ending a replay may select: {", ".join(REPLAYABLE_KINDS)}.'
            raise CommandError(msg)
        rows = TelegramEvent.objects.using(log_alias()).filter(kind__in=kinds)
        if identifiers:
            rows = rows.filter(correlation_id__in=[_as_uuid(one) for one in identifiers])
        if options['since']:
            rows = rows.filter(created_at__gte=_moment(options['since'], '--since'))
        # the help said "default: now" and the code left the upper end open, so a row dated in
        # the future -- a clock that ran ahead on the process that wrote it, or a caller building
        # an `Event` by hand -- was selected and sent. Read once at the start of the run rather
        # than per query, so a walk of several windows does not creep forward past rows arriving
        # while it runs: those belong to the next run, which is the same reason `claim` in the
        # mover takes one `moment` for a whole pass
        until = _moment(options['until'], '--until') if options['until'] else self._started
        rows = rows.filter(created_at__lt=until)
        if options['chat'] is not None:
            rows = rows.filter(chat_id=options['chat'])
        return rows.order_by('created_at', 'id')

    @staticmethod
    def _bound(limit: int) -> int | None:
        """How many messages this run may send, or ``None`` for the unbounded mode.

        ``None`` rather than a large number, because the two are read in different places and
        one of them is a ``while``. A negative limit is a typo rather than the unbounded mode:
        ``0`` is that, and it is a deliberate thing to type.
        """
        if limit < 0:
            msg = f'--limit {limit} is not a bound. 0 is the unbounded mode, and it is a deliberate thing to type.'
            raise CommandError(msg)
        return None if limit == 0 else limit

    def _replay(self, row: TelegramEvent, *, dry_run: bool) -> Verdict:
        """Queue one message again, or answer with why it cannot be.

        **One row never takes the run down with it**, which is the difference between a
        partial replay an operator can read and a traceback halfway through a hundred of them
        -- with no way to tell which half went. A payload the queue refuses, a function the
        allowlist has since stopped allowing, a broker that dropped: each is counted as a
        refusal beside the honest ones and named in the report.
        """
        arguments, reason = self._arguments_for(row)
        if reason:
            self.stdout.write(f'refused {row.short_id or row.correlation_id} ({row.function}): {reason}')
            return Verdict(reason)
        if dry_run:
            return self._would_replay(row, arguments)

        claim, held = self._claim(row)
        if claim is None:
            return self._say_claimed(row, held)

        from django_aiogram import bot  # noqa: PLC0415 - building a bot is the last thing this does

        try:
            replacement = bot.enqueue(row.function, **arguments)
        except Exception as error:
            # kept, not released: a raise here is not proof the message stayed out of the queue.
            # `publishing` records exactly this as a *queueing* drop, which the event log defines
            # as a write that may still have been applied -- all three networked transports can
            # fail after the bytes went. Deleting the claim would hand the same failure to the
            # next run and send somebody a second copy; holding it makes this the case the lease
            # already exists for, and `--claim-lease` is the operator's answer when they know it
            # did not land
            logger.exception(
                'could not replay a failed send',
                extra={'tg_replay_of': str(row.correlation_id), 'tg_claimed_by': claim.claimed_by},
            )
            failed = (
                f'the queue write raised {type(error).__name__}; the claim stands until '
                f'--claim-lease, since the message may have reached the queue anyway'
            )
            self.stdout.write(f'refused {row.short_id or row.correlation_id} ({row.function}): {failed}')
            return Verdict(failed)
        # the claim says the message reached the queue before anything else is written: a crash
        # after this line leaves a claim a later run can read as finished rather than as stale
        claim.queued_at = timezone.now()
        claim.replacement_id = replacement
        claim.save(update_fields=('queued_at', 'replacement_id'))
        recorded = self._record_replay(row, replacement)
        logger.info(
            'replayed a failed send',
            extra={'tg_function': row.function, 'tg_replay_of': str(row.correlation_id)},
        )
        note = '' if recorded else ' (the feed has no row joining them; see the log)'
        if not recorded:
            logger.error(
                'a replay was queued and the feed did not record it, so its history has a hole',
                extra={'tg_replay_of': str(row.correlation_id)},
            )
        self.stdout.write(f'replayed {row.short_id or row.correlation_id} as {replacement}{note}')
        self._unrecorded += 0 if recorded else 1
        return Verdict('')

    def _would_replay(self, row: TelegramEvent, arguments: dict[str, Any]) -> Verdict:
        """Say what a live run would do with this row, and take nothing while saying it.

        A dry run is read *instead of* the live one, so it answers the same questions -- but it
        may not answer them by claiming: a claim taken here would be held by a run that queued
        nothing, and every row it looked at would be locked until the lease ran out.

        Which leaves one thing it has to do for itself. A live run reads back the claim it just
        took, so a second ending for the same message is skipped; nothing is written here, so
        this remembers instead.
        """
        held = self._claim_on(row, takeover=False)
        if held is not None:
            return self._say_claimed(row, held)
        if row.correlation_id in self._simulated:
            self.stdout.write(f'skipped {row.short_id or row.correlation_id} ({row.function}): {SKIPPED_REPLAYED}')
            return Verdict(SKIPPED_REPLAYED, skipped=True)
        self._simulated.add(row.correlation_id)
        self.stdout.write(f'would replay {row.short_id or row.correlation_id}: {row.function}({_shown(arguments)})')
        return Verdict('')

    def _claim(self, row: TelegramEvent) -> 'tuple[TelegramReplayClaim | None, TelegramReplayClaim | None]':
        """Take this failure, or answer with the claim that already holds it.

        One ``INSERT`` against a unique column, which is the only claim that is atomic on every
        database this package supports -- so the second run to arrive is told by an
        ``IntegrityError`` rather than by a read it would have to trust. Everything else here is
        deciding what a claim that is *already* there means.
        """
        held = self._claim_on(row)
        if held is not None:
            return None, held
        try:
            with transaction.atomic():
                return TelegramReplayClaim.objects.create(
                    correlation_id=row.correlation_id, claimed_by=worker_identity()
                ), None
        except IntegrityError:
            # the other run inserted between the read above and this line, which is exactly the
            # race the constraint exists for. Read its claim back so the report can name it
            return None, self._claim_on(row)

    def _claim_on(self, row: TelegramEvent, *, takeover: bool = True) -> 'TelegramReplayClaim | None':
        """Answer with the claim standing in the way of replaying this failure, if one does.

        A claim whose replacement reached the queue always stands: that failure is handled, this
        run or an earlier one. A claim with no ``queued_at`` belongs to a run that is working --
        or to one whose queue write never answered, by dying or by raising -- and after
        ``--claim-lease`` this treats it as the latter, which is the mover's trade in the same
        words: the alternative is a message nothing will ever put back.

        ``takeover=False`` answers the same question and **writes nothing**, which is what a dry
        run needs: it has to predict the live run -- so a lapsed claim is no obstacle there
        either -- while leaving the row for the run that will actually take it. Deleting from a
        dry run was the shape this had first, and it made ``--dry-run`` a command that changes
        the database.
        """
        claim = TelegramReplayClaim.objects.filter(correlation_id=row.correlation_id).first()
        if claim is None:
            return None
        if claim.queued_at is not None:
            return claim
        if not self._lapsed(claim):
            return claim
        if takeover:
            logger.warning(
                'taking over a replay claim whose queue write never answered',
                extra={'tg_replay_of': str(row.correlation_id), 'tg_claimed_by': claim.claimed_by},
            )
            claim.delete()
        return None

    def _lapsed(self, claim: 'TelegramReplayClaim') -> bool:
        """Whether a claim is old enough that the run holding it must be gone."""
        if not self._claim_lease:
            return False
        return claim.claimed_at < self._started - datetime.timedelta(seconds=self._claim_lease)

    def _say_claimed(self, row: TelegramEvent, held: 'TelegramReplayClaim | None') -> Verdict:
        """Report a failure somebody else owns, and answer with the reason it was skipped."""
        if held is not None and held.queued_at is None:
            reason = SKIPPED_CLAIMED.format(by=held.claimed_by or 'another run', at=_local(held.claimed_at))
        else:
            reason = SKIPPED_REPLAYED
        self.stdout.write(f'skipped {row.short_id or row.correlation_id} ({row.function}): {reason}')
        return Verdict(reason, skipped=True)

    @staticmethod
    def _nothing_to_do_for(rows: 'list[TelegramEvent]') -> dict[uuid.UUID, str]:
        """Answer which messages in this window Telegram already has.

        One question, one query for the whole window: an ``outbound.sent`` under the id means
        the ending selected was not the end of the story -- a mover that failed three times and
        published on the fourth leaves three drop rows and a delivery, and so does a send the
        caller retried.

        **It used to ask two**, the second being whether a replay already stood in for the row.
        That answer comes from the claim table now -- :meth:`_claim_on`, per row and against a
        unique constraint, so it decides the race rather than merely narrowing it. It read the
        feed's ``detail.replay_of`` until 4.1, which was both later than this window pass and
        weaker than a claim, and needed a JSON key lookup to behave the same on three
        databases; the claim is an indexed column and needs nothing of the sort.

        Not a refusal: nothing here is wrong, and the report counts these apart from the rows a
        replay cannot be made from. An operator reads the two differently.
        """
        delivered = (
            TelegramEvent.objects.using(log_alias())
            .filter(correlation_id__in={row.correlation_id for row in rows}, kind=EventKind.OUTBOUND_SENT.value)
            .values_list('correlation_id', flat=True)
        )
        return dict.fromkeys(delivered, SKIPPED_DELIVERED)

    def _arguments_for(self, row: TelegramEvent) -> tuple[dict[str, Any], str]:
        """Find what the failed call was made with, or say why it cannot be known.

        The newest describing row wins, and the two kinds are tried in order: an ordinary send
        has an ``outbound.queued`` row, and a scheduled one has ``outbound.scheduled`` with the
        due time mixed in beside the arguments -- which is why ``DUE_AT_DETAIL`` is a name
        rather than a string typed twice.
        """
        described = (
            TelegramEvent.objects.using(log_alias())
            .filter(correlation_id=row.correlation_id, kind__in=ARGUMENT_KINDS)
            .order_by('-created_at', '-id')
        )
        for candidate in described:
            arguments = {key: value for key, value in (candidate.detail or {}).items() if key != DUE_AT_DETAIL}
            if not arguments:
                continue
            reason = lossy_reason(arguments)
            return ({}, reason) if reason else (arguments, '')
        return {}, f'no {" or ".join(ARGUMENT_KINDS)} row carries its arguments'

    @staticmethod
    def _record_replay(row: TelegramEvent, replacement: uuid.UUID) -> bool:
        """Say in the feed that this message is a replay of that one, and answer whether it landed.

        Under the **new** id, with the old one in ``detail``: reusing the id would make one
        message look as though it had been sent twice, and the failure rate is read off these
        kinds. Recorded after the queue write, so a replay that could not be queued claims
        nothing.

        **Written through the writer rather than handed to the recorder**, which is the one
        place in this package that does that, and the reason is the recorder's own contract: it
        never blocks and drops rather than waiting, so a queue write nobody watched could report
        ``replayed 1`` while the feed shows a send that stands in for nothing. The claim is what
        keeps the next run off that failure; this row is what lets anybody read afterwards which
        failure was repaired, and a dropped one is a hole in that history rather than a duplicate
        message. The recorder is built that way because it sits in the send path. This is a
        management command an operator is watching, so it can afford to wait and to be told.
        """
        event = Event(
            kind=EventKind.OUTBOUND_REPLAYED.value,
            correlation_id=replacement,
            function=row.function,
            chat_id=row.chat_id,
            worker=worker_identity(),
            detail={'replay_of': str(row.correlation_id), 'replay_of_kind': row.kind},
        )
        try:
            return write_batch([event]) == 0
        except Exception:
            # a total refusal raises where a partial one is counted, and the message has
            # already gone: letting it out of here would end the run at whichever row hit it,
            # with the rows after it neither replayed nor reported and this one's uncertainty
            # never counted. Which is the failure the uncertainty exists to describe
            logger.exception(
                'the feed refused the row joining a replay to its failure',
                extra={'tg_replay_of': str(row.correlation_id)},
            )
            return False

    def _report(self, replayed: int, refused: 'Counter[str]', skipped: 'Counter[str]', *, dry_run: bool) -> None:
        """Say what happened, and what did not, with the reasons counted rather than repeated.

        Skipped apart from refused, because an operator reads them differently: skipped is a
        message that needs nothing -- it went in the end, or a replay already stands in for it
        -- and refused is one this command cannot make a message from, which is a decision
        somebody has to take. Counting them together made a walk past a previous run's work
        look like five hundred problems.
        """
        verb = 'would replay' if dry_run else 'replayed'
        self.stdout.write(f'{verb} {replayed}; refused {sum(refused.values())}; skipped {sum(skipped.values())}')
        for reason, count in skipped.most_common():
            self.stdout.write(f'  {count} x {reason}')
        if self._unrecorded:
            were = 'replay was' if self._unrecorded == 1 else 'replays were'
            self.stdout.write(
                f'{self._unrecorded} {were} queued without a row joining it to the failure in the '
                f'feed: the claim records it, so no later run will send it again, but the history '
                f'of those messages has a hole in it -- read the log'
            )
        for reason, count in refused.most_common():
            self.stdout.write(f'  {count} x {reason}')
        if not dry_run:
            recorder.flush(timeout=5)
        logger.info(
            'replay finished',
            extra={
                'tg_replayed': replayed,
                'tg_refused': sum(refused.values()),
                'tg_skipped': sum(skipped.values()),
            },
        )


def _local(moment: datetime.datetime) -> datetime.datetime:
    """Render a stored moment in the project's timezone, whichever kind it stores.

    ``timezone.localtime`` raises ``ValueError`` on a naive datetime, and a project with
    ``USE_TZ = False`` stores nothing else -- so the one line that formats a claim's age for a
    person took the whole command down there. Measured: *"localtime() cannot be applied to a
    naive datetime"*.
    """
    return timezone.localtime(moment) if timezone.is_aware(moment) else moment


def _deliberate(row: TelegramEvent) -> str:
    """Say why this ending is not a loss, or ``''`` where it is one.

    Two ways for an ending to be nothing to act on, and they are different facts: the
    deployment chose it, or the transport still has the message.

    Counted with the skips and not with the refusals, because there is nothing for an operator
    to act on: `--grace` exists so that a mover which was down for a day does not deliver a
    day of stale messages at once, and a replay that sent them anyway would be that outage
    twice. A refusal asks somebody to decide; this decision has been taken.
    """
    if row.error_code in DELIBERATE_DROPS:
        return SKIPPED_DELIBERATE.format(code=row.error_code)
    if row.error_code in UNACKNOWLEDGED_DROPS:
        return SKIPPED_UNACKNOWLEDGED.format(code=row.error_code)
    return ''


def _as_uuid(value: str) -> uuid.UUID:
    """Read a correlation id the way an operator has it, refusing a typo by name."""
    try:
        return uuid.UUID(value)
    except ValueError as error:
        msg = f'--correlation-id {value!r} is not a uuid: {error}'
        raise CommandError(msg) from error


def _moment(value: str, flag: str) -> datetime.datetime:
    """Read an ISO 8601 moment in whichever flavour of datetime this project stores.

    A naive string is read in the project's ``TIME_ZONE`` rather than refused, which is the
    opposite of what an ``eta`` does -- and deliberately: an ``eta`` is a promise about the
    future written by code, while this is an operator typing a moment they have just read off
    a log line. Refusing ``--since 2026-09-04T10:00`` would teach them to add an offset they
    do not have.
    """
    moment = parse_datetime(value)
    if moment is None:
        msg = f'{flag} {value!r} is not an ISO 8601 moment, like 2026-09-04T10:00:00.'
        raise CommandError(msg)
    if django_settings.USE_TZ:
        return timezone.make_aware(moment) if timezone.is_naive(moment) else moment
    return timezone.make_naive(moment) if timezone.is_aware(moment) else moment


def _shown(arguments: dict[str, Any]) -> str:
    """Render arguments for one line of a dry run, without pasting a whole message body.

    Sorted, because the order they arrive in is the database's rather than the caller's:
    PostgreSQL stores ``detail`` as ``jsonb``, which does not keep key order, while SQLite
    keeps the text as written -- so the same row printed on two backends read differently, and
    a dry run of a hundred rows is something an operator scans down a column of.
    """
    parts = []
    for key, value in sorted(arguments.items()):
        text = str(value)
        parts.append(f'{key}={text[:SHOWN_WIDTH]}…' if len(text) > SHOWN_WIDTH else f'{key}={text}')
    return ', '.join(parts)
