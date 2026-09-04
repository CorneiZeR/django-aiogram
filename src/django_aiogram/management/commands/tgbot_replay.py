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
from typing import TYPE_CHECKING, Any

from django.conf import settings as django_settings
from django.core.management import BaseCommand, CommandError
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from django_aiogram.config.enums import EventKind
from django_aiogram.config.settings import SETTINGS_NAME, coerce_bool, conf
from django_aiogram.eventlog.events import worker_identity
from django_aiogram.eventlog.recorder import recorder
from django_aiogram.eventlog.records import Event
from django_aiogram.eventlog.writer import log_alias, write_batch
from django_aiogram.models import TelegramEvent
from django_aiogram.producer.scheduling import DUE_AT_DETAIL
from django_aiogram.wire.payloads import lossy_reason

if TYPE_CHECKING:
    from django.db.models import QuerySet

logger = logging.getLogger('django_aiogram')

#: the kinds a replay may select. Both are ends: a send that failed, and one dropped before it
#: was ever attempted. `outbound.retried` is not one -- that message went on to succeed or fail
#: under the same id, and replaying it would duplicate whichever it was
REPLAYABLE_KINDS = (EventKind.OUTBOUND_FAILED.value, EventKind.OUTBOUND_DROPPED.value)

#: where the arguments are, in the order they are looked for. The queued row is the ordinary
#: case; the scheduled row is where an `eta` send's arguments live, since nothing queued it
#: until a mover did and the mover records no description of its own
ARGUMENT_KINDS = (EventKind.OUTBOUND_QUEUED.value, EventKind.OUTBOUND_SCHEDULED.value)

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

    **One run at a time.** Two of these with overlapping selections can both read a failure
    before either has written the row that says it was replayed, and then both send it. There
    is nothing here to claim against that, and the absence is a design decision rather than an
    omission: the event log is insert-only -- which is what lets a web process and a worker
    write one message's history with no coordination -- so there is no row a run can take
    ownership of the way ``tgbot_dispatch_scheduled`` claims a scheduled send.

    What is bounded is the damage. The guard is a row written per message immediately after it
    is queued, so a second run started later duplicates only the messages the first had not
    yet reached, or the ones whose join row the feed refused -- which the first run reports and
    logs. The window is the overlap between two runs rather than the incident. Serialising two
    operators' shells is not something a management command can do, so this says so instead,
    the way the rest of this package states the races it cannot close.
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
        parser.add_argument('--until', default=None, help='and before this one, as ISO 8601 (default: now).')
        parser.add_argument(
            '--kind',
            action='append',
            default=None,
            choices=REPLAYABLE_KINDS,
            help=f'which endings to replay; repeatable (default {EventKind.OUTBOUND_FAILED.value}).',
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
            '--dry-run',
            action='store_true',
            help='say what would be replayed, and why anything is refused, without queueing.',
        )

    #: replays whose join row the feed would not take, counted so the report can say so
    _unrecorded = 0

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
                nothing = done.get(row.correlation_id)
                if nothing:
                    self.stdout.write(f'skipped {row.short_id or row.correlation_id} ({row.function}): {nothing}')
                    skipped[nothing] += 1
                    continue
                reason = self._replay(row, dry_run=options['dry_run'])
                if reason:
                    refused[reason] += 1
                    continue
                # so a second ending for this message, later in the same walk, is skipped for
                # the same reason a second *run* would skip it
                done[row.correlation_id] = 'it has been replayed already; the row joining them says so'
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
                f'failure -- and a later run would select that failure again. Add the kind, or '
                f'read the selection with --dry-run.'
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
        kinds = options['kind'] or [REPLAYABLE_KINDS[0]]
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
        if options['until']:
            rows = rows.filter(created_at__lt=_moment(options['until'], '--until'))
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

    def _replay(self, row: TelegramEvent, *, dry_run: bool) -> str:
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
            return reason
        if dry_run:
            self.stdout.write(f'would replay {row.short_id or row.correlation_id}: {row.function}({_shown(arguments)})')
            return ''
        from django_aiogram import bot  # noqa: PLC0415 - building a bot is the last thing this does

        try:
            replacement = bot.enqueue(row.function, **arguments)
        except Exception as error:
            logger.exception('could not replay a failed send', extra={'tg_replay_of': str(row.correlation_id)})
            failed = f'the queue write refused it: {type(error).__name__}'
            self.stdout.write(f'refused {row.short_id or row.correlation_id} ({row.function}): {failed}')
            return failed
        recorded = self._record_replay(row, replacement)
        logger.info(
            'replayed a failed send',
            extra={'tg_function': row.function, 'tg_replay_of': str(row.correlation_id)},
        )
        note = '' if recorded else ' (the feed did not take the row joining them; see the log)'
        if not recorded:
            logger.error(
                'a replay was queued and not recorded, so a later run may select it again',
                extra={'tg_replay_of': str(row.correlation_id)},
            )
        self.stdout.write(f'replayed {row.short_id or row.correlation_id} as {replacement}{note}')
        self._unrecorded += 0 if recorded else 1
        return ''

    @staticmethod
    def _nothing_to_do_for(rows: 'list[TelegramEvent]') -> dict[uuid.UUID, str]:
        """Answer which messages in this window need nothing, and what they need nothing for.

        Two questions, two queries for the whole window rather than two per row:

        * **Telegram has it.** An ``outbound.sent`` under the id means the ending selected was
          not the end of the story -- a mover that failed three times and published on the
          fourth leaves three drop rows and a delivery, and so does a send the caller retried.
        * **A replay already stands in for it**, read from ``detail.replay_of`` on the
          ``outbound.replayed`` rows. The join row is the record, so the record is what is
          asked, and that is what lets a bounded run be repeated until the incident is walked.

        Not refusals: nothing here is wrong, and the report counts them apart from the rows a
        replay cannot be made from. An operator reads the two differently.
        """
        identifiers = {row.correlation_id for row in rows}
        delivered = (
            TelegramEvent.objects.using(log_alias())
            .filter(correlation_id__in=identifiers, kind=EventKind.OUTBOUND_SENT.value)
            .values_list('correlation_id', flat=True)
        )
        nothing = dict.fromkeys(delivered, 'it was sent in the end, so nothing was lost')
        already = (
            TelegramEvent.objects.using(log_alias())
            .filter(
                kind=EventKind.OUTBOUND_REPLAYED.value,
                detail__replay_of__in=sorted(str(one) for one in identifiers),
            )
            .values_list('detail', flat=True)
        )
        for detail in already:
            if detail and detail.get('replay_of'):
                nothing.setdefault(
                    uuid.UUID(str(detail['replay_of'])),
                    'it has been replayed already; the row joining them says so',
                )
        return nothing

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
        never blocks and drops rather than waiting, so a queue write nobody watched could
        report ``replayed 1`` with no row joining the new message to the failure -- and the next
        run would select that failure again and send a second copy. The recorder is built that
        way because it sits in the send path. This is a management command an operator is
        watching, so it can afford to wait and to be told.
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
                f'{self._unrecorded} {were} queued without a row joining it to the failure: '
                f'a later run will select that failure again, so read the log before repeating this one'
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
