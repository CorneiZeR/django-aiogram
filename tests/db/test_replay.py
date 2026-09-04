"""Putting a failed send back on the queue, from the row that recorded it.

The command's whole risk is in the other direction from the rest of this package: its mistake
is measured in messages people receive. So most of these cases are about what it *refuses* --
a row whose arguments were summarized, redacted or capped, and a failure whose queued row is
gone -- and the two that replay assert the new id and the row that joins it to the old one.
"""

import datetime
import uuid
from io import StringIO

import pytest
from django.core.management import CommandError, call_command
from django.test import override_settings
from django.utils import timezone

from django_aiogram import TelegramBot
from django_aiogram.broker.registry import use_broker
from django_aiogram.config.enums import EventKind
from django_aiogram.eventlog.events import new_correlation_id
from django_aiogram.eventlog.recorder import recorder
from django_aiogram.models import TelegramEvent
from django_aiogram.testing import InMemoryBroker
from django_aiogram.testing.capture import Captured

SETTINGS = {
    'TOKEN': '42:x',
    'FSM_STORAGE': 'memory',
    'RATE_LIMIT': None,
    'BROKER': 'django_aiogram.testing.InMemoryBroker',
    'EVENT_LOG': True,
    'EVENT_LOG_SYNC': True,
    'EVENT_LOG_PAYLOAD': 'full',
}


@pytest.fixture
def queued():
    """The messages a replay put on the queue, read as records rather than as bytes."""
    broker = InMemoryBroker()
    with use_broker(broker):
        yield Captured(broker)


def a_failure(function='send_message', chat_id=42, arguments=None, minutes_old=1, kind=None):
    """One ended send, with the queued row that carries what it was called with.

    Two rows, because that is the shape the command reads: the ending names the function and
    the chat, and the arguments are on the row the producer wrote before a worker ever saw it.
    """
    identifier = new_correlation_id()
    when = timezone.now() - datetime.timedelta(minutes=minutes_old)
    for row_kind, detail in (
        (EventKind.OUTBOUND_QUEUED.value, arguments if arguments is not None else {'chat_id': chat_id, 'text': 'lost'}),
        (kind or EventKind.OUTBOUND_FAILED.value, {'stage': 'sending'}),
    ):
        row = TelegramEvent.objects.create(
            kind=row_kind,
            correlation_id=identifier,
            function=function,
            chat_id=chat_id,
            detail=detail,
        )
        TelegramEvent.objects.filter(pk=row.pk).update(created_at=when)
    return identifier


def replay(**options):
    out = StringIO()
    call_command('tgbot_replay', stdout=out, **options)
    return out.getvalue()


def since(minutes=60):
    return (timezone.now() - datetime.timedelta(minutes=minutes)).isoformat()


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_failed_send_goes_back_on_the_queue(queued):
    """The whole point: the arguments the feed recorded, queued again."""
    a_failure()

    output = replay(since=since())

    assert queued.kwargs == [{'chat_id': 42, 'text': 'lost'}]
    assert queued[0].function == 'send_message'
    assert 'replayed 1; refused 0' in output


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_replay_gets_an_id_of_its_own_joined_to_the_one_it_stands_in_for(queued):
    """Reusing the id would make one message look as though it had been sent twice."""
    original = a_failure()

    replay(since=since())
    recorder.flush(timeout=5)

    replacement = queued[0].correlation_id
    assert replacement != original, 'the replay reused the id of the send it stands in for'
    row = TelegramEvent.objects.get(kind=EventKind.OUTBOUND_REPLAYED.value)
    assert row.correlation_id == replacement, 'the replay row is not under the id it queued'
    assert row.detail['replay_of'] == str(original)
    assert row.detail['replay_of_kind'] == EventKind.OUTBOUND_FAILED.value


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_dry_run_queues_nothing_and_says_what_it_would_have_sent(queued):
    """The look before the leap, for a command whose mistake is a message somebody reads."""
    a_failure()

    output = replay(since=since(), dry_run=True)

    assert list(queued) == []
    assert 'would replay 1' in output
    # sorted by name, which is what makes this line the same on every backend: PostgreSQL
    # stores `detail` as `jsonb` and does not keep key order. CI caught it, which is what the
    # postgres leg is for
    assert 'send_message(chat_id=42, text=lost)' in output


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
@pytest.mark.parametrize(
    ('arguments', 'expected'),
    [
        ({'chat_id': 42, 'photo': {'__omitted__': 'bytes', 'size': 12}}, 'omitted'),
        ({'chat_id': 42, 'text': {'__truncated__': True, 'size': 9000, 'preview': 'a'}}, 'truncated'),
        ({'chat_id': 42, 'text': 'hi', 'token': '***'}, 'redacted'),
        ({}, 'no outbound.queued or outbound.scheduled row'),
    ],
    ids=['omitted', 'truncated', 'redacted', 'nothing recorded'],
)
def test_a_row_that_is_not_what_was_sent_is_refused_by_name(queued, arguments, expected):
    """A summary is not a payload, and redaction is one-way.

    Per row rather than per setting: all four of these are recorded under
    `EVENT_LOG_PAYLOAD: 'full'`, which is necessary and not sufficient -- 'full' still replaces
    bytes with a marker, still caps a payload too big for the column, and still redacts. The
    refusal names which, because "it did not replay" is not an answer an operator can act on.
    """
    a_failure(arguments=arguments)

    output = replay(since=since())

    assert list(queued) == [], 'a message was sent from arguments that are not what was sent'
    assert 'replayed 0; refused 1' in output
    assert expected in output


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_body_too_long_to_record_is_refused_end_to_end(queued):
    """The case the local review caught before this shipped, driven through the real producer.

    A body over `MAX_STRING` used to be recorded as a prefix and an ellipsis, with nothing to
    say so -- so `lossy_reason` saw an ordinary string and the replay would have sent two
    thousand characters of a longer message to somebody. The cap is a marker now, at the point
    it happens, and this is the whole path: a real send records the row, and the replay reads
    it back and refuses.
    """
    from django_aiogram.wire.payloads import MAX_STRING

    identifier = TelegramBot().send(chat_id=42, text='x' * (MAX_STRING + 100))
    recorder.flush(timeout=5)
    TelegramEvent.objects.create(
        kind=EventKind.OUTBOUND_FAILED.value,
        correlation_id=identifier,
        function='send_message',
        chat_id=42,
    )
    queued_before = len(queued)

    output = replay(since=since())

    assert len(queued) == queued_before, 'a truncated body was sent as though it were the message'
    assert 'truncated' in output


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_row_written_before_the_cap_was_marked_is_refused_by_its_length(queued):
    """4.0 wrote the truncation as a prefix and an ellipsis, and those rows are still in the table.

    Nothing else can produce a stored string longer than `MAX_STRING`, so the length is the
    signal -- and it has to be, because a replay reads history rather than only what this
    version wrote.
    """
    from django_aiogram.wire.payloads import MAX_STRING

    a_failure(arguments={'chat_id': 42, 'text': 'x' * MAX_STRING + '…'})

    output = replay(since=since())

    assert list(queued) == []
    assert 'truncated' in output


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
@pytest.mark.parametrize(
    'arguments',
    [
        {'chat_id': 1, 'reply_markup': {'__omitted__': 'keys', 'keys': 70}},
        {'chat_id': 1, 'media': {'__truncated__': True, 'size': 70, 'preview': []}},
    ],
    ids=['keys', 'items'],
)
def test_a_structure_cut_to_its_cap_is_refused(queued, arguments):
    """The other two losses that used to be invisible: fifty keys kept out of seventy, and
    fifty items out of seventy. Both are marked at the source now, so both are refused here."""
    a_failure(arguments=arguments)

    output = replay(since=since())

    assert list(queued) == []
    assert 'replayed 0; refused 1' in output


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
@pytest.mark.parametrize('shape', ['keys', 'items'], ids=['keys', 'items'])
def test_a_structure_sitting_at_a_cap_is_refused_however_it_was_written(queued, shape):
    """The loss with no signal: 4.0 cut a mapping to fifty keys and stored fifty keys.

    A mapping of exactly fifty and a mapping cut to fifty are the same bytes, so this cannot
    be classified from the stored shape -- and the safe direction is refusing it. What that
    costs is a keyboard of exactly fifty rows, retyped by hand; what accepting it costs is a
    call sent with items missing.

    New rows carry a marker and are caught by the case above, so this fires on the rows 4.0
    wrote and on the rare whole structure that happens to sit on the boundary.
    """
    from django_aiogram.wire.payloads import MAX_ITEMS, MAX_KEYS

    at_the_cap = {str(index): index for index in range(MAX_KEYS)} if shape == 'keys' else list(range(MAX_ITEMS))
    a_failure(arguments={'chat_id': 42, 'reply_markup': at_the_cap})

    output = replay(since=since())

    assert list(queued) == [], 'a structure that may be missing items was sent'
    assert 'is the cap' in output


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_an_eta_send_replays_from_its_scheduled_row_without_the_due_time(queued):
    """A scheduled send has no queued row of its own until a mover writes one, and that one
    carries no description -- so the arguments are on `outbound.scheduled`, beside `due_at`.

    Falsifiable: leave `due_at` in and the replay calls `send_message(due_at=...)`, which the
    worker refuses.
    """
    identifier = new_correlation_id()
    row = TelegramEvent.objects.create(
        kind=EventKind.OUTBOUND_SCHEDULED.value,
        correlation_id=identifier,
        function='send_message',
        chat_id=7,
        detail={'chat_id': 7, 'text': 'later', 'due_at': timezone.now().isoformat()},
    )
    TelegramEvent.objects.filter(pk=row.pk).update(created_at=timezone.now() - datetime.timedelta(minutes=2))
    TelegramEvent.objects.create(
        kind=EventKind.OUTBOUND_FAILED.value,
        correlation_id=identifier,
        function='send_message',
        chat_id=7,
    )

    replay(since=since())

    assert queued.kwargs == [{'chat_id': 7, 'text': 'later'}]


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_bound_holds_and_is_the_default(queued):
    """No unbounded replay: a slipped date range must not empty a month into the queue."""
    for _ in range(3):
        a_failure()

    replay(since=since(), limit=2)

    assert len(queued) == 2


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_selection_narrows_by_window_chat_and_id(queued):
    """Three ways to select, because the operator knows one of them and not the others.

    A failure of its own per sub-run, since a replay is not offered twice: the row joining the
    two says it has been done, which is the case below this one.
    """
    a_failure(chat_id=1, minutes_old=600)
    a_failure(chat_id=2, minutes_old=5)
    named = a_failure(chat_id=3, minutes_old=900)

    replay(since=since(60))
    assert [one.kwargs['chat_id'] for one in queued] == [2], 'the window did not hold'

    replay(since=since(1000), chat=1)
    assert [one.kwargs['chat_id'] for one in queued][1:] == [1], 'the chat filter did not hold'

    replay(correlation_id=[str(named)])
    assert [one.kwargs['chat_id'] for one in queued][2:] == [3], 'an id did not name its own rows'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_dropped_send_is_only_replayed_when_asked_for(queued):
    """Two endings, and the default is the one an operator means by "what did we lose"."""
    a_failure(kind=EventKind.OUTBOUND_DROPPED.value)

    replay(since=since())
    assert list(queued) == [], 'a drop was replayed without being asked for'

    replay(since=since(), kind=[EventKind.OUTBOUND_DROPPED.value])
    assert len(queued) == 1


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_retry_is_not_selectable_at_all():
    """`outbound.retried` is not an ending: that message went on to succeed or fail under the
    same id, and replaying it would duplicate whichever it was.

    Checked in the command rather than left to argparse, which is what this case found:
    `choices` is enforced when a command line is parsed and not when `call_command` is handed
    a list, so the same run through Python's API selected retries happily.
    """
    with pytest.raises(CommandError, match='is not an ending a replay may select'):
        replay(since=since(), kind=[EventKind.OUTBOUND_RETRIED.value])


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_credential_redacted_inside_a_body_is_refused(queued):
    """Redaction happens *inside* a string as well as instead of a whole value.

    `redact_values` replaces a value whose key matched; `redact_text` substitutes a
    token-shaped run inside text. So a body reads `'the token is ***, keep it'` and an equality
    test sees nothing wrong with it -- measured, and the replay would have sent that sentence
    to the chat.
    """
    a_failure(arguments={'chat_id': 42, 'text': 'the token is ***, keep it'})

    output = replay(since=since())

    assert list(queued) == [], 'a message with a blanked credential in it was sent'
    assert 'redacted' in output


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_body_that_merely_contains_stars_is_refused_too(queued):
    """The cost of the check above, asserted rather than left to be discovered.

    Recording which keys were redacted instead would read as complete and not be:
    `eventlog.writer.to_row` redacts `detail` again at the boundary, for the caller who builds
    an `Event` by hand, and nothing there knows which of a row's keys are a call's arguments.
    So a message that writes `***` for emphasis is retyped by hand, and a credential is never
    sent.
    """
    a_failure(arguments={'chat_id': 42, 'text': 'this is ***bold*** in some dialects'})

    output = replay(since=since())

    assert list(queued) == []
    assert 'redacted' in output


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_feed_that_refuses_the_join_row_outright_does_not_end_the_run(queued, monkeypatch):
    """A total refusal raises where a partial one is counted, and the message has already gone.

    Letting it out would end the run at whichever row hit it -- rows after it neither replayed
    nor reported, and this one's uncertainty never counted, which is the failure the
    uncertainty exists to describe.
    """
    from django_aiogram.eventlog.writer import EventLogRefusedError
    from django_aiogram.management.commands import tgbot_replay as command

    a_failure(chat_id=1, minutes_old=5)
    a_failure(chat_id=2, minutes_old=4)

    def refuse_outright(events):
        """What the writer does when the database took none of the batch."""
        raise EventLogRefusedError(len(events))

    monkeypatch.setattr(command, 'write_batch', refuse_outright)

    output = replay(since=since())

    assert [one.kwargs['chat_id'] for one in queued] == [1, 2], 'the run stopped at the first refusal'
    assert '2 replays were queued without a row joining it' in output


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_window_nobody_bounded_is_refused():
    """A replay of everything ever recorded is not a default."""
    with pytest.raises(CommandError, match='--since is required'):
        replay()


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT={**SETTINGS, 'EVENT_LOG': False})
def test_a_replay_without_the_log_says_where_it_reads_from():
    """The feed is the only source there is, so an empty report would be a lie about it."""
    with pytest.raises(CommandError, match='EVENT_LOG'):
        replay(since=since())


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT={**SETTINGS, 'ENABLED': False})
def test_a_replay_on_a_disabled_process_refuses_rather_than_reporting_nothing():
    """With `ENABLED` off every send is a no-op that answers with an id, so a run would have
    reported a hundred messages queued and queued none. `--dry-run` still works, because
    reading the selection is exactly what a disabled process can do."""
    a_failure()

    with pytest.raises(CommandError, match='ENABLED'):
        replay(since=since())

    assert 'would replay 1' in replay(since=since(), dry_run=True)


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_one_row_the_queue_refuses_does_not_take_the_run_down(queued, monkeypatch):
    """A traceback halfway through a hundred replays leaves nobody able to say which half went.

    The refusal is counted beside the honest ones and named, and the rows after it still go.
    Falsifiable: without the `try`, the first row's exception escapes `handle` and the second
    message is never queued.
    """
    a_failure(chat_id=1, minutes_old=5)
    a_failure(chat_id=2, minutes_old=4)
    calls = []
    real = TelegramBot.enqueue

    def refuse_the_first(self, function='send_message', **kwargs):
        """Refuse once, the way a broker that dropped would."""
        calls.append(kwargs)
        if len(calls) == 1:
            msg = 'the broker refused the write'
            raise ConnectionError(msg)
        return real(self, function, **kwargs)

    monkeypatch.setattr(TelegramBot, 'enqueue', refuse_the_first)

    output = replay(since=since())

    assert len(calls) == 2, 'the run stopped at the row that raised'
    assert queued.kwargs == [{'chat_id': 2, 'text': 'lost'}]
    assert 'replayed 1; refused 1' in output
    assert 'the queue write refused it: ConnectionError' in output


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_message_that_was_sent_in_the_end_is_not_replayed(queued):
    """The ending selected is not always the end of the story.

    A mover that failed three times and published on the fourth leaves three `outbound.dropped`
    rows and one `outbound.sent`; a send the caller retried itself leaves the same shape.
    Telegram has that message, and replaying it would send a second copy to somebody.
    """
    identifier = a_failure()
    TelegramEvent.objects.create(
        kind=EventKind.OUTBOUND_SENT.value,
        correlation_id=identifier,
        function='send_message',
        chat_id=42,
        message_id=1001,
    )

    output = replay(since=since())

    assert list(queued) == [], 'a message Telegram already has was sent again'
    assert 'it was sent in the end' in output


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_several_endings_for_one_message_replay_it_once(queued):
    """The mover writes an `outbound.dropped` row per failed publish and retries the same row,
    so one lost message can have a column of endings -- and one message is what should go.

    An id is one message here, the way `bot.outcome()` reads it. A caller who reuses an id
    across several sends has told the feed they are one thread, and this run replays one.
    """
    identifier = a_failure(kind=EventKind.OUTBOUND_DROPPED.value)
    for _ in range(2):
        TelegramEvent.objects.create(
            kind=EventKind.OUTBOUND_DROPPED.value,
            correlation_id=identifier,
            function='send_message',
            chat_id=42,
            detail={'stage': 'queueing'},
        )

    output = replay(since=since(), kind=[EventKind.OUTBOUND_DROPPED.value])

    assert len(queued) == 1, 'one lost message was sent once per ending recorded for it'
    assert 'it has been replayed already' in output


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_bounded_run_repeated_walks_the_incident_rather_than_the_first_page(queued):
    """`--limit` counts replays, not rows examined, which is the difference between a bound
    and a wall.

    Applied to the raw selection it was a wall: the second run selected the same oldest rows,
    skipped every one as already replayed, and never reached the next -- so "run it again for
    the next hundred" was false in the one place an operator relies on it. Three runs of one,
    over two failures, is the smallest shape that shows all of it: no duplicate, the second
    run reaching the second failure, and the third finding nothing left.
    """
    a_failure(chat_id=1)
    a_failure(chat_id=2)

    replay(since=since(), limit=1)
    assert [one.kwargs['chat_id'] for one in queued] == [1]

    output = replay(since=since(), limit=1)

    assert [one.kwargs['chat_id'] for one in queued] == [1, 2], 'the second run never reached the second failure'
    assert 'it has been replayed already' in output, 'it walked past the first failure without saying so'

    third = replay(since=since(), limit=1)

    assert [one.kwargs['chat_id'] for one in queued] == [1, 2], 'a failure was replayed twice'
    assert 'replayed 0; refused 0; skipped 2' in third


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_what_needs_nothing_is_counted_apart_from_what_cannot_be_replayed(queued):
    """An operator reads the two differently, so the report does not add them up.

    Skipped is a message that needs nothing -- it went in the end, or a replay already stands
    in for it. Refused is one this command cannot make a message from, which is a decision
    somebody has to take. Counted together, a walk past a previous run's work read as five
    hundred problems.
    """
    sent = a_failure(chat_id=1)
    TelegramEvent.objects.create(
        kind=EventKind.OUTBOUND_SENT.value,
        correlation_id=sent,
        function='send_message',
        chat_id=1,
        message_id=7,
    )
    a_failure(chat_id=2, arguments={'chat_id': 2, 'photo': {'__omitted__': 'bytes', 'size': 9}})
    a_failure(chat_id=3)

    output = replay(since=since())

    assert [one.kwargs['chat_id'] for one in queued] == [3]
    assert 'replayed 1; refused 1; skipped 1' in output


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_negative_limit_is_not_the_unbounded_mode(queued):
    """`--limit 0` is the deliberate one; `--limit -1` is a typo that would replay everything."""
    a_failure()

    with pytest.raises(CommandError, match='is not a bound'):
        replay(since=since(), limit=-1)

    assert list(queued) == []


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT={**SETTINGS, 'EVENT_LOG_KINDS': ['outbound.failed', 'outbound.queued']})
def test_a_replay_the_feed_would_not_record_is_refused(queued):
    """`EVENT_LOG_KINDS` excluding the replay kind means the message goes and nothing joins it
    to the failure -- so the next run selects that failure again and sends a second copy.

    Refused rather than warned, because the operator can fix it in one line and the cost of
    not fixing it is a duplicate message nobody predicted.
    """
    a_failure()

    with pytest.raises(CommandError, match='EVENT_LOG_KINDS'):
        replay(since=since())

    assert list(queued) == []
    assert 'would replay 1' in replay(since=since(), dry_run=True), 'a dry run needs no audit row'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_join_row_the_feed_would_not_take_is_reported_rather_than_assumed(queued, monkeypatch):
    """The audit row is written through the writer and its answer is read.

    Handed to the recorder instead, the row would be dropped rather than waited for -- that is
    the recorder's contract, and it is right for the send path and wrong here: the run would
    print `replayed 1; refused 0` with nothing joining the new message to the failure, and the
    next run would select that failure and send a second copy.
    """
    from django_aiogram.management.commands import tgbot_replay as command

    a_failure()

    def refuse_them_all(events):
        """What the writer answers when the database took none of the batch."""
        return len(events)

    monkeypatch.setattr(command, 'write_batch', refuse_them_all)

    output = replay(since=since())

    assert len(queued) == 1, 'the message itself still went'
    assert 'the feed did not take the row joining them' in output
    assert '1 replay was queued without a row joining it' in output


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_correlation_id_that_is_not_one_is_refused_by_name():
    """An operator pastes these by hand, so the typo has to say what it was."""
    with pytest.raises(CommandError, match='is not a uuid'):
        replay(correlation_id=['not-an-id'])


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_moment_that_is_not_one_is_refused_by_name():
    """`--since yesterday` is a reasonable thing to try and a bad thing to guess at."""
    with pytest.raises(CommandError, match='ISO 8601'):
        replay(since='yesterday')


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_naive_moment_is_read_in_the_project_s_timezone(queued):
    """The opposite of what an `eta` does, and deliberately: this is an operator typing a
    moment they just read off a log line, not code promising a future one.

    Both directions, because "it selected something" would pass while the string was read in
    UTC: with `TIME_ZONE` five hours from UTC, a naive moment read the wrong way lands hours
    out and the window either takes everything or nothing. So one moment before the failure
    and one after it, both naive, both local.
    """
    a_failure(chat_id=1, minutes_old=5)
    a_failure(chat_id=2, minutes_old=5)
    local = timezone.localtime()
    before = (local - datetime.timedelta(minutes=30)).replace(tzinfo=None).isoformat()
    after = (local - datetime.timedelta(minutes=1)).replace(tzinfo=None).isoformat()

    replay(since=before, limit=1)
    assert len(queued) == 1, 'a naive moment before the failure did not select it'

    output = replay(since=after)

    assert len(queued) == 1, 'a naive moment after the failures selected one anyway'
    # on the *report*, and with a second failure nothing has touched: asserting the queue alone
    # passed however `--since` was read, because the first run's join row made the second skip
    # that failure whatever window it selected. My own de-duplication made the assertion
    # vacuous, which is what a review found
    assert 'replayed 0; refused 0; skipped 0' in output, 'the window selected rows it should not have'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_newest_description_wins(queued):
    """A message queued, failed, replayed and failed again has two describing rows, and the
    one that describes *this* attempt is the later of them."""
    identifier = uuid.uuid4()
    for text, minutes in (('first', 20), ('second', 2)):
        row = TelegramEvent.objects.create(
            kind=EventKind.OUTBOUND_QUEUED.value,
            correlation_id=identifier,
            function='send_message',
            chat_id=9,
            detail={'chat_id': 9, 'text': text},
        )
        TelegramEvent.objects.filter(pk=row.pk).update(created_at=timezone.now() - datetime.timedelta(minutes=minutes))
    TelegramEvent.objects.create(
        kind=EventKind.OUTBOUND_FAILED.value,
        correlation_id=identifier,
        function='send_message',
        chat_id=9,
    )

    replay(since=since())

    assert queued.kwargs == [{'chat_id': 9, 'text': 'second'}]
