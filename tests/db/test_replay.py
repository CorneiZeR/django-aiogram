"""Putting a failed send back on the queue, from the row that recorded it.

The command's whole risk is in the other direction from the rest of this package: its mistake
is measured in messages people receive. So most of these cases are about what it *refuses* --
a row whose arguments were summarized, redacted or capped, and a failure whose queued row is
gone -- and the two that replay assert the new id and the row that joins it to the old one.
"""

import datetime
import logging
import uuid
from io import StringIO

import pytest
from django.core.management import CommandError, call_command
from django.db import IntegrityError
from django.test import override_settings
from django.utils import timezone

from django_aiogram import TelegramBot
from django_aiogram.broker.registry import use_broker
from django_aiogram.config.enums import EventKind
from django_aiogram.eventlog.events import new_correlation_id
from django_aiogram.eventlog.recorder import recorder
from django_aiogram.management.commands.tgbot_replay import DEFAULT_LIMIT
from django_aiogram.models import TelegramEvent, TelegramReplayClaim
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
def test_a_dry_run_predicts_the_live_one(queued):
    """A dry run is read *instead of* the live run, so it has to answer the same.

    Two ways it did not. The already-replayed check ran after the dry-run branch, so a failure
    a live run would skip was reported as one it would send. And a dry run writes no join row,
    so a second ending for the same message -- which a live run skips because the row it just
    wrote says so -- was counted as a second message.
    """
    replayed_before = a_failure(chat_id=1)
    replay(since=since())
    assert len(queued) == 1

    twice = a_failure(chat_id=2, kind=EventKind.OUTBOUND_DROPPED.value)
    TelegramEvent.objects.create(
        kind=EventKind.OUTBOUND_DROPPED.value,
        correlation_id=twice,
        function='send_message',
        chat_id=2,
        detail={'stage': 'sending'},
    )

    output = replay(since=since(), dry_run=True)

    # whole ids, not prefixes: these are UUIDv7, so two made in the same millisecond share their
    # first characters and a `[:8]` slice matched the wrong one -- an assertion of mine that
    # failed on correct output
    assert f'would replay {twice}' in output, 'the message with two endings was not offered once'
    assert f'would replay {replayed_before}' not in output, 'a failure the live run would skip was offered'
    assert 'would replay 1; refused 0; skipped 2' in output, output


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
def test_the_bound_holds(queued):
    """No unbounded replay: a slipped date range must not empty a month into the queue."""
    for _ in range(3):
        a_failure()

    replay(since=since(), limit=2)

    assert len(queued) == 2


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_bound_applies_with_nobody_asking_for_it(queued):
    """And the default is the bound, which the case above claimed and never exercised.

    It passed `--limit` every time, so it would have held with `DEFAULT_LIMIT` changed to
    anything, or with the parser not applying it at all.

    **Both numbers written out, and it took two rounds to get there.** Sized
    `DEFAULT_LIMIT + 1`, the fixture grew with the constant and raising the default to a
    thousand left the case green. Then, with a literal fixture but `DEFAULT_LIMIT` as the
    expectation, *lowering* the default to fifty still passed -- fifty rows queued, fifty
    expected. Only a literal on both sides fails in both directions, which is the same reason
    `test_public_surface.py` writes its names out rather than reading them off the class: a
    number derived from the code agrees with every change to it.

    So a hundred is a decision, and moving it means editing this line on purpose.
    """
    for index in range(101):
        a_failure(chat_id=index)

    replay(since=since())

    assert len(queued) == 100, f'the default bound is not 100 (DEFAULT_LIMIT is {DEFAULT_LIMIT})'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_walk_crosses_its_own_window(queued, monkeypatch):
    """The rows are read in windows, and nothing tested the second one.

    `WINDOW` is two hundred and every other case has a handful of rows, so the offset
    bookkeeping -- the part that makes a bounded run reach past a previous run's work -- ran
    exactly once per test. Found by re-applying the swaps of earlier rounds: breaking the walk
    after the first window left the suite green. Patched small rather than fixtured large,
    because it is the bookkeeping under test and not the number.
    """
    from django_aiogram.management.commands import tgbot_replay as command

    monkeypatch.setattr(command, 'WINDOW', 2)
    for index in range(3):
        a_failure(chat_id=index)

    replay(since=since())

    assert sorted(one.kwargs['chat_id'] for one in queued) == [0, 1, 2], 'the walk stopped at its first window'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_selection_narrows_by_window_chat_and_id(queued):
    """Three ways to select, because the operator knows one of them and not the others.

    A failure of its own per sub-run, since a replay is not offered twice: the row joining the
    two says it has been done, which is the case below this one.

    **The last sub-run needs an untouched failure beside it**, or it proves nothing: by then the
    other two have been replayed, so they would be skipped whatever the filter did, and the
    assertion could not tell a working filter from a selection that was already empty. Chat 4
    is that witness, and it is the one the id must leave alone.
    """
    a_failure(chat_id=1, minutes_old=600)
    a_failure(chat_id=2, minutes_old=5)
    named = a_failure(chat_id=3, minutes_old=900)

    replay(since=since(60))
    assert [one.kwargs['chat_id'] for one in queued] == [2], 'the window did not hold'

    replay(since=since(1000), chat=1)
    assert [one.kwargs['chat_id'] for one in queued][1:] == [1], 'the chat filter did not hold'

    a_failure(chat_id=4, minutes_old=900)

    replay(correlation_id=[str(named)])

    assert [one.kwargs['chat_id'] for one in queued][2:] == [3], 'an id did not name its own rows'
    assert 4 not in [one.kwargs['chat_id'] for one in queued], 'the id filter let an unrelated failure through'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_both_endings_are_replayed_by_default_because_exhaustion_is_a_drop(queued):
    """The default had to be both kinds, and the reason is not guessable from the names.

    Rate-limit exhaustion -- the case an operator means by "Telegram was down for ten minutes"
    -- is recorded as `outbound.dropped` with `detail.max_retries`, not as `outbound.failed`;
    measured in `producer/client.py`. So a default of `outbound.failed` alone missed the
    largest loss there is, and the page that told an operator to run this told them to replay
    the smaller half and conclude the rest was fine.
    """
    a_failure(chat_id=1)
    exhausted = a_failure(chat_id=2, kind=EventKind.OUTBOUND_DROPPED.value)
    TelegramEvent.objects.filter(correlation_id=exhausted, kind=EventKind.OUTBOUND_DROPPED.value).update(
        detail={'max_retries': 10}
    )

    replay(since=since())

    assert sorted(one.kwargs['chat_id'] for one in queued) == [1, 2], 'the default missed an ending'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_one_ending_may_be_selected_alone(queued):
    """`--kind` narrows, for an operator who knows which half they are looking at."""
    a_failure(chat_id=1)
    a_failure(chat_id=2, kind=EventKind.OUTBOUND_DROPPED.value)

    replay(since=since(), kind=[EventKind.OUTBOUND_DROPPED.value])

    assert [one.kwargs['chat_id'] for one in queued] == [2]


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_send_the_worker_never_acknowledged_is_left_to_the_queue(queued):
    """`NotScheduled` has two callers and they do not mean the same thing.

    A direct `send_raw` refused at shutdown was never queued, so it has no arguments recorded
    and the arguments rule refuses it anyway. But the *consumer* reaches the same code with a
    message it took off the queue -- which has an `outbound.queued` row and its arguments --
    and the worker deliberately does not acknowledge it there ("the slot back, not the
    acknowledgement", `producer/client.py`), so the transport hands it back when the container
    comes up.

    Replaying that is the second copy. Found by a review reading the page rather than the code:
    my own enumeration had filed `NotScheduled` under "no arguments recorded", which is true of
    one caller and false of the other.
    """
    consumed = a_failure(chat_id=5, kind=EventKind.OUTBOUND_DROPPED.value)
    TelegramEvent.objects.filter(correlation_id=consumed, kind=EventKind.OUTBOUND_DROPPED.value).update(
        error_code='NotScheduled', error='cancelled at shutdown'
    )

    output = replay(since=since())

    assert list(queued) == [], 'a message the queue still holds was sent again'
    assert 'never acknowledged it (NotScheduled)' in output
    assert 'replayed 0; refused 0; skipped 1' in output


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_message_the_deployment_discarded_on_purpose_is_not_a_loss(queued):
    """`--grace` refused it deliberately, and replaying it would be that outage twice.

    Counted with the skips rather than the refusals: a refusal asks somebody to decide, and
    this decision has been taken. It is the one ending in `outbound.dropped` that is not a
    loss, which is why the default can be both kinds at all.
    """
    late = a_failure(chat_id=3, kind=EventKind.OUTBOUND_DROPPED.value)
    TelegramEvent.objects.filter(correlation_id=late, kind=EventKind.OUTBOUND_DROPPED.value).update(
        error_code='TooLate', error='90000s overdue, past the 3600s grace'
    )

    output = replay(since=since())

    assert list(queued) == [], 'a message the grace policy discarded was sent anyway'
    assert 'discarded it on purpose (TooLate)' in output
    assert 'replayed 0; refused 0; skipped 1' in output


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
    assert '2 replays were queued without a row joining it' in output  # the claim still records them


@pytest.mark.django_db(transaction=True)
@override_settings(
    TELEGRAM_BOT={**SETTINGS, 'EVENT_LOG_DATABASE': 'logs'},
    DATABASE_ROUTERS=['django_aiogram.eventlog.dbrouter.TelegramEventLogRouter'],
)
def test_the_claim_is_not_created_on_the_log_database():
    """A claim on a database the log may point elsewhere is not a claim at all.

    The same rule the schedule has, and for a sharper reason: the schedule merely needs to be
    where the mover reads, while this one has to be where the constraint can refuse a second
    run. Asked of `django.db.router` rather than the class, because that is what decides at
    migrate time.
    """
    from django.db import router

    assert router.allow_migrate('logs', 'django_aiogram', model=TelegramReplayClaim) is False
    assert router.allow_migrate('default', 'django_aiogram', model=TelegramReplayClaim) is True
    assert router.allow_migrate('logs', 'django_aiogram', model=TelegramEvent) is True


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
    assert 'the queue write raised ConnectionError' in output


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
def test_a_failure_another_run_holds_is_left_to_it(queued, monkeypatch):
    """The claim decides the race now, rather than narrowing it.

    Until 4.1 this read the feed for an `outbound.replayed` row, which is written *after* the
    message is queued -- so two runs could both read nothing and both send. A claim is taken
    before the queue write and enforced by a unique constraint, so the second run is told by
    the database.

    The competitor lands while this row is being prepared, which is the ordering that used to
    produce the duplicate.
    """
    from django_aiogram.management.commands.tgbot_replay import Command

    a_failure()
    real = Command._arguments_for

    def land_a_competitor(self, row):
        """What another run's claim looks like, arriving between the read and the send."""
        TelegramReplayClaim.objects.get_or_create(
            correlation_id=row.correlation_id, defaults={'claimed_by': 'another-run'}
        )
        return real(self, row)

    monkeypatch.setattr(Command, '_arguments_for', land_a_competitor)

    output = replay(since=since())

    assert list(queued) == [], 'the message was sent although another run held the failure'
    assert 'another run holds it (another-run' in output
    assert 'replayed 0; refused 0; skipped 1' in output


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_claim_taken_over_mid_queue_does_not_take_the_run_down(queued, monkeypatch, caplog):
    """A queue write slower than `--claim-lease` loses the row it is holding.

    Another run reads the claim as lapsed, deletes it and makes its own -- and the write back
    here then matches nothing. `save(update_fields=...)` raises `DatabaseError` on that, which
    would end the run at that row and leave the rest neither replayed nor reported: the one
    thing this loop promises not to do. A lease of a second is not hypothetical either, since
    that is what the docs tell an operator to pass when they know a write did not land.
    """
    first, second = a_failure(chat_id=1), a_failure(chat_id=2)
    enqueue = TelegramBot.enqueue

    def take_the_claim_over(self, function='send_message', **kwargs):
        """The other run, arriving while this call is still in the transport."""
        if kwargs.get('chat_id') == 1:
            TelegramReplayClaim.objects.all().delete()
        return enqueue(self, function, **kwargs)

    monkeypatch.setattr(TelegramBot, 'enqueue', take_the_claim_over)

    with caplog.at_level(logging.WARNING, logger='django_aiogram'):
        output = replay(since=since())

    assert len(queued) == 2, 'the run stopped at the row whose claim went'
    assert 'replayed 2; refused 0' in output
    assert '1 claim was taken over while the message was being queued' in output
    assert 'a replay claim was taken over while its message was being queued' in caplog.text
    assert TelegramReplayClaim.objects.get(correlation_id=second).queued_at is not None, (
        'the row that kept its claim still recorded reaching the queue'
    )
    assert not TelegramReplayClaim.objects.filter(correlation_id=first).exists(), (
        'and the one that lost it was not resurrected by the write back'
    )


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_two_runs_cannot_both_take_one_failure(queued):
    """The constraint, asked directly: one insert wins and the other is refused.

    Not through the command -- this is the property the whole change rests on, and it is worth
    one case that names it in the database's own terms.
    """
    identifier = new_correlation_id()
    TelegramReplayClaim.objects.create(correlation_id=identifier, claimed_by='first')

    with pytest.raises(IntegrityError):
        TelegramReplayClaim.objects.create(correlation_id=identifier, claimed_by='second')


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_claim_whose_run_died_is_taken_over_after_its_lease(queued):
    """A claim whose queue write never answered outlives its run -- it died, or `publish` raised.

    Then the message is neither queued nor claimable, which is a message nothing will ever put
    back -- so the lease takes it over, and what that can cost is a second copy: the queue may
    have taken it in the instant before the answer stopped coming. The mover's `--lease` makes
    the same trade in the same words.
    """
    identifier = a_failure()
    stale = TelegramReplayClaim.objects.create(correlation_id=identifier, claimed_by='a-run-that-died')
    TelegramReplayClaim.objects.filter(pk=stale.pk).update(claimed_at=timezone.now() - datetime.timedelta(seconds=7200))

    replay(since=since(), claim_lease=3600)

    assert len(queued) == 1, 'a claim from a dead run kept the message from ever going back'
    taken = TelegramReplayClaim.objects.get(correlation_id=identifier)
    assert taken.queued_at is not None
    assert taken.claimed_by != 'a-run-that-died', 'the claim was reused rather than retaken'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_fresh_claim_is_not_taken_over(queued):
    """The other half of the lease: a run that is working is left alone."""
    identifier = a_failure()
    TelegramReplayClaim.objects.create(correlation_id=identifier, claimed_by='a-run-still-working')

    output = replay(since=since())

    assert list(queued) == []
    assert 'another run holds it (a-run-still-working' in output


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_queue_write_that_raised_keeps_its_claim(queued, monkeypatch):
    """A raise is not proof the message stayed out of the queue.

    `publishing` records this as a *queueing* drop, which the event log defines as a write that
    may still have been applied -- Redis, RabbitMQ and Kafka can all fail after the bytes went.
    Releasing the claim would offer that failure to the very next run and send a second copy to
    somebody; holding it makes this the case `--claim-lease` already exists for. Found by a
    review reading the transports rather than the comment, which claimed nothing was queued.
    """
    identifier = a_failure()

    def refuse(self, function='send_message', **kwargs):
        """A broker that raised after the write may already have landed."""
        msg = 'the broker stopped answering'
        raise ConnectionError(msg)

    monkeypatch.setattr(TelegramBot, 'enqueue', refuse)

    replay(since=since())

    claim = TelegramReplayClaim.objects.get(correlation_id=identifier)
    assert claim.queued_at is None, 'nothing said the message reached the queue'

    monkeypatch.undo()
    assert 'another run holds it' in replay(since=since()), 'the next run offered the same failure again'
    assert list(queued) == [], 'and would have sent a second copy of a message that may have landed'

    TelegramReplayClaim.objects.filter(pk=claim.pk).update(claimed_at=timezone.now() - datetime.timedelta(seconds=5))
    assert 'replayed 1' in replay(since=since(), claim_lease=1), 'the lease is how an operator says it did not land'
    assert len(queued) == 1


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_dry_run_takes_no_claims(queued):
    """It is read instead of the live run, and a claim taken here would be held by a run that
    queued nothing -- locking every row it looked at until the lease ran out."""
    a_failure()

    replay(since=since(), dry_run=True)

    assert not TelegramReplayClaim.objects.exists()


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_dry_run_does_not_release_a_stale_claim_either(queued):
    """Reading is not writing, and the takeover is a delete.

    A dry run has to *predict* the live run -- so a lapsed claim is no obstacle to what it
    reports -- while leaving the row for the run that will actually take it. The first version
    of the lease deleted it from here, which made `--dry-run` a command that changes the
    database: the opposite of what it is read for.
    """
    identifier = a_failure()
    stale = TelegramReplayClaim.objects.create(correlation_id=identifier, claimed_by='a-run-that-died')
    TelegramReplayClaim.objects.filter(pk=stale.pk).update(claimed_at=timezone.now() - datetime.timedelta(seconds=7200))

    output = replay(since=since(), dry_run=True, claim_lease=3600)

    assert TelegramReplayClaim.objects.filter(pk=stale.pk).exists(), 'a dry run deleted a claim'
    assert 'would replay 1' in output, 'a dry run did not predict the takeover the live run would make'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS, USE_TZ=False)
def test_a_claim_is_reported_on_a_project_that_stores_naive_datetimes(queued):
    """`timezone.localtime` raises on a naive datetime, and `USE_TZ = False` stores nothing else.

    Measured: *localtime() cannot be applied to a naive datetime*. So the one line that renders
    a claim's age for a person took the whole command down on those projects -- a crash in the
    reporting of a skip, which is the least deserving place for one.
    """
    identifier = a_failure()
    TelegramReplayClaim.objects.create(
        correlation_id=identifier,
        claimed_by='a-run-still-working',
        # the shape `USE_TZ = False` stores, which is the whole case
        claimed_at=datetime.datetime(2026, 9, 5, 12, 0),  # noqa: DTZ001
    )

    output = replay(since=since())

    assert 'another run holds it (a-run-still-working, since 12:00:00)' in output
    assert list(queued) == []


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
    to the failure -- the feed shows a fresh send, and no one can read which failure it repaired.

    Refused rather than warned, because the operator can fix it in one line and what is lost
    otherwise cannot be reconstructed afterwards.
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
    print `replayed 1; refused 0` while the feed showed a send standing in for nothing, and the
    history of what was repaired would have a hole in it.
    """
    from django_aiogram.management.commands import tgbot_replay as command

    a_failure()

    def refuse_them_all(events):
        """What the writer answers when the database took none of the batch."""
        return len(events)

    monkeypatch.setattr(command, 'write_batch', refuse_them_all)

    output = replay(since=since())

    assert len(queued) == 1, 'the message itself still went'
    assert 'the feed has no row joining them' in output
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
def test_a_row_dated_in_the_future_is_left_alone(queued):
    """`--until` says it defaults to now, and the code left the upper end open.

    A row can be dated ahead of the clock -- a process whose clock ran fast wrote it, or a
    caller built an `Event` by hand -- and with no upper bound it was selected and sent. The
    run reads one moment at its start and uses that, so rows arriving *while* it walks belong
    to the next run rather than creeping into this one.
    """
    a_failure(chat_id=1, minutes_old=5)
    ahead = a_failure(chat_id=2, minutes_old=-600)

    replay(since=since())

    assert [one.kwargs['chat_id'] for one in queued] == [1], 'a row dated in the future was sent early'
    assert TelegramEvent.objects.filter(correlation_id=ahead, kind=EventKind.OUTBOUND_REPLAYED.value).count() == 0


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
