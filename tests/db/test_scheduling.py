"""Sends that wait for a time: writing them down, moving them, calling them off.

The wait lives in a table because three of the four transports cannot delay a message at
all, so these tests are about the table and the mover rather than about any broker -- the
publish itself is the same one every other send makes, and the point is that it happens
exactly once, at the right moment, with the bytes the caller meant.
"""

import asyncio
import datetime
import logging
import uuid

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.core.management import CommandError, call_command
from django.db import transaction
from django.test import override_settings
from django.utils import timezone

from django_aiogram import TelegramBot
from django_aiogram.broker.exceptions import BrokerNotConfiguredError
from django_aiogram.broker.redis_list import RedisListBroker
from django_aiogram.config.enums import EventKind
from django_aiogram.eventlog.recorder import recorder
from django_aiogram.management.commands.tgbot_dispatch_scheduled import Command
from django_aiogram.models import TelegramEvent, TelegramScheduledSend
from django_aiogram.producer import scheduling
from django_aiogram.producer.scheduling import claim
from django_aiogram.wire.envelope import unpack
from django_aiogram.wire.serializers import loads

BROKER = 'tests.db.test_scheduling.RecordingBroker'
SETTINGS = {
    'TOKEN': '42:x',
    'FSM_STORAGE': 'memory',
    'RATE_LIMIT': None,
    'BROKER': BROKER,
    'EVENT_LOG': True,
    'EVENT_LOG_SYNC': True,
}


class RecordingBroker(RedisListBroker):
    """Keeps what it was asked to publish, and refuses on demand, without a server."""

    published: list[bytes] = []  # noqa: RUF012 - one list for the process, cleared per test
    refuses = False

    def publish(self, payloads):
        if RecordingBroker.refuses:
            raise ConnectionError('the broker refused the write')
        RecordingBroker.published.extend(payloads)

    async def apublish(self, payloads):
        self.publish(payloads)


@pytest.fixture
def published():
    """The payloads the mover put on the queue during this test."""
    RecordingBroker.published.clear()
    RecordingBroker.refuses = False
    yield RecordingBroker.published
    RecordingBroker.published.clear()
    RecordingBroker.refuses = False


def schedule_and_then_fail():
    """The block this is about: a send scheduled, and then a rollback."""
    with transaction.atomic():
        TelegramBot().send(chat_id=7, text='announced too early', eta=in_a_while())
        msg = 'and then the block failed'
        raise RuntimeError(msg)


def in_a_while(seconds=60):
    """A due time that has not arrived."""
    return timezone.now() + datetime.timedelta(seconds=seconds)


def a_while_ago(seconds=60):
    """A due time that has."""
    return timezone.now() - datetime.timedelta(seconds=seconds)


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_an_eta_writes_a_row_and_publishes_nothing(published):
    due = in_a_while()

    identifier = TelegramBot().send(chat_id=7, text='later', eta=due)

    row = TelegramScheduledSend.objects.get()
    assert row.correlation_id == identifier
    assert row.function == 'send_message'
    assert row.chat_id == 7
    assert row.claimed_at is None
    assert abs((row.due_at - due).total_seconds()) < 1
    assert published == [], 'a scheduled send reached the broker at once'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_mover_publishes_a_due_row_and_deletes_it(published):
    TelegramBot().send(chat_id=7, text='now', eta=a_while_ago())

    call_command('tgbot_dispatch_scheduled')

    assert len(published) == 1
    assert not TelegramScheduledSend.objects.exists(), 'a published row was left behind'
    envelope = unpack(loads(published[0]))
    assert envelope.function == 'send_message'
    assert envelope.kwargs['text'] == 'now'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_row_that_is_not_due_is_left_alone(published):
    TelegramBot().send(chat_id=7, text='later', eta=in_a_while())

    call_command('tgbot_dispatch_scheduled')

    assert published == []
    assert TelegramScheduledSend.objects.get().claimed_at is None, 'a row was claimed before its time'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_envelope_is_stamped_with_the_due_time_and_not_the_scheduling_time(published):
    """Otherwise the delivered row reports the whole wait as time spent in the queue.

    A message scheduled for tomorrow would arrive looking like a day-old backlog, which is
    the one number an operator reads to know whether delivery is keeping up.
    """
    due = a_while_ago(3600)
    TelegramBot().send(chat_id=7, text='now', eta=due)

    call_command('tgbot_dispatch_scheduled')

    stamped = unpack(loads(published[0])).queued_at
    assert abs(stamped - due.timestamp()) < 1, 'the envelope was stamped when it was scheduled'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_row_another_mover_owns_is_not_selected(published):
    """The cheap half of running two movers: a claimed row is filtered out of the query."""
    TelegramBot().send(chat_id=7, text='once', eta=a_while_ago())

    first = claim(10)
    call_command('tgbot_dispatch_scheduled')

    assert len(first) == 1, 'the first mover did not claim the row'
    assert published == [], 'the second mover published a row it did not own'
    assert TelegramScheduledSend.objects.count() == 1


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_row_claimed_between_the_select_and_the_update_is_not_won_twice(monkeypatch, published):
    """The half the filter cannot cover, which is the whole reason for the update's condition.

    Two movers can both *select* a row before either writes, and the filter above has
    already answered by then. Reproduced by stealing the row inside the transaction the
    claim opens -- exactly the window a second container occupies -- and the claim must come
    back empty rather than hand out a row it does not own.
    """
    TelegramBot().send(chat_id=7, text='once', eta=a_while_ago())
    row = TelegramScheduledSend.objects.get()
    stolen = []

    real_atomic = scheduling.transaction.atomic

    def steal_then_open(*args, **kwargs):
        """Claim the row the way another mover would, once, then behave normally."""
        if not stolen:
            stolen.append(
                TelegramScheduledSend.objects.filter(pk=row.pk, claimed_at__isnull=True).update(
                    claimed_at=timezone.now(), claimed_by='another-mover'
                )
            )
        return real_atomic(*args, **kwargs)

    monkeypatch.setattr(scheduling.transaction, 'atomic', steal_then_open)

    assert claim(10) == [], 'a row already claimed by another mover was handed out'
    assert stolen == [1], 'the test did not manage to steal the row'
    assert TelegramScheduledSend.objects.get().claimed_by == 'another-mover'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_waiting_send_can_be_called_off(published):
    identifier = TelegramBot().send(chat_id=7, text='never mind', eta=in_a_while())

    assert TelegramBot().cancel_scheduled(identifier) == 1
    assert not TelegramScheduledSend.objects.exists()

    call_command('tgbot_dispatch_scheduled')
    assert published == []


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_claimed_send_cannot_be_called_off(published):
    """It is already on its way to the broker, and deleting the row would not stop it."""
    identifier = TelegramBot().send(chat_id=7, text='too late', eta=a_while_ago())
    claim(10)

    assert TelegramBot().cancel_scheduled(identifier) == 0
    assert TelegramScheduledSend.objects.count() == 1


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_row_past_its_grace_is_dropped_rather_than_sent_late(published):
    """A mover that was down for a day would otherwise deliver a day of stale messages."""
    TelegramBot().send(chat_id=7, text='stale', eta=a_while_ago(3600))

    call_command('tgbot_dispatch_scheduled', '--grace', '60')

    assert published == [], 'a message an hour overdue went out under a 60s grace'
    assert not TelegramScheduledSend.objects.exists()
    dropped = TelegramEvent.objects.get(kind=EventKind.OUTBOUND_DROPPED.value)
    assert dropped.detail['stage'] == 'scheduling'
    assert dropped.error_code == 'TooLate'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT={**SETTINGS, 'REDIS_TIMEOUT': 30})
def test_a_lease_no_longer_than_a_publish_is_reported(published, caplog):
    """Nothing can fence a call already in flight to another system, so this is arithmetic.

    While the lease is comfortably longer than the deadline the transport puts on one call,
    the window where a second mover joins a publish in progress does not open. Set the other
    way round it does, and nobody would guess the connection between a transport timeout and
    a duplicate message without being told.
    """
    TelegramBot().send(chat_id=7, text='now', eta=a_while_ago())

    with caplog.at_level(logging.WARNING, logger='django_aiogram'):
        call_command('tgbot_dispatch_scheduled', '--lease', '10')

    assert 'may outlive its claim' in caplog.text


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT={**SETTINGS, 'REDIS_TIMEOUT': 10})
def test_a_lease_longer_than_a_publish_says_nothing(published, caplog):
    TelegramBot().send(chat_id=7, text='now', eta=a_while_ago())

    with caplog.at_level(logging.WARNING, logger='django_aiogram'):
        call_command('tgbot_dispatch_scheduled')

    assert 'may outlive its claim' not in caplog.text


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_dry_run_reports_a_lapsed_claim_as_work_it_would_take(published, capsys):
    """The dry run has to ask what a real pass asks, or it understates what one takes."""
    TelegramBot().send(chat_id=7, text='now', eta=a_while_ago())
    claim(10)
    TelegramScheduledSend.objects.update(claimed_until=timezone.now() - datetime.timedelta(seconds=1))

    call_command('tgbot_dispatch_scheduled', '--dry-run')

    assert '1 due now' in capsys.readouterr().out, 'a row the next pass would publish was reported as held'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_dry_run_claims_nothing(published):
    TelegramBot().send(chat_id=7, text='now', eta=a_while_ago())

    call_command('tgbot_dispatch_scheduled', '--dry-run')

    assert published == []
    assert TelegramScheduledSend.objects.get().claimed_at is None, 'a dry run claimed a row'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS, USE_TZ=True)
def test_a_naive_eta_is_refused_rather_than_read_in_the_projects_timezone(published):
    """An hour early is not a value worth guessing at."""
    with pytest.raises(ImproperlyConfigured, match='aware datetime'):
        TelegramBot().send(chat_id=7, text='when?', eta=datetime.datetime(2030, 1, 1, 9, 0))  # noqa: DTZ001

    assert not TelegramScheduledSend.objects.exists()


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_an_eta_inside_the_bot_container_still_waits(published):
    """`send` there calls Telegram directly, and an `eta` is the one case it must not."""
    instance = TelegramBot()
    instance._polling = True
    try:
        assert instance.is_worker
        instance.send(chat_id=7, text='later', eta=in_a_while())
    finally:
        instance._polling = False

    assert TelegramScheduledSend.objects.count() == 1, 'the worker sent a scheduled message at once'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT={**SETTINGS, 'ENABLED': False})
def test_a_disabled_process_writes_no_row_and_still_answers(published):
    identifier = TelegramBot().send(chat_id=7, text='later', eta=in_a_while())

    assert isinstance(identifier, uuid.UUID)
    assert not TelegramScheduledSend.objects.exists()


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT={**SETTINGS, 'ENABLED': False})
def test_a_disabled_fan_out_does_not_judge_an_eta_it_will_not_use(published):
    """`send` returns before looking at anything; the fan-out was refusing first.

    Two producers judging one argument differently is the defect, whichever of them is
    right -- a disabled process is meant to do nothing and answer with the ids.
    """
    naive = datetime.datetime(2030, 1, 1, 9, 0)  # noqa: DTZ001 - the shape `due_moment` refuses

    assert TelegramBot().send(chat_id=7, text='x', eta=naive) is not None
    identifiers = TelegramBot().send_many([1, 2], text='x', eta=naive)

    assert len(identifiers) == 2
    assert not TelegramScheduledSend.objects.exists()


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT={**SETTINGS, 'ENABLED': False})
def test_the_mover_refuses_where_nothing_can_be_sent(published):
    with pytest.raises(CommandError, match='disabled'):
        call_command('tgbot_dispatch_scheduled')


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_scheduled_send_rolls_back_with_the_transaction_that_made_it(published):
    """It needs nothing from `TRANSACTIONAL`: the row is the caller's own write."""
    with pytest.raises(RuntimeError):
        schedule_and_then_fail()

    assert not TelegramScheduledSend.objects.exists(), 'a rolled-back block left a send scheduled'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_scheduling_is_recorded_with_the_time_it_waits_for(published):
    due = in_a_while()

    TelegramBot().send(chat_id=7, text='later', eta=due)
    recorder.flush(timeout=5)

    row = TelegramEvent.objects.get(kind=EventKind.OUTBOUND_SCHEDULED.value)
    assert row.chat_id == 7
    assert row.detail['due_at'].startswith(due.isoformat()[:16])


@pytest.mark.django_db(transaction=True)
def test_a_broker_that_cannot_be_resolved_claims_nothing(published):
    """A misconfiguration is not a message that failed, and must not cost one.

    Claimed first, the rows would keep a claim nobody owns: no drop row explains them and
    every later pass filters them out, so the messages are invisible until an operator
    clears the claims by hand. `enqueue` resolves the broker before it writes for the same
    reason.
    """
    with override_settings(TELEGRAM_BOT=SETTINGS):
        TelegramBot().send(chat_id=7, text='now', eta=a_while_ago())

    with (
        override_settings(TELEGRAM_BOT={**SETTINGS, 'BROKER': 'tests.db.test_scheduling.NoSuchBroker'}),
        pytest.raises(BrokerNotConfiguredError),
    ):
        call_command('tgbot_dispatch_scheduled')

    assert TelegramScheduledSend.objects.get().claimed_at is None, 'a row was claimed for a broker that does not exist'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_publish_that_fails_is_counted_and_left_for_its_lease(published):
    """The claim stays so the lease paces the retry, and the attempt is counted."""
    TelegramBot().send(chat_id=7, text='doomed', eta=a_while_ago())
    RecordingBroker.refuses = True

    call_command('tgbot_dispatch_scheduled')

    row = TelegramScheduledSend.objects.get()
    assert row.claimed_at is not None
    assert row.attempts == 1, 'the failure was not counted'
    assert TelegramEvent.objects.filter(kind=EventKind.OUTBOUND_DROPPED.value).count() == 1


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_row_the_broker_keeps_refusing_is_given_up_on(published):
    """The lease turned "not retried" into "every lease, for ever", which needs a bound.

    Without one, a payload the broker refuses permanently writes another drop row every
    lease: an event log growing without end over one message, and an operator reading the
    same failure a hundred times.
    """
    TelegramBot().send(chat_id=7, text='doomed', eta=a_while_ago())
    RecordingBroker.refuses = True

    for _ in range(3):
        call_command('tgbot_dispatch_scheduled', '--max-attempts', '3')
        TelegramScheduledSend.objects.update(claimed_until=timezone.now() - datetime.timedelta(seconds=1))

    assert not TelegramScheduledSend.objects.exists(), 'the row is still being retried'
    given_up = TelegramEvent.objects.get(error_code='TooManyAttempts')
    assert given_up.attempt == 3
    assert given_up.detail['stage'] == 'scheduling'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_two_movers_failing_on_one_row_both_count(published):
    """`row.attempts + 1` written twice loses a failure, so the bound arrives late or never.

    After a lease lapses two movers can be counting the same row, and each would write the
    same absolute number over the other's. `F('attempts') + 1` is the database doing the
    arithmetic it is holding the row for.
    """
    TelegramBot().send(chat_id=7, text='doomed', eta=a_while_ago())
    RecordingBroker.refuses = True
    row = TelegramScheduledSend.objects.get()

    # both movers are inside `_count_failure` for the same row, each holding the copy it
    # loaded before the other wrote
    Command()._count_failure(row, attempts=0)
    Command()._count_failure(row, attempts=0)

    assert TelegramScheduledSend.objects.get().attempts == 2, 'one failure was written over the other'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_dry_run_survives_a_limit_the_real_pass_would_clamp(published, capsys):
    """The raw value reached a queryset slice, where Django refuses a negative index."""
    TelegramBot().send(chat_id=7, text='now', eta=a_while_ago())

    call_command('tgbot_dispatch_scheduled', '--dry-run', '--limit', '-1')

    assert '1 due now' in capsys.readouterr().out


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_max_attempts_of_zero_retries_without_end(published):
    """The escape hatch, for a queue an operator would rather never give up on."""
    TelegramBot().send(chat_id=7, text='doomed', eta=a_while_ago())
    RecordingBroker.refuses = True

    for _ in range(4):
        call_command('tgbot_dispatch_scheduled', '--max-attempts', '0')
        TelegramScheduledSend.objects.update(claimed_until=timezone.now() - datetime.timedelta(seconds=1))

    assert TelegramScheduledSend.objects.get().attempts == 4
    assert not TelegramEvent.objects.filter(error_code='TooManyAttempts').exists()


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_scheduled_fan_out_writes_a_row_per_chat(published):
    identifiers = TelegramBot().send_many([1, 2, 3], chunk_size=2, text='digest', eta=a_while_ago())

    assert len(identifiers) == 3
    # each chat gets its own id, which is what makes a per-message cancellation possible --
    # the model's own comment claimed they shared one, and they do not
    assert len(set(identifiers)) == 3
    assert TelegramScheduledSend.objects.values('correlation_id').distinct().count() == 3
    assert TelegramScheduledSend.objects.count() == 3
    assert published == []

    call_command('tgbot_dispatch_scheduled')

    assert len(published) == 3
    assert not TelegramScheduledSend.objects.exists()


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_awaiting_producers_can_schedule_at_all(published):
    """`bulk_create` from a coroutine raises `SynchronousOnlyOperation` -- measured.

    So `aenqueue(eta=...)` did not merely block where its twin blocks, as a comment here
    claimed: it raised, and the whole awaiting half of the surface was unusable.
    """
    identifier = asyncio.run(TelegramBot().aenqueue(chat_id=7, text='awaited', eta=in_a_while()))

    assert TelegramScheduledSend.objects.get().correlation_id == identifier


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_awaiting_fan_out_can_schedule_too(published):
    identifiers = asyncio.run(TelegramBot().asend_many([1, 2], chunk_size=1, text='awaited', eta=in_a_while()))

    assert len(identifiers) == 2
    assert TelegramScheduledSend.objects.count() == 2


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS, USE_TZ=False)
def test_an_aware_eta_is_refused_where_the_database_would_refuse_it(published):
    """Measured: SQLite answers *does not support timezone-aware datetimes when USE_TZ is
    False*, from inside `bulk_create` -- a long way from the `eta` that caused it."""
    with pytest.raises(ImproperlyConfigured, match='naive datetime while USE_TZ is False'):
        TelegramBot().send(chat_id=7, text='when?', eta=datetime.datetime.now(datetime.timezone.utc))

    assert not TelegramScheduledSend.objects.exists()


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_stranded_claim_is_taken_back_after_its_lease(published):
    """A mover that died holding a row must not strand the message for ever.

    The recovery on offer was "an operator clears the claim by hand", which is not one -- so
    a claim is a lease. What it costs is a message going out twice where the mover died
    *after* publishing, which is the trade this package makes everywhere.
    """
    TelegramBot().send(chat_id=7, text='stranded', eta=a_while_ago(3600))
    dead = claim(10)

    assert len(dead) == 1
    assert TelegramScheduledSend.objects.get().claimed_until is not None, 'the claim carries no expiry'
    assert claim(10) == [], 'a fresh claim was taken back before its lease expired'

    # the row carries its own expiry, so this is the mechanism rather than a stand-in for it
    TelegramScheduledSend.objects.update(claimed_until=timezone.now() - datetime.timedelta(seconds=1))

    assert len(claim(10)) == 1, 'a claim past the expiry on its own row was never taken back'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_lease_of_zero_claims_with_no_expiry_at_all(published):
    """The escape hatch for an operator who would rather nothing be re-sent."""
    TelegramBot().send(chat_id=7, text='held', eta=a_while_ago(3600))

    assert len(claim(10, lease=0)) == 1
    assert TelegramScheduledSend.objects.get().claimed_until is None, 'lease 0 still wrote an expiry'

    TelegramScheduledSend.objects.update(claimed_at=timezone.now() - datetime.timedelta(days=7))

    assert claim(10) == [], 'a claim taken with no expiry was taken back anyway'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_lapsed_claim_can_be_called_off_again(published):
    """`claim` and `cancel` have to read the same fact, or they disagree about one row.

    They did: `claim` honoured the lease and `cancel` looked only at `claimed_at`, so a row
    that had come free was publishable and not cancellable -- a caller told "nothing was
    waiting" about a message that was about to go out.
    """
    identifier = TelegramBot().send(chat_id=7, text='lapsing', eta=a_while_ago())
    claim(10)

    assert TelegramBot().cancel_scheduled(identifier) == 0, 'a live claim was cancelled'

    TelegramScheduledSend.objects.update(claimed_until=timezone.now() - datetime.timedelta(seconds=1))

    assert TelegramBot().cancel_scheduled(identifier) == 1, 'a row nobody holds was not cancellable'
    assert not TelegramScheduledSend.objects.exists()


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT={**SETTINGS, 'EVENT_LOG_SYNC': False})
def test_no_event_outlives_the_block_that_rolled_the_schedule_back(published):
    """The rows are the caller's write; the recorder's writer commits on its own.

    Recorded before the commit, the feed keeps a durable `outbound.scheduled` row about a
    send that never existed, and nothing will ever take it back.

    **`EVENT_LOG_SYNC` has to be off for this to be a test at all.** With it on the row is
    written on the calling thread, inside the caller's block, so the rollback removes it
    whatever this code does -- the first draft of this case ran that way and passed with
    `after_commit` taken out.
    """
    with pytest.raises(RuntimeError):
        schedule_and_then_fail()
    recorder.flush(timeout=5)

    assert not TelegramEvent.objects.filter(kind=EventKind.OUTBOUND_SCHEDULED.value).exists()


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT={**SETTINGS, 'EVENT_LOG_SYNC': False})
def test_the_event_does_land_once_the_block_commits(published):
    """The other half: waiting for the commit must not mean waiting for ever."""
    with transaction.atomic():
        TelegramBot().send(chat_id=7, text='later', eta=in_a_while())
    recorder.flush(timeout=5)

    assert TelegramEvent.objects.filter(kind=EventKind.OUTBOUND_SCHEDULED.value).count() == 1


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT={**SETTINGS, 'EVENT_LOG_SYNC': False})
def test_no_event_outlives_a_manually_managed_rollback(published):
    """Autocommit off is not an excuse to record a send that never existed.

    An `atomic()` entered while autocommit is off does not commit on exit, and `TRANSACTIONAL`
    refuses to defer a *publish* there for good reason -- the message would leave when the
    caller happened to restore autocommit. An *event* has no such objection to arriving late,
    and every objection to arriving at all when the row was rolled back. So `after_commit`
    tests a weaker condition than `defer` does, and this is the case that separates them:
    measured, the hook queued here runs after `commit()` and not after `rollback()`.

    Falsifiable: with `after_commit` reading `defer`'s predicate, the event is recorded inline
    and this row survives the rollback.
    """
    transaction.set_autocommit(False)
    try:
        with pytest.raises(RuntimeError):
            schedule_and_then_fail()
        transaction.rollback()
    finally:
        transaction.set_autocommit(True)
    recorder.flush(timeout=5)

    assert not TelegramScheduledSend.objects.exists(), 'the rollback left the row behind'
    assert not TelegramEvent.objects.filter(kind=EventKind.OUTBOUND_SCHEDULED.value).exists()


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT={**SETTINGS, 'EVENT_LOG_SYNC': False})
def test_the_event_lands_when_a_manually_managed_block_commits(published):
    """The other half again: the weaker condition must not swallow the event either."""
    transaction.set_autocommit(False)
    try:
        with transaction.atomic():
            TelegramBot().send(chat_id=7, text='later', eta=in_a_while())
        transaction.commit()
    finally:
        transaction.set_autocommit(True)
    recorder.flush(timeout=5)

    assert TelegramEvent.objects.filter(kind=EventKind.OUTBOUND_SCHEDULED.value).count() == 1


def test_an_unlimited_retry_count_has_no_column_to_overflow():
    """`--max-attempts 0` retries for ever, so `attempts` must have nowhere to stop.

    A `PositiveSmallIntegerField` ends at 32767 -- four months of the default 300-second lease
    -- and PostgreSQL then refuses the increment, which fails the pass rather than the row.
    Asserted against Django's own portable ranges and not against this backend: SQLite
    enforces no range at all and reports none, so a case that drove the real column here would
    have been green with the narrow field in place.
    """
    from django.db.backends.base.operations import BaseDatabaseOperations

    field = TelegramScheduledSend._meta.get_field('attempts')
    floor, ceiling = BaseDatabaseOperations.integer_field_ranges[field.get_internal_type()]

    assert floor == 0, 'a count of failures went signed'
    assert ceiling == 2**63 - 1, 'the retry counter has a ceiling a failing row can reach'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_bound_past_the_feed_s_column_still_records_the_give_up(published):
    """`--max-attempts` takes any number, and the event it writes has the narrow column.

    Widening that one is a rewrite of the largest table the package has, for a value nobody
    reads past a handful -- so `eventlog.writer` saturates it, the way it already truncates
    text to its width. The exact count stays on the schedule row, which is where the mover
    reads it from anyway.
    """
    identifier = TelegramBot().send(chat_id=7, text='hopeless', eta=a_while_ago())
    TelegramScheduledSend.objects.update(attempts=40000)
    RecordingBroker.refuses = True
    call_command('tgbot_dispatch_scheduled', '--max-attempts', '40001')

    # by its code and not its kind: the failed publish itself records a drop as well
    dropped = TelegramEvent.objects.get(error_code='TooManyAttempts', correlation_id=identifier)
    assert dropped.attempt == 32767, 'the feed stored a count its column cannot hold'
    assert dropped.detail['attempts'] == 40001, 'the exact count was lost as well as saturated'


@pytest.mark.django_db(transaction=True)
@override_settings(
    TELEGRAM_BOT={**SETTINGS, 'EVENT_LOG_DATABASE': 'default'},
    DATABASE_ROUTERS=['django_aiogram.eventlog.dbrouter.TelegramEventLogRouter'],
)
def test_naming_the_default_alias_for_the_log_still_leaves_the_schedule_somewhere(published):
    """`EVENT_LOG_DATABASE: 'default'` says the log lives with everything else.

    Keeping the schedule off "the log's database" then refused it on the only database there
    is, so `migrate` created the table nowhere and every `eta` send failed on its first
    write. The refusal is about a log database *of its own*.
    """
    from django.db import router

    assert router.allow_migrate('default', 'django_aiogram', model=TelegramScheduledSend) is True
    assert router.allow_migrate('default', 'django_aiogram', model=TelegramEvent) is True

    identifier = TelegramBot().send(chat_id=7, text='later', eta=in_a_while())
    assert TelegramScheduledSend.objects.get().correlation_id == identifier


@pytest.mark.django_db(transaction=True, databases=['default', 'logs'])
@override_settings(
    TELEGRAM_BOT={**SETTINGS, 'EVENT_LOG_DATABASE': 'logs'},
    DATABASE_ROUTERS=['django_aiogram.eventlog.dbrouter.TelegramEventLogRouter'],
)
def test_the_schedule_is_not_created_on_the_log_database(published):
    """`None` from `allow_migrate` means *no opinion*, so Django created it on both.

    Measured before the fix: the table existed on `default` and on `logs`, and the copy on
    the log alias is never read or written -- which is why it should not be there.

    **Introspection cannot answer this here, which is why the assertions are about the
    decision instead.** The suite's databases are migrated when the runner sets them up,
    before any `override_settings` router exists, so `django_aiogram_scheduled` is present on
    every alias whatever this router says -- a `not in table_names()` case would fail with the
    fix in place, and an `in table_names()` one passes without it. What decides at migrate
    time is `django.db.router`, so that is what is asked, through the settings rather than by
    hand: a class that returns the right answers while nothing consults it is the failure this
    shape rules out.
    """
    from django.db import router

    assert router.allow_migrate('logs', 'django_aiogram', model=TelegramScheduledSend) is False
    assert router.allow_migrate('default', 'django_aiogram', model=TelegramScheduledSend) is True
    assert router.allow_migrate('logs', 'django_aiogram', model=TelegramEvent) is True
