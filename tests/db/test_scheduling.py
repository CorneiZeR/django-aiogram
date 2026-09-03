"""Sends that wait for a time: writing them down, moving them, calling them off.

The wait lives in a table because three of the four transports cannot delay a message at
all, so these tests are about the table and the mover rather than about any broker -- the
publish itself is the same one every other send makes, and the point is that it happens
exactly once, at the right moment, with the bytes the caller meant.
"""

import datetime
import uuid

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.core.management import CommandError, call_command
from django.db import transaction
from django.test import override_settings
from django.utils import timezone

from django_aiogram import TelegramBot
from django_aiogram.broker.redis_list import RedisListBroker
from django_aiogram.config.enums import EventKind
from django_aiogram.eventlog.recorder import recorder
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
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_publish_that_fails_leaves_the_row_claimed_and_says_so(published):
    """Not retried by the next pass: the claim stays, and the drop row is the decision."""
    TelegramBot().send(chat_id=7, text='doomed', eta=a_while_ago())
    RecordingBroker.refuses = True

    call_command('tgbot_dispatch_scheduled')

    row = TelegramScheduledSend.objects.get()
    assert row.claimed_at is not None
    assert TelegramEvent.objects.filter(kind=EventKind.OUTBOUND_DROPPED.value).exists()


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_scheduled_fan_out_writes_a_row_per_chat(published):
    identifiers = TelegramBot().send_many([1, 2, 3], chunk_size=2, text='digest', eta=a_while_ago())

    assert len(identifiers) == 3
    assert TelegramScheduledSend.objects.count() == 3
    assert published == []

    call_command('tgbot_dispatch_scheduled')

    assert len(published) == 3
    assert not TelegramScheduledSend.objects.exists()
