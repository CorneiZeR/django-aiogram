"""Reading back what became of one message, by the id `send()` handed out.

The write side is `test_outbound.py`'s subject; this one is about the answer a process that
only queued the send can get, and about the two configurations where there is no answer to
be had and saying so beats a permanent `unknown`.
"""

import asyncio
import datetime
import uuid

import pytest
from django.test import override_settings

from django_aiogram import TelegramBot
from django_aiogram.config.enums import EventKind, OutcomeState
from django_aiogram.eventlog.outcomes import aoutcome, outcome
from django_aiogram.eventlog.recorder import recorder
from django_aiogram.eventlog.records import Event
from django_aiogram.exceptions import OutcomesUnavailableError
from django_aiogram.models import TelegramEvent

SETTINGS = {'EVENT_LOG': True, 'EVENT_LOG_SYNC': True, 'TOKEN': '42:x', 'FSM_STORAGE': 'memory', 'RATE_LIMIT': None}


class Sent:
    """What aiogram hands back, carrying the only id Telegram gives."""

    message_id = 4321


def a_bot(behavior):
    """A stand-in for the aiogram Bot, with a session close() that does nothing."""

    class Fake:
        async def send_message(self, **kwargs):
            return behavior(kwargs)

        class session:
            @staticmethod
            async def close():
                return None

    return Fake()


def record(identifier, kind, **fields):
    """Write one row through the recorder, which is the path a real send takes."""
    recorder.record(Event(kind=kind.value, correlation_id=identifier, function='send_message', **fields))


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_send_and_then_its_outcome_is_the_whole_point():
    """End to end, because every test below builds the rows by hand instead.

    `send_raw` is what the bot container runs; `outcome` is what the web tier asks. Nothing
    passes between them but the correlation id and the table.
    """
    instance = TelegramBot()
    instance._bot = a_bot(lambda _kwargs: Sent())
    try:
        identifier = instance.send_raw(chat_id=7, text='hi')
        recorder.flush(timeout=5)
    finally:
        instance._bot = None
        instance.close()

    answer = instance.outcome(identifier)

    assert answer.state is OutcomeState.SENT
    assert answer.message_id == Sent.message_id, 'the id Telegram gave did not come back'
    assert answer.chat_id == 7
    assert answer.at is not None


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_failed_send_is_told_apart_from_one_nothing_recorded():
    failed, silent = uuid.uuid4(), uuid.uuid4()
    record(failed, EventKind.OUTBOUND_FAILED, error='telegram said no', error_code='RuntimeError', attempt=3)

    assert outcome(failed).state is OutcomeState.FAILED
    assert outcome(failed).error == 'telegram said no'
    assert outcome(failed).attempt == 3
    assert outcome(failed).message_id is None
    assert outcome(silent).state is OutcomeState.UNKNOWN


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
@pytest.mark.parametrize(
    'kind',
    [EventKind.OUTBOUND_QUEUED, EventKind.OUTBOUND_CONSUMED, EventKind.OUTBOUND_RETRIED],
)
def test_a_message_still_on_its_way_is_pending(kind):
    identifier = uuid.uuid4()
    record(identifier, kind)

    assert outcome(identifier).state is OutcomeState.PENDING


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_sent_row_settles_the_state_whatever_was_written_after_it():
    """A later `retried` belongs to another message under the same id, not to this one.

    Deciding from the newest row alone would call a delivered message pending, which is the
    one answer a caller acts on by waiting for something that already happened.
    """
    identifier = uuid.uuid4()
    record(identifier, EventKind.OUTBOUND_SENT, chat_id=7, message_id=11)
    record(identifier, EventKind.OUTBOUND_RETRIED, chat_id=8, attempt=1)

    assert outcome(identifier).state is OutcomeState.SENT
    assert outcome(identifier).message_id == 11


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_several_messages_under_one_id_all_come_back_newest_first():
    """A handler's replies inherit the update's id, so one id can name several messages."""
    identifier = uuid.uuid4()
    record(identifier, EventKind.OUTBOUND_SENT, chat_id=7, message_id=11)
    record(identifier, EventKind.OUTBOUND_SENT, chat_id=7, message_id=12)

    answer = outcome(identifier)

    assert [message.message_id for message in answer.sent] == [12, 11]
    assert answer.message_id == 12, 'the convenience reader is not the newest message'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT={**SETTINGS, 'EVENT_LOG': False})
def test_the_log_being_off_is_a_refusal_rather_than_a_permanent_unknown():
    with pytest.raises(OutcomesUnavailableError, match='EVENT_LOG'):
        outcome(uuid.uuid4())


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT={**SETTINGS, 'EVENT_LOG_KINDS': ('outbound.queued',)})
def test_kinds_that_leave_the_result_out_are_refused_too():
    """The send is recorded and its result is not, so the answer would never arrive."""
    with pytest.raises(OutcomesUnavailableError, match='EVENT_LOG_KINDS'):
        outcome(uuid.uuid4())


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_id_may_arrive_as_the_string_a_project_stored():
    identifier = uuid.uuid4()
    record(identifier, EventKind.OUTBOUND_SENT, chat_id=7, message_id=11)

    assert outcome(str(identifier)).message_id == 11
    with pytest.raises(ValueError, match='must be a UUID'):
        outcome('not an id')


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_awaiting_twin_answers_the_same():
    identifier = uuid.uuid4()
    record(identifier, EventKind.OUTBOUND_SENT, chat_id=7, message_id=11)

    answer = asyncio.run(aoutcome(identifier))

    assert answer.state is OutcomeState.SENT
    assert answer.message_id == 11


@pytest.mark.django_db(transaction=True, databases=['default', 'logs'])
@override_settings(TELEGRAM_BOT={**SETTINGS, 'EVENT_LOG_DATABASE': 'logs'})
def test_it_reads_the_alias_the_log_is_written_to():
    """Read from `default`, every outcome on a project with a log database of its own is
    `unknown` -- which reads as "not yet" for a message that was delivered."""
    identifier = uuid.uuid4()
    record(identifier, EventKind.OUTBOUND_SENT, chat_id=7, message_id=11)

    assert TelegramEvent.objects.using('logs').filter(correlation_id=identifier).exists()
    assert not TelegramEvent.objects.using('default').filter(correlation_id=identifier).exists()
    assert outcome(identifier).message_id == 11


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_recorded_instant_comes_back_as_a_datetime():
    """`at` is what a caller compares against its own timestamps, so it is not a float."""
    identifier = uuid.uuid4()
    record(identifier, EventKind.OUTBOUND_SENT, chat_id=7, message_id=11)

    assert isinstance(outcome(identifier).at, datetime.datetime)
