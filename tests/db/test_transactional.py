"""`TRANSACTIONAL`: when the queue write happens, relative to the caller's transaction.

Every test here needs `transaction=True`, and that is not a detail: Django's plain
`TestCase` wraps each test in an atomic block that never commits, so `on_commit` would
never run and every assertion below would be about the harness rather than the package.
"""

import asyncio
import logging
from typing import ClassVar

import pytest
from django.db import transaction
from django.test import override_settings

from django_aiogram.broker.redis_list import RedisListBroker
from django_aiogram.exceptions import SerializationError
from django_aiogram.models import TelegramEvent
from django_aiogram.producer import committing
from django_aiogram.producer.client import TelegramBot

BROKER = 'tests.db.test_transactional.RecordingBroker'
SETTINGS = {'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost:6379/0', 'BROKER': BROKER, 'TRANSACTIONAL': True}
LOGGED = {**SETTINGS, 'EVENT_LOG': True, 'EVENT_LOG_SYNC': True}


class RecordingBroker(RedisListBroker):
    """Keeps what it was asked to publish, and refuses on demand, without a server."""

    published: ClassVar[list[bytes]] = []
    refuses: ClassVar[bool] = False

    def publish(self, payloads):
        if RecordingBroker.refuses:
            raise ConnectionError('the broker refused the write')
        RecordingBroker.published.extend(payloads)

    async def apublish(self, payloads):
        self.publish(payloads)


def announce_and_then_fail():
    """The block this is all about: a row, a message about it, and then a rollback."""
    with transaction.atomic():
        TelegramBot().send(chat_id=1, text='the order was accepted')
        raise RuntimeError('and then the block failed')


@pytest.fixture
def published():
    """The payloads this test's sends reached the broker with."""
    RecordingBroker.published.clear()
    RecordingBroker.refuses = False
    committing._manual_mentioned.clear()
    yield RecordingBroker.published
    RecordingBroker.published.clear()
    RecordingBroker.refuses = False


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_rolled_back_block_queues_nothing(published):
    with pytest.raises(RuntimeError):
        announce_and_then_fail()

    assert published == [], 'a message went out for a transaction that rolled back'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_write_waits_for_the_commit_rather_than_being_skipped(published):
    """Both halves: it does not happen inside the block, and it does happen after it."""
    with transaction.atomic():
        TelegramBot().send(chat_id=1, text='the order was accepted')
        assert published == [], 'the write did not wait for the commit'

    assert len(published) == 1


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_send_outside_a_transaction_publishes_where_it_stands(published):
    """There is no commit to wait for, so the setting has nothing to change."""
    TelegramBot().send(chat_id=1, text='no transaction here')

    assert len(published) == 1


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT={**SETTINGS, 'TRANSACTIONAL': False})
def test_with_the_setting_off_the_write_still_happens_inside_the_block(published):
    """The behaviour every release before 4.1 had, pinned so the default cannot drift."""
    with transaction.atomic():
        TelegramBot().send(chat_id=1, text='the order was accepted')
        assert len(published) == 1


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_fan_out_defers_every_chunk(published):
    with transaction.atomic():
        TelegramBot().send_many([1, 2, 3], chunk_size=2, text='to everyone')
        assert published == []

    assert len(published) == 3


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_awaiting_producer_has_no_transaction_to_wait_for(published):
    """Pinned rather than aspired to: a coroutine holds its own connection.

    Measured — inside `asyncio.run` the `default` connection is a different object and its
    `in_atomic_block` is False — so the block below is invisible to the send. Django has no
    asynchronous transactions either, so there is nothing here this could defer to.
    """
    with transaction.atomic():
        asyncio.run(TelegramBot().aenqueue(chat_id=1, text='awaited'))
        assert len(published) == 1


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_payload_that_cannot_be_serialized_raises_where_the_call_was_written(published):
    """Serialization does not wait, so the traceback points at the send and not at a hook."""
    with transaction.atomic(), pytest.raises(SerializationError):
        TelegramBot().send(chat_id=1, text=object())

    assert published == []


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_arguments_are_snapshotted_at_the_call_not_at_the_commit(published):
    """A dict the caller kept and changed cannot rewrite a send that already returned."""
    kwargs = {'chat_id': 1, 'text': 'as it was written'}
    with transaction.atomic():
        TelegramBot().send(**kwargs)
        kwargs['text'] = 'as it was changed'

    assert b'as it was written' in published[0]


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_publish_that_fails_after_the_commit_does_not_reach_the_caller(published, caplog):
    """The transaction has landed by then, so raising would only break the hooks behind it."""
    RecordingBroker.refuses = True

    with caplog.at_level(logging.ERROR, logger='django_aiogram'), transaction.atomic():
        TelegramBot().send(chat_id=1, text='the order was accepted')

    assert 'a deferred publish failed after its transaction committed' in caplog.text


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT={**SETTINGS, 'RAISE_EXCEPTION': True})
def test_raise_exception_lets_a_failed_deferred_publish_out(published):
    RecordingBroker.refuses = True

    with pytest.raises(ConnectionError), transaction.atomic():
        TelegramBot().send(chat_id=1, text='the order was accepted')


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_autocommit_off_publishes_where_it_stands_and_says_so_once(published, caplog):
    """`on_commit` refuses a manually managed transaction, so honouring the setting there
    would turn every send into a failure. Publishing now is what it falls back to."""
    transaction.set_autocommit(False)
    try:
        with caplog.at_level(logging.WARNING, logger='django_aiogram'):
            TelegramBot().send(chat_id=1, text='one')
            TelegramBot().send(chat_id=2, text='two')
    finally:
        transaction.rollback()
        transaction.set_autocommit(True)

    said = [record for record in caplog.records if 'no commit hook to wait on' in record.message]
    assert len(published) == 2
    assert len(said) == 1, 'the fallback said so once per send rather than once per process'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=LOGGED)
def test_the_queued_row_waits_with_the_write(published):
    """Nothing is recorded for a message that was never queued."""
    with pytest.raises(RuntimeError):
        announce_and_then_fail()

    assert TelegramEvent.objects.count() == 0

    with transaction.atomic():
        TelegramBot().send(chat_id=1, text='the order was accepted')
        assert TelegramEvent.objects.count() == 0

    assert [event.kind for event in TelegramEvent.objects.all()] == ['outbound.queued']


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=LOGGED)
def test_a_deferred_publish_that_failed_is_recorded_as_a_queueing_drop(published):
    """The same stage an immediate failure records: the write may still have been applied."""
    RecordingBroker.refuses = True

    with transaction.atomic():
        TelegramBot().send(chat_id=1, text='the order was accepted')

    dropped = TelegramEvent.objects.filter(kind='outbound.dropped')
    assert [event.detail['stage'] for event in dropped] == ['queueing']
