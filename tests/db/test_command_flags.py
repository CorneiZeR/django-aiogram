"""Every command acts on the bots and queues it was told about, and on nothing else.

`tgbot_reclaim` requeueing another client's in-flight messages, or the mover publishing one
client's row to another's queue, is the kind of failure only production finds — and it finds
it as messages arriving where nobody expected them.
"""

from io import StringIO

import pytest
from django.core.management import CommandError, call_command
from django.test import override_settings
from django.utils import timezone

from django_aiogram.models import TelegramEvent, TelegramScheduledSend
from django_aiogram.runtime import providers

pytestmark = pytest.mark.django_db

SETTINGS = {
    'BROKER': 'django_aiogram.testing.InMemoryBroker',
    'FSM_STORAGE': 'memory',
    'EVENT_LOG': True,
}


@pytest.fixture(autouse=True)
def _no_read_kept():
    """The providers cache by watermark, and these cases write rows under it."""
    providers.forget()
    yield
    providers.forget()


def a_row(**kwargs):
    """One feed row, written around the recorder the way the other feed cases do."""
    from django_aiogram.eventlog.events import new_correlation_id, short_id

    identifier = new_correlation_id()
    fields = {
        'kind': 'outbound.failed',
        'correlation_id': identifier,
        'short_id': short_id(identifier),
        'function': 'send_message',
        'chat_id': 1,
        'detail': {'kwargs': {'chat_id': 1, 'text': 'hi'}},
    }
    fields.update(kwargs)
    return TelegramEvent.objects.create(**fields)


def a_scheduled(**kwargs):
    """One row waiting for its moment, due now unless a case says otherwise."""
    from django_aiogram.eventlog.events import new_correlation_id

    fields = {
        'correlation_id': new_correlation_id(),
        'function': 'send_message',
        'chat_id': 1,
        'payload': b'{}',
        'due_at': timezone.now() - timezone.timedelta(seconds=1),
    }
    fields.update(kwargs)
    return TelegramScheduledSend.objects.create(**fields)


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_pruning_one_bots_history_leaves_the_others():
    """A client who left takes their history with them, and nobody else's."""
    old = timezone.now() - timezone.timedelta(days=90)
    a_row(bot_id=111111, created_at=old)
    a_row(bot_id=222222, created_at=old)

    call_command('tgbot_prune_events', days=30, bot=[111111], stdout=StringIO())

    assert {row.bot_id for row in TelegramEvent.objects.all()} == {222222}


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_replay_sends_only_the_bot_it_was_told_about():
    """An incident is one client's, and a replay is the one command that *sends*."""
    a_row(bot_id=111111)
    a_row(bot_id=222222)
    out = StringIO()

    call_command('tgbot_replay', since='2000-01-01', bot=[111111], dry_run=True, stdout=out)

    said = out.getvalue()
    assert '1 ' in said, said
    assert TelegramEvent.objects.filter(kind='outbound.replayed').count() == 0


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_the_mover_claims_only_the_rows_it_was_told_about():
    """Claiming another client's row publishes it to their queue from a container nobody asked."""
    mine = a_scheduled(bot_id=111111)
    theirs = a_scheduled(bot_id=222222)

    call_command('tgbot_dispatch_scheduled', bot=[111111], stdout=StringIO(), stderr=StringIO())

    assert not TelegramScheduledSend.objects.filter(pk=mine.pk).exists(), 'the named bot was not moved'
    assert TelegramScheduledSend.objects.filter(pk=theirs.pk).exists(), "another bot's row was taken"


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_dry_run_of_the_mover_counts_only_the_rows_it_was_told_about():
    """A rehearsal that counted every bot promises a pass this one will not make."""
    a_scheduled(bot_id=111111)
    a_scheduled(bot_id=222222)
    out = StringIO()

    call_command('tgbot_dispatch_scheduled', bot=[111111], dry_run=True, stdout=out)

    assert '1 due now' in out.getvalue(), out.getvalue()


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'BROKER': 'django_aiogram.broker.redis_list.RedisListBroker'})
def test_reclaiming_from_an_undeclared_queue_is_refused():
    """A typo would otherwise read an empty list and report that nothing is in flight.

    Which is indistinguishable from the queue having been drained already — the one answer
    that stops somebody looking further.
    """
    from django_aiogram.models import TelegramQueue

    TelegramQueue.objects.create(name='client-a', pool='default')

    with pytest.raises(CommandError, match='not a declared queue'):
        call_command('tgbot_reclaim', worker='dead-worker', queue='client-z', stdout=StringIO())
