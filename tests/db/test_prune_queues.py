"""What becomes of a client's queue once no bot publishes to it.

A queue per client is what keeps one client's backlog off another's, and it is also how a
deployment leaks: a Redis key, an AMQP queue or a consumer group per client that ever
existed. Every case here is about the three answers to that, and about the two things the
command must never do -- remove a queue a bot still names, or decide anything by itself.
"""

import pytest
from django.core.management import CommandError, call_command
from django.test import override_settings

from django_aiogram.models import TelegramBot, TelegramQueue
from django_aiogram.runtime import groups

pytestmark = pytest.mark.django_db

MEMORY = {'BROKER': 'django_aiogram.testing.InMemoryBroker', 'FSM_STORAGE': 'memory', 'TOKEN': '123456:AAaa'}


@pytest.fixture(autouse=True)
def _no_groups_left_behind():
    """A group holds a transport, and one left behind decides the next case's answer."""
    groups.close_groups()
    yield
    groups.close_groups()


def out(*flags):
    """Run the command and hand back what it wrote."""
    from io import StringIO

    written = StringIO()
    call_command('tgbot_prune_queues', *flags, stdout=written)
    return written.getvalue()


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUES': ('gone',)})
def test_a_queue_a_bot_still_names_is_left_alone():
    """A bot switched off has not given up its backlog, so its queue is not unreferenced."""
    queue = TelegramQueue.objects.create(name='gone')
    TelegramBot.objects.create(bot_id=123456, token='123456:AAaa', queue=queue, enabled=False)

    assert out('--policy', 'drop') == ''
    assert TelegramQueue.objects.filter(name='gone').exists()


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUES': ('gone',)})
def test_parking_reports_the_queue_and_removes_nothing():
    """The default, and the one that destroys nothing: an operator decides."""
    TelegramQueue.objects.create(name='gone')

    written = out()

    assert 'gone' in written
    assert 'parked' in written
    assert TelegramQueue.objects.filter(name='gone').exists()


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUES': ('gone',)})
def test_dropping_removes_the_queue_and_its_row():
    """With whatever is still in it: the caller has already decided that."""
    TelegramQueue.objects.create(name='gone')

    written = out('--policy', 'drop')

    assert 'removed' in written
    assert not TelegramQueue.objects.filter(name='gone').exists()


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUES': ('gone',)})
def test_holding_waits_until_the_queue_is_empty():
    """The middle answer: remove it, but not while it still holds messages."""
    TelegramQueue.objects.create(name='gone')
    # through `settings_for`, which is what the command resolves: a partial dict is a
    # different profile, so it would be a different in-memory queue and the case would be
    # asserting against an empty one
    from django_aiogram.runtime.queues import settings_for

    groups.group_for(settings_for('gone')).broker.publish([b'{}'])

    written = out('--policy', 'hold')

    assert 'held' in written
    assert TelegramQueue.objects.filter(name='gone').exists()


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUES': ('gone',)})
def test_a_dry_run_says_what_would_happen_and_changes_nothing():
    """Because the alternative to reading the plan is finding out from the transport."""
    TelegramQueue.objects.create(name='gone')

    written = out('--policy', 'drop', '--dry-run')

    assert 'would be removed' in written
    assert TelegramQueue.objects.filter(name='gone').exists()


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUES': ('mine',)})
def test_naming_a_queue_a_bot_publishes_to_is_refused():
    """Named by hand, the answer is a refusal rather than a queue quietly skipped."""
    queue = TelegramQueue.objects.create(name='mine')
    TelegramBot.objects.create(bot_id=123456, token='123456:AAaa', queue=queue)

    with pytest.raises(CommandError, match='still have bots'):
        out('--queue', 'mine', '--policy', 'drop')


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'REMOVED_QUEUE_POLICY': 'burn'})
def test_a_policy_nobody_has_is_refused_by_name():
    """`E061` reports it at boot; this is the same refusal where the command is the reader."""
    TelegramQueue.objects.create(name='gone')

    with pytest.raises(CommandError, match='park'):
        out()


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUES': ('one', 'two')})
def test_only_the_queues_named_are_touched():
    """A run bounded by hand is how an operator removes one client's queue and no others."""
    TelegramQueue.objects.create(name='one')
    TelegramQueue.objects.create(name='two')

    out('--queue', 'one', '--policy', 'drop')

    assert [row.name for row in TelegramQueue.objects.all()] == ['two']
