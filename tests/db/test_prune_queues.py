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


def broker_for(queue):
    """The transport the command will reach for that queue, so a case can seed and read it."""
    from django_aiogram.runtime.queues import settings_for

    return groups.group_for(settings_for(queue)).broker


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
    broker_for('gone').publish([b'{}'])

    assert out('--policy', 'drop') == ''
    assert TelegramQueue.objects.filter(name='gone').exists()
    assert broker_for('gone').depth() == 1, "a switched-off client's messages were thrown away"


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUES': ('gone',)})
def test_parking_reports_the_queue_and_removes_nothing():
    """The default, and the one that destroys nothing: an operator decides."""
    TelegramQueue.objects.create(name='gone')
    broker_for('gone').publish([b'{}'])

    written = out()

    assert 'gone' in written
    assert 'parked' in written
    assert TelegramQueue.objects.filter(name='gone').exists()
    assert broker_for('gone').depth() == 1, 'parking removed something'


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUES': ('gone',)})
def test_dropping_removes_the_queue_and_its_row():
    """With whatever is still in it: the caller has already decided that."""
    TelegramQueue.objects.create(name='gone')
    broker_for('gone').publish([b'{}'])

    written = out('--policy', 'drop')

    assert 'removed' in written
    assert not TelegramQueue.objects.filter(name='gone').exists()
    assert broker_for('gone').depth() == 0, 'the row went and the queue stayed'


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
    assert broker_for('gone').depth() == 1, 'a queue reported as held was emptied'


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUES': ('gone',)})
def test_a_dry_run_says_what_would_happen_and_changes_nothing():
    """Because the alternative to reading the plan is finding out from the transport."""
    TelegramQueue.objects.create(name='gone')

    broker_for('gone').publish([b'{}'])

    written = out('--policy', 'drop', '--dry-run')

    assert 'would be removed' in written
    assert TelegramQueue.objects.filter(name='gone').exists()
    assert broker_for('gone').depth() == 1, 'a dry run removed something'


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


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUES': ('gone',)})
def test_a_bot_pointed_at_the_queue_while_the_run_was_going_is_the_answer():
    """The candidates come from a query, and a client's bot can arrive in the seconds since.

    Removing it then destroys a live client's messages -- and the row delete would fail
    anyway, since `TelegramBot.queue` is `PROTECT`. Locked and re-read in the transaction that
    deletes, so the two cannot disagree.
    """
    queue = TelegramQueue.objects.create(name='gone')
    broker_for('gone').publish([b'{}'])
    from unittest import mock

    from django_aiogram.management.commands import tgbot_prune_queues as command_module

    original = command_module.Command._remove

    def points_a_bot_at_it_first(self, row, broker, policy, held):
        TelegramBot.objects.create(bot_id=123456, token='123456:AAaa', queue=queue)
        return original(self, row, broker, policy, held)

    with mock.patch.object(command_module.Command, '_remove', points_a_bot_at_it_first):
        written = out('--policy', 'drop')

    assert 'left alone' in written, written
    assert TelegramQueue.objects.filter(name='gone').exists()
    assert broker_for('gone').depth() == 1, "a live client's messages were thrown away"


@override_settings(
    TELEGRAM_BOT_DEFAULTS={
        'BROKER': 'django_aiogram.broker.kafka.KafkaBroker',
        'KAFKA_BOOTSTRAP': 'localhost:9092',
        'KAFKA_TOPIC': 'gone',
        'TOKEN': '123456:AAaa',
        'QUEUES': ('gone',),
    }
)
def test_a_dry_run_says_a_transport_cannot_remove_rather_than_promising_it_would():
    """Asked of the capability, not discovered by trying: a dry run may not discover anything.

    Reported the other way round, an operator reads "would be removed", runs it for real, and
    is told the topic is theirs to drop after all.
    """
    TelegramQueue.objects.create(name='gone')

    written = out('--policy', 'drop', '--dry-run')

    assert 'cannot remove a queue' in written, written
    assert 'would be removed' not in written


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUES': ('gone',)})
def test_a_queue_holding_a_taken_message_is_held_rather_than_removed():
    """`depth` is what is waiting; a taken message is being sent, and that is not empty."""
    TelegramQueue.objects.create(name='gone')
    broker = broker_for('gone')
    broker.publish([b'{}'])
    broker.take_nowait()

    written = out('--policy', 'hold')

    assert 'held' in written, written
    assert TelegramQueue.objects.filter(name='gone').exists()
    assert broker.inflight_depth() == 1, 'a message a consumer was sending was thrown away'
