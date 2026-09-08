"""One container consuming several queues, and where it is told which.

A queue is the isolation boundary, so serving more than one is what makes a VIP client's
queue worth having: the container is the same, the backlogs are not. These are about the set
of queues one run consumes -- named, selected by pool, or the one queue the settings name.
"""

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.core.management import CommandError, call_command
from django.db import OperationalError
from django.test import override_settings

from django_aiogram.models import TelegramQueue
from django_aiogram.runtime.queues import served_by, settings_for

pytestmark = pytest.mark.django_db

MEMORY = {
    'BROKER': 'django_aiogram.testing.InMemoryBroker',
    'FSM_STORAGE': 'memory',
    'TOKEN': '123456:AAaa',
}


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUES': ('vip', 'bulk')})
def test_the_queues_named_are_the_queues_served():
    """`--queues default,vip` is the Celery model, and the order asked for is kept."""
    assert served_by(['bulk', 'vip'], []) == ('bulk', 'vip')


@override_settings(TELEGRAM_BOT_DEFAULTS=MEMORY)
def test_a_container_told_nothing_serves_the_queue_its_settings_name():
    """Every deployment before this, and it must keep working with no flags at all."""
    assert served_by([], []) == ('',)

    with override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUES': ('vip',), 'QUEUE': 'vip'}):
        assert served_by([], []) == ('vip',)


@override_settings(TELEGRAM_BOT_DEFAULTS=MEMORY)
def test_a_pool_serves_a_queue_created_after_the_container_started():
    """Which is what enumeration cannot do: the client arrived after the deploy.

    No globs — a glob includes a queue by the accident of its name, and a client called
    `vip-2` would join the pool by being spelled that way.
    """
    TelegramQueue.objects.create(name='client-1', pool='vip')
    TelegramQueue.objects.create(name='client-2', pool='vip')
    TelegramQueue.objects.create(name='everybody', pool='default')

    assert served_by([], ['vip']) == ('client-1', 'client-2')


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUES': ('extra',)})
def test_a_queue_named_and_a_pool_asked_for_are_one_set_without_repeats():
    """A container moving a client between pools is told both, and serves each queue once."""
    TelegramQueue.objects.create(name='client-1', pool='vip')

    assert served_by(['extra', 'client-1'], ['vip']) == ('extra', 'client-1')


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUES': ('vip',)})
def test_a_queue_nothing_declares_is_refused_on_the_consuming_side_too():
    """A container consuming a queue nobody publishes to looks healthy and delivers nothing."""
    with pytest.raises(ImproperlyConfigured, match='not declared'):
        served_by(['viip'], [])


@override_settings(TELEGRAM_BOT_DEFAULTS=MEMORY)
def test_pools_that_cannot_be_read_are_a_failure_rather_than_an_empty_set(monkeypatch):
    """A pool is the table's answer and nothing else's, so an unreadable table is not "none".

    Read as an empty set, this container consumes nothing while every probe reads it as
    healthy — the failure the refusals here exist to prevent, arriving through the door
    marked *the database blinked*.
    """

    def refuse(*args, **kwargs):
        msg = 'the database is not reachable'
        raise OperationalError(msg)

    monkeypatch.setattr(TelegramQueue.objects, 'filter', refuse)

    with pytest.raises(OperationalError):
        served_by([], ['vip'])


@override_settings(TELEGRAM_BOT_DEFAULTS=MEMORY)
def test_a_pool_that_holds_nothing_refuses_the_run():
    """Asking for a pool with no queues is not the same as asking for nothing."""
    with pytest.raises(CommandError, match='hold none'):
        call_command('start_tgbot', '--pools', 'empty')


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUES': ('vip', 'bulk')})
def test_each_queue_gets_its_own_settings():
    """What a consumer of one queue is built from, and the only key that differs."""
    vip = settings_for('vip')

    assert vip['QUEUE'] == 'vip'
    assert vip['BROKER'] == MEMORY['BROKER']
    assert settings_for('bulk')['QUEUE'] == 'bulk'


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUES': ('vip', 'bulk')})
def test_a_container_serving_two_queues_delivers_a_message_queued_on_the_second():
    """The claim #117 is for: two queues, one container, and the second one is not the default.

    Asserted through the consumers the command builds rather than through a hand-built pair,
    because what is being tested is that each of them reads *its own* queue: one transport
    each, one in-flight budget each, and a backlog on one is a backlog on one.
    """
    from django_aiogram.consumer.delivery import get_delivery
    from django_aiogram.runtime import groups
    from django_aiogram.wire.serializers import get_serializer

    delivered = []
    groups.close_groups()
    try:

        def handler_for(queue):
            """A handler that says which queue's consumer handed it the message."""
            return lambda function=None, correlation_id=None, queued_at=0.0, **call: delivered.append(
                (queue, call['text'])
            )

        consumers = [get_delivery(handler=handler_for(name), settings=settings_for(name)) for name in ('vip', 'bulk')]
        assert len({id(consumer.broker) for consumer in consumers}) == 2, 'both queues shared one transport'
        assert [consumer.queue_key for consumer in consumers] == ['vip', 'bulk']

        import uuid

        from django_aiogram.wire.envelope import pack

        payload = get_serializer().dumps(
            pack('send_message', {'chat_id': 1, 'text': 'for the second queue'}, uuid.uuid4(), 0.0)
        )
        consumers[1].broker.publish([payload])

        assert consumers[0].broker.depth() == 0, 'the message reached the wrong queue'
        consumers[1].consume_pending()
    finally:
        groups.close_groups()

    assert delivered == [('bulk', 'for the second queue')]
