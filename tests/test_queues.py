"""Which queue a bot publishes to, and what happens to a name nobody declared.

A queue is the isolation boundary, so `QUEUE` is transport-neutral: one setting reaches a
Redis list key, a stream, an AMQP queue and a Kafka topic alike. And a name that is not
declared is refused, because the alternative is a queue nothing consumes holding messages the
feed reports as sent.
"""

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.test import override_settings

from django_aiogram.broker.kafka import KafkaBroker
from django_aiogram.broker.rabbitmq import RabbitMQBroker
from django_aiogram.broker.redis_list import RedisListBroker
from django_aiogram.broker.redis_streams import RedisStreamsBroker
from django_aiogram.runtime import groups
from django_aiogram.runtime.queues import declared, refuse_undeclared

MEMORY = {'BROKER': 'django_aiogram.testing.InMemoryBroker', 'FSM_STORAGE': 'memory'}

#: every shipped transport with the option it addresses a queue by, and a value for it
ADDRESSED = [
    (RedisListBroker, {'REDIS_MESSAGES_KEY': 'from-its-own-option'}),
    (RedisStreamsBroker, {'REDIS_STREAM_KEY': 'from-its-own-option'}),
    (RabbitMQBroker, {'RABBITMQ_QUEUE': 'from-its-own-option'}),
    (KafkaBroker, {'KAFKA_TOPIC': 'from-its-own-option'}),
]


@pytest.mark.parametrize(('broker', 'own'), ADDRESSED, ids=lambda value: getattr(value, '__name__', ''))
def test_every_broker_addresses_the_queue_it_is_given(broker, own):
    """One setting for four transports, which is what makes a queue a deployment's word.

    `QUEUE_OPTION` names what each of them calls a queue, and `queue()` is what puts `QUEUE`
    in front of it — so a class that declares the option wrongly is a class whose bots publish
    somewhere else, and this is what says so.
    """
    assert broker.QUEUE_OPTION in broker.OPTIONS, 'a broker names an option it does not declare'

    assert broker.queue({**own, 'QUEUES': ('vip',), 'QUEUE': 'vip'}) == 'vip'
    assert broker.queue(own) == 'from-its-own-option', 'the transport ignored its own option'


@pytest.mark.parametrize(('broker', 'own'), ADDRESSED, ids=lambda value: getattr(value, '__name__', ''))
def test_an_instance_addresses_the_queue_its_own_settings_name(broker, own):
    """A group chooses the class by a bot's settings and has to build it *with* them.

    Read off `conf` instead, every instance in the process addresses whatever the shared
    defaults say — so a bot with a queue of its own would be served by a transport pointed at
    everybody else's, and would read their messages off it.
    """
    built = broker.configured({**own, 'QUEUES': ('vip',), 'QUEUE': 'vip'})

    assert built.addressed() == 'vip'
    assert broker.configured(own).addressed() == 'from-its-own-option'


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUES': ('vip', 'bulk')})
def test_a_queue_is_declared_by_the_settings():
    """Which is every deployment that knows its queues when it deploys."""
    assert declared() == {'vip', 'bulk'}
    refuse_undeclared({'QUEUE': 'vip'})


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUES': ('vip',)})
def test_a_queue_nobody_declared_is_refused_by_name():
    """A typo has no other symptom: the send succeeds and the message waits where nobody reads."""
    with pytest.raises(ImproperlyConfigured, match="'viip'") as refused:
        refuse_undeclared({'QUEUE': 'viip'})

    assert 'vip' in str(refused.value), 'the refusal did not say what is declared'


@override_settings(TELEGRAM_BOT_DEFAULTS=MEMORY)
def test_a_deployment_that_declares_nothing_may_name_nothing():
    """Its one queue is whatever its transport addresses, and that needs no name."""
    refuse_undeclared({})
    refuse_undeclared({'QUEUE': ''})
    with pytest.raises(ImproperlyConfigured, match='none'):
        refuse_undeclared({'QUEUE': 'vip'})


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUES': 'vip'})
def test_a_bare_string_declares_no_queues_rather_than_three():
    """`'vip'` is a collection of its characters, so read as one it would declare `v`, `i`, `p`.

    `E058` reports it; this is what keeps the reading from being the one that quietly works.
    """
    assert declared() == frozenset()


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUES': ('vip',)})
def test_a_transport_is_not_built_for_a_queue_nothing_declares():
    """Refused where the transport is built rather than at the first send, and by name."""
    groups.close_groups()
    group = groups.group_for({**MEMORY, 'QUEUES': ('vip',), 'QUEUE': 'typo'})
    with pytest.raises(ImproperlyConfigured, match='has not declared'):
        assert group.broker
    groups.close_groups()


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUES': ('vip', 'bulk')})
def test_two_bots_on_different_queues_do_not_share_a_transport():
    """Sharing one, each would take the other's messages off the queue it is addressed to."""
    groups.close_groups()
    try:
        vip = groups.group_for({**MEMORY, 'QUEUES': ('vip', 'bulk'), 'QUEUE': 'vip'})
        bulk = groups.group_for({**MEMORY, 'QUEUES': ('vip', 'bulk'), 'QUEUE': 'bulk'})

        assert vip is not bulk, 'two queues resolved to one runtime group'
        assert vip.broker is not bulk.broker
    finally:
        groups.close_groups()
