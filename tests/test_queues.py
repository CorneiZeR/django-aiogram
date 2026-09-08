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
#: the shipped Redis list, whose own queue option is the one `QUEUE` overrides
LIST = {'BROKER': 'django_aiogram.broker.redis_list.RedisListBroker', 'REDIS_URL': 'redis://localhost:6379/0'}

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


@pytest.mark.parametrize('broker', [RedisListBroker, RedisStreamsBroker], ids=lambda value: value.__name__)
def test_a_redis_transport_talks_to_the_server_its_own_settings_name(broker, monkeypatch):
    """The queue key was already this bot's; the server it lives on has to be too.

    Cached for the process, a client hands one bot's key to another bot's Redis — the key is
    right, the data is somewhere else, and nothing reports it. Both halves are asserted, the
    synchronous and the async, because the two caches are separate and only one of them was
    keyed by anything at all.
    """
    import asyncio

    from django_aiogram import redis as redis_module

    built = {}

    class Closes:
        """A client that answers the one call a reset makes on it."""

        def close(self):
            """Do nothing, loudly enough to be closed."""

        async def aclose(self):
            """The async half of the same."""

    def sync_client(settings=None):
        built.setdefault('sync', []).append(redis_module.url_for(settings))
        return Closes()

    def async_client(settings=None):
        built.setdefault('async', []).append(redis_module.url_for(settings))
        return Closes()

    monkeypatch.setattr(redis_module, 'build_client', sync_client)
    monkeypatch.setattr(redis_module, 'build_async_client', async_client)
    redis_module.reset_redis()

    mine = broker.configured({'REDIS_URL': 'redis://mine:6379/0', 'REDIS_TIMEOUT': 5})
    theirs = broker.configured({'REDIS_URL': 'redis://theirs:6379/0', 'REDIS_TIMEOUT': 5})

    assert mine._redis() is not theirs._redis(), 'two servers were served by one client'
    assert built['sync'] == ['redis://mine:6379/0', 'redis://theirs:6379/0']

    async def both():
        await mine._aredis()
        await theirs._aredis()

    asyncio.run(both())
    redis_module.reset_redis()

    assert built['async'] == ['redis://mine:6379/0', 'redis://theirs:6379/0']


def test_a_queue_named_with_stray_space_is_the_same_queue():
    """Everything that reads the name strips it, so the profile has to strip it too.

    Hashed raw, `' vip '` and `'vip'` are two profiles: two runtime groups, two transports,
    both addressed at `vip` — and each consuming the messages the other's bot sent.
    """
    from django_aiogram.runtime.profiles import profile_of

    settings = {**MEMORY, 'QUEUES': ('vip',)}

    assert profile_of({**settings, 'QUEUE': ' vip '}) == profile_of({**settings, 'QUEUE': 'vip'})
    assert profile_of({**settings, 'QUEUE': 'vip'}) != profile_of({**settings, 'QUEUE': 'bulk'})


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUES': ('vip',)})
def test_a_section_may_not_declare_the_deployment_queues():
    """`QUEUES` is read off the process's settings, so a section setting it declares nothing.

    Silently, and worse than silently: the bot would name a queue its own section declares,
    `in_settings()` would not see it, and the send would be refused as a typo.
    """
    from django_aiogram.config.checks import check_settings

    with override_settings(TELEGRAM_BOTS={'a': {'TOKEN': '123456:AAaa', 'QUEUES': ('mine',), 'QUEUE': 'mine'}}):
        reported = [message for message in check_settings() if str(message.id).endswith('E053')]

    assert reported, 'a section declaring the deployment queues was accepted'
    assert 'QUEUES' in reported[0].msg


def test_a_leftover_transport_key_does_not_split_bots_addressed_at_one_queue():
    """`Broker.queue` stops reading the transport's own option once `QUEUE` is set.

    So two bots on one queue with different leftover values for it behave identically — and
    split by it, each would get a connection and a consumer of its own for nothing, which is
    the cost profiles exist to avoid.
    """
    from django_aiogram.runtime.profiles import profile_of

    settings = {**LIST, 'QUEUES': ('vip',), 'QUEUE': 'vip'}
    mine = {**settings, 'REDIS_MESSAGES_KEY': 'left-over'}
    theirs = {**settings, 'REDIS_MESSAGES_KEY': 'something-else'}

    assert profile_of(mine) == profile_of(theirs)


def test_the_transport_key_still_splits_bots_that_have_no_queue_named():
    """The other half: with `QUEUE` empty it is what addresses the queue, so it decides."""
    from django_aiogram.runtime.profiles import profile_of

    mine = {**LIST, 'REDIS_MESSAGES_KEY': 'mine'}
    theirs = {**LIST, 'REDIS_MESSAGES_KEY': 'theirs'}

    assert profile_of(mine) != profile_of(theirs)
