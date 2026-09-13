"""Removing the queue a transport addresses, and what it may not take with it.

`Broker.discard` is the destructive end of the contract, so every case here is about a
boundary: what belongs to this queue and what belongs to the next one, and what "empty" means
when a message can be taken but unsettled.
"""

import pytest
from django.test import override_settings

from django_aiogram.broker.kafka import KafkaBroker
from django_aiogram.broker.rabbitmq import RabbitMQBroker
from django_aiogram.broker.redis_list import RedisListBroker
from django_aiogram.testing import InMemoryBroker

LIST = {
    'BROKER': 'django_aiogram.broker.redis_list.RedisListBroker',
    'REDIS_URL': 'redis://localhost:6379/0',
    'REDIS_TIMEOUT': 10,
}


def test_only_the_transports_that_own_their_queues_say_they_remove_them():
    """A dry run has to say what a real one would do, so it asks before deciding.

    Kafka is the one that cannot: dropping a topic is an administrative act against the
    cluster, not something a producer may take. A transport a project writes inherits that
    answer, so nothing is ever removed by accident.
    """
    assert RedisListBroker().removes_queues
    assert RabbitMQBroker().removes_queues
    assert InMemoryBroker().removes_queues
    assert not KafkaBroker().removes_queues


@override_settings(TELEGRAM_BOT_DEFAULTS={**LIST, 'QUEUES': ('client', 'client:archive')})
def test_removing_one_queue_leaves_another_whose_name_it_prefixes(redis_server):
    """A queue name may contain a colon, and Redis reads a colon as ordinary text.

    So a scan for `client:*` matches `client:archive` — *another declared queue's own list* —
    and deleting it destroys a live client's messages. Measured before this: the archive queue
    came back empty.
    """
    mine = RedisListBroker.configured({**LIST, 'QUEUES': ('client', 'client:archive'), 'QUEUE': 'client'})
    theirs = RedisListBroker.configured({**LIST, 'QUEUES': ('client', 'client:archive'), 'QUEUE': 'client:archive'})
    mine.publish([b'mine'])
    theirs.publish([b'theirs'])

    assert mine.discard()

    assert mine.depth() == 0
    assert theirs.depth() == 1, "another queue's messages went with it"


@override_settings(TELEGRAM_BOT_DEFAULTS={**LIST, 'QUEUES': ('client',)})
def test_a_queue_holding_a_taken_message_is_not_empty(redis_server):
    """`depth` counts what is waiting, and a taken message is not waiting — it is being sent.

    Under `hold` that difference is the whole question: a queue whose only message is in
    flight would be deleted, and the message with it, while a consumer was still sending it.
    """
    broker = RedisListBroker.configured({**LIST, 'QUEUES': ('client',), 'QUEUE': 'client'})
    broker.publish([b'taken'])
    assert broker.take_nowait() is not None
    assert broker.depth() == 0, 'the case needs the message taken rather than waiting'

    assert broker.discard(if_empty=True) is False
    assert broker.inflight_depth() == 1, 'the message a consumer was sending was thrown away'


@override_settings(TELEGRAM_BOT_DEFAULTS={**LIST, 'QUEUES': ('client',)})
def test_an_empty_queue_is_removed_under_if_empty(redis_server):
    """The other half, and the one `hold` exists for."""
    broker = RedisListBroker.configured({**LIST, 'QUEUES': ('client',), 'QUEUE': 'client'})
    broker.publish([b'one'])
    taken = broker.take_nowait()
    broker.ack(taken.handle)

    assert broker.discard(if_empty=True)
    assert broker.depth() == 0


def test_the_memory_queue_reads_taken_messages_as_held():
    """The suite's own transport has to answer the same question the same way."""
    broker = InMemoryBroker()
    broker.publish([b'taken'])
    broker.take_nowait()

    assert broker.discard(if_empty=True) is False

    broker.reclaim()
    taken = broker.take_nowait()
    broker.ack(taken.handle)

    assert broker.discard(if_empty=True)


def test_rabbitmq_refuses_the_empty_only_removal():
    """AMQP's own `if_empty` counts ready messages, so it cannot see another container's
    unacknowledged delivery — and deleting the queue would take that message with it.

    Refused rather than guessed: `hold` reports the queue as still held, and an operator who
    knows what is sending uses `drop`.
    """
    assert RabbitMQBroker().discard(if_empty=True) is False


def test_a_transport_that_says_nothing_removes_nothing():
    """The base answer, which every transport a project writes starts from."""

    class Quiet(InMemoryBroker):
        """A transport that has not thought about removal."""

    from django_aiogram.broker.base import Broker

    assert Broker.discard(Quiet(), if_empty=False) is False
    assert not Broker.removes_queues.fget(Quiet())  # type: ignore[attr-defined]


@pytest.mark.parametrize('policy', ['park', 'hold', 'drop'])
def test_every_policy_is_a_word_the_command_and_the_setting_agree_on(policy):
    """One list, in the enum, so the flag's choices and the setting cannot drift apart."""
    from django_aiogram.config.enums import RemovedQueuePolicy

    assert RemovedQueuePolicy(policy).value == policy
