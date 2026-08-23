"""The RabbitMQ transport against a real broker, because it has no in-memory double.

Every case here needs a server, which is the difference from the Redis transports: fakeredis
answers for those well enough that only the awkward questions come here. So this module is
gated as a whole, and the conformance suite covers this transport against the same server —
what is left for here is what the contract cannot ask.

The kill test is the reason the module exists. RabbitMQ returns an unacknowledged message when
the channel that held it drops, and that is the whole of this transport's crash safety: no
in-flight list, no worker name, nothing for a restart to reclaim. Dropping the connection is
what a killed worker does to it, so that is what this arranges.
"""

import asyncio

import pytest
from django.test import override_settings

from django_aiogram.broker.rabbitmq import RabbitMQBroker
from django_aiogram.broker.rabbitmq.client import close_connections
from django_aiogram.broker.rabbitmq.exceptions import QueueRefusedError
from django_aiogram.wire.serializers import JsonSerializer

from .conftest import AMQP_QUEUE

pytestmark = pytest.mark.integration


def payload(chat_id):
    return JsonSerializer().dumps({'function': 'send_message', 'chat_id': chat_id})


def settings_for(url):
    return {'TOKEN': '42:x', 'RATE_LIMIT': None, 'RABBITMQ_URL': url, 'RABBITMQ_QUEUE': AMQP_QUEUE}


@pytest.fixture
def broker(broker_channel, amqp_url):
    with override_settings(TELEGRAM_BOT=settings_for(amqp_url)):
        yield RabbitMQBroker()


def test_a_message_a_killed_worker_held_comes_back(broker, broker_channel, amqp_url):
    """The transport's entire crash-safety story, arranged the way a kill arranges it.

    The message is taken and never settled, and then the connection holding it goes away. No
    reclaim is called, no worker is named and nothing is written down: RabbitMQ puts it back
    because the channel that owed an acknowledgement is gone.

    A second broker takes it, which is the replacement container — and it does not have to be
    the same one, which is what `needs_identity` being false means.
    """
    with override_settings(TELEGRAM_BOT=settings_for(amqp_url)):
        broker.publish([payload(7)])
        taken = broker.take_nowait()
        assert taken is not None, 'the message was not delivered in the first place'
        assert broker.inflight_depth() == 1

        close_connections()  # what dying does to a worker's channel

        replacement = RabbitMQBroker()
        again = replacement.take_nowait()

        assert again is not None, 'an unacknowledged message did not come back'
        assert again.payload == payload(7)
        assert replacement.reclaim() is None, 'this transport claims to need a reclaim'


def test_an_acknowledged_message_does_not_come_back_after_a_reconnect(broker, amqp_url):
    """The other half: settled work must not return, or every restart would resend.

    The same drop as above, so the two differ only in the acknowledgement — which is the thing
    being tested.
    """
    with override_settings(TELEGRAM_BOT=settings_for(amqp_url)):
        broker.publish([payload(8)])
        taken = broker.take_nowait()
        assert taken is not None
        broker.ack(taken.handle)

        close_connections()

        assert RabbitMQBroker().take_nowait() is None, 'an acknowledged message came back'


def test_a_release_puts_it_back_at_once(broker, amqp_url):
    """`basic_nack` with requeue, and no waiting for an idle threshold.

    Worth asserting against a server because this is the one transport where giving a message
    up is a single command: the Redis list leaves it in place and a stream has to move an idle
    counter first.
    """
    with override_settings(TELEGRAM_BOT=settings_for(amqp_url)):
        broker.publish([payload(9)])
        taken = broker.take_nowait()
        assert taken is not None

        broker.release(taken.handle)

        assert broker.inflight_depth() == 0, 'a released message is still counted as in flight'
        again = broker.take_nowait()
        assert again is not None, 'a released message was lost'
        assert again.payload == payload(9)


def test_depth_counts_what_is_ready_and_inflight_counts_what_is_held(broker, amqp_url):
    """The two numbers answer different questions, and AMQP only reports one of them.

    `message_count` is ready messages — measured, it reads 0 while a message is out with a
    consumer — so the in-flight count is this broker's own tally. Asserted together because
    the pair is what a queue-depth alert reads.
    """
    with override_settings(TELEGRAM_BOT=settings_for(amqp_url)):
        broker.publish([payload(1), payload(2), payload(3)])

        assert broker.depth() == 3
        assert broker.inflight_depth() == 0

        taken = broker.take_nowait()
        assert taken is not None

        assert broker.depth() == 2, 'a delivered message is still counted as waiting'
        assert broker.inflight_depth() == 1
        assert broker.depth() + broker.inflight_depth() == 3, 'a message went missing'


def test_a_publish_that_cannot_be_routed_raises(broker, broker_channel, amqp_url):
    """Confirmed and mandatory, so a message with nowhere to go is an error.

    Arranged by deleting the queue behind the broker's back, which is what an operator doing
    housekeeping does. Without `mandatory` the exchange would drop it silently and `send()`
    would return an id for a message that never existed.
    """
    with override_settings(TELEGRAM_BOT=settings_for(amqp_url)):
        broker.publish([payload(1)])  # opens the channel and declares the queue
        broker_channel.queue_delete(queue=AMQP_QUEUE)

        with pytest.raises(QueueRefusedError) as refused:
            broker.publish([payload(2)])

        assert AMQP_QUEUE in str(refused.value), str(refused.value)


def test_the_awaited_halves_work_off_the_loop(broker, amqp_url):
    """`apublish` and `adepth` go through a thread, because the driver is synchronous.

    That hand-off is the price the driver decision put on this face — about 100 microseconds,
    measured, which is what the other driver would have charged the synchronous caller. Worth
    a case because a thread and a `BlockingConnection` are exactly the combination that
    deadlocks when it is done wrong.
    """
    with override_settings(TELEGRAM_BOT=settings_for(amqp_url)):

        async def on_a_loop():
            await broker.apublish([])
            after_nothing = await broker.adepth()
            await broker.apublish([payload(11), payload(12)])
            return after_nothing, await broker.adepth(), await broker.ainflight_depth()

        after_nothing, after_two, inflight = asyncio.run(on_a_loop())

        assert after_nothing == 0, 'awaiting an empty publish queued something'
        assert after_two == 2, 'the awaited publishes did not arrive'
        assert inflight == 0


def test_waiting_for_a_message_returns_without_one(broker, amqp_url):
    """`take` has to give the consumer its turn back, or a shutdown waits for traffic."""
    with override_settings(TELEGRAM_BOT=settings_for(amqp_url)):
        assert broker.take(0.2) is None

        broker.publish([payload(5)])
        taken = broker.take(2)

        assert taken is not None, 'a published message did not arrive within the timeout'
        assert taken.payload == payload(5)
