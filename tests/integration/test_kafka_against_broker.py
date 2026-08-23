"""The Kafka transport against a real broker, because it has no in-memory double.

What is here is what the contract cannot ask, and on this transport that is more than usual:
Kafka settles a *position* rather than a message, so the questions worth asking are about
order — what a partly-settled batch commits, and what a rewind costs.

The kill test is the same shape as RabbitMQ's and rests on a different mechanism: nothing was
committed, so the group hands the partition to somebody else at the last committed offset.
"""

import asyncio

import pytest
from django.test import override_settings

from django_aiogram.broker.kafka import KafkaBroker
from django_aiogram.broker.kafka.client import close_clients
from django_aiogram.broker.kafka.exceptions import ProduceRefusedError
from django_aiogram.wire.serializers import JsonSerializer

pytestmark = pytest.mark.integration


def payload(chat_id):
    return JsonSerializer().dumps({'function': 'send_message', 'chat_id': chat_id})


def settings_for(bootstrap, topic):
    return {
        'TOKEN': '42:x',
        'RATE_LIMIT': None,
        'KAFKA_BOOTSTRAP': bootstrap,
        'KAFKA_TOPIC': topic,
        'KAFKA_GROUP': topic,
    }


@pytest.fixture
def broker(kafka_bootstrap, kafka_topic):
    with override_settings(TELEGRAM_BOT=settings_for(kafka_bootstrap, kafka_topic)):
        yield KafkaBroker()


def test_a_message_a_killed_worker_held_comes_back(broker, kafka_bootstrap, kafka_topic):
    """Nothing was committed, so the next consumer of that partition sees it again.

    The whole of this transport's crash safety, and it needs nothing from this package: no
    in-flight list, no worker name, no reclaim. Closing the client is what dying does — the
    member stops heartbeating and the group gives its partition away.
    """
    with override_settings(TELEGRAM_BOT=settings_for(kafka_bootstrap, kafka_topic)):
        broker.publish([payload(7)])
        taken = broker.take_nowait()
        assert taken is not None, 'the message was not delivered in the first place'
        assert broker.inflight_depth() == 1

        close_clients()  # what dying does to a consumer's membership

        replacement = KafkaBroker()
        again = replacement.take_nowait()

        assert again is not None, 'an uncommitted message did not come back'
        assert again.payload == payload(7)
        assert replacement.reclaim() is None, 'this transport claims to need a reclaim'


def test_an_acknowledged_message_does_not_come_back(broker, kafka_bootstrap, kafka_topic):
    """The other half, and on Kafka it is a commit rather than a removal."""
    with override_settings(TELEGRAM_BOT=settings_for(kafka_bootstrap, kafka_topic)):
        broker.publish([payload(8)])
        taken = broker.take_nowait()
        assert taken is not None
        broker.ack(taken.handle)

        close_clients()

        assert KafkaBroker().take_nowait() is None, 'a committed message came back'


def test_settling_out_of_order_commits_only_what_is_contiguous(broker, kafka_bootstrap, kafka_topic):
    """The rule that keeps an offset honest, and the reason this transport needs one.

    Committing offset N claims every message below it. So a consumer holding three sends —
    which `MAX_IN_FLIGHT` allows — and finishing the second one first must not commit it:
    that would claim the first, which is still on its way to Telegram.

    Asserted by killing the client after the out-of-order settle. What comes back is the
    evidence: the first message, because nothing above it could be committed while it was
    outstanding.
    """
    with override_settings(TELEGRAM_BOT=settings_for(kafka_bootstrap, kafka_topic)):
        broker.publish([payload(1), payload(2), payload(3)])
        held = [broker.take_nowait() for _ in range(3)]
        assert all(item is not None for item in held), f'expected three messages, got {held}'

        broker.ack(held[1].handle)  # the middle one finishes first

        close_clients()
        replacement = KafkaBroker()
        again = replacement.take_nowait()

        assert again is not None, 'nothing came back, so something was committed that should not have been'
        assert again.payload == payload(1), 'the unsettled first message was not the one redelivered'


def test_settling_the_gap_commits_both(broker, kafka_bootstrap, kafka_topic):
    """And when the gap closes, the commit catches up past both of them.

    Without this the case above would be satisfied by a broker that never commits anything.
    """
    with override_settings(TELEGRAM_BOT=settings_for(kafka_bootstrap, kafka_topic)):
        broker.publish([payload(1), payload(2)])
        first, second = (broker.take_nowait() for _ in range(2))
        assert first is not None, 'the first message was not delivered'
        assert second is not None, 'the second message was not delivered'

        broker.ack(second.handle)
        broker.ack(first.handle)

        close_clients()

        assert KafkaBroker().take_nowait() is None, 'a settled pair was redelivered'


def test_a_release_rewinds_and_says_so(broker, kafka_bootstrap, kafka_topic):
    """Kafka has no per-message nack, so giving one up rewinds to its position.

    Everything after it is delivered again too, which is why this transport's documentation
    insists on idempotency more loudly than the others. Asserted rather than described: the
    message after the released one comes back as well.
    """
    with override_settings(TELEGRAM_BOT=settings_for(kafka_bootstrap, kafka_topic)):
        broker.publish([payload(1), payload(2)])
        first, second = (broker.take_nowait() for _ in range(2))
        assert first is not None, 'the first message was not delivered'
        assert second is not None, 'the second message was not delivered'

        broker.release(first.handle)

        seen = []
        for _ in range(2):
            taken = broker.take_nowait()
            if taken is None:
                break
            seen.append(taken.payload)

        assert seen == [payload(1), payload(2)], f'a rewind delivered {len(seen)} message(s): {seen}'


def test_depth_answers_a_process_that_consumes_nothing(broker, kafka_bootstrap, kafka_topic):
    """A web process asking how deep the queue is has no assignment and must still be told.

    Read from metadata rather than from this consumer's partitions: answering from the
    assignment would report an empty queue to every process that only publishes, which is most
    of them.
    """
    with override_settings(TELEGRAM_BOT=settings_for(kafka_bootstrap, kafka_topic)):
        broker.publish([payload(1), payload(2), payload(3)])

        # a broker that has never consumed, exactly like a view or a Celery task
        publisher = KafkaBroker()

        assert publisher.depth() == 3, 'a process that consumes nothing was told the queue is empty'
        assert publisher.inflight_depth() == 0


def test_producing_to_a_topic_that_cannot_be_created_is_refused(kafka_bootstrap):
    """A publish waits for the broker, so a refusal is a refusal rather than a silent loss.

    An invalid topic name is the reliable way to be refused without depending on the broker's
    `auto.create.topics.enable`: Kafka rejects the name itself.

    Asserted on the *reason*, not on the message containing the topic. The first version of
    this looked for "not a valid topic name" in the text — which is the topic name this case
    configured, so it matched the error's own subject and would have passed with the reason
    empty. Measured, librdkafka reports `TOPIC_EXCEPTION … Broker: Invalid topic`.
    """
    bad = 'this is not a valid topic name'
    with override_settings(TELEGRAM_BOT=settings_for(kafka_bootstrap, bad)):
        with pytest.raises(ProduceRefusedError) as refused:
            KafkaBroker().publish([payload(1)])

        assert refused.value.topic == bad, refused.value.topic
        assert 'Invalid topic' in str(refused.value), str(refused.value)


def test_asking_the_depth_does_not_join_the_group(broker, kafka_bootstrap, kafka_topic):
    """A subscription is group membership, and a process that only publishes must not be one.

    Reading the depth from a web process used to subscribe: the coordinator then hands that
    process partitions it never polls, and on a single-partition topic the real worker receives
    nothing until the member's session times out. A healthcheck could starve the consumer it
    was checking on.

    Asserted on the group's members as the broker sees them, which is the only place the
    difference shows — the depth is the same number either way.
    """
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({'bootstrap.servers': kafka_bootstrap})

    with override_settings(TELEGRAM_BOT=settings_for(kafka_bootstrap, kafka_topic)):
        publisher = KafkaBroker()
        publisher.publish([payload(1)])

        assert publisher.depth() == 1, 'the depth read did not work at all'

    described = admin.describe_consumer_groups([kafka_topic])[kafka_topic].result(timeout=30)

    assert described.members == [], f'reading the depth joined the group: {described.members}'


def test_the_awaited_halves_work_off_the_loop(broker, kafka_bootstrap, kafka_topic):
    """`apublish` and `adepth` go through a thread, because the driver is synchronous.

    The hand-off costs about 100 microseconds and is invisible here: waiting for the broker's
    acknowledgement costs five times that, which is the measurement that made the driver choice
    a question about the consumer rather than about latency.
    """
    with override_settings(TELEGRAM_BOT=settings_for(kafka_bootstrap, kafka_topic)):

        async def on_a_loop():
            await broker.apublish([])
            after_nothing = await broker.adepth()
            await broker.apublish([payload(11), payload(12)])
            return after_nothing, await broker.adepth(), await broker.ainflight_depth()

        after_nothing, after_two, inflight = asyncio.run(on_a_loop())

        assert after_nothing == 0, 'awaiting an empty publish queued something'
        assert after_two == 2, 'the awaited publishes did not arrive'
        assert inflight == 0


def test_a_handle_from_another_broker_is_refused(broker, kafka_bootstrap, kafka_topic):
    """A position is a pair, so anything else came from a different transport."""
    with override_settings(TELEGRAM_BOT=settings_for(kafka_bootstrap, kafka_topic)):
        with pytest.raises(TypeError, match='partition, offset, epoch'):
            broker.ack(b'a redis payload')

        with pytest.raises(TypeError, match='partition, offset, epoch'):
            broker.release(7)

        # a pair is the shape this broker used to hand out, and it is no longer enough: the
        # rewind count is what makes a position mean something
        with pytest.raises(TypeError, match='partition, offset, epoch'):
            broker.ack((0, 0))


def test_messages_stranded_in_the_producer_are_reported(kafka_bootstrap, caplog):
    """A produce accepted locally and never handed over is a loss, so it is said out loud.

    `publish` waits for the broker, so anything still in librdkafka's queue when its producer
    is replaced was accepted and then blocked on the way out. Dropping the producer on top of
    that loses those messages in silence — and a settings change during a burst is the ordinary
    way there.

    Arranged against a bootstrap address with nothing behind it, which is what a broker that
    has gone away looks like from here. Measured: `flush` answers with how many are left.
    """
    from django_aiogram.broker.kafka import client

    with override_settings(TELEGRAM_BOT=settings_for('127.0.0.1:1', 'unreachable')):
        producer = client.shared_producer('127.0.0.1:1')
        for _ in range(3):
            producer.produce('unreachable', b'{}')

        with caplog.at_level('WARNING', logger='django_aiogram'):
            client.close_clients()

    assert 'never reached the broker' in caplog.text, caplog.text
    assert any(record.tg_count == 3 for record in caplog.records if hasattr(record, 'tg_count')), (
        f'the count was not reported: {[getattr(r, "tg_count", None) for r in caplog.records]}'
    )


def test_a_stale_ack_after_a_rewind_cannot_commit_past_it(broker, kafka_bootstrap, kafka_topic, caplog):
    """A `release` rewinds a whole partition, so every earlier handle names a delivery that is gone.

    The losing sequence: offsets 0 and 1 in flight, `release` on 0 rewinds the partition, and
    then the send that was already running for 1 finishes and calls `ack`. With nothing
    outstanding, accepting it commits offset 2 — and a restart then skips **both** messages.

    So the handle carries the partition's rewind count and a stale one settles nothing. What
    proves it is what comes back afterwards: both messages, in order.
    """
    with override_settings(TELEGRAM_BOT=settings_for(kafka_bootstrap, kafka_topic)):
        broker.publish([payload(1), payload(2)])
        first, second = (broker.take_nowait() for _ in range(2))
        assert first is not None, 'the first message was not delivered'
        assert second is not None, 'the second message was not delivered'

        broker.release(first.handle)
        with caplog.at_level('WARNING', logger='django_aiogram'):
            broker.ack(second.handle)  # the send that was already running finishes now

        close_clients()
        replacement = KafkaBroker()
        seen = [taken.payload for taken in (replacement.take_nowait() for _ in range(2)) if taken]

        # the loss first, because it is the consequence: asserting the warning before it means a
        # falsification stops at the log line and never shows the messages going missing
        assert seen == [payload(1), payload(2)], f'a stale ack committed past the rewind: {seen}'
        assert 'its partition was rewound' in caplog.text, caplog.text
