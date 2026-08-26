"""What one worker holds, asked about another worker, on four transports.

`inflight_depth()` answers for the caller everywhere. `inflight_depth('some-worker')` is the
question a monitor asks about a container that never came back, and it used to be answered by
reaching a Redis client directly, whatever `BROKER` said:

```python
if worker is None:
    return get_broker().inflight_depth()
return int(get_redis().llen(processing_key(worker)) or 0)
```

On a RabbitMQ or Kafka deployment that branch raised about a missing `REDIS_URL` -- or, worse,
answered `0` from an unrelated Redis the project happened to run for caching, which is the one
answer that stops anybody looking.

So the question goes through the seam now, and the four transports do not answer it alike, which
is the point of this module:

* the **Redis list** keeps a key per worker that outlives the process, so a name is exactly what
  it can be asked about -- the same key `tgbot_reclaim` addresses;
* **Redis Streams** records the consumer each entry went to, and `XPENDING key group` carries the
  per-consumer breakdown in the summary it already fetches, so a name costs no extra round trip;
* **RabbitMQ** knows unacknowledged deliveries as a *channel*'s, and **Kafka** knows uncommitted
  offsets as a *member*'s. Neither is a name this package chose, so both refuse rather than
  return a number that would read as "nothing is stranded".
"""

import asyncio
from typing import ClassVar

import pytest
from django.test import override_settings
from django.utils.module_loading import import_string

from django_aiogram import TelegramBot
from django_aiogram.broker.exceptions import WorkerDepthUnavailableError
from django_aiogram.broker.redis_list import RedisListBroker
from django_aiogram.broker.redis_streams import RedisStreamsBroker

SETTINGS = {'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost:6379/0'}
STREAMS = {**SETTINGS, 'BROKER': 'django_aiogram.broker.redis_streams.RedisStreamsBroker', 'REDIS_STREAM_KEY': 'tg'}

#: the two that cannot answer, with the settings each needs to be built at all
REFUSING = {
    'django_aiogram.broker.kafka.KafkaBroker': {'KAFKA_BOOTSTRAP': 'localhost:9092', 'KAFKA_TOPIC': 'tg'},
    'django_aiogram.broker.rabbitmq.RabbitMQBroker': {
        'RABBITMQ_URL': 'amqp://guest:guest@localhost:5672/',
        'RABBITMQ_QUEUE': 'tg',
    },
}


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_list_reads_the_key_belonging_to_the_name(redis_server):
    """A worker that is gone left a list behind, and its name is how anyone reaches it."""
    redis_server.rpush('TELEGRAM_BOT_MESSAGE:processing:gone', b'one', b'two')
    broker = RedisListBroker()

    assert broker.inflight_depth() == 0, 'this process holds nothing, and that is the other question'
    assert broker.inflight_depth('gone') == 2


@override_settings(TELEGRAM_BOT=STREAMS)
def test_streams_takes_one_consumers_share_from_the_summary(redis_server):
    """The breakdown rides along with the total, so a name costs no second round trip.

    Measured against a real server and against fakeredis alike: `XPENDING key group` answers
    `{'pending': 3, ..., 'consumers': [{'name': b'worker-one', 'pending': 2}, ...]}`.
    """
    broker = RedisStreamsBroker()
    broker.publish([b'one', b'two', b'three'])
    redis_server.xreadgroup('django-aiogram', 'first', {'tg': '>'}, count=2)
    redis_server.xreadgroup('django-aiogram', 'second', {'tg': '>'}, count=1)

    assert broker.inflight_depth() == 3, 'the group holds three, which is the unnamed answer'
    assert broker.inflight_depth('first') == 2
    assert broker.inflight_depth('second') == 1
    assert broker.inflight_depth('never-seen') == 0, 'a name the group never met is stranded nothing'


@pytest.mark.parametrize('path', sorted(REFUSING))
def test_a_transport_that_cannot_answer_refuses_rather_than_saying_zero(path):
    """Zero is the answer that stops somebody looking, so neither of these may give it.

    The refusal carries both halves, as every refusal in this package does: a monitor sweeping
    worker names wants to know which transport declined and which name it was asking about
    without parsing the sentence back apart.
    """
    with override_settings(TELEGRAM_BOT={**SETTINGS, 'BROKER': path, **REFUSING[path]}):
        broker = import_string(path)()

        assert broker.inflight_depth() == 0, 'this process holds nothing, and can still say so'
        with pytest.raises(WorkerDepthUnavailableError) as refused:
            broker.inflight_depth('a-worker-that-died')

    assert refused.value.worker == 'a-worker-that-died'
    assert refused.value.broker == path.rsplit('.', 1)[1], 'the transport names itself by its class'
    assert 'inflight_depth()' in str(refused.value), 'the message does not say what can be asked instead'


@pytest.mark.parametrize('path', sorted(REFUSING))
def test_the_awaiting_half_refuses_the_same_way(path):
    """Two entry points, one rule -- and the async one is the exporter's, so it must not differ."""
    with override_settings(TELEGRAM_BOT={**SETTINGS, 'BROKER': path, **REFUSING[path]}):
        broker = import_string(path)()

        with pytest.raises(WorkerDepthUnavailableError):
            asyncio.run(broker.ainflight_depth('a-worker-that-died'))


class RecordingBroker(RedisListBroker):
    """A broker that records what it was asked about, and answers a number nothing else would."""

    asked: ClassVar[list[str | None]] = []

    def inflight_depth(self, worker: str | None = None) -> int:
        RecordingBroker.asked.append(worker)
        return 11


@override_settings(TELEGRAM_BOT={**SETTINGS, 'BROKER': 'tests.test_inflight_by_worker.RecordingBroker'})
def test_the_public_method_asks_the_transport_and_touches_no_client(monkeypatch):
    """The defect as a caller met it: a name went to a Redis client whatever `BROKER` said.

    Two assertions rather than one, because either alone is weak. That the name reaches the
    broker says the seam is used; that reaching for a Redis client explodes says nothing goes
    round it -- and on a deployment with `REDIS_URL` set for a cache, going round it returned a
    confident `0` from a key nothing writes, which is the answer that ends an investigation.
    """

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError('the producer reached for a Redis client to answer about a worker')

    monkeypatch.setattr('django_aiogram.redis.get_redis', forbidden)
    RecordingBroker.asked.clear()

    assert TelegramBot().inflight_depth('a-worker-that-died') == 11
    assert RecordingBroker.asked == ['a-worker-that-died'], 'the name did not reach the transport'
