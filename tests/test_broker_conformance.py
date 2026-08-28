"""One suite, run against every transport there is.

The point of the contract is that `Delivery` can be written once. That only holds if every
broker answers the same questions the same way, and the only way to know is to ask all of
them the same questions — so a transport is added here on the day it is added to the package.
A broker that cannot pass this is not a broker.

Parametrised over `SHIPPED`, so a new entry in the registry is covered without anybody
remembering to come back here. What each case needs to *run* differs — Redis wants a server,
Kafka wants a broker — so a fixture per transport supplies that, and a transport with no
fixture yet is skipped loudly rather than silently passing.
"""

import asyncio
import os
import time
from collections import Counter

import pytest
from django.test import override_settings
from django.utils.module_loading import import_string

from django_aiogram.broker.base import Broker
from django_aiogram.broker.registry import SHIPPED
from django_aiogram.wire.serializers import JsonSerializer

# `REDIS_STREAM_KEY` has no default — the Streams broker requires it, so a settings dict
# without it makes every case for that transport an `ImproperlyConfigured` about the fixture
# rather than an answer about the contract
#: every transport's required settings, because each case carries its own `override_settings`
#: and that replaces the whole dict — keys a fixture added on the way in are gone by the time
#: the body runs. Empty when a server is not configured, which is what the skips are for
#: a real Redis, when one is configured. Without it the two Redis transports face `fakeredis`,
#: which is what lets this suite run offline — and is also why it could not answer for them: a
#: double that agrees with everything cannot say whether the contract holds against a server.
#: The CI leg with a Redis service sets this, so the contract is checked both ways
REDIS_URL = os.environ.get('DJANGO_AIOGRAM_TEST_REDIS_URL', '')
AMQP_URL = os.environ.get('DJANGO_AIOGRAM_TEST_AMQP_URL', '')
AMQP_QUEUE = 'conformance'
KAFKA_BOOTSTRAP = os.environ.get('DJANGO_AIOGRAM_TEST_KAFKA_BOOTSTRAP', '')
SETTINGS = {
    'TOKEN': '42:x',
    'REDIS_URL': REDIS_URL or 'redis://localhost:6379/0',
    'RATE_LIMIT': None,
    'REDIS_STREAM_KEY': 'TELEGRAM_BOT_STREAM',
    'RABBITMQ_URL': AMQP_URL,
    'RABBITMQ_QUEUE': AMQP_QUEUE,
    'KAFKA_BOOTSTRAP': KAFKA_BOOTSTRAP,
    # a topic per case, because Kafka cannot delete messages from one and a group that has
    # committed cannot be rewound cheaply — the fixture makes the name unique instead
    'KAFKA_TOPIC': 'conformance',
    'KAFKA_GROUP': 'conformance',
}


#: transports whose waiting-count cannot be asserted against the in-memory server, and why.
#: Not a transport's answer being excused — a fake's arithmetic being wrong: measured, two
#: entries published into a fresh group make real Redis answer `lag` 1 then 2 and fakeredis
#: answer 0 then 1. The real coverage is `tests/integration/test_streams_against_redis.py`,
#: which the entry names so this cannot quietly become no coverage at all
UNTRUSTWORTHY_DEPTH = {
    'django_aiogram.broker.redis_streams.RedisStreamsBroker': (
        'fakeredis computes XINFO GROUPS lag one short; depth() is asserted against a real '
        'server in tests/integration/test_streams_against_redis.py'
    ),
}


def payload(chat_id: int) -> bytes:
    """One message, in the shape a producer writes."""
    return JsonSerializer().dumps({'function': 'send_message', 'chat_id': chat_id})


@pytest.fixture(params=sorted(SHIPPED), ids=lambda path: path.rsplit('.', 1)[-1])
def broker(request):
    """One instance of each shipped transport, ready to publish and take.

    A branch per transport for what it needs to *run*, and the assertions below do not change —
    which is the whole point. `redis_server` is requested lazily rather than taken as an
    argument, because the Redis branch does not want it when a real server is configured.
    """
    path = request.param
    if 'rabbitmq' in path:
        yield from _against_rabbitmq(path)
        return
    if 'kafka' in path:
        yield from _against_kafka(path)
        return
    if 'redis' not in path:
        pytest.skip(f'no fixture yet for {path}: add one rather than skipping the contract')
    yield from _against_redis(request, path)


def _against_redis(request, path):
    """The contract against a real Redis where one is configured, `fakeredis` otherwise.

    Both, deliberately. `fakeredis` is what lets this suite run with no server at all, and it
    is also the reason the two Redis transports were the ones whose contract nobody had
    checked: a double answers every question the way the code expects, so it cannot report a
    disagreement. The CI leg that has a Redis service sets ``DJANGO_AIOGRAM_TEST_REDIS_URL``,
    and there this asks the server.

    The URL goes into ``SETTINGS`` rather than into an override here, because every case carries
    its own ``override_settings`` and that replaces the whole dict — a key a fixture added on the
    way in is gone by the time the body runs. Learnt by doing it the other way and watching
    eighteen cases dial ``localhost:6379``.

    The database is flushed around each case, exactly as the integration suite's `server`
    fixture does — **it erases the whole selected database**, so point that variable at a
    throwaway.
    """
    from django.utils.module_loading import import_string

    if not REDIS_URL:
        request.getfixturevalue('redis_server')
        with override_settings(TELEGRAM_BOT=SETTINGS):
            broker = import_string(path)()
            broker.conformance_path = path
            yield broker
        return

    from redis import Redis

    from django_aiogram.redis import reset_redis

    client = Redis.from_url(REDIS_URL)
    if 'streams' in path and not _reports_lag(client):
        pytest.skip(f'{REDIS_URL} has no XINFO GROUPS lag: Redis Streams needs 7.0')
    client.flushdb()
    reset_redis()
    try:
        with override_settings(TELEGRAM_BOT=SETTINGS):
            broker = import_string(path)()
            broker.conformance_path = path
            yield broker
    finally:
        client.flushdb()
        reset_redis()
        client.close()


def _reports_lag(client) -> bool:
    """Whether this server can answer a Streams depth, which is the 7.0 question.

    Asked of the server rather than of its version string, for the reason the broker asks it
    that way: a fork reports whatever version it likes and what matters is the field.
    """
    key = 'conformance-lag-probe'
    try:
        client.xadd(key, {'x': '1'})
        client.xgroup_create(key, 'probe', id='0')
        return any('lag' in dict(group) for group in client.xinfo_groups(key))
    except Exception:
        return False
    finally:
        client.delete(key)


def _against_kafka(path):
    """The contract against a real Kafka, on a topic of this case's own.

    A fresh topic per case rather than a cleaned one: Kafka does not delete a message when it
    is settled — an offset moves — so "the queue is empty" cannot be arranged by tidying up.
    The group name goes with it, or a group that had committed on the previous topic would
    bring that position along.

    Skipped loudly without a broker, for the reason the AMQP branch is: a transport whose
    contract nobody checked is worse than one that says so.
    """
    if not KAFKA_BOOTSTRAP:
        pytest.skip('set DJANGO_AIOGRAM_TEST_KAFKA_BOOTSTRAP to run the contract against Kafka')
    import uuid

    from django.utils.module_loading import import_string

    from django_aiogram.broker.kafka.client import close_clients

    unique = f'conformance-{uuid.uuid4().hex[:12]}'
    # written into the shared mapping, not into a copy of it. Each case carries its own
    # `override_settings(TELEGRAM_BOT=SETTINGS)`, and that replaces the whole dict when the
    # case starts — after this fixture has run — so a per-case value has to be *in* the
    # mapping the decorator reads, or the body gets the module-level topic instead. The same
    # shape of mistake as the AMQP settings, from the other side
    SETTINGS['KAFKA_TOPIC'] = SETTINGS['KAFKA_GROUP'] = unique
    # created before the broker is built, because subscribing to a topic that does not exist
    # yet leaves the consumer with no assignment — the coordinator has nothing to give it — and
    # every `take` then waits out its whole budget for a topic the first publish would create.
    # A transport's contract is that the topic exists; making it is the operator's job, or the
    # broker's `auto.create.topics.enable`, and here it is the fixture's
    _make_kafka_topic(unique)
    with override_settings(TELEGRAM_BOT=SETTINGS):
        broker = import_string(path)()
        broker.conformance_path = path
        try:
            yield broker
        finally:
            close_clients()


def _make_kafka_topic(name):
    """Create a topic and wait until the cluster reports it, or skip saying it could not."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP})
    for future in admin.create_topics([NewTopic(name, num_partitions=1, replication_factor=1)]).values():
        future.result(timeout=30)
    for _ in range(60):
        if name in admin.list_topics(timeout=10).topics:
            return
        time.sleep(0.1)
    pytest.skip(f'kafka did not report the topic {name!r} after creating it')


def _against_rabbitmq(path):
    """The contract against a real RabbitMQ, or a skip that names how to run it.

    There is no fakeredis for AMQP, so this one needs a server. Skipped loudly rather than
    quietly passing: a transport whose contract nobody checked is worse than one that says so.

    The queue is deleted and redeclared per case, because these assertions are about counts
    and a message left by the previous one would answer them wrongly.
    """
    if not AMQP_URL:
        pytest.skip('set DJANGO_AIOGRAM_TEST_AMQP_URL to run the contract against RabbitMQ')
    import pika
    from django.utils.module_loading import import_string

    from django_aiogram.broker.rabbitmq.client import close_connections

    scrub = pika.BlockingConnection(pika.URLParameters(AMQP_URL)).channel()
    scrub.queue_delete(queue=AMQP_QUEUE)
    with override_settings(TELEGRAM_BOT=SETTINGS):
        broker = import_string(path)()
        broker.conformance_path = path
        try:
            yield broker
        finally:
            close_connections()
            scrub.queue_delete(queue=AMQP_QUEUE)
            scrub.connection.close()


@pytest.fixture
def countable(broker):
    """The same broker, for a case that asks it how many messages are waiting.

    Separate from `broker` so that skipping is impossible to do by accident: a case wanting a
    trustworthy count says so by asking for this one, and every other case keeps running.
    """
    reason = UNTRUSTWORTHY_DEPTH.get(getattr(broker, 'conformance_path', ''))
    if reason:
        pytest.skip(reason)
    return broker


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_published_message_can_be_taken(broker: Broker):
    """Publish then take, which is the whole of what a transport is for."""
    broker.publish([payload(7)])

    taken = broker.take_nowait()

    assert taken is not None, 'nothing came back from a queue that was just written to'
    assert taken.payload == payload(7)


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_publishing_nothing_queues_nothing_and_raises_nothing(broker: Broker):
    """The transports disagree by nature, so the contract has to decide.

    A batching producer accepts an empty batch quietly; `RPUSH key` with no values is a
    syntax error to Redis — measured, `wrong number of arguments for 'rpush' command`. A
    caller holding a list that turned out empty should not have to know which transport it
    is talking to, so the answer is: nothing happens.

    Not reachable through this package's own producers — `_chunks` yields no chunk for an
    empty iterable, so the loop body never runs — which is exactly why it belongs here
    rather than in a producer test. The contract accepts a `Sequence[bytes]` from anyone.
    """
    before = broker.depth()

    broker.publish([])

    assert broker.depth() == before, 'publishing nothing changed the queue'


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_awaiting_half_publishes_and_counts_the_same(countable: Broker):
    """Every `a*` method is a second implementation, and a second place to regress.

    The synchronous cases above would all pass with `apublish`, `adepth` and
    `ainflight_depth` broken — they are separate code reaching a separate client, one per
    loop. Asserted together in one case because they are one round trip in practice: a
    producer under ASGI publishes and then reads a depth on the same loop.
    """

    async def on_a_loop() -> tuple[int, int, int]:
        """Publish nothing, then one, and read both depths without leaving the loop."""
        await countable.apublish([])
        after_nothing = await countable.adepth()
        await countable.apublish([payload(11)])
        return after_nothing, await countable.adepth(), await countable.ainflight_depth()

    after_nothing, after_one, inflight = asyncio.run(on_a_loop())

    assert after_nothing == 0, 'awaiting an empty publish queued something'
    assert after_one == 1, 'the awaited publish did not arrive'
    assert inflight == 0, 'nothing was taken, so nothing is in flight'


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_an_empty_queue_answers_none_rather_than_blocking(broker: Broker):
    """`take_nowait` on nothing is `None`, not an exception and not a wait."""
    assert broker.take_nowait() is None


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_an_acknowledged_message_does_not_come_back(broker: Broker):
    """What `ack` means, stated as the only thing that can be checked from outside."""
    broker.publish([payload(1)])
    taken = broker.take_nowait()
    assert taken is not None

    broker.ack(taken.handle)

    assert broker.inflight_depth() == 0, 'the message is still in flight after being settled'
    assert broker.reclaim() in (0, None), 'a settled message was reclaimed'


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_released_message_comes_back(broker: Broker):
    """The difference between refused and delivered, which is why `release` exists.

    A broker where leaving a message alone already means "redeliver it" implements this as a
    no-op — and then reclaiming is what brings it back, which is the same promise reached the
    other way. Either is conformant; losing the message is not.
    """
    broker.publish([payload(2)])
    taken = broker.take_nowait()
    assert taken is not None

    broker.release(taken.handle)
    broker.reclaim()

    again = broker.take_nowait()
    assert again is not None, 'a released message was lost'
    assert again.payload == payload(2)


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_depth_counts_what_is_waiting(countable: Broker):
    """Two published, two waiting; one taken, one waiting."""
    countable.publish([payload(3), payload(4)])
    assert countable.depth() == 2

    countable.take_nowait()

    assert countable.depth() == 1


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_what_is_taken_and_unsettled_is_in_flight(broker: Broker):
    """The count `MAX_IN_FLIGHT` is compared against, from the transport's own books."""
    broker.publish([payload(5)])
    assert broker.inflight_depth() == 0

    taken = broker.take_nowait()

    assert taken is not None
    assert broker.inflight_depth() == 1, 'a taken message is not counted as in flight'


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_handle_is_opaque_and_round_trips(broker: Broker):
    """`Delivery` passes a handle back unread, so the broker must accept its own.

    Asserted by settling with the handle exactly as given, not by looking at it: what it
    names is the transport's business, and a test that reads it would be pinning one
    transport's answer as the contract.
    """
    broker.publish([payload(6)])
    taken = broker.take_nowait()
    assert taken is not None

    broker.ack(taken.handle)

    assert broker.take_nowait() is None, 'the message survived being settled by its handle'


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_broker_says_whether_a_kill_loses_a_message(broker: Broker):
    """Whatever the answer, there has to be one — a deployment refuses on it."""
    assert isinstance(broker.crash_safe, bool)


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_broker_says_whether_it_needs_a_worker_name(broker: Broker):
    """True only where the transport cannot say which consumer holds a message."""
    assert isinstance(broker.needs_identity, bool)


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_liveness_answers_without_a_consumer_running(broker: Broker):
    """A probe asks this of a process that may never have consumed anything."""
    liveness = broker.liveness()

    assert isinstance(liveness.reported, bool)
    assert liveness.age is None or liveness.age >= 0


def test_every_transport_has_a_kill_case_and_the_map_names_it():
    """The map in `tests/integration/conftest.py` is documentation, so it can rot.

    Each transport's kill case is named for its own mechanism — a reclaim on the Redis list, a
    group claim on Streams, a dropped channel on RabbitMQ, an uncommitted offset on Kafka — so
    there is no naming convention a reader can grep for, and the coverage is invisible from
    outside. That is what the map exists to fix, and this is what keeps the map true.

    Both directions. A renamed case must not leave the map pointing at nothing, and a new
    transport must not arrive without one: the entry is due on the day the broker lands, the
    same rule this file already holds itself to.
    """
    import pathlib
    import re

    root = pathlib.Path(__file__).resolve().parent
    doc = (root / 'integration' / 'conftest.py').read_text(encoding='utf-8')
    mapping = doc[doc.index('kill case') : doc.index('"""', doc.index('kill case'))]

    named = Counter(re.findall(r'`(test_[a-z_]+)`', mapping))
    assert named, 'the kill-case map has no case names in it at all'

    # counted, not merely looked up: RabbitMQ and Kafka name their kill case identically, so
    # a set membership test passed with one of the two renamed away -- the name was still
    # defined, in the other transport's module. The map mentions it twice, so the suite has to
    # define it twice
    defined = Counter()
    for module in (root / 'integration').glob('test_*.py'):
        defined.update(re.findall(r'^def (test_[a-z_]+)', module.read_text(encoding='utf-8'), re.MULTILINE))
    short = sorted(
        f'{name} (map wants {want}, suite has {defined[name]})' for name, want in named.items() if defined[name] < want
    )
    assert short == [], f'the kill-case map is ahead of the suite: {short}'

    # every shipped transport, by the last word of its dotted path — `RedisListBroker` is
    # "redis list" in the map, so the check is on the words rather than on the class name
    for path in SHIPPED:
        kind = path.rsplit('.', 2)[-2].replace('_', ' ')
        assert kind in mapping.lower(), (
            f'no kill case is named for {kind!r} ({path}); add one and list it, '
            f'rather than leaving a transport whose crash behaviour nobody checked'
        )


@pytest.mark.parametrize('path', sorted(SHIPPED))
def test_the_ceiling_follows_the_setting_the_broker_names(path):
    """`CALL_TIMEOUT_OPTION` is a name and `call_ceiling` is a number: they must be one fact.

    Two declarations for one thing invite drift, and this is where the drift would be invisible:
    `W004`'s hint quotes the name while the consumer's cap uses the number, so a broker whose name
    said one setting and whose ceiling read another would tell an operator to raise a setting that
    changes nothing.

    Both directions. Moving the named setting moves the ceiling, and moving *another* transport's
    timeout does not -- which is the defect #41 was: `REDIS_TIMEOUT` bound the cap on every
    transport.
    """
    broker = import_string(path)
    named = broker.CALL_TIMEOUT_OPTION
    assert named, f'{path} declares no CALL_TIMEOUT_OPTION'
    assert named in broker.OPTIONS, f'{path} names {named!r}, which it does not declare'

    others = {'REDIS_TIMEOUT', 'RABBITMQ_TIMEOUT', 'KAFKA_TIMEOUT'} - {named}
    # every required option of every transport, so one `override_settings` serves all four
    required = {'REDIS_STREAM_KEY': 'tg', 'KAFKA_TOPIC': 'tg', 'RABBITMQ_QUEUE': 'tg'}
    base = {**SETTINGS, 'BROKER': path, **required}
    with override_settings(TELEGRAM_BOT={**base, named: 37}):
        assert broker.call_timeout() == 37, f'{path} does not read {named}'
    for other in others:
        with override_settings(TELEGRAM_BOT={**base, named: 37, other: 3}):
            assert broker.call_timeout() == 37, f'{path} reads {other} as well as {named}'
