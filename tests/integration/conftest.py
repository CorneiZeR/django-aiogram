"""Fixtures for the suite that needs a real server — Redis, RabbitMQ or Kafka.

fakeredis agrees with everything, which is exactly why it cannot answer the
questions that broke 1.x deployments: whether `LMOVE` exists, what a server
without it says when asked, and whether FSM state written by one process is
readable by the next. RabbitMQ and Kafka have no double at all.

**Every transport has a kill case, and they are named for its own mechanism rather than
uniformly** — which makes the coverage hard to see from outside, so here it is:

* Redis list — `test_a_message_left_in_flight_is_reclaimed`, with
  `test_a_worker_does_not_reclaim_another_workers_message` for the identity half, because this
  is the transport where a name decides who may take the work back.
* Redis Streams — `test_nothing_is_lost_when_a_consumer_never_comes_back` and
  `test_a_dead_consumers_work_is_reclaimed_under_any_name`; the second is the point, since the
  pending list belongs to the group rather than to a worker.
* RabbitMQ — `test_a_message_a_killed_worker_held_comes_back`, paired with
  `test_an_acknowledged_message_does_not_come_back_after_a_reconnect` so the two differ only in
  the acknowledgement.
* Kafka — `test_a_message_a_killed_worker_held_comes_back`, where the redelivery is the group's
  doing rather than a reclaim.

**Two cases need the broker *stopped*, and they are the only ones that do** —
`test_a_confirmed_publish_survives_the_broker_going_away` on RabbitMQ and
`test_a_produced_record_survives_the_broker_going_away` on Kafka. A confirm is not an fsync
barrier and an acknowledged produce is not a flushed one, so what either promises across a
restart cannot be arranged from a client. Hence `DJANGO_AIOGRAM_TEST_AMQP_CONTAINER` and
`DJANGO_AIOGRAM_TEST_KAFKA_CONTAINER`, and hence both skip until one is named: restarting a
container is not something a test suite should decide to do to a machine it was not pointed at.

The CI legs that run them are named per transport, so a red check says which mechanism broke.
Measured against Valkey 8.1.9 as well, which answers `redis_version:7.2.4` for compatibility —
the fork the legs exist to check.
"""

import os
import shutil
import subprocess
import time

import pytest
from redis import Redis

from django_aiogram.producer.throttling import reset_rate_limiters
from django_aiogram.redis import reset_redis

# the marker is registered in pyproject.toml, and each module carries it itself
REDIS_URL = os.environ.get('DJANGO_AIOGRAM_TEST_REDIS_URL', '')
#: the AMQP half of the same idea. A transport with no in-memory double needs a server
#: for every one of its cases, so this one gates a whole module rather than a few tests
AMQP_URL = os.environ.get('DJANGO_AIOGRAM_TEST_AMQP_URL', '')
#: and the Kafka half. A topic per test rather than a cleaned one: Kafka does not remove a
#: message when it is settled — an offset moves — so an empty queue cannot be arranged by
#: tidying up, and a group that has committed carries its position with its name
KAFKA_BOOTSTRAP = os.environ.get('DJANGO_AIOGRAM_TEST_KAFKA_BOOTSTRAP', '')
#: the container each server runs in, for the two cases that need it *stopped*. Durability
#: across a restart cannot be arranged from a client: a confirm says the broker took
#: responsibility, and whether it kept it is only answerable by taking the broker away. Named
#: rather than discovered, because a fixture that went looking for "the RabbitMQ container" on a
#: developer's machine could find one belonging to something else and restart that instead
AMQP_CONTAINER = os.environ.get('DJANGO_AIOGRAM_TEST_AMQP_CONTAINER', '')
KAFKA_CONTAINER = os.environ.get('DJANGO_AIOGRAM_TEST_KAFKA_CONTAINER', '')


@pytest.fixture(scope='session')
def redis_url():
    if not REDIS_URL:
        pytest.skip('set DJANGO_AIOGRAM_TEST_REDIS_URL to run the integration suite')
    return REDIS_URL


@pytest.fixture(scope='session')
def amqp_url():
    if not AMQP_URL:
        pytest.skip('set DJANGO_AIOGRAM_TEST_AMQP_URL to run the RabbitMQ suite')
    return AMQP_URL


@pytest.fixture(scope='session')
def kafka_bootstrap():
    if not KAFKA_BOOTSTRAP:
        pytest.skip('set DJANGO_AIOGRAM_TEST_KAFKA_BOOTSTRAP to run the Kafka suite')
    return KAFKA_BOOTSTRAP


@pytest.fixture
def amqp_container(amqp_url):
    if not AMQP_CONTAINER:
        pytest.skip('set DJANGO_AIOGRAM_TEST_AMQP_CONTAINER to run the cases that restart the broker')
    return _restartable(AMQP_CONTAINER)


@pytest.fixture
def kafka_container(kafka_bootstrap):
    if not KAFKA_CONTAINER:
        pytest.skip('set DJANGO_AIOGRAM_TEST_KAFKA_CONTAINER to run the cases that restart the broker')
    return _restartable(KAFKA_CONTAINER)


def _restartable(name):
    """The container, having proved that this machine can restart it.

    Skipped rather than failed when docker is absent or the container is not running: naming one
    is opting in, and a machine that cannot honour the opt-in has not broken anything. A wrong
    *name*, though, is a failure — it is a configured expectation that cannot be met, and the
    case it silently skipped is the only one that answers the question.
    """
    docker = shutil.which('docker')
    if docker is None:
        pytest.skip('docker is not on PATH, so no container can be restarted')
    # resolved rather than named: an absolute path is what makes this a call to the docker on
    # this machine rather than to whatever a PATH entry answers with by then
    listed = subprocess.run(  # noqa: S603 - a resolved absolute path and a name from this suite's own environment
        [docker, 'ps', '--filter', f'name=^{name}$', '--format', '{{.Names}}'],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if listed.returncode != 0:
        pytest.skip(f'docker ps failed, so no container can be restarted: {listed.stderr.strip()}')
    assert listed.stdout.split() == [name], (
        f'{name!r} is configured as the container to restart and docker does not report it running'
    )
    return name


@pytest.fixture
def restart_container():
    """Restart a container and wait until the server inside it answers again.

    The wait is the whole fixture. `docker restart` returns when the process has been started,
    not when the broker is accepting connections, and a take issued into that window fails for a
    reason that has nothing to do with what the case is asking. So the caller passes a probe --
    one that returns falsey while the server is not ready -- and this holds until it agrees.

    Bounded and then failed, never skipped: a server that never comes back is a real answer.
    """

    def restart(name, ready, timeout=120.0):
        docker = shutil.which('docker')
        assert docker is not None, 'docker left PATH between naming the container and restarting it'
        done = subprocess.run(  # noqa: S603 - as above: a resolved path, and a name the fixture already verified
            [docker, 'restart', name], capture_output=True, text=True, timeout=timeout, check=False
        )
        assert done.returncode == 0, f'could not restart {name!r}: {done.stderr.strip()}'
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                if ready():
                    return
            except Exception:  # noqa: S110 - the server not answering yet is the expected state here
                pass
            time.sleep(0.5)
        pytest.fail(f'{name!r} restarted but the server did not answer within {timeout}s')

    return restart


@pytest.fixture
def kafka_topic(kafka_bootstrap):
    """A topic of this test's own, created and waited for before the test runs.

    Created up front because subscribing to a topic that does not exist leaves the consumer
    with no assignment — the coordinator has nothing to give it — so every take would wait out
    its whole budget for a topic the first publish would have made.
    """
    import uuid

    from confluent_kafka.admin import AdminClient, NewTopic

    from django_aiogram.broker.kafka.client import close_clients

    name = f'integration-{uuid.uuid4().hex[:12]}'
    admin = AdminClient({'bootstrap.servers': kafka_bootstrap})
    for future in admin.create_topics([NewTopic(name, num_partitions=1, replication_factor=1)]).values():
        future.result(timeout=30)
    for _ in range(100):
        if name in admin.list_topics(timeout=10).topics:
            break
        time.sleep(0.1)
    else:
        pytest.skip(f'kafka did not report the topic {name!r} after creating it')
    try:
        yield name
    finally:
        close_clients()
        # waited for: the futures are the request, and a fixture that returns without reading
        # them leaves the topic on the broker for every later run to trip over
        for future in admin.delete_topics([name]).values():
            future.result(timeout=30)


@pytest.fixture
def broker_channel(amqp_url):
    """A raw channel for arranging and inspecting, with the queue deleted around each test.

    **This deletes the queue named below**, before and after every test, so point
    `DJANGO_AIOGRAM_TEST_AMQP_URL` at a throwaway broker or vhost.
    """
    import pika

    from django_aiogram.broker.rabbitmq.client import close_connections

    connection = pika.BlockingConnection(pika.URLParameters(amqp_url))
    channel = connection.channel()
    channel.queue_delete(queue=AMQP_QUEUE)
    close_connections()
    try:
        yield channel
    finally:
        close_connections()
        channel.queue_delete(queue=AMQP_QUEUE)
        connection.close()


#: the queue every case in the RabbitMQ suite uses, deleted around each of them
AMQP_QUEUE = 'integration'


@pytest.fixture
def server(redis_url):
    """A real client, flushed before each test so nothing leaks between them.

    **This erases the whole selected database**, before and after every test.
    Point `DJANGO_AIOGRAM_TEST_REDIS_URL` at a throwaway server or at
    least a database nothing else uses.
    """
    client = Redis.from_url(redis_url)
    client.flushdb()
    reset_redis()
    reset_rate_limiters()
    try:
        yield client
    finally:
        client.flushdb()
        reset_redis()
        reset_rate_limiters()
        client.close()


@pytest.fixture
def version(server):
    """The server's Redis version as a tuple, for skipping what it cannot do."""
    raw = str(server.info('server')['redis_version'])
    return tuple(int(part) for part in raw.split('.')[:2])
