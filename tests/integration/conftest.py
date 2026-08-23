"""Fixtures for the suite that needs a real Redis.

fakeredis agrees with everything, which is exactly why it cannot answer the
questions that broke 1.x deployments: whether `LMOVE` exists, what a server
without it says when asked, and whether FSM state written by one process is
readable by the next.
"""

import os

import pytest
from redis import Redis

from django_aiogram.producer.throttling import reset_rate_limiters
from django_aiogram.redis import reset_redis

# the marker is registered in pyproject.toml, and each module carries it itself
REDIS_URL = os.environ.get('DJANGO_AIOGRAM_TEST_REDIS_URL', '')
#: the AMQP half of the same idea. A transport with no in-memory double needs a server
#: for every one of its cases, so this one gates a whole module rather than a few tests
AMQP_URL = os.environ.get('DJANGO_AIOGRAM_TEST_AMQP_URL', '')


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
