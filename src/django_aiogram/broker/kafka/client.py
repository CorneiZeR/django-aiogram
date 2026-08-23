"""One producer per process, one consumer per thread, because that is what librdkafka allows.

``confluent-kafka`` rather than ``aiokafka``, and unlike RabbitMQ the latency did not decide
it: held to the same guarantee the two are 479 and 502 microseconds, which is the round trip to
the broker with the driver invisible behind it. What decided it is that the consumer here is a
thread — a synchronous driver belongs in one — and that `aiokafka` would need an event loop
inside that thread, which is the machinery that lost `aio-pika` the RabbitMQ decision.

The other argument in the plan turned out to be false and is recorded so it is not reopened:
`aiokafka` is **not** pure Python. It ships no ``py3-none-any`` wheel, so both drivers are
compiled and there is no portability difference between them.
"""

import threading
from contextlib import suppress
from typing import TYPE_CHECKING, Any

from django.core.exceptions import ImproperlyConfigured
from django.core.signals import setting_changed

from django_aiogram.config.settings import SETTINGS_NAME

if TYPE_CHECKING:
    from confluent_kafka import Consumer, Producer
else:  # the annotations below are evaluated at runtime in a `|` union
    Consumer = Producer = Any

__all__ = ('close_clients', 'consumer_for_thread', 'metadata_client', 'shared_producer')

#: librdkafka's producer is thread-safe and keeps its own I/O thread, so one per process is
#: right — and a second would mean a second connection pool for no reason
_producer_lock = threading.Lock()
_producer: tuple[str, 'Producer'] | None = None

#: the consumer is *not* thread-safe, and it also carries the group membership: a second one on
#: another thread would be a second member, taking a share of the partitions
_local = threading.local()
_consumers: list[Any] = []
#: and the ones that only read. Kept apart because they differ in the thing that matters: a
#: subscribed consumer is a group member and an unsubscribed one is not
_readers: list[Any] = []
_consumer_lock = threading.Lock()


def shared_producer(bootstrap: str) -> 'Producer':
    """Reach the process's producer, built on first use and rebuilt when its servers change."""
    from confluent_kafka import Producer as KafkaProducer  # noqa: PLC0415 - the driver is an extra

    global _producer  # noqa: PLW0603 - one per process, like the connection it holds
    with _producer_lock:
        if _producer is not None and _producer[0] == bootstrap:
            return _producer[1]
        if not bootstrap:
            msg = f"{SETTINGS_NAME}['KAFKA_BOOTSTRAP'] is required to talk to Kafka."
            raise ImproperlyConfigured(msg)
        if _producer is not None:
            # flushed rather than dropped: the queue is librdkafka's, and abandoning it would
            # abandon messages this process has already been told it accepted
            _producer[1].flush(5)
        _producer = (bootstrap, KafkaProducer({'bootstrap.servers': bootstrap}))
        return _producer[1]


def consumer_for_thread(bootstrap: str, topic: str, group: str, timeout: float) -> 'Consumer':
    """Reach the calling thread's consumer, subscribed to ``topic`` as a member of ``group``.

    ``enable.auto.commit`` is off, and that is the whole reason this transport can promise
    anything: a committed offset means "this message has been sent", and letting librdkafka
    commit on a timer would mean it says so about messages still in flight.

    ``auto.offset.reset='earliest'``, so a group joining a topic that already has messages
    starts at the beginning rather than skipping what nobody has read — the same choice the
    Streams broker makes by creating its group at id 0.
    """
    from confluent_kafka import Consumer as KafkaConsumer  # noqa: PLC0415 - the driver is an extra

    identity = (bootstrap, topic, group, timeout)
    existing: Consumer | None = getattr(_local, 'consumer', None)
    if existing is not None and getattr(_local, 'identity', None) == identity:
        return existing
    if not bootstrap:
        msg = f"{SETTINGS_NAME}['KAFKA_BOOTSTRAP'] is required to talk to Kafka."
        raise ImproperlyConfigured(msg)
    _drop_this_threads_consumer()
    consumer = KafkaConsumer(
        {
            'bootstrap.servers': bootstrap,
            'group.id': group,
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest',
            'socket.timeout.ms': int(timeout * 1000),
        }
    )
    consumer.subscribe([topic])
    _local.identity, _local.consumer = identity, consumer
    with _consumer_lock:
        _consumers.append(consumer)
    return consumer


def metadata_client(bootstrap: str, group: str, timeout: float) -> 'Consumer':
    """Reach a consumer that reads metadata and committed offsets and joins no group.

    ``subscribe`` is what makes a consumer a *member*, and a process that only publishes must
    not become one: the coordinator would give it partitions it never polls, and on a
    single-partition topic the real worker then receives nothing until that member's session
    times out. A healthcheck asking how deep the queue is could starve the consumer it was
    checking on.

    Measured: ``list_topics``, ``committed`` and ``get_watermark_offsets`` all answer on a
    consumer that has never subscribed. ``group.id`` is still needed — a committed offset
    belongs to a group — which is why this takes one and joins nothing.
    """
    from confluent_kafka import Consumer as KafkaConsumer  # noqa: PLC0415 - the driver is an extra

    identity = (bootstrap, group, timeout)
    existing: Consumer | None = getattr(_local, 'reader', None)
    if existing is not None and getattr(_local, 'reader_identity', None) == identity:
        return existing
    if not bootstrap:
        msg = f"{SETTINGS_NAME}['KAFKA_BOOTSTRAP'] is required to talk to Kafka."
        raise ImproperlyConfigured(msg)
    _drop_this_threads_reader()
    reader = KafkaConsumer(
        {
            'bootstrap.servers': bootstrap,
            'group.id': group,
            'enable.auto.commit': False,
            'socket.timeout.ms': int(timeout * 1000),
        }
    )
    _local.reader_identity, _local.reader = identity, reader
    with _consumer_lock:
        _readers.append(reader)
    return reader


def _drop_this_threads_reader() -> None:
    """Close and forget the calling thread's metadata client, if it has one."""
    reader = getattr(_local, 'reader', None)
    if reader is None:
        return
    with _consumer_lock:
        _readers[:] = [held for held in _readers if held is not reader]
    with suppress(Exception):
        reader.close()
    for attribute in ('reader_identity', 'reader'):
        if hasattr(_local, attribute):
            delattr(_local, attribute)


def _drop_this_threads_consumer() -> None:
    """Close and forget the calling thread's consumer, if it has one.

    ``close()`` leaves the group cleanly, which matters more here than for the other
    transports: a member that disappears without saying so holds its partitions until the
    session times out, and nothing is delivered from them meanwhile.
    """
    consumer = getattr(_local, 'consumer', None)
    if consumer is None:
        return
    with _consumer_lock:
        _consumers[:] = [held for held in _consumers if held is not consumer]
    # a consumer that is already gone is the outcome being asked for
    with suppress(Exception):
        consumer.close()
    for attribute in ('identity', 'consumer'):
        if hasattr(_local, attribute):
            delattr(_local, attribute)


def close_clients(**_kwargs: object) -> None:
    """Flush the producer and close this thread's consumer.

    The other threads' consumers are left alone. librdkafka's are not thread-safe either, and
    unlike pika there is no documented way to ask one to close itself — so the honest thing is
    to leave it to the thread that owns it, which reaches this on its own next use because
    :func:`consumer_for_thread` rebuilds when the settings behind it have moved.
    """
    global _producer  # noqa: PLW0603 - as above
    with _producer_lock:
        if _producer is not None:
            _producer[1].flush(5)
            _producer = None
    _drop_this_threads_consumer()
    _drop_this_threads_reader()


def _forget(**kwargs: object) -> None:
    """Drop the clients when the settings that built them change."""
    if kwargs.get('setting') == SETTINGS_NAME:
        close_clients()


setting_changed.connect(_forget)
