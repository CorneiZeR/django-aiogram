"""One producer per process, one consumer per thread, because that is what librdkafka allows.

``confluent-kafka`` rather than ``aiokafka``, and for two reasons that were measured in that
order. The consumer here is a thread, and a synchronous driver belongs in one: `aiokafka` would
need an event loop inside it, which is the machinery that lost `aio-pika` the RabbitMQ decision.

And it is faster on the face that matters. Held to the same guarantee — both waiting for the
broker — six runs put ``confluent-kafka`` at 166 to 237 microseconds against ``aiokafka``'s
354 to 492, so 1.6 to 2.2 times. Both spreads are about 40 per cent of their own floor, which
is what a laptop's broker does to a half-millisecond round trip -- the gap between the drivers
is what survives it. An earlier single run showed 479 against 502 and the parity was
written down as the finding; it was a cold broker, and the number that survived repetition is
this one. Re-take them with ``scripts/measurements`` rather than trusting either.

The other argument in the plan turned out to be false and is recorded so it is not reopened:
`aiokafka` is **not** pure Python. It ships no ``py3-none-any`` wheel, so both drivers are
compiled and there is no portability difference between them.
"""

import logging
import threading
from contextlib import suppress
from typing import TYPE_CHECKING, Any

from django.core.exceptions import ImproperlyConfigured
from django.core.signals import setting_changed

from django_aiogram.config.settings import SETTINGS_NAME

logger = logging.getLogger('django_aiogram')

if TYPE_CHECKING:
    from confluent_kafka import Consumer, Producer
else:  # the annotations below are evaluated at runtime in a `|` union
    Consumer = Producer = Any

__all__ = ('close_clients', 'consumer_for_thread', 'metadata_client', 'shared_producer')

#: how long a producer being replaced or closed is given to hand over what it holds. Not the
#: transport's `KAFKA_TIMEOUT`: this runs while settings are changing or a process is going
#: away, which is exactly when reading a setting is least reliable
_DRAIN_SECONDS = 5

#: librdkafka's producer is thread-safe and keeps its own I/O thread, so one per process is
#: right — and a second would mean a second connection pool for no reason
_producer_lock = threading.Lock()
_producer: tuple[str, 'Producer'] | None = None

#: the consumer is *not* thread-safe, and it also carries the group membership: a second one on
#: another thread would be a second member, taking a share of the partitions. The metadata
#: reader beside it is the same object with no subscription, which is the whole difference
#: between a member and an observer.
#:
#: Held in thread-local storage and nowhere else, deliberately. A registry of every client this
#: process opened would have to be pruned by the thread that owns each one — nothing else may
#: touch a librdkafka client — so a thread that exits without closing would leave its entry
#: behind for ever, keeping a socket and librdkafka's own threads alive with it. A server that
#: recycles worker threads and asks for a queue depth on them would grow that list without
#: bound. Thread-local storage is discarded with its thread, which is the behaviour wanted
_local = threading.local()


def shared_producer(bootstrap: str) -> 'Producer':
    """Reach the process's producer, built on first use and rebuilt when its servers change.

    Configured for one message at a time, because that is what a send is: see the comment on
    ``linger.ms`` below for the 6.4 milliseconds the driver's default charges for it.

    The lock is held only long enough to hand back a reference, so a settings change or a
    shutdown can drain and drop this producer while a `publish` is still using it. That was
    considered and left alone, because a drain is not a close: ``flush`` empties the queue and
    the object goes on working for whoever holds it. Measured -- a producer flushed and dropped
    from here accepted another record and had it acknowledged, with no error. And `publish`
    polls the producer it was given, so its own records are answered on its own thread; if they
    are not, it raises rather than returning as though they went.

    A lease that made `close_clients` wait for publishes in flight would close a window that
    loses nothing and open one that matters: a publish blocked on an unreachable broker would
    hold shutdown for ``KAFKA_TIMEOUT`` on top of the grace arithmetic `Deployment.md` states.
    What the window does cost is the count `_drain` reports, which is taken before those later
    records exist and can therefore understate what was still in the queue at shutdown.
    """
    from confluent_kafka import Producer as KafkaProducer  # noqa: PLC0415 - the driver is an extra

    global _producer  # noqa: PLW0603 - one per process, like the connection it holds
    with _producer_lock:
        if _producer is not None and _producer[0] == bootstrap:
            return _producer[1]
        if not bootstrap:
            msg = f"{SETTINGS_NAME}['KAFKA_BOOTSTRAP'] is required to talk to Kafka."
            raise ImproperlyConfigured(msg)
        if _producer is not None:
            _drain(_producer[1])
        # `linger.ms` at 0 because `publish` waits for the broker to answer, so librdkafka's
        # default of 5 milliseconds is paid on every send while it holds a batch open for
        # records that are not coming: measured, one confirmed publish costs 6.4ms on the
        # default against 241us at 0. Batching still happens -- a `publish` of a hundred
        # payloads costs 0.44ms here against 7.01ms on the default, 4.4us a message -- so what
        # is switched off is the waiting, and the bulk path is faster for it too
        _producer = (bootstrap, KafkaProducer({'bootstrap.servers': bootstrap, 'linger.ms': 0}))
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
    return consumer


def _drain(producer: 'Producer') -> None:
    """Flush what the producer still holds, and say so when it could not all go.

    The queue is librdkafka's, and this process has already told a caller that every message
    in it was accepted — `publish` waits for the broker before it returns, so anything left
    here was accepted by librdkafka and then blocked on the way out. Dropping the producer on
    top of that loses them in silence.

    ``flush`` answers with how many are still queued — measured, 3 against an unreachable
    broker — so the count is reported. It cannot be recovered from here: the producer is being
    replaced because its servers changed, or the process is shutting down, and there is nowhere
    for them to go either way. Saying how many is the honest part.
    """
    remaining = producer.flush(_DRAIN_SECONDS)
    if remaining:
        logger.warning(
            'kafka messages were accepted locally and never reached the broker',
            extra={'tg_count': remaining},
        )


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
    return reader


def _drop_this_threads_reader() -> None:
    """Close and forget the calling thread's metadata client, if it has one."""
    reader = getattr(_local, 'reader', None)
    if reader is None:
        return
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
    # a consumer that is already gone is the outcome being asked for
    with suppress(Exception):
        consumer.close()
    for attribute in ('identity', 'consumer'):
        if hasattr(_local, attribute):
            delattr(_local, attribute)


def close_clients(**_kwargs: object) -> None:
    """Flush the producer and close this thread's consumer.

    The other threads' clients are left alone, and are not even recorded: librdkafka's are not
    thread-safe either, and unlike pika there is no documented way to ask one to close itself.
    So the honest thing is to leave each to the thread that owns it, which reaches this on its
    own next use because :func:`consumer_for_thread` rebuilds when the settings behind it have
    moved — and to keep no list of them, since a list nothing may act on is a list that only
    keeps sockets alive.
    """
    global _producer  # noqa: PLW0603 - as above
    with _producer_lock:
        if _producer is not None:
            _drain(_producer[1])
            _producer = None
    _drop_this_threads_consumer()
    _drop_this_threads_reader()


def _forget(**kwargs: object) -> None:
    """Drop the clients when the settings that built them change."""
    if kwargs.get('setting') == SETTINGS_NAME:
        close_clients()


setting_changed.connect(_forget, dispatch_uid='django_aiogram.broker.kafka.client')
