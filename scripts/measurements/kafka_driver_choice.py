"""Which Kafka driver to use, and the answer is not the one latency alone would give.

Both drivers on both faces, with the guarantee held constant: ``produce`` answers locally, so a
run that does not wait for the broker is measuring librdkafka's queue rather than a delivery.

The first run of this said the two drivers were the same speed and the conclusion was written
down as "latency does not decide it". Repeating it on a warm broker said otherwise —
``confluent-kafka`` is 1.3 to 2.2 times faster with both waiting — so the parity was a cold
cluster rather than a finding. That is why this is kept rather than remembered: **run it three
times before believing it**, which the first attempt did not.

Run it against a throwaway broker whose advertised listener the host can reach — with the
image's own default it answers ``localhost:9092`` from inside the container and a client on the
host retries into a refusal loop rather than failing. `README.md` has the full command.
"""

import asyncio
import contextlib
import os
import threading
import time

from aiokafka import AIOKafkaProducer
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from scripts.measurements._timing import configure_reporting, logger, measure, run_name

#: 200 rather than more: every confirmed produce here is a broker round trip of about half a
#: millisecond, so this is already half a minute of waiting
ROUNDS = 200
BOOTSTRAP = os.environ.get('DJANGO_AIOGRAM_TEST_KAFKA_BOOTSTRAP', '127.0.0.1:9093')
TOPIC = run_name('kafka')
BODY = b'{"function": "send_message", "chat_id": 1}'


class RefusedRecordError(RuntimeError):
    """The broker declined a record, so there is nothing here worth timing.

    Its own class because the alternative is a long message at the raise site, which the
    package's own rules refuse — and because a measurement that hits this has to stop rather
    than average a refusal into its median.
    """

    def __init__(self, reason: object) -> None:
        """Name what librdkafka said, since it is the only clue to why."""
        super().__init__(f'the broker refused a record: {reason}')


def main() -> None:
    """Measure both drivers with and without waiting, and report what decides it."""
    configure_reporting()
    admin = _declared()
    try:
        _report()
    finally:
        _deleted(admin)


def _report() -> None:
    """Time every row on the topic this run made, and say what the numbers decide."""
    producer = Producer({'bootstrap.servers': BOOTSTRAP, 'linger.ms': 0})
    loop, awaiting = _aiokafka_producer()
    try:
        logger.info('confluent-kafka, synchronous, its own face:')
        measure('queued locally, not confirmed', lambda: _queue_one(producer), rounds=ROUNDS)
        confluent = measure('waited for the broker ack', lambda: _confirm_one(producer), rounds=ROUNDS)

        logger.info('aiokafka, handed to a loop thread (what a sync producer needs):')
        measure('queued locally, not confirmed', _sender(loop, awaiting, confirmed=False), rounds=ROUNDS)
        aiokafka = measure('waited for the broker ack', _sender(loop, awaiting, confirmed=True), rounds=ROUNDS)

        logger.info('')
        logger.info(
            'confluent-kafka %.1f us against aiokafka %.1f us  -> %.2fx',
            confluent,
            aiokafka,
            aiokafka / confluent,
        )
        logger.info(
            'run this three times before believing it: the first single run of it showed a parity that was not real'
        )
    finally:
        try:
            _stopped(loop, awaiting)
        finally:
            # the synchronous producer is drained whatever the async one did: it holds records
            # this run produced, and they are the only thing here that can still be lost
            producer.flush(10)


def _stopped(loop: asyncio.AbstractEventLoop, awaiting: AIOKafkaProducer) -> None:
    """Close the async producer on its own loop, and only then stop the loop.

    In that order, and in a ``finally``: stopping the loop first leaves the producer's sender
    task and its connections behind with nothing left to run them, which strands whatever the
    last row queued and, on an async driver that notices, ends a successful run in a page of
    pending-task warnings.
    """
    try:
        asyncio.run_coroutine_threadsafe(awaiting.stop(), loop).result(30)
    finally:
        # even if stopping the producer failed: a loop left running holds its thread, and the
        # caller's `flush` below never runs if this raises on the way out
        loop.call_soon_threadsafe(loop.stop)


def _declared() -> AdminClient:
    """Create this run's topic and wait until the broker reports it.

    Explicitly, rather than leaving it to automatic creation: the topic name is unique per run
    so nothing can be measured against somebody else's, which also means the first produce would
    be the one discovering the topic does not exist yet. `measure` makes one warm-up call and
    does not count it, but on a fresh name that call is the one that gets refused -- and the
    refusal is raised rather than averaged, so the run would end instead of warming up.

    The waiting loop is the same shape `tests/integration/conftest.py` uses, and for the same
    reason: `create_topics` returning is not the broker having the topic.
    """
    admin = AdminClient({'bootstrap.servers': BOOTSTRAP})
    for future in admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1)]).values():
        future.result(timeout=30)
    # from here the topic exists, so every way out of this function has to remove it: the
    # caller's cleanup only runs once this has returned, and a readiness check that times out
    # or raises would otherwise leave the topic behind for every later run to trip over
    try:
        ready = _reported(admin)
    except BaseException:
        _deleted(admin)
        raise
    if ready:
        return admin
    _deleted(admin)
    msg = f'kafka did not report the topic {TOPIC!r} after creating it'
    raise RuntimeError(msg)


def _reported(admin: AdminClient) -> bool:
    """Wait for the broker to list this run's topic, and say whether it did.

    `create_topics` returning is not the broker having the topic, and a produce to one it has
    not caught up with is refused rather than queued.
    """
    for _ in range(100):
        if TOPIC in admin.list_topics(timeout=10).topics:
            return True
        time.sleep(0.1)
    return False


def _deleted(admin: AdminClient) -> None:
    """Remove this run's topic, waiting for the broker to say it is gone.

    Waited for, like the creation: ``delete_topics`` returns futures, and a process that exits
    without reading them can leave the topic standing -- measured, it did.
    """
    for future in admin.delete_topics([TOPIC]).values():
        future.result(timeout=30)


def _queue_one(producer: Producer) -> None:
    """Hand one record to librdkafka and return without waiting for the broker."""
    producer.produce(TOPIC, BODY)
    producer.poll(0)


def _confirm_one(producer: Producer) -> None:
    """Hand over one record and wait for the broker to answer for it.

    The delivery callback is what says it arrived, so this waits on that rather than on
    ``flush`` — which would wait for every record in a producer this measurement shares with
    nothing, but the shape is the one the transport uses.

    A *failed* delivery reaches the same callback, and timing that as an acknowledgement would
    be measuring how fast this broker says no. So the error is kept and raised: a measurement
    that quietly averages refusals into its median is worse than one that stops.
    """
    answered = threading.Event()
    refusal: list[object] = []

    def delivered(error: object, _message: object) -> None:
        """Record a refusal before releasing the wait, so the raise below cannot miss it."""
        if error is not None:
            refusal.append(error)
        answered.set()

    producer.produce(TOPIC, BODY, on_delivery=delivered)
    producer.poll(0)
    while not answered.is_set():
        producer.poll(0.001)
    if refusal:
        raise RefusedRecordError(refusal[0])


def _aiokafka_producer() -> tuple[asyncio.AbstractEventLoop, AIOKafkaProducer]:
    """Start a loop on a thread of its own and a producer on it.

    A thread because that is what a synchronous caller would have to do to reach an
    async-native driver, and the synchronous caller is the one this package has.
    """
    loop = asyncio.new_event_loop()
    runner = threading.Thread(target=loop.run_forever, daemon=True)
    runner.start()

    #: the producer as soon as it exists, so the failure path can close it without depending on
    #: cancellation being processed first: `Future.cancel` asks, and `loop.stop` can win the race
    #: -- which would leave aiokafka's own 'Unclosed AIOKafkaProducer' behind, measured
    made: list[AIOKafkaProducer] = []

    async def start() -> AIOKafkaProducer:
        producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP, linger_ms=0)
        made.append(producer)
        await producer.start()
        return producer

    starting = asyncio.run_coroutine_threadsafe(start(), loop)
    try:
        return loop, starting.result(60)
    except BaseException:
        # the loop is running before there is anything to run on it, so a start that fails or
        # times out would otherwise leave this thread turning for the life of the process --
        # the caller's cleanup only begins once this has returned something to clean up
        starting.cancel()
        if made:
            # closed on the loop that made it and *waited for*, before that loop is stopped:
            # scheduling the stop and moving on leaves the same open socket as not scheduling it
            with contextlib.suppress(BaseException):
                asyncio.run_coroutine_threadsafe(made[0].stop(), loop).result(10)
        loop.call_soon_threadsafe(loop.stop)
        runner.join(timeout=10)
        raise


def _sender(loop: asyncio.AbstractEventLoop, producer: AIOKafkaProducer, *, confirmed: bool) -> object:
    """Build a synchronous call that sends one record through ``loop``.

    ``send`` answers with a future for the delivery; awaiting that future is what makes this the
    confirmed row, and dropping it is the queued one.
    """

    async def queued() -> None:
        await producer.send(TOPIC, BODY)

    async def waited() -> None:
        await (await producer.send(TOPIC, BODY))

    chosen = waited if confirmed else queued
    return lambda: asyncio.run_coroutine_threadsafe(chosen(), loop).result(60)


if __name__ == '__main__':
    main()
