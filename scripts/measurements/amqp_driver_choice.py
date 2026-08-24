"""Which RabbitMQ driver to use, measured on both faces with the guarantee held constant.

The producer's ordinary caller is synchronous — a view, a task, a management command — and
``asend`` is not, so whichever driver ships, one of those two faces reaches the other across a
thread boundary. This measures both drivers on both faces, and each bridge is timed from the
side that pays it: a synchronous caller reaching aio-pika's loop, and a coroutine reaching
pika's thread. Timing the second from the caller instead would add the first, which is how an
earlier version of this made them look like the same number.

**The guarantee has to be held constant, and the first attempt at this did not.**
``aio_pika.Connection.channel()`` confirms publishes by default and ``pika``'s does not, so
comparing ``basic_publish`` with ``default_exchange.publish`` put a confirmed publish next to
fire-and-forget and read as a 15x difference in the driver. Most of it was the promise. Both are
measured here with confirms off and on.

Run it against a throwaway broker:

    python -m venv .measure && .measure/bin/pip install pika aio-pika
    docker run -d --rm --name amqp -p 5673:5672 rabbitmq:4
    .measure/bin/python -m scripts.measurements.amqp_driver_choice
"""

import asyncio
import os
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from typing import NamedTuple

import aio_pika
import pika
from aio_pika.abc import AbstractChannel
from pika.adapters.blocking_connection import BlockingChannel
from scripts.measurements._timing import ROUNDS, configure_reporting, logger, measure, report, run_name

#: a throwaway broker, overridable because a reader's port is their own business
URL = os.environ.get('DJANGO_AIOGRAM_TEST_AMQP_URL', 'amqp://guest:guest@127.0.0.1:5673/')
QUEUE = run_name('amqp')
#: the shape of a queued call this package actually sends
BODY = b'{"function": "send_message", "chat_id": 1}'
#: everything the transport asks for on every publish except the confirm, which is the variable
#: here. Persistence makes the broker write to disk before it answers and ``mandatory`` makes an
#: unroutable message an error, so a row without them times a cheaper promise than the one this
#: package makes -- the same mistake as putting a confirmed publish next to a fire-and-forget
#: one, one level down. Held constant across every row so that only the confirm varies.
PERSISTENT = pika.DeliveryMode.Persistent

#: the bridged rows' channels and the connections behind them, on the pool's single worker: both
#: rows run there, each wanting a channel of its own, and a connection can only be closed by the
#: thread that owns it. The connections are kept separately because one may exist without a
#: channel -- `channel()` is a round trip and can fail
_bridged = threading.local()


def main() -> None:
    """Measure both drivers on both faces and report what the numbers decide.

    Each thing is released by the ``finally`` of the block that acquired it, rather than by one
    teardown at the end: opening the aio-pika side is itself a step that can fail, and a single
    ``try`` entered after both factories have returned would leak the pika connections when the
    second one raised. The rule is that a failed run leaves the broker as it was found too.
    """
    configure_reporting()
    plain, confirming = _pika_channels()
    try:
        loop, runner, connection, unconfirmed_channel, confirmed_channel = _aio_pika_channels()
        try:
            _report(plain, confirming, loop, unconfirmed_channel, confirmed_channel)
        finally:
            _tear_down_aio_pika(loop, runner, connection)
    finally:
        _tear_down_pika(plain, confirming)


def _report(
    plain: BlockingChannel,
    confirming: BlockingChannel,
    loop: asyncio.AbstractEventLoop,
    unconfirmed_channel: AbstractChannel,
    confirmed_channel: AbstractChannel,
) -> None:
    """Time every row and say what the numbers decide.

    The executor for the bridged rows is made and unmade here, because that is the whole of its
    life: one worker, so the pika connections it opens have a nameable thread to be closed on.
    """
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix='bridged') as pool:
        try:
            _time_them(Channels(plain, confirming, unconfirmed_channel, confirmed_channel), loop, pool)
        finally:
            _closed_on_its_thread(pool)


class Channels(NamedTuple):
    """The four channels a run opens: two pika, two aio-pika, confirmed and not.

    Grouped rather than passed one by one, so the function that times them takes three arguments
    instead of six -- and so a reader sees them as what they are, one fixture per row.
    """

    plain: BlockingChannel
    confirming: BlockingChannel
    unconfirmed: AbstractChannel
    confirmed: AbstractChannel


def _time_them(channels: Channels, loop: asyncio.AbstractEventLoop, pool: ThreadPoolExecutor) -> None:
    """Time every row and say what the numbers decide."""
    plain, confirming = channels.plain, channels.confirming
    unconfirmed_channel, confirmed_channel = channels.unconfirmed, channels.confirmed
    logger.info('pika, on its own synchronous face:')
    measure('unconfirmed', _purged(_pika_publish(plain), plain))
    pika_confirmed = measure('confirmed', _purged(_pika_publish(confirming), plain))

    logger.info('aio-pika, handed to a loop thread (what a sync producer needs):')
    aio_queued = measure('unconfirmed', _purged(_publisher(loop, unconfirmed_channel), plain))
    aio_confirmed = measure('confirmed', _purged(_publisher(loop, confirmed_channel), plain))

    logger.info('pika, awaited from a loop via to_thread:')
    # each of these publishes through a connection its *own* thread opened, which is what the
    # transport does and what pika allows: a `BlockingConnection` belongs to one thread, so
    # sharing the main thread's from an executor would be measuring an unsupported path
    # purged here rather than through `_purged`, because this row brings back samples instead of
    # a callable to wrap — the rule is the same, an empty queue before each row starts
    plain.queue_purge(QUEUE)
    pika_queued_bridged = report('unconfirmed', _from_a_coroutine(loop, pool, confirmed=False))
    plain.queue_purge(QUEUE)
    report('confirmed', _from_a_coroutine(loop, pool, confirmed=True))

    logger.info('')
    logger.info(
        'the synchronous face: pika %.1f us against aio-pika %.1f us  -> %.1fx',
        pika_confirmed,
        aio_confirmed,
        aio_confirmed / pika_confirmed,
    )
    # computed rather than asserted in prose. The claim the driver decision rests on is that
    # crossing the thread boundary costs the same either way, so the run has to show it: these
    # two rows are the same work bridged in opposite directions
    logger.info(
        'the bridge each driver has to pay: into a loop %.1f us, into a thread %.1f us -> %.2fx',
        aio_queued,
        pika_queued_bridged,
        pika_queued_bridged / aio_queued,
    )
    logger.info(
        'below 1 is the finding: reaching a thread costs about half what reaching a loop does, '
        'so pika is free on the common face and the cheaper of the two on the rare one'
    )


def _pika_channels() -> tuple[BlockingChannel, BlockingChannel]:
    """Open a channel that confirms nothing and one that confirms everything.

    Both opens are guarded, because a helper that raises halfway leaves the caller nothing to
    clean up with: whatever it had opened would be open and unreachable. Every connection it got
    as far as making is closed, not only the first -- ``confirm_delivery`` is a round trip and
    can fail once the second connection already exists.
    """
    opened: list[pika.BlockingConnection] = []
    channels: list[BlockingChannel] = []
    try:
        for confirms in (False, True):
            connection = pika.BlockingConnection(pika.URLParameters(URL))
            opened.append(connection)
            channel = connection.channel()
            if confirms:
                channel.confirm_delivery()
            else:
                channel.queue_declare(queue=QUEUE, durable=True)
            channels.append(channel)
    except BaseException:
        # the queue too, if this got as far as declaring it: the rule is that a run leaves the
        # broker as it found it, and a failure here would otherwise declare a durable queue that
        # nothing will ever delete -- observed, from a test of this very path
        if channels:
            with suppress(BaseException):
                channels[0].queue_delete(QUEUE)
        # every connection this got as far as opening, not just the first: `confirm_delivery`
        # is a round trip and can fail after the second connection exists
        for connection in opened:
            with suppress(BaseException):
                connection.close()
        raise
    plain, confirming = channels
    return plain, confirming


def _aio_pika_channels() -> tuple[
    asyncio.AbstractEventLoop,
    threading.Thread,
    aio_pika.abc.AbstractRobustConnection,
    AbstractChannel,
    AbstractChannel,
]:
    """Start a loop on a thread of its own and open two channels on it.

    A thread, because that is what a synchronous caller would need: aio-pika's connections are
    loop-affine, so ``async_to_sync`` over one built elsewhere raises ``attached to a different
    loop`` — measured, and the reason this row is what a real implementation would cost.
    """
    loop = asyncio.new_event_loop()
    runner = threading.Thread(target=loop.run_forever, daemon=True)
    runner.start()

    async def open_them() -> tuple[aio_pika.abc.AbstractRobustConnection, AbstractChannel, AbstractChannel]:
        connection = await aio_pika.connect_robust(URL)
        declaring: AbstractChannel | None = None
        try:
            confirmed = await connection.channel(publisher_confirms=True)
            declaring = confirmed
            unconfirmed = await connection.channel(publisher_confirms=False)
            await confirmed.declare_queue(QUEUE, durable=True)
        except BaseException:
            # the queue first, on the channel that would have declared it: `declare_queue` can
            # raise after the broker has already made it -- a timeout waiting for the reply is
            # enough -- and a durable queue nobody deletes is exactly what this run must not
            # leave behind. Then the connection, whether that worked or not
            try:
                if declaring is not None:
                    with suppress(BaseException):
                        await declaring.queue_delete(QUEUE)
            finally:
                await connection.close()
            raise
        return connection, unconfirmed, confirmed

    try:
        connection, unconfirmed, confirmed = asyncio.run_coroutine_threadsafe(open_them(), loop).result(30)
    except BaseException:
        # the loop is turning before there is anything on it, so a connect that fails would
        # otherwise leave this thread running for the life of the process -- measured, it did,
        # and an earlier check of this path looked at queues and connections but not threads
        _stopped(loop, runner)
        raise
    return loop, runner, connection, unconfirmed, confirmed


def _stopped(loop: asyncio.AbstractEventLoop, runner: threading.Thread) -> None:
    """Stop the loop, wait for its thread, and close it -- in that order and always.

    Closing before the thread has stopped raises, and leaving the loop open on a daemon thread
    is what makes a run end in a `ResourceWarning` rather than in silence.
    """
    loop.call_soon_threadsafe(loop.stop)
    runner.join(timeout=10)
    loop.close()


def _publisher(loop: asyncio.AbstractEventLoop, channel: AbstractChannel) -> Callable[[], None]:
    """Build a synchronous call that hands one publish to ``loop`` and waits for it."""

    async def publish() -> None:
        await channel.default_exchange.publish(
            aio_pika.Message(BODY, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key=QUEUE,
            mandatory=True,
        )

    return lambda: asyncio.run_coroutine_threadsafe(publish(), loop).result(30)


def _from_a_coroutine(loop: asyncio.AbstractEventLoop, pool: ThreadPoolExecutor, *, confirmed: bool) -> list[float]:
    """Time, from *inside* the loop, what a coroutine pays to publish through pika.

    Timed on the loop rather than from the caller, and that is the point of the shape: timing it
    from the caller would add the caller-to-loop hand-off, which the aio-pika row does not pay --
    that row *is* a synchronous caller reaching a loop. Comparing the two would put one hand-off
    against two and call them the same number, which is the mistake the guarantee-constant rule
    exists to stop, one level up.

    So a coroutine on the loop collects the samples, timing only its own ``run_in_executor`` and
    the publish at the end of it, and brings them back for `report`.

    The channel is opened by the thread the publish lands on, and kept there. That is not
    ceremony: pika documents ``add_callback_threadsafe`` as the only operation another thread may
    perform on a ``BlockingConnection``, so a shared connection would make this row a
    measurement of something the driver does not support. The transport keeps one connection per
    thread for the same reason.

    ``pool`` has one worker, which is what makes the connection closable: a connection belongs
    to its thread, so the only way to close it is to hand the close to that same thread — and
    with an arbitrary pool there is no way to name it. One worker also makes the row measure one
    hand-off rather than an average over however many threads the default pool grew.
    """

    def publish() -> None:
        opened = getattr(_bridged, 'channels', None)
        if opened is None:
            opened = _bridged.channels = {}
        held = getattr(_bridged, 'connections', None)
        if held is None:
            held = _bridged.connections = []
        channel = opened.get(confirmed)
        if channel is None:
            connection = pika.BlockingConnection(pika.URLParameters(URL))
            # recorded before the channel is asked for, because `channel()` and
            # `confirm_delivery` are round trips: a failure in either would otherwise leave a
            # connection nothing knows about, and the closer works from what it knows
            held.append(connection)
            channel = connection.channel()
            if confirmed:
                channel.confirm_delivery()
            opened[confirmed] = channel
        _pika_publish(channel)()

    async def timed() -> list[float]:
        """Warm up once uncounted, then time each hand-off from the thread that makes it."""
        await loop.run_in_executor(pool, publish)
        samples = []
        for _ in range(ROUNDS):
            started = time.perf_counter()
            await loop.run_in_executor(pool, publish)
            samples.append((time.perf_counter() - started) * 1e6)
        return samples

    return asyncio.run_coroutine_threadsafe(timed(), loop).result(300)


def _closed_on_its_thread(pool: ThreadPoolExecutor) -> None:
    """Close whatever pika connection the pool's worker opened, on that worker.

    Every bridged row runs on this one thread, so this is where its connections live and the
    only place they can be closed from. Without it a run leaves them open until the process
    exits, which is the same untidiness as leaving the queue behind.
    """

    def close() -> None:
        # every connection this thread opened, whether or not it got a channel, and each in a
        # `suppress` of its own: one close failing must not strand the rest
        for connection in getattr(_bridged, 'connections', []):
            with suppress(BaseException):
                connection.close()
        _bridged.channels, _bridged.connections = {}, []

    pool.submit(close).result(30)


def _pika_publish(channel: BlockingChannel) -> Callable[[], None]:
    """Build the publish this package makes: persistent, mandatory, on the given channel."""
    properties = pika.BasicProperties(delivery_mode=PERSISTENT)
    return lambda: channel.basic_publish('', QUEUE, BODY, properties=properties, mandatory=True)


def _purged(call: Callable[[], None], keeper: BlockingChannel) -> Callable[[], None]:
    """Wrap ``call`` so the queue is emptied before the row it is timed in.

    Nothing consumes what these publish, so without this each row is measured against the
    backlog the rows before it left — and a deep enough queue puts the broker into flow control,
    which is a different thing to be timing. Purged once per row rather than per publish: the
    purge itself is a round trip and would swamp an 18-microsecond measurement.
    """
    purged = [False]

    def once() -> None:
        if not purged[0]:
            keeper.queue_purge(QUEUE)
            purged[0] = True
        call()

    return once


def _tear_down_aio_pika(
    loop: asyncio.AbstractEventLoop,
    runner: threading.Thread,
    connection: aio_pika.abc.AbstractRobustConnection,
) -> None:
    """Close the aio-pika connection on its own loop, then stop and close that loop.

    In that order: stopping first leaves the connection's reader, writer and heartbeat tasks
    pending, which is what made a successful run end in a page of ``Task was destroyed but it is
    pending``. The stop is in a ``finally`` because a close that times out must not leave the
    thread turning either.
    """
    try:
        asyncio.run_coroutine_threadsafe(connection.close(), loop).result(30)
    finally:
        _stopped(loop, runner)


def _tear_down_pika(plain: BlockingChannel, confirming: BlockingChannel) -> None:
    """Delete the run's queue and close both synchronous connections.

    The queue goes first because it is the only durable trace a run leaves behind, and each
    close is in a ``finally`` of its own so that one failing does not strand the other.
    """
    try:
        plain.queue_delete(QUEUE)
    finally:
        try:
            plain.connection.close()
        finally:
            confirming.connection.close()


if __name__ == '__main__':
    main()
