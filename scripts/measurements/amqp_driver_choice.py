"""Which RabbitMQ driver to use, measured on both faces with the guarantee held constant.

The producer's ordinary caller is synchronous — a view, a task, a management command — and
``asend`` is not, so whichever driver ships, one of those two faces reaches the other across a
thread boundary. This measures both drivers on both faces.

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
from collections.abc import Callable

import aio_pika
import pika
from aio_pika.abc import AbstractChannel
from pika.adapters.blocking_connection import BlockingChannel
from scripts.measurements._timing import configure_reporting, logger, measure

#: a throwaway broker, overridable because a reader's port is their own business
URL = os.environ.get('DJANGO_AIOGRAM_TEST_AMQP_URL', 'amqp://guest:guest@127.0.0.1:5673/')
QUEUE = 'measurement'
#: the shape of a queued call this package actually sends
BODY = b'{"function": "send_message", "chat_id": 1}'


def main() -> None:
    """Measure both drivers on both faces and report what the numbers decide."""
    configure_reporting()
    plain, confirming = _pika_channels()
    loop, connection, unconfirmed_channel, confirmed_channel = _aio_pika_channels()
    try:
        _report(plain, confirming, loop, unconfirmed_channel, confirmed_channel)
    finally:
        _tear_down(plain, confirming, loop, connection)


def _report(
    plain: BlockingChannel,
    confirming: BlockingChannel,
    loop: asyncio.AbstractEventLoop,
    unconfirmed_channel: AbstractChannel,
    confirmed_channel: AbstractChannel,
) -> None:
    """Time every row and say what the numbers decide."""
    logger.info('pika, on its own synchronous face:')
    measure('unconfirmed', _purged(lambda: plain.basic_publish('', QUEUE, BODY), plain))
    pika_confirmed = measure('confirmed', _purged(lambda: confirming.basic_publish('', QUEUE, BODY), plain))

    logger.info('aio-pika, handed to a loop thread (what a sync producer needs):')
    aio_queued = measure('unconfirmed', _purged(_publisher(loop, unconfirmed_channel), plain))
    aio_confirmed = measure('confirmed', _purged(_publisher(loop, confirmed_channel), plain))

    logger.info('pika, awaited from a loop via to_thread:')
    # each of these publishes through a connection its *own* thread opened, which is what the
    # transport does and what pika allows: a `BlockingConnection` belongs to one thread, so
    # sharing the main thread's from an executor would be measuring an unsupported path
    pika_queued_bridged = measure('unconfirmed', _purged(_threaded(loop, confirmed=False), plain))
    measure('confirmed', _purged(_threaded(loop, confirmed=True), plain))

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
        'bridged either way: aio-pika %.1f us against pika %.1f us  -> %.2fx',
        aio_queued,
        pika_queued_bridged,
        pika_queued_bridged / aio_queued,
    )
    logger.info('a ratio near 1 is the finding: the hand-off is the price, not the library')


def _pika_channels() -> tuple[BlockingChannel, BlockingChannel]:
    """Open a channel that confirms nothing and one that confirms everything."""
    plain = pika.BlockingConnection(pika.URLParameters(URL)).channel()
    plain.queue_declare(queue=QUEUE, durable=True)
    confirming = pika.BlockingConnection(pika.URLParameters(URL)).channel()
    confirming.confirm_delivery()
    return plain, confirming


def _aio_pika_channels() -> tuple[
    asyncio.AbstractEventLoop, aio_pika.abc.AbstractRobustConnection, AbstractChannel, AbstractChannel
]:
    """Start a loop on a thread of its own and open two channels on it.

    A thread, because that is what a synchronous caller would need: aio-pika's connections are
    loop-affine, so ``async_to_sync`` over one built elsewhere raises ``attached to a different
    loop`` — measured, and the reason this row is what a real implementation would cost.
    """
    loop = asyncio.new_event_loop()
    threading.Thread(target=loop.run_forever, daemon=True).start()

    async def open_them() -> tuple[aio_pika.abc.AbstractRobustConnection, AbstractChannel, AbstractChannel]:
        connection = await aio_pika.connect_robust(URL)
        confirmed = await connection.channel(publisher_confirms=True)
        unconfirmed = await connection.channel(publisher_confirms=False)
        await confirmed.declare_queue(QUEUE, durable=True)
        return connection, unconfirmed, confirmed

    connection, unconfirmed, confirmed = asyncio.run_coroutine_threadsafe(open_them(), loop).result(30)
    return loop, connection, unconfirmed, confirmed


def _publisher(loop: asyncio.AbstractEventLoop, channel: AbstractChannel) -> Callable[[], None]:
    """Build a synchronous call that hands one publish to ``loop`` and waits for it."""

    async def publish() -> None:
        await channel.default_exchange.publish(aio_pika.Message(BODY), routing_key=QUEUE)

    return lambda: asyncio.run_coroutine_threadsafe(publish(), loop).result(30)


def _threaded(loop: asyncio.AbstractEventLoop, *, confirmed: bool) -> Callable[[], None]:
    """Build a call that publishes through pika from a thread, awaited from ``loop``.

    The channel is opened by whichever thread the publish lands on, and kept there. That is not
    ceremony: pika documents ``add_callback_threadsafe`` as the only operation another thread may
    perform on a ``BlockingConnection``, so a shared connection would make this row a
    measurement of something the driver does not support. The transport keeps one connection per
    thread for the same reason.
    """
    local = threading.local()

    def publish() -> None:
        channel = getattr(local, 'channel', None)
        if channel is None:
            connection = pika.BlockingConnection(pika.URLParameters(URL))
            channel = connection.channel()
            if confirmed:
                channel.confirm_delivery()
            local.channel = channel
        channel.basic_publish('', QUEUE, BODY)

    async def awaited() -> None:
        await asyncio.to_thread(publish)

    return lambda: asyncio.run_coroutine_threadsafe(awaited(), loop).result(30)


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


def _tear_down(
    plain: BlockingChannel,
    confirming: BlockingChannel,
    loop: asyncio.AbstractEventLoop,
    connection: aio_pika.abc.AbstractRobustConnection,
) -> None:
    """Close what was opened and leave the broker as it was found.

    The aio-pika connection is closed on the loop that owns it and *before* the loop stops:
    stopping first leaves its reader, writer and heartbeat tasks pending, which is what made a
    successful run end in a page of ``Task was destroyed but it is pending``. The queue goes last
    because it is the only durable trace a run leaves behind.
    """
    asyncio.run_coroutine_threadsafe(connection.close(), loop).result(30)
    loop.call_soon_threadsafe(loop.stop)
    plain.queue_delete(QUEUE)
    plain.connection.close()
    confirming.connection.close()


if __name__ == '__main__':
    main()
