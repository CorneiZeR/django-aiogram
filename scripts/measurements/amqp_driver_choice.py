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
    logger.info('pika, on its own synchronous face:')
    plain, confirming = _pika_channels()
    measure('unconfirmed', lambda: plain.basic_publish('', QUEUE, BODY))
    pika_confirmed = measure('confirmed', lambda: confirming.basic_publish('', QUEUE, BODY))

    logger.info('aio-pika, handed to a loop thread (what a sync producer needs):')
    loop, unconfirmed_channel, confirmed_channel = _aio_pika_channels()
    aio_queued = measure('unconfirmed', _publisher(loop, unconfirmed_channel))
    aio_confirmed = measure('confirmed', _publisher(loop, confirmed_channel))

    logger.info('pika, awaited from a loop via to_thread:')
    pika_queued_bridged = measure('unconfirmed', _threaded(loop, lambda: plain.basic_publish('', QUEUE, BODY)))
    measure('confirmed', _threaded(loop, lambda: confirming.basic_publish('', QUEUE, BODY)))

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
    loop.call_soon_threadsafe(loop.stop)


def _pika_channels() -> tuple[BlockingChannel, BlockingChannel]:
    """Open a channel that confirms nothing and one that confirms everything."""
    plain = pika.BlockingConnection(pika.URLParameters(URL)).channel()
    plain.queue_declare(queue=QUEUE, durable=True)
    confirming = pika.BlockingConnection(pika.URLParameters(URL)).channel()
    confirming.confirm_delivery()
    return plain, confirming


def _aio_pika_channels() -> tuple[asyncio.AbstractEventLoop, AbstractChannel, AbstractChannel]:
    """Start a loop on a thread of its own and open two channels on it.

    A thread, because that is what a synchronous caller would need: aio-pika's connections are
    loop-affine, so ``async_to_sync`` over one built elsewhere raises ``attached to a different
    loop`` — measured, and the reason this row is what a real implementation would cost.
    """
    loop = asyncio.new_event_loop()
    threading.Thread(target=loop.run_forever, daemon=True).start()

    async def open_them() -> tuple[AbstractChannel, AbstractChannel]:
        connection = await aio_pika.connect_robust(URL)
        confirmed = await connection.channel(publisher_confirms=True)
        unconfirmed = await connection.channel(publisher_confirms=False)
        await confirmed.declare_queue(QUEUE, durable=True)
        return unconfirmed, confirmed

    unconfirmed, confirmed = asyncio.run_coroutine_threadsafe(open_them(), loop).result(30)
    return loop, unconfirmed, confirmed


def _publisher(loop: asyncio.AbstractEventLoop, channel: AbstractChannel) -> Callable[[], None]:
    """Build a synchronous call that hands one publish to ``loop`` and waits for it."""

    async def publish() -> None:
        await channel.default_exchange.publish(aio_pika.Message(BODY), routing_key=QUEUE)

    return lambda: asyncio.run_coroutine_threadsafe(publish(), loop).result(30)


def _threaded(loop: asyncio.AbstractEventLoop, call: Callable[[], None]) -> Callable[[], None]:
    """Build a call that runs a synchronous publish in a thread, awaited from ``loop``."""

    async def awaited() -> None:
        await asyncio.to_thread(call)

    return lambda: asyncio.run_coroutine_threadsafe(awaited(), loop).result(30)


if __name__ == '__main__':
    main()
