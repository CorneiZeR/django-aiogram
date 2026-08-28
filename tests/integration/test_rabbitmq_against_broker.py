"""The RabbitMQ transport against a real broker, because it has no in-memory double.

Every case here needs a server, which is the difference from the Redis transports: fakeredis
answers for those well enough that only the awkward questions come here. So this module is
gated as a whole, and the conformance suite covers this transport against the same server —
what is left for here is what the contract cannot ask.

The kill test is the reason the module exists. RabbitMQ returns an unacknowledged message when
the channel that held it drops, and that is the whole of this transport's crash safety: no
in-flight list, no worker name, nothing for a restart to reclaim. Dropping the connection is
what a killed worker does to it, so that is what this arranges.
"""

import asyncio
import gc
import threading
import weakref
from concurrent.futures import ThreadPoolExecutor

import pytest
from django.test import override_settings

from django_aiogram.broker.rabbitmq import RabbitMQBroker
from django_aiogram.broker.rabbitmq.client import close_connections
from django_aiogram.broker.rabbitmq.exceptions import QueueRefusedError
from django_aiogram.wire.serializers import JsonSerializer

from .conftest import AMQP_QUEUE

pytestmark = pytest.mark.integration


def payload(chat_id):
    return JsonSerializer().dumps({'function': 'send_message', 'chat_id': chat_id})


def settings_for(url):
    return {'TOKEN': '42:x', 'RATE_LIMIT': None, 'RABBITMQ_URL': url, 'RABBITMQ_QUEUE': AMQP_QUEUE}


@pytest.fixture
def broker(broker_channel, amqp_url):
    with override_settings(TELEGRAM_BOT=settings_for(amqp_url)):
        yield RabbitMQBroker()


def test_a_message_a_killed_worker_held_comes_back(broker, broker_channel, amqp_url):
    """The transport's entire crash-safety story, arranged the way a kill arranges it.

    The message is taken and never settled, and then the connection holding it goes away. No
    reclaim is called, no worker is named and nothing is written down: RabbitMQ puts it back
    because the channel that owed an acknowledgement is gone.

    A second broker takes it, which is the replacement container — and it does not have to be
    the same one, which is what `needs_identity` being false means.
    """
    with override_settings(TELEGRAM_BOT=settings_for(amqp_url)):
        broker.publish([payload(7)])
        taken = broker.take_nowait()
        assert taken is not None, 'the message was not delivered in the first place'
        assert broker.inflight_depth() == 1

        close_connections()  # what dying does to a worker's channel

        replacement = RabbitMQBroker()
        again = replacement.take_nowait()

        assert again is not None, 'an unacknowledged message did not come back'
        assert again.payload == payload(7)
        assert replacement.reclaim() is None, 'this transport claims to need a reclaim'


def test_an_acknowledged_message_does_not_come_back_after_a_reconnect(broker, amqp_url):
    """The other half: settled work must not return, or every restart would resend.

    The same drop as above, so the two differ only in the acknowledgement — which is the thing
    being tested.
    """
    with override_settings(TELEGRAM_BOT=settings_for(amqp_url)):
        broker.publish([payload(8)])
        taken = broker.take_nowait()
        assert taken is not None
        broker.ack(taken.handle)

        close_connections()

        assert RabbitMQBroker().take_nowait() is None, 'an acknowledged message came back'


def test_a_release_puts_it_back_at_once(broker, amqp_url):
    """`basic_nack` with requeue, and no waiting for an idle threshold.

    Worth asserting against a server because this is the one transport where giving a message
    up is a single command: the Redis list leaves it in place and a stream has to move an idle
    counter first.
    """
    with override_settings(TELEGRAM_BOT=settings_for(amqp_url)):
        broker.publish([payload(9)])
        taken = broker.take_nowait()
        assert taken is not None

        broker.release(taken.handle)

        assert broker.inflight_depth() == 0, 'a released message is still counted as in flight'
        again = broker.take_nowait()
        assert again is not None, 'a released message was lost'
        assert again.payload == payload(9)


def test_depth_counts_what_is_ready_and_inflight_counts_what_is_held(broker, amqp_url):
    """The two numbers answer different questions, and AMQP only reports one of them.

    `message_count` is ready messages — measured, it reads 0 while a message is out with a
    consumer — so the in-flight count is this broker's own tally. Asserted together because
    the pair is what a queue-depth alert reads.
    """
    with override_settings(TELEGRAM_BOT=settings_for(amqp_url)):
        broker.publish([payload(1), payload(2), payload(3)])

        assert broker.depth() == 3
        assert broker.inflight_depth() == 0

        taken = broker.take_nowait()
        assert taken is not None

        assert broker.depth() == 2, 'a delivered message is still counted as waiting'
        assert broker.inflight_depth() == 1
        assert broker.depth() + broker.inflight_depth() == 3, 'a message went missing'


def test_a_publish_that_cannot_be_routed_raises(broker, broker_channel, amqp_url):
    """Confirmed and mandatory, so a message with nowhere to go is an error.

    Arranged by deleting the queue behind the broker's back, which is what an operator doing
    housekeeping does. Without `mandatory` the exchange would drop it silently and `send()`
    would return an id for a message that never existed.
    """
    with override_settings(TELEGRAM_BOT=settings_for(amqp_url)):
        broker.publish([payload(1)])  # opens the channel and declares the queue
        broker_channel.queue_delete(queue=AMQP_QUEUE)

        with pytest.raises(QueueRefusedError) as refused:
            broker.publish([payload(2)])

        assert refused.value.queue == AMQP_QUEUE, refused.value.queue
        # the pair, not just the queue: a caller deciding whether to retry reads what the
        # broker said, and reading it out of the sentence is what this attribute replaced
        assert refused.value.reason, 'the refusal carries no reason from the broker'
        assert refused.value.reason in str(refused.value), 'the reason left the message'
        assert AMQP_QUEUE in str(refused.value), str(refused.value)


def test_the_awaited_halves_work_off_the_loop(broker, amqp_url):
    """`apublish` and `adepth` go through a thread, because the driver is synchronous.

    That hand-off is the price the driver decision put on this face — 67 to 85 microseconds,
    measured, against the 121 to 131 the other driver would have charged the synchronous caller
    on its face instead. Worth a case because a thread and a `BlockingConnection` are exactly
    the combination that deadlocks when it is done wrong.

    The depths alone cannot say whether the hand-off happened: they would read the same if
    `apublish` called `publish` inline on the loop's own thread. So the thread each synchronous
    half ran on is recorded, and none of them may be the loop's.
    """
    # one list per method, not one between them: a single list is non-empty as soon as *either*
    # half reaches its synchronous method, so a half that stopped calling its own would leave the
    # assertion looking satisfied by the other one
    publishing: list[int] = []
    measuring: list[int] = []
    published, measured = RabbitMQBroker.publish, RabbitMQBroker.depth

    def watched_publish(self, payloads):
        publishing.append(threading.get_ident())
        return published(self, payloads)

    def watched_depth(self):
        measuring.append(threading.get_ident())
        return measured(self)

    with override_settings(TELEGRAM_BOT=settings_for(amqp_url)):

        async def on_a_loop():
            await broker.apublish([])
            after_nothing = await broker.adepth()
            await broker.apublish([payload(11), payload(12)])
            return threading.get_ident(), after_nothing, await broker.adepth(), await broker.ainflight_depth()

        RabbitMQBroker.publish, RabbitMQBroker.depth = watched_publish, watched_depth
        try:
            loop_thread, after_nothing, after_two, inflight = asyncio.run(on_a_loop())
        finally:
            RabbitMQBroker.publish, RabbitMQBroker.depth = published, measured

        assert after_nothing == 0, 'awaiting an empty publish queued something'
        assert after_two == 2, 'the awaited publishes did not arrive'
        assert inflight == 0

        assert publishing, 'apublish never reached publish, so this proves nothing about it'
        assert measuring, 'adepth never reached depth, so this proves nothing about it'
        assert loop_thread not in publishing, f'publish ran on the loop thread: {publishing}'
        assert loop_thread not in measuring, f'depth ran on the loop thread: {measuring}'


def test_waiting_for_a_message_returns_without_one(broker, amqp_url):
    """`take` has to give the consumer its turn back, or a shutdown waits for traffic."""
    with override_settings(TELEGRAM_BOT=settings_for(amqp_url)):
        assert broker.take(0.2) is None

        broker.publish([payload(5)])
        taken = broker.take(2)

        assert taken is not None, 'a published message did not arrive within the timeout'
        assert taken.payload == payload(5)


def test_the_prefetch_is_always_stated_even_when_it_is_unlimited(broker, amqp_url, monkeypatch):
    """Skipping `basic_qos` is not the same as asking for no limit.

    A server with `default_consumer_prefetch` configured applies it to a consumer that never
    sent QoS, so a package documenting 0 as unlimited has to say 0 out loud — otherwise the
    documented default is whatever the operator's `rabbitmq.conf` happens to say, silently.
    """
    from pika.adapters.blocking_connection import BlockingChannel

    asked: list[int] = []
    original = BlockingChannel.basic_qos

    def recording(self, prefetch_size=0, prefetch_count=0, **kwargs):
        asked.append(prefetch_count)
        return original(self, prefetch_size, prefetch_count, **kwargs)

    monkeypatch.setattr(BlockingChannel, 'basic_qos', recording)

    with override_settings(TELEGRAM_BOT=settings_for(amqp_url)):
        broker.publish([payload(1)])

    assert asked == [0], f'prefetch was not stated to the broker: {asked}'


def test_a_blocked_connection_cannot_hold_a_call_for_ever(broker, amqp_url):
    """pika leaves `blocked_connection_timeout` unset, and this transport does not.

    RabbitMQ blocks a publisher's connection under memory or disk pressure, and a blocked
    connection with no timeout holds every synchronous call on it indefinitely — on a request
    thread, that is a web worker that never comes back. Asserted on the parameters the
    connection was built with, because the condition itself needs a broker under pressure.
    """
    from django_aiogram.broker.rabbitmq import client

    with override_settings(TELEGRAM_BOT={**settings_for(amqp_url), 'RABBITMQ_TIMEOUT': 3}):
        broker.publish([payload(1)])

        # `_impl.params` because a `BlockingConnection` is a facade and does not expose the
        # parameters it was built with — measured, there is no public accessor for them
        assert client._local.connection._impl.params.blocked_connection_timeout == 3.0

    explicit = f'{amqp_url}?blocked_connection_timeout=7'
    with override_settings(TELEGRAM_BOT={**settings_for(explicit), 'RABBITMQ_TIMEOUT': 3}):
        RabbitMQBroker().publish([payload(2)])

        assert client._local.connection._impl.params.blocked_connection_timeout == 7.0, (
            'a timeout written into the URL was overridden by the setting'
        )


def test_a_settings_change_does_not_reach_across_a_thread(broker, amqp_url, monkeypatch):
    """The connection a worker thread opened is *asked* to close, not closed from here.

    `BlockingConnection` belongs to the thread that opened it — pika documents
    `add_callback_threadsafe` as the only thing another thread may do to one — so closing a
    worker's connection from the settings receiver would race whatever frame it is in.

    Asserted on which call the connection received, not on whether the publishes worked: those
    succeed either way while the worker is idle, which is exactly why a direct cross-thread
    close *looks* fine. The recorder notes the thread each call came from, so a close arriving
    from anywhere but the owner is the failure.

    `apublish` is what opens a connection on a thread this one does not own.
    """
    from pika.adapters.blocking_connection import BlockingConnection

    from django_aiogram.broker.rabbitmq import client

    closed_from: list[int] = []
    asked_from: list[int] = []
    close, ask = BlockingConnection.close, BlockingConnection.add_callback_threadsafe

    def recording_close(self, *args, **kwargs):
        closed_from.append(threading.get_ident())
        return close(self, *args, **kwargs)

    def recording_ask(self, callback):
        asked_from.append(threading.get_ident())
        return ask(self, callback)

    monkeypatch.setattr(BlockingConnection, 'close', recording_close)
    monkeypatch.setattr(BlockingConnection, 'add_callback_threadsafe', recording_ask)

    with override_settings(TELEGRAM_BOT=settings_for(amqp_url)):
        asyncio.run(broker.apublish([payload(1)]))
        mine = getattr(client._local, 'connection', None)
        assert [c for c in client._opened if c is not mine], 'apublish opened nothing on another thread'
        closed_from.clear()
        asked_from.clear()

        client.close_connections()

    here = threading.get_ident()
    assert asked_from == [here], f'the foreign connection was not asked to close: {asked_from}'
    assert here not in closed_from, 'a connection was closed directly from a thread that does not own it'

    with override_settings(TELEGRAM_BOT=settings_for(amqp_url)):
        # measured across the publish, because the first message is still in the queue: a
        # `>= 1` here would have held whether or not this one arrived
        before = broker.depth()
        asyncio.run(broker.apublish([payload(2)]))

        assert broker.depth() == before + 1, 'a publish after a settings change did not arrive'


def test_a_handle_from_another_broker_is_refused(broker, amqp_url):
    """A delivery tag is an integer this channel assigned, so anything else came from elsewhere.

    The Redis list makes the same refusal for the same reason: saying so beats letting the
    driver complain about a type it was handed, which is a traceback naming a method.
    """
    with override_settings(TELEGRAM_BOT=settings_for(amqp_url)):
        with pytest.raises(TypeError, match='delivery tag'):
            broker.ack(b'a redis payload')

        with pytest.raises(TypeError, match='delivery tag'):
            broker.release('not a tag')


def test_taking_again_after_the_connection_was_replaced(broker, amqp_url):
    """A consumer generator belongs to the channel it was opened on.

    `take` caches the generator, because `consume` fixes its inactivity timeout when the
    generator is made. Cache it across a *connection* being replaced and the next take with the
    same timeout advances a generator whose channel is dead — a failure where opening a new
    consumer was the whole intent. Settings moving under a running consumer is a live path, not
    a theoretical one.
    """
    with override_settings(TELEGRAM_BOT=settings_for(amqp_url)):
        broker.publish([payload(1)])
        first = broker.take(2)
        assert first is not None, 'the first take found nothing'
        # settled before the drop, or the transport correctly returns it to the queue and the
        # take below gets *that* message — which would say nothing about the generator
        broker.ack(first.handle)

        close_connections()

        broker.publish([payload(2)])
        again = broker.take(2)

        assert again is not None, 'the take after a replaced connection found nothing'
        assert again.payload == payload(2)


def test_replacing_a_connection_does_not_leave_the_old_one_open(broker, amqp_url):
    """Closing a channel does not close its connection, so whoever replaces it has to.

    Arranged by closing the *channel* and leaving the connection up, which is what a broker
    does when it takes exception to something on that channel — a `queue_declare` against a
    queue declared with different arguments, most often. `channel_for_thread` then sees a
    channel that is not open and builds a replacement.

    A settings change does not exercise this: `override_settings` fires the receiver on the way
    in and on the way out, so the connection is already closed by the time a replacement is
    asked for. That is why the first version of this case passed with the fix removed.
    """
    from django_aiogram.broker.rabbitmq import client

    with override_settings(TELEGRAM_BOT=settings_for(amqp_url)):
        broker.publish([payload(1)])
        stranded = client._local.connection
        client._local.channel.close()
        assert stranded.is_open, 'closing the channel closed the connection, so there is nothing to leak'

        broker.publish([payload(2)])

        assert not stranded.is_open, 'the replaced connection was left open'
        assert client._local.connection is not stranded, 'the connection was not replaced at all'
        assert len(client._opened) == 1, f'{len(client._opened)} connections are still held'


def test_a_tag_from_a_replaced_channel_is_not_sent(broker, amqp_url, caplog):
    """A delivery tag means something only on the channel that issued it.

    Settling with a tag from a channel that has been replaced would acknowledge whichever
    delivery now holds that number on the new one — or draw `PRECONDITION_FAILED - unknown
    delivery tag`, which closes the channel and takes the rest of the work with it. Neither is
    a thing to do with a send that finished across a reconnect.

    So nothing is sent, and that is the safe half: the channel that owed the acknowledgement
    dropped, so RabbitMQ has already put the message back. The message coming back is what the
    second half asserts.
    """
    with override_settings(TELEGRAM_BOT=settings_for(amqp_url)):
        broker.publish([payload(4)])
        taken = broker.take_nowait()
        assert taken is not None

        close_connections()

        with caplog.at_level('WARNING', logger='django_aiogram'):
            broker.ack(taken.handle)

        assert 'its channel was replaced' in caplog.text, caplog.text
        again = broker.take_nowait()
        assert again is not None, 'the message was neither settled nor returned'
        assert again.payload == payload(4)
        assert again.handle != taken.handle, 'the same handle came back, so nothing was reissued'


def test_a_connection_whose_setup_fails_is_closed(broker, amqp_url, monkeypatch):
    """A connection built and then abandoned mid-setup is one nothing can ever close.

    It is not in `_opened` and not in the thread's slot, so neither shutdown nor a settings
    change reaches it. A `queue_declare` the broker disagrees with is the ordinary way to get
    here, and a project retrying past it would leak a socket per attempt.
    """
    from pika.adapters.blocking_connection import BlockingChannel

    from django_aiogram.broker.rabbitmq import client

    opened: list[object] = []

    def refuse(self, *args, **kwargs):
        opened.append(self.connection)
        msg = 'the broker disagreed about this queue'
        raise RuntimeError(msg)

    monkeypatch.setattr(BlockingChannel, 'queue_declare', refuse)

    with override_settings(TELEGRAM_BOT=settings_for(amqp_url)), pytest.raises(RuntimeError):
        broker.publish([payload(1)])

    assert opened, 'the setup was never reached, so this proves nothing'
    assert not opened[0].is_open, 'the abandoned connection was left open'
    assert not client._opened, f'it was recorded anyway: {list(client._opened)}'


def test_a_connection_whose_thread_is_gone_is_not_held(broker, amqp_url, broker_channel):
    """The registry has to be able to reach a foreign connection, and must not be why it lives.

    `_opened` exists because pika allows another thread exactly one operation on a
    `BlockingConnection` — `add_callback_threadsafe` — so `close_connections` can ask a
    connection it did not open to close itself, and asking needs a reference.

    A **strong** reference made that job into a leak. The list was pruned only by a close that
    went through this module, so a worker thread that ended without closing left its connection
    referenced for the life of the process, with its socket. The growth was bounded by how often
    `close_connections` runs — at shutdown and on a settings change — which in a server with a
    pool of worker threads is not often.

    Arranged with a pool of one, shut down. After that there is no thread and no thread-local
    slot, so anything still holding the connection is this module and nothing else.
    """
    from django_aiogram.broker.rabbitmq import client

    with override_settings(TELEGRAM_BOT=settings_for(amqp_url)):
        pool = ThreadPoolExecutor(max_workers=1)
        pool.submit(broker.publish, [payload(21)]).result()

        # a weak reference and never a strong one: holding the connection to assert about it
        # would be the very reference this case is looking for
        assert len(client._opened) == 1, f'the publish opened {len(client._opened)} connections on that thread'
        ref = weakref.ref(next(iter(client._opened)))
        assert ref() is not None

        pool.shutdown(wait=True)
        gc.collect()

        assert ref() is None, 'the connection outlived the thread that owned it'
        assert not client._opened, f'the registry is still holding it: {list(client._opened)}'


def test_the_in_flight_count_forgets_a_lost_channel(broker, amqp_url):
    """An unacknowledged delivery goes back to the queue when its channel closes.

    So it is no longer this worker's work, and counting it as in flight is wrong twice over:
    the number is too high, and it grows by one per reconnect. Taking the requeued message
    again would then be counted as a second delivery of something this process holds once.
    """
    with override_settings(TELEGRAM_BOT=settings_for(amqp_url)):
        broker.publish([payload(3)])
        assert broker.take_nowait() is not None, 'the message was not delivered'
        assert broker.inflight_depth() == 1

        close_connections()

        again = broker.take_nowait()

        assert again is not None, 'the requeued message did not come back'
        assert broker.inflight_depth() == 1, 'the handle from the closed channel is still counted'


def test_two_threads_never_share_a_channel_number(amqp_url):
    """A handle from one thread must not look current on another.

    The number in a handle says which channel issued the tag, and per-thread numbering would
    make the first channel on every thread number 1 — so a handle made on one thread and
    settled on another would pass a check that compares numbers. `apublish` opens a channel on
    a worker thread, so more than one thread opens them here.

    Asserted on the numbers rather than on a settle, because the collision is what makes the
    settle unsafe and it is the thing that can be observed directly.
    """
    from django_aiogram.broker.rabbitmq import client

    seen: list[int] = []

    with override_settings(TELEGRAM_BOT=settings_for(amqp_url)):
        broker = RabbitMQBroker()
        broker.publish([payload(1)])
        seen.append(client.channel_generation())

        def on_another_thread():
            broker.publish([payload(2)])
            return client.channel_generation()

        worker = ThreadPoolExecutor(max_workers=1)
        try:
            seen.append(worker.submit(on_another_thread).result(timeout=30))
        finally:
            worker.shutdown(wait=True)

    assert 0 not in seen, f'a thread reported no channel at all: {seen}'
    assert len(set(seen)) == len(seen), f'two threads share a channel number: {seen}'


@pytest.fixture
def clean_queue(amqp_url):
    """Delete the queue before and after, each time on a connection made for the purpose.

    Not `broker_channel`, which every other case here uses: that fixture holds one channel for
    the length of the test, and a case that restarts the broker leaves it pointing at a
    connection that no longer exists — so the tidying up would raise instead of tidying.
    """
    import pika

    def wipe():
        connection = pika.BlockingConnection(pika.URLParameters(amqp_url))
        connection.channel().queue_delete(queue=AMQP_QUEUE)
        connection.close()

    wipe()
    try:
        yield
    finally:
        close_connections()
        wipe()


def test_a_confirmed_publish_survives_the_broker_going_away(amqp_url, amqp_container, restart_container, clean_queue):
    """The confirm is not an fsync barrier, so what it promises has to be tested by taking the
    broker away.

    `RabbitMQ.md` says the publish is persistent, mandatory and confirmed, and that the confirm
    means the broker has taken responsibility rather than that the bytes are on a disk. Nothing
    tested it, and the two one-line regressions that would break it — a publish without
    `delivery_mode=Persistent`, a queue declared non-durable — are both invisible to every other
    case here, because a message that never leaves memory is indistinguishable from a durable one
    until the memory goes.

    **Arranged with nothing consuming**, which is the whole point. With a consumer attached a
    confirm can follow the consume-and-acknowledge rather than the store, so a passing round trip
    would prove nothing about a restart — and a restart with no consumer is the case an operator
    actually has.
    """

    def answering():
        import pika

        connection = pika.BlockingConnection(pika.URLParameters(amqp_url))
        connection.close()
        return True

    with override_settings(TELEGRAM_BOT=settings_for(amqp_url)):
        RabbitMQBroker().publish([payload(11)])
        # before the restart, not after: this is what the process holds, and a connection to a
        # broker that has gone is not a connection this package should be asked to notice
        close_connections()

        restart_container(amqp_container, answering)

        taken = RabbitMQBroker().take(30)

        assert taken is not None, 'a confirmed publish did not survive the broker restarting'
        assert taken.payload == payload(11), 'something came back, but not the message published'
