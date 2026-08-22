"""What the Redis Streams broker can only be asked of a real server.

Three of these exist because the in-memory server answers them wrongly rather than not at
all, which is the more dangerous shape: measured, fakeredis computes ``XINFO GROUPS`` `lag`
one short — two entries published into a fresh group make real Redis answer 1 then 2 and
fakeredis 0 then 1 — so `tests/test_broker_conformance.py` skips its two counting cases for
this transport and names this module as where they really happen.

The rest are properties a fake cannot have an opinion about: what a consumer that never came
back leaves behind, whether anyone can pick it up, and what trimming is allowed to remove.
"""

import asyncio

import pytest
from django.test import override_settings

from django_aiogram.broker.redis_streams import RedisStreamsBroker
from django_aiogram.broker.redis_streams.exceptions import (
    StreamLagUnknownError,
    StreamServerTooOldError,
)
from django_aiogram.wire.serializers import JsonSerializer

pytestmark = pytest.mark.integration

STREAM = 'TELEGRAM_BOT_STREAM'
GROUP = 'django-aiogram'
#: HEARTBEAT_INTERVAL of 1 makes the liveness TTL 3 seconds, so "long enough to look dead"
#: is a number this module can name instead of sleeping for
SETTINGS = {
    'REDIS_STREAM_KEY': STREAM,
    'REDIS_STREAM_GROUP': GROUP,
    'HEARTBEAT_INTERVAL': 1,
    'BLPOP_TIMEOUT': 1,
}
#: past the 3-second TTL above, in the milliseconds XCLAIM takes
IDLE_PAST_THE_TTL = 4_000


def payload(chat_id):
    return JsonSerializer().dumps({'function': 'send_message', 'chat_id': chat_id})


@pytest.fixture
def broker(server, redis_url, version):
    """A Streams broker against the real server, skipping where `lag` does not exist.

    The skip is the transport's own floor showing up in the suite: below 7.0 this broker
    refuses at first use, and that refusal has its own test.
    """
    if version < (7, 0):
        pytest.skip(f'the Streams broker needs Redis 7.0 for XINFO GROUPS lag; this is {version}')
    with override_settings(TELEGRAM_BOT={**SETTINGS, 'REDIS_URL': redis_url}):
        yield RedisStreamsBroker()


def test_depth_counts_what_is_waiting(broker, redis_url):
    """The assertion the in-memory server gets wrong, made against a real one."""
    with override_settings(TELEGRAM_BOT={**SETTINGS, 'REDIS_URL': redis_url}):
        broker.publish([payload(3), payload(4)])

        assert broker.depth() == 2, 'two published entries are not two waiting'

        assert broker.take_nowait() is not None
        assert broker.depth() == 1, 'a delivered entry is still counted as waiting'


def test_the_awaiting_half_publishes_and_counts_the_same(broker, redis_url):
    """`apublish` and `adepth` are a second implementation, so they are asked separately."""
    with override_settings(TELEGRAM_BOT={**SETTINGS, 'REDIS_URL': redis_url}):

        async def on_a_loop():
            await broker.apublish([])
            after_nothing = await broker.adepth()
            await broker.apublish([payload(11), payload(12)])
            return after_nothing, await broker.adepth(), await broker.ainflight_depth()

        after_nothing, after_two, inflight = asyncio.run(on_a_loop())

        assert after_nothing == 0, 'awaiting an empty publish queued something'
        assert after_two == 2, 'the awaited publishes did not arrive'
        assert inflight == 0, 'nothing was taken, so nothing is in flight'


def test_nothing_is_lost_when_a_consumer_never_comes_back(broker, server, redis_url):
    """Publish three, take one, and never settle it: the total has to stay three.

    This is the `kill -9` question in the only form a test can ask it without a second
    process — the entry is delivered and unacknowledged, which is exactly the state a killed
    worker leaves, and what matters is that the two counts still add up to what was sent.
    """
    with override_settings(TELEGRAM_BOT={**SETTINGS, 'REDIS_URL': redis_url}):
        broker.publish([payload(1), payload(2), payload(3)])
        taken = broker.take_nowait()

        assert taken is not None
        assert broker.depth() == 2
        assert broker.inflight_depth() == 1
        assert broker.depth() + broker.inflight_depth() == 3, 'a message went missing'


def test_a_dead_consumers_work_is_reclaimed_under_any_name(broker, server, redis_url):
    """The pending list belongs to the group, so a fresh name recovers it.

    The idle counter is pushed past the liveness TTL with `XCLAIM` rather than by sleeping:
    what is under test is the threshold, not the clock, and four seconds of real waiting in
    every CI run buys nothing. This is the same move as writing straight into the list
    broker's processing key to represent a worker that died holding a message.
    """
    with override_settings(TELEGRAM_BOT={**SETTINGS, 'REDIS_URL': redis_url}):
        broker.publish([payload(8)])
        taken = broker.take_nowait()
        assert taken is not None
        server.xclaim(
            STREAM, GROUP, 'the-worker-that-died', min_idle_time=0, message_ids=[taken.handle], idle=IDLE_PAST_THE_TTL
        )

        # a different process, and deliberately a different consumer name
        with override_settings(TELEGRAM_BOT={**SETTINGS, 'REDIS_URL': redis_url, 'WORKER_NAME': 'a-new-container'}):
            replacement = RedisStreamsBroker()

            assert replacement.reclaim() == 1, "the dead consumer's entry was not claimed"

            again = replacement.take_nowait()

        assert again is not None, 'the reclaimed entry was never delivered again'
        assert again.payload == payload(8)


def test_a_released_message_is_reclaimable_at_once(broker, redis_url):
    """`release` sets the idle counter to the threshold, so no waiting is needed.

    The boundary is inclusive — measured on this server and on fakeredis alike — which is
    what makes setting idle to exactly the threshold enough rather than one millisecond
    short of it.
    """
    with override_settings(TELEGRAM_BOT={**SETTINGS, 'REDIS_URL': redis_url}):
        broker.publish([payload(9)])
        taken = broker.take_nowait()
        assert taken is not None

        broker.release(taken.handle)

        assert broker.reclaim() == 1, 'a released entry was not reclaimable'
        again = broker.take_nowait()
        assert again is not None, 'a released message was lost'
        assert again.payload == payload(9)


def test_trimming_stops_at_the_oldest_unacknowledged_entry(broker, server, redis_url):
    """The whole reason this broker refuses `MAXLEN`: in-flight work must survive a trim."""
    with override_settings(TELEGRAM_BOT={**SETTINGS, 'REDIS_URL': redis_url}):
        broker.publish([payload(1), payload(2), payload(3)])
        settled = broker.take_nowait()
        assert settled is not None
        broker.ack(settled.handle)
        held = broker.take_nowait()
        assert held is not None

        broker.trim()

        assert server.xlen(STREAM) == 2, 'trimming did not drop the acknowledged entry'
        assert broker.inflight_depth() == 1, 'trimming dropped an unacknowledged entry'
        assert held.handle in [entry[0] for entry in server.xrange(STREAM)], (
            'the entry still in flight is no longer in the stream'
        )


def test_deleting_entries_makes_the_count_refuse_rather_than_guess(broker, server, redis_url):
    """`XDEL` costs Redis the ability to answer `lag`, so `depth()` says so.

    Measured: one delete of an undelivered entry turns a lag of 4 into nil, and it stays nil
    for the life of the group. A number here would read as a healthy queue.
    """
    with override_settings(TELEGRAM_BOT={**SETTINGS, 'REDIS_URL': redis_url}):
        broker.publish([payload(1), payload(2), payload(3)])
        ids = [entry[0] for entry in server.xrange(STREAM)]

        server.xdel(STREAM, ids[1])

        with pytest.raises(StreamLagUnknownError) as refused:
            broker.depth()

        assert 'XSETID' in str(refused.value), 'the refusal does not say how to recover'


def test_a_server_without_lag_refuses_by_name(server, redis_url, version):
    """Below 7.0 the transport refuses at first use instead of counting badly.

    The only case here that wants an *old* server, so it is the mirror image of the `broker`
    fixture's skip: pointing the suite at a 6.2 leg runs this one and skips the rest. Verified
    by hand against redis:6.2.24, where `XINFO GROUPS` answers without a `lag` field at all —
    which is what the probe looks for, rather than reading a version and inferring.
    """
    if version >= (7, 0):
        pytest.skip(f'this server has lag; the refusal needs one below 7.0, this is {version}')
    with override_settings(TELEGRAM_BOT={**SETTINGS, 'REDIS_URL': redis_url}):
        broker = RedisStreamsBroker()

        with pytest.raises(StreamServerTooOldError) as refused:
            broker.publish([payload(1)])

        assert 'lag' in str(refused.value), 'the refusal does not name the field it needs'


def test_crash_safety_is_answered_by_the_transport(broker):
    """True without probing, because the pending list is how the transport works."""
    assert broker.crash_safe is True
    assert broker.needs_identity is False
