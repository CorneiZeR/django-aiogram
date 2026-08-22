"""Two properties of the Streams transport that are pure logic over the driver.

Both were defects in the first draft of that broker, and both are invisible to the conformance
suite because they need a pending list arranged by hand: an entry this package cannot read
sitting in front of a valid one, and the async half reaching for the synchronous client.
"""

import asyncio

import pytest
from django.test import override_settings

from django_aiogram.broker.redis_streams import RedisStreamsBroker
from django_aiogram.wire.serializers import JsonSerializer

STREAM = 'TELEGRAM_BOT_STREAM'
SETTINGS = {
    'TOKEN': '42:x',
    'REDIS_URL': 'redis://localhost:6379/0',
    'REDIS_STREAM_KEY': STREAM,
    'RATE_LIMIT': None,
}


def payload(chat_id):
    return JsonSerializer().dumps({'function': 'send_message', 'chat_id': chat_id})


@pytest.fixture
def broker(redis_server):
    with override_settings(TELEGRAM_BOT=SETTINGS):
        yield RedisStreamsBroker()


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_recovery_steps_over_an_entry_this_package_cannot_read(broker, redis_server):
    """One undecodable entry must not hide every valid one behind it.

    A stream can hold entries this package did not write — that is what makes requiring
    `REDIS_STREAM_KEY` worth the keystrokes — and acknowledging one would be a guess about
    somebody else's data, so it is left pending. Left pending at the *front* of the pending
    list, it used to stop recovery dead: the recovering read always started at `0` and always
    got that entry, so a valid message a reclaim had just handed over was never delivered.

    The cursor is what fixes it, and the release below is what arms the cursor without
    reaching into the broker's internals: it is the documented way an unsent message becomes
    reclaimable again.
    """
    redis_server.xadd(STREAM, {b'written-by': b'something-else'})
    broker.publish([payload(1)])

    assert broker.take_nowait() is None, 'an entry with no payload field was handed to a caller'
    good = broker.take_nowait()
    assert good is not None, 'the valid entry behind it was never delivered'

    broker.release(good.handle)
    again = broker.take_nowait()

    assert again is not None, 'recovery stopped at the entry it could not read'
    assert again.payload == payload(1)


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_async_half_does_not_reach_for_the_synchronous_client(broker, redis_server, monkeypatch):
    """Creating the group is two round trips, and on `apublish` they belong on the loop.

    The async methods all need the consumer group to exist. Making it with the synchronous
    client would put a blocking connect and two blocking commands on the event loop the async
    half exists to keep free — a first send from an ASGI request paying a connect timeout
    inside the handler serving it.

    Asserted by taking the synchronous accessor away: anything still reaching for it fails
    here rather than in production, where it would only ever look like latency.
    """

    def refuse():
        raise AssertionError('the async path used the synchronous client')

    monkeypatch.setattr('django_aiogram.broker.redis_streams.broker.get_redis', refuse)

    async def on_a_loop():
        await broker.apublish([payload(2)])
        # both of the other `a*` methods, because each has its own `_aensure` call and one
        # left reaching for the synchronous client would fail here and nowhere else
        return await broker.adepth(), await broker.ainflight_depth()

    _depth, inflight = asyncio.run(on_a_loop())

    # counted with XLEN rather than from what `adepth()` returned: this runs on fakeredis,
    # whose `lag` is one short — measured — so the awaited depth is exactly the number this
    # test must not depend on. What it is about is that the publish landed
    assert redis_server.xlen(STREAM) == 1, 'the awaited publish did not queue anything'
    assert inflight == 0, 'nothing was taken, so nothing is in flight'
