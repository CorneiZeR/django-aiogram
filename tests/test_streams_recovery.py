"""Two properties of the Streams transport that are pure logic over the driver.

Both were defects in the first draft of that broker, and both are invisible to the conformance
suite because they need a pending list arranged by hand: an entry this package cannot read
sitting in front of a valid one, and the async half reaching for the synchronous client.
"""

import asyncio

import fakeredis
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
def test_every_reclaimed_entry_comes_back_not_just_the_first(broker, redis_server):
    """A page of pending entries must be delivered entry by entry.

    The recovering read takes a page so that one entry it cannot decode does not hide the rest.
    Advancing the cursor to the end of that page and returning its first entry — which is what
    the first version of this did — loses everything behind it: those entries stay pending,
    now behind the cursor, and `XREADGROUP … >` never returns an entry that was already
    delivered. Three reclaimed messages came back as one.

    `release` on each is how the cursor is armed without reaching into the broker: it is the
    documented way an unsent message becomes reclaimable.
    """
    sent = [payload(n) for n in (1, 2, 3)]
    broker.publish(sent)
    held = [broker.take_nowait() for _ in sent]
    assert all(item is not None for item in held), 'the fixture did not deliver three messages'
    for item in held:
        broker.release(item.handle)

    seen = []
    while (again := broker.take_nowait()) is not None:
        seen.append(again.payload)

    assert sorted(seen) == sorted(sent), f'{len(seen)} of {len(sent)} released messages came back'


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_release_does_not_hand_back_a_send_that_is_still_running(broker, redis_server):
    """The consumer holds several messages at once, so recovery must skip the live ones.

    `MAX_IN_FLIGHT` exists because sends complete asynchronously: the consumer takes a
    message, hands it to the loop and takes the next one. So when a *later* message is
    refused and released, the earlier one may still be on its way to Telegram — and arming
    recovery from the start of the pending list would hand it out again. A real person gets
    that message twice, which is worse than the delay this whole mechanism exists to avoid.

    Asserted on identity, not on count: what must come back is the released message, and what
    must not is the one nobody has settled.
    """
    broker.publish([payload(1), payload(2)])
    still_sending = broker.take_nowait()
    refused = broker.take_nowait()
    assert still_sending is not None, 'the fixture did not deliver the first message'
    assert refused is not None, 'the fixture did not deliver the second message'

    broker.release(refused.handle)
    again = broker.take_nowait()

    assert again is not None, 'the released message never came back'
    assert again.handle == refused.handle, 'recovery handed back a send that was still running'
    assert broker.take_nowait() is None, 'something else came back too'


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_url_that_asks_for_decoding_still_delivers(monkeypatch):
    """`decode_responses` is tolerated, so it must not silently deliver nothing.

    One `REDIS_URL` is often shared with a cache backend that wants decoding, which is why
    `as_bytes` exists and why `E043` refuses the setting only alongside pickle. Measured on
    such a client: the entry id, the field name *and* the payload all come back as `str`. A
    reader that only knows `b'payload'` treats every entry as one written by something else —
    a warning per message, and a queue that never drains.

    The client is built here rather than through the `redis_server` fixture, and that is the
    point of the case: that fixture hands out a fake with decoding **off**, so setting
    `decode_responses` in the URL changes nothing about it and a test written that way passes
    without ever meeting a `str` key. Measured — the plain fake answers `b'payload'`, the
    decoding one answers `'payload'`.
    """
    decoding = fakeredis.FakeRedis(server=fakeredis.FakeServer(), decode_responses=True)
    monkeypatch.setattr('django_aiogram.broker.redis_streams.broker.get_redis', lambda: decoding)
    broker = RedisStreamsBroker()

    broker.publish([payload(5)])
    taken = broker.take_nowait()

    assert taken is not None, 'a decoding client delivered nothing'
    assert isinstance(taken.payload, bytes), f'the payload arrived as {type(taken.payload).__name__}'
    assert taken.payload == payload(5), 'the payload did not survive the round trip'


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
