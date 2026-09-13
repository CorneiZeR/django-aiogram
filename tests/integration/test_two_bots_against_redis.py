"""Two bots, one Redis, and the things that must not cross between them.

The release scenario #128 asks for, and the one no unit leg can express: fakeredis answers
every question the way the code expects, so "these two keys are different" is a claim only a
real server settles -- it is the server that has to hold two queues, two in-flight lists and
two conversations without letting one reach the other.

**No Telegram.** Every property here is about what this package writes down: which key a
message lands on, which state a chat has, and what a reclaim moves. Sending is what needs a
token Telegram issued, and that is the one thing a suite cannot arrange.
"""

import asyncio
from io import StringIO

import pytest
from django.core.management import call_command
from django.test import override_settings

from django_aiogram.producer.from_settings import build_storage
from django_aiogram.runtime.registry import bots

pytestmark = pytest.mark.integration

#: two bots, one Redis, and a queue each. The identities are what everything below joins on
SHOP = '111111:AAshop'
HELP = '222222:BBhelp'
WORKER = 'two-bots'


def settings_for(redis_url):
    """The shared half: one server, one worker name, and the two queues declared."""
    return {
        'REDIS_URL': redis_url,
        'WORKER_NAME': WORKER,
        'FSM_STORAGE': 'redis',
        'QUEUES': ('shop', 'help'),
        'BLPOP_TIMEOUT': 1,
    }


def sections():
    """One section per bot, differing only in the token and the queue."""
    return {
        'shop': {'TOKEN': SHOP, 'QUEUE': 'shop'},
        'help': {'TOKEN': HELP, 'QUEUE': 'help'},
    }


@pytest.fixture
def two_bots(server, redis_url):
    """Both bots resolved against a flushed server, with the registry cleared afterwards."""
    with override_settings(TELEGRAM_BOT_DEFAULTS=settings_for(redis_url), TELEGRAM_BOTS=sections()):
        yield bots


def test_each_bot_queues_to_its_own_key(two_bots, server):
    """A queue is the isolation boundary, and the server is what has to hold two of them."""
    two_bots['shop'].send(chat_id=1, text='for the shop')

    # before the second send, which is what says the shop's message went to the shop's queue:
    # two sends and two depths of one hold just as well with the two queues swapped
    assert server.llen('shop') == 1, 'the shop queue did not get its message'
    assert server.llen('help') == 0, "the shop's message went to the other bot's queue"

    two_bots['help'].send(chat_id=2, text='for support')

    assert server.llen('help') == 1, 'the support queue did not get its message'
    assert two_bots['shop'].queue_depth() == 1
    assert two_bots['help'].queue_depth() == 1


def test_a_message_names_the_bot_that_queued_it(two_bots, server):
    """Which is what lets one container serve both without sending under the wrong token."""
    from django_aiogram.wire.envelope import unpack
    from django_aiogram.wire.serializers import loads

    two_bots['shop'].send(chat_id=1, text='for the shop')
    two_bots['help'].send(chat_id=2, text='for support')

    named = {queue: unpack(loads(server.lindex(queue, 0))).bot_id for queue in ('shop', 'help')}

    # which bot is in which queue, not merely that both are somewhere: the set holds with the
    # two swapped, which is each bot sending under the other's token
    assert named == {'shop': 111111, 'help': 222222}, named


def test_one_bots_in_flight_list_is_not_the_others(two_bots, server):
    """Taken is per queue, so a worker holding one client's message holds nothing of another's."""
    two_bots['shop'].send(chat_id=1, text='for the shop')
    two_bots['help'].send(chat_id=2, text='for support')

    taken = two_bots['shop'].broker.take_nowait()

    assert taken is not None, 'the shop queue delivered nothing'
    assert server.llen(f'shop:processing:{WORKER}') == 1
    assert server.llen(f'help:processing:{WORKER}') == 0, "the other bot's list was written to"
    # which message moved, not only which list it landed in: a take that read the wrong queue
    # and wrote it to this one's list holds both assertions above
    assert server.llen('shop') == 0, 'the shop queue still holds the message that was taken'
    assert server.llen('help') == 1, "the take came out of the other bot's queue"
    assert two_bots['shop'].inflight_depth() == 1
    assert two_bots['help'].inflight_depth() == 0


def test_a_reclaim_of_one_queue_leaves_the_other_alone(two_bots, server):
    """`tgbot_reclaim --queue` is per queue for exactly this reason.

    A worker that died holding both clients' messages is reclaimed one queue at a time, and a
    run against the wrong one would put another client's message back on a queue nobody asked
    about -- sent twice, to somebody else's chat.

    The dead worker's lists are written straight into Redis, which is what the delivery suite
    does and what a death looks like from here: the command refuses to reclaim the name this
    process is running under, since a live consumer reclaims its own.
    """
    dead = 'the-worker-that-died'
    server.rpush(f'shop:processing:{dead}', b'{"function": "send_message", "chat_id": 1}')
    server.rpush(f'help:processing:{dead}', b'{"function": "send_message", "chat_id": 2}')

    out = StringIO()
    call_command('tgbot_reclaim', worker=dead, queues=['shop'], stdout=out)

    assert server.llen('shop') == 1, 'the shop queue was not reclaimed'
    assert server.llen(f'shop:processing:{dead}') == 0
    assert server.llen('help') == 0, "the other bot's message was moved"
    assert server.llen(f'help:processing:{dead}') == 1, "the other bot's in-flight list was drained"


def test_one_person_talking_to_both_bots_has_two_conversations(two_bots, server):
    """The FSM store is the process's, and its keys carry the identity.

    Against a real Redis rather than a key builder, because what is under test is the key this
    deployment actually writes: one person, one chat, two bots, and no state crossing.
    """
    from aiogram.fsm.storage.base import StorageKey

    storage = build_storage()

    async def one_each():
        """Set a state for the shop bot, and read what the support bot has for the same person."""
        here = StorageKey(bot_id=111111, chat_id=5, user_id=5)
        beside = StorageKey(bot_id=222222, chat_id=5, user_id=5)
        await storage.set_state(here, 'waiting_for_address')
        return await storage.get_state(here), await storage.get_state(beside)

    mine, theirs = asyncio.run(one_each())

    assert mine == 'waiting_for_address'
    assert theirs is None, 'one person talking to two bots shares one state between them'
    assert len(server.keys('fsm:*')) == 1, server.keys('fsm:*')
