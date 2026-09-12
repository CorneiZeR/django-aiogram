"""The helpers a project imports to assert a send, held to what they promise.

`django_aiogram.testing` exists to take four internal names out of other people's test
suites -- the transport's key, `wire.serializers.loads`, `wire.envelope.unpack` and a
fakeredis fixture. So these cases assert the *public* shape: records with names on them, a
broker that answers the contract, and settings that come back exactly as they were.

The contract itself is checked in `test_broker_conformance.py`, against `InMemoryBroker`
alongside the four transports a deployment can choose. This file is about the rest.
"""

import uuid

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.test import SimpleTestCase, override_settings
from django.utils.module_loading import import_string

from django_aiogram import TelegramBot
from django_aiogram.broker.base import Broker
from django_aiogram.broker.exceptions import BrokerNotConfiguredError
from django_aiogram.broker.registry import get_broker, use_broker
from django_aiogram.runtime.registry import bots
from django_aiogram.testing import InMemoryBroker, NotCapturedError, SendCaptureMixin, capture_sends
from django_aiogram.testing.capture import BROKER_PATH

SETTINGS = {'TOKEN': '42:x', 'FSM_STORAGE': 'memory', 'RATE_LIMIT': None, 'BROKER': 'unused.Broker'}

#: two bots a case can tell apart on the wire. The identity is the number before the colon,
#: so these are 111111 and 222222 without anything having to be looked up
A_TOKEN = '111111:AAone'
B_TOKEN = '222222:BBtwo'
#: the memory transport by name, so the bot a capture is *not* watching still has somewhere to
#: publish -- which is the half of the invariant that makes the other half worth asserting
MULTI = {'BROKER': BROKER_PATH, 'FSM_STORAGE': 'memory', 'RATE_LIMIT': None}
TWO_BOTS = {'a': {'TOKEN': A_TOKEN}, 'b': {'TOKEN': B_TOKEN}}
#: the same two, one per queue. `QUEUES` declares both, or the group of the uncaptured bot
#: refuses to publish to a queue nothing consumes
LANES = {'a': {'TOKEN': A_TOKEN, 'QUEUE': 'vip'}, 'b': {'TOKEN': B_TOKEN, 'QUEUE': 'bulk'}}
#: both on one queue, which is where a scope read as an either-or captures the wrong bot
SHARED_LANE = {'a': {'TOKEN': A_TOKEN, 'QUEUE': 'vip'}, 'b': {'TOKEN': B_TOKEN, 'QUEUE': 'vip'}}


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_capture_reads_the_call_the_caller_wrote():
    """Function, arguments and the id `send` answered with -- and no bytes anywhere."""
    with capture_sends() as sent:
        identifier = TelegramBot().send(chat_id=42, text='Order approved')

    assert len(sent) == 1
    assert sent[0].function == 'send_message'
    assert sent[0].kwargs == {'chat_id': 42, 'text': 'Order approved'}
    assert sent[0].correlation_id == identifier, 'the record does not name the id the caller holds'


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_capture_keeps_what_it_read_after_the_block_ends():
    """The assertions are written after the block, so the records have to outlive it.

    True by construction rather than by machinery: the queue is an object this holds, and
    leaving the block only stops it being *the process's* broker. Asserted anyway, because it
    is the shape every case in a project's suite is written in -- act inside, assert outside --
    and a change that made the helper release its queue on the way out would break all of them
    at once.
    """
    with capture_sends() as sent:
        TelegramBot().send(chat_id=1, text='hello')

    assert sent.kwargs == [{'chat_id': 1, 'text': 'hello'}]


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_capture_does_not_eat_the_messages_it_read():
    """Reading is not taking, so a case may assert *and then* run the consumer over the same
    messages.

    A helper built on `take_nowait` would empty the queue as a side effect of an assertion,
    which is the kind of thing that turns a second assertion into a mystery.
    """
    with capture_sends() as sent:
        TelegramBot().send(chat_id=1, text='hello')

        assert len(sent) == 1
        taken = get_broker().take_nowait()

    assert taken is not None, 'the capture consumed the message it was asked about'


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_an_override_inside_the_block_does_not_take_the_broker_away():
    """The reason the broker is installed ahead of the settings rather than through them.

    `override_settings(TELEGRAM_BOT_DEFAULTS=...)` replaces the dict whole, and pytest applies a
    decorator on the test method *after* the fixtures it asked for have run -- so a capture
    that worked by overriding `BROKER` was undone by the case's own override, and the
    assertions ran against a queue nothing had written to. Measured: this file's fixture and
    mixin cases failed exactly that way before `use_broker` existed.
    """
    with capture_sends() as sent, override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'BROKER': 'unused.Other'}):
        TelegramBot().send(chat_id=1, text='through an override')

    assert sent.kwargs == [{'chat_id': 1, 'text': 'through an override'}]


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_the_process_gets_its_own_broker_back_afterwards():
    """An override is for the length of a block, and `BROKER` decides again after it."""
    with capture_sends():
        assert isinstance(get_broker(), InMemoryBroker)

    with pytest.raises(BrokerNotConfiguredError):
        get_broker()


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'ENABLED': False})
def test_a_disabled_bot_queues_nothing_to_capture():
    """One of the three things the docstring says this does not catch, pinned as a claim.

    `ENABLED = False` makes a send a no-op that still answers with an id, so a capture is
    empty rather than absent -- which is worth knowing before somebody reads an empty list as
    a broken helper.
    """
    with capture_sends() as sent:
        identifier = TelegramBot().send(chat_id=1, text='nothing doing')

    assert isinstance(identifier, uuid.UUID), 'a disabled send stopped answering with an id'
    assert list(sent) == []


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_the_fixture_captures_the_whole_test(telegram_sends):
    """The pytest half, which is the same context manager entered by a fixture."""
    TelegramBot().send(chat_id=5, text='from a fixture')

    assert telegram_sends.kwargs == [{'chat_id': 5, 'text': 'from a fixture'}]


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_only_the_sends_that_named_this_method():
    """`of` exists because a block that answers a user and notifies an admin queues two."""
    with capture_sends() as sent:
        TelegramBot().send(chat_id=1, text='to the user')
        TelegramBot().send('send_photo', chat_id=2, photo='receipt.png')

    assert [one.kwargs for one in sent.of('send_photo')] == [{'chat_id': 2, 'photo': 'receipt.png'}]
    assert len(sent.of('send_message')) == 1


class TheMixinCapturesToo(SendCaptureMixin, SimpleTestCase):
    """The `TestCase` half, for the projects that never adopted pytest."""

    @override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
    def test_the_case_reads_its_own_sends(self):
        """`self.sent` is the same object the context manager yields."""
        TelegramBot().send(chat_id=9, text='from a TestCase')

        assert self.sent.kwargs == [{'chat_id': 9, 'text': 'from a TestCase'}]


def test_the_path_a_project_writes_for_the_in_memory_broker_resolves():
    """`BROKER` holds this string, typed by hand into a test settings module.

    Pinned for the reason `test_package_layout` pins the four deployable ones, and pinned
    *here* because it is not one of them: `SHIPPED` decides which extra installs a driver and
    which page documents a transport, and this broker has neither. Written out rather than
    read off the class, which is the difference between a test and a tautology.
    """
    resolved = import_string('django_aiogram.testing.InMemoryBroker')

    assert resolved is InMemoryBroker
    assert issubclass(resolved, Broker)
    assert BROKER_PATH == 'django_aiogram.testing.InMemoryBroker', 'the constant and the path disagree'


def test_the_broker_starts_empty_for_every_block():
    """One block's messages must not be visible to the next one.

    The registry builds one broker per process and drops it when `TELEGRAM_BOT_DEFAULTS` changes, so
    this holds by construction -- and it is the property everything else here rests on, which
    is why it is asserted rather than assumed.
    """
    with override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS), capture_sends() as first:
        TelegramBot().send(chat_id=1, text='one')
    with override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS), capture_sends() as second:
        pass

    assert len(first) == 1
    assert len(second) == 0, "a later block saw an earlier block's messages"


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_two_overrides_that_end_out_of_order_leave_neither_behind():
    """Blocks need not end in the order they began, and the stack must survive that.

    A fixture held open across cases, an `ExitStack` closed in the order it was built, two
    threads each capturing: any of them can close the outer block first. A version that kept
    the broker it displaced and put it back would then reinstate a broker whose block has
    ended -- and the block that ends last would leave it installed for the rest of the process.

    Falsifiable: with `use_broker` restoring a saved `previous` instead of removing its own
    entry, `get_broker()` answers with `outer` after both blocks have ended.
    """
    outer, inner = InMemoryBroker(), InMemoryBroker()
    first, second = use_broker(outer), use_broker(inner)
    first.__enter__()
    second.__enter__()

    assert get_broker() is inner, 'the innermost block did not win'

    first.__exit__(None, None, None)

    assert get_broker() is inner, 'a block that ended took a live override with it'

    second.__exit__(None, None, None)

    with pytest.raises(BrokerNotConfiguredError):
        get_broker()


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_handle_from_another_transport_is_refused_by_name():
    """The contract's rule, and the message names the broker so a reader knows what they hold."""
    broker = InMemoryBroker()

    with pytest.raises(TypeError, match='handle must be an int issued by InMemoryBroker'):
        broker.ack(b'a redis payload')


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_released_message_goes_back_to_the_front():
    """A refusal is about *this* message, so it must not be reordered behind later ones."""
    broker = InMemoryBroker()
    broker.publish([b'first', b'second'])
    taken = broker.take_nowait()
    assert taken is not None
    assert taken.payload == b'first'

    broker.release(taken.handle)

    again = broker.take_nowait()
    assert again is not None
    assert again.payload == b'first', 'a released message came back behind the ones queued after it'


@override_settings(TELEGRAM_BOT_DEFAULTS=MULTI, TELEGRAM_BOTS=TWO_BOTS)
def test_a_capture_for_one_bot_does_not_see_another_bots_send():
    """The case the whole of this exists for: a capture narrowed to `a` is not `b`'s transport.

    Both halves are asserted, and the second is the one that keeps the first honest: an empty
    capture would also be what a broken send looks like, so the case reads `b`'s own queue and
    finds the message there. `a` and `b` share a profile here -- nothing in `SHARED` differs --
    so they share a group too, which is exactly the arrangement a scope that worked per group
    rather than per bot would get wrong.
    """
    with capture_sends(bot='a') as sent:
        bots['a'].send(chat_id=1, text='to a')
        bots['b'].send(chat_id=2, text='to b')
        elsewhere = bots['b'].broker.messages

    assert sent.kwargs == [{'chat_id': 1, 'text': 'to a'}]
    assert len(elsewhere) == 1, "b's send did not reach the transport it was configured with"


@override_settings(TELEGRAM_BOT_DEFAULTS=MULTI, TELEGRAM_BOTS=TWO_BOTS)
def test_asking_a_narrowed_capture_about_another_bot_is_refused():
    """An empty list is what a passing assertion is made of, so it is not the answer here."""
    with capture_sends(bot='a') as sent:
        bots['a'].send(chat_id=1, text='to a')

    assert [one.kwargs for one in sent.for_bot('a')] == [{'chat_id': 1, 'text': 'to a'}]
    with pytest.raises(NotCapturedError, match='not watching bot 222222'):
        sent.for_bot('b')


@override_settings(TELEGRAM_BOT_DEFAULTS=MULTI, TELEGRAM_BOTS=TWO_BOTS)
def test_an_unnarrowed_capture_sorts_the_bots_out_afterwards():
    """The other way round, and the one a suite already sharing a queue wants: capture
    everything, then ask per bot.

    By alias or by identity, because a project writes whichever it has to hand.
    """
    with capture_sends() as sent:
        bots['a'].send(chat_id=1, text='to a')
        bots['b'].send(chat_id=2, text='to b')

    assert [one.kwargs for one in sent.for_bot('a')] == [{'chat_id': 1, 'text': 'to a'}]
    assert [one.kwargs for one in sent.for_bot(222222)] == [{'chat_id': 2, 'text': 'to b'}]


@override_settings(TELEGRAM_BOT_DEFAULTS={**MULTI, 'QUEUES': ('vip', 'bulk')}, TELEGRAM_BOTS=LANES)
def test_a_capture_for_one_queue_leaves_the_other_queue_alone():
    """`queue=` narrows it the other way, for a deployment whose isolation is the queue."""
    with capture_sends(queue='vip') as sent:
        bots['a'].send(chat_id=1, text='vip')
        bots['b'].send(chat_id=2, text='bulk')
        elsewhere = bots['b'].broker.messages

    assert sent.kwargs == [{'chat_id': 1, 'text': 'vip'}]
    assert len(elsewhere) == 1, "the bulk queue's send was taken by the capture on vip"


@override_settings(TELEGRAM_BOT_DEFAULTS=MULTI, TELEGRAM_BOTS=TWO_BOTS)
def test_the_factory_fixture_narrows_the_capture(capture_telegram_sends):
    """The pytest half of narrowing: a fixture cannot be passed an argument, so it hands back
    something that can.
    """
    sent = capture_telegram_sends(bot='a')

    bots['a'].send(chat_id=1, text='to a')
    bots['b'].send(chat_id=2, text='to b')

    assert sent.kwargs == [{'chat_id': 1, 'text': 'to a'}]


@override_settings(TELEGRAM_BOT_DEFAULTS=MULTI, TELEGRAM_BOTS=TWO_BOTS)
class TheMixinNarrowsByClassAttribute(SendCaptureMixin, SimpleTestCase):
    """`setUp` runs before the method and cannot be passed anything, so the class says it."""

    capture_bot = 'a'

    def test_the_case_sees_only_its_own_bot(self):
        """And the one it was not watching is refused rather than answered with nothing."""
        bots['a'].send(chat_id=9, text='to a')
        bots['b'].send(chat_id=8, text='to b')

        assert self.sent.kwargs == [{'chat_id': 9, 'text': 'to a'}]
        with pytest.raises(NotCapturedError):
            self.sent.for_bot('b')


@override_settings(TELEGRAM_BOT_DEFAULTS=MULTI, TELEGRAM_BOTS=TWO_BOTS)
def test_a_bot_nothing_is_configured_as_is_refused_where_it_is_named():
    """A typo in an alias is a question about a bot that does not exist, and it says so."""
    with pytest.raises(ImproperlyConfigured, match='No bot is configured'), capture_sends(bot='nobody'):
        pass  # pragma: no cover - the capture never starts


@override_settings(TELEGRAM_BOT_DEFAULTS=MULTI, TELEGRAM_BOTS={'a': {'TOKEN': 'no-identity-here'}})
def test_a_bot_whose_token_names_nobody_cannot_be_captured_by_name():
    """`E052` reports the token; here the point is that the capture does not silently watch 0.

    A capture that swallowed this would watch an identity no send ever carries, so every
    assertion about it would be about an empty list.
    """
    with pytest.raises(NotCapturedError, match='no identity'), capture_sends(bot='a'):
        pass  # pragma: no cover - the capture never starts


@override_settings(TELEGRAM_BOT_DEFAULTS=MULTI, TELEGRAM_BOTS=TWO_BOTS)
def test_a_capture_for_one_bot_is_not_its_groups_transport():
    """A group's transport is shared by every bot on the profile, so one bot's capture is not it.

    The consumer reads through the group, and so does every depth the healthcheck asks for --
    installing one client's test queue there would answer all of those from a queue that exists
    for the length of one block.
    """
    with capture_sends(bot='a') as sent:
        # built from `a`'s settings, because `a` asked for it first -- and those are the
        # settings a group answers about itself with, although it serves `b` just as much.
        # That is the arrangement where a scope read per group rather than per bot hands `b`
        # the capture that was narrowed to `a`
        group = bots['a'].group
        bots['b'].send(chat_id=2, text='to b')

        assert group.broker is bots['b'].broker, "the group handed out the capture's queue"
        assert len(group.broker.messages) == 1

    assert sent.kwargs == []


@override_settings(TELEGRAM_BOT_DEFAULTS={**MULTI, 'QUEUES': ('vip',)}, TELEGRAM_BOTS=SHARED_LANE)
def test_naming_a_bot_and_a_queue_means_both_of_them():
    """Both bots are on `vip`, so a scope that answered on *either* half would capture `b` too.

    Which is the whole difference between "this bot on this queue" and "this bot or this
    queue": the narrower reading is the one a case asking for both wrote down.
    """
    with capture_sends(bot='a', queue='vip') as sent:
        bots['a'].send(chat_id=1, text='to a')
        bots['b'].send(chat_id=2, text='to b')
        elsewhere = bots['b'].broker.messages

    assert sent.kwargs == [{'chat_id': 1, 'text': 'to a'}]
    assert len(elsewhere) == 1, "b's send on the same queue did not reach its own transport"


@override_settings(TELEGRAM_BOT_DEFAULTS={**MULTI, 'QUEUES': ('vip', 'bulk')}, TELEGRAM_BOTS=LANES)
def test_a_queue_capture_refuses_a_bot_from_another_queue():
    """A capture on a queue watches whichever bots name it, so one that does not is outside it.

    The same refusal as a capture narrowed to bots, and for the same reason: `b` queued its
    message somewhere this capture cannot see, and an empty list would read as *nothing was
    sent*.
    """
    with capture_sends(queue='vip') as sent:
        bots['a'].send(chat_id=1, text='vip')
        bots['b'].send(chat_id=2, text='bulk')

    assert [one.kwargs for one in sent.for_bot('a')] == [{'chat_id': 1, 'text': 'vip'}]
    with pytest.raises(NotCapturedError, match="queue 'vip'"):
        sent.for_bot('b')
