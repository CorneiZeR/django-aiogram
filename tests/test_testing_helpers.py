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
from django.test import SimpleTestCase, override_settings
from django.utils.module_loading import import_string

from django_aiogram import TelegramBot
from django_aiogram.broker.base import Broker
from django_aiogram.broker.exceptions import BrokerNotConfiguredError
from django_aiogram.broker.registry import get_broker
from django_aiogram.testing import InMemoryBroker, SendCaptureMixin, capture_sends
from django_aiogram.testing.capture import BROKER_PATH

SETTINGS = {'TOKEN': '42:x', 'FSM_STORAGE': 'memory', 'RATE_LIMIT': None, 'BROKER': 'unused.Broker'}


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_capture_reads_the_call_the_caller_wrote():
    """Function, arguments and the id `send` answered with -- and no bytes anywhere."""
    with capture_sends() as sent:
        identifier = TelegramBot().send(chat_id=42, text='Order approved')

    assert len(sent) == 1
    assert sent[0].function == 'send_message'
    assert sent[0].kwargs == {'chat_id': 42, 'text': 'Order approved'}
    assert sent[0].correlation_id == identifier, 'the record does not name the id the caller holds'


@override_settings(TELEGRAM_BOT=SETTINGS)
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


@override_settings(TELEGRAM_BOT=SETTINGS)
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


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_an_override_inside_the_block_does_not_take_the_broker_away():
    """The reason the broker is installed ahead of the settings rather than through them.

    `override_settings(TELEGRAM_BOT=...)` replaces the dict whole, and pytest applies a
    decorator on the test method *after* the fixtures it asked for have run -- so a capture
    that worked by overriding `BROKER` was undone by the case's own override, and the
    assertions ran against a queue nothing had written to. Measured: this file's fixture and
    mixin cases failed exactly that way before `use_broker` existed.
    """
    with capture_sends() as sent, override_settings(TELEGRAM_BOT={**SETTINGS, 'BROKER': 'unused.Other'}):
        TelegramBot().send(chat_id=1, text='through an override')

    assert sent.kwargs == [{'chat_id': 1, 'text': 'through an override'}]


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_process_gets_its_own_broker_back_afterwards():
    """An override is for the length of a block, and `BROKER` decides again after it."""
    with capture_sends():
        assert isinstance(get_broker(), InMemoryBroker)

    with pytest.raises(BrokerNotConfiguredError):
        get_broker()


@override_settings(TELEGRAM_BOT={**SETTINGS, 'ENABLED': False})
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


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_fixture_captures_the_whole_test(telegram_sends):
    """The pytest half, which is the same context manager entered by a fixture."""
    TelegramBot().send(chat_id=5, text='from a fixture')

    assert telegram_sends.kwargs == [{'chat_id': 5, 'text': 'from a fixture'}]


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_only_the_sends_that_named_this_method():
    """`of` exists because a block that answers a user and notifies an admin queues two."""
    with capture_sends() as sent:
        TelegramBot().send(chat_id=1, text='to the user')
        TelegramBot().send('send_photo', chat_id=2, photo='receipt.png')

    assert [one.kwargs for one in sent.of('send_photo')] == [{'chat_id': 2, 'photo': 'receipt.png'}]
    assert len(sent.of('send_message')) == 1


class TheMixinCapturesToo(SendCaptureMixin, SimpleTestCase):
    """The `TestCase` half, for the projects that never adopted pytest."""

    @override_settings(TELEGRAM_BOT=SETTINGS)
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

    The registry builds one broker per process and drops it when `TELEGRAM_BOT` changes, so
    this holds by construction -- and it is the property everything else here rests on, which
    is why it is asserted rather than assumed.
    """
    with override_settings(TELEGRAM_BOT=SETTINGS), capture_sends() as first:
        TelegramBot().send(chat_id=1, text='one')
    with override_settings(TELEGRAM_BOT=SETTINGS), capture_sends() as second:
        pass

    assert len(first) == 1
    assert len(second) == 0, "a later block saw an earlier block's messages"


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_handle_from_another_transport_is_refused_by_name():
    """The contract's rule, and the message names the broker so a reader knows what they hold."""
    broker = InMemoryBroker()

    with pytest.raises(TypeError, match='handle must be an int issued by InMemoryBroker'):
        broker.ack(b'a redis payload')


@override_settings(TELEGRAM_BOT=SETTINGS)
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
