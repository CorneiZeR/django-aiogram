"""Which bot a queued message is for, and what a consumer does with the answer.

The identity rides in the envelope, and every case here is about one of the three ways that
can go wrong: a payload written before there was more than one bot, a payload for a bot this
process does not serve, and a payload whose identity is not one anybody wrote.
"""

import uuid

import pytest
from django.test import override_settings

from django_aiogram.consumer.delivery import BlpopDelivery, get_delivery
from django_aiogram.exceptions import DeliveryNotConfiguredError
from django_aiogram.wire import envelope
from django_aiogram.wire.envelope import (
    ENVELOPE_KEY,
    ENVELOPE_VERSION,
    READABLE_VERSIONS,
    MalformedEnvelopeError,
    UnknownEnvelopeVersionError,
    pack,
    unpack,
)

TOKEN = '123456:AAone'
OTHER = '654321:BBtwo'
BOTS = {'default': {'TOKEN': TOKEN}, 'support': {'TOKEN': OTHER}}
SETTINGS = {'BROKER': 'django_aiogram.testing.InMemoryBroker', 'FSM_STORAGE': 'memory'}


def a_payload(**extra):
    """One packed call, with whatever a case wants to say about it."""
    return {**pack('send_message', {'chat_id': 1, 'text': 'x'}, uuid.uuid4(), 0.0), **extra}


def test_a_send_names_the_bot_that_made_it():
    """The producer is the only place that knows, so it is where the identity is written."""
    packed = pack('send_message', {'chat_id': 1}, uuid.uuid4(), 0.0, 123456)

    assert packed['bot'] == 123456
    assert unpack(packed).bot_id == 123456


def test_a_bot_with_no_identity_names_none():
    """`E052` reports a token that has no identity; the wire says nothing rather than guessing."""
    packed = pack('send_message', {'chat_id': 1}, uuid.uuid4(), 0.0, None)

    assert 'bot' not in packed
    assert unpack(packed).bot_id is None


def test_the_version_did_not_move_when_the_field_arrived():
    """A 4.x consumer handed this payload has to deliver it, not refuse it.

    Refusing would mean losing every message the web tier queued before the bot container was
    deployed — and a reader that ignores keys it does not know is what makes the field free.
    """
    packed = pack('send_message', {'chat_id': 1}, uuid.uuid4(), 0.0, 123456)

    assert packed[ENVELOPE_KEY] == 1, 'the field cost a version bump after all'


def test_a_payload_from_before_the_field_still_reads():
    """Every 4.x payload on the queue during an upgrade, and it names no bot."""
    read = unpack(a_payload())

    assert read.bot_id is None
    assert read.function == 'send_message'


@pytest.mark.parametrize('written', [1.0, '123456', True, 0, -1, None, [123456]])
def test_an_identity_that_is_not_one_is_refused(written):
    """This came off an untrusted queue, so a value that merely parses must not route.

    Refused rather than read as "no bot named", which is the difference that matters: the
    no-bot answer means *deliver through this process's own*, so a value nobody can read would
    otherwise send a message under a token the producer did not name, into a chat that bot may
    not be in — and neither side hears that it happened. Nothing can be inferred from it, so
    it is dropped like any other envelope this cannot read.
    """
    with pytest.raises(MalformedEnvelopeError):
        unpack(a_payload(bot=written))


def test_a_payload_that_names_no_bot_is_not_refused():
    """The other half, and the one an upgrade rests on: absent is not the same as unreadable."""
    assert unpack(a_payload()).bot_id is None


def test_the_reader_accepts_every_version_it_understands(monkeypatch):
    """A bump has to keep reading the older shape, or it throws the backlog away.

    An unreadable envelope is *recorded and acknowledged* — dropped — while a newer one is
    left in flight for an upgraded consumer. So the set of versions this reads is what a bump
    grows, and the version it writes is a different number: reading is the generous half.

    Played out on a reader that writes 2 and reads both, because with one version in existence
    the old rule and the new one cannot be told apart — a case written against today's numbers
    passes either way, which is what the first draft of this did. The old rule refused
    anything below the version it writes, and every message a not-yet-deployed producer had
    left on the queue would have gone into `MalformedEnvelopeError`, which is dropped.
    """
    assert ENVELOPE_VERSION in READABLE_VERSIONS, 'a reader that cannot read what it writes'

    monkeypatch.setattr(envelope, 'ENVELOPE_VERSION', 2)
    monkeypatch.setattr(envelope, 'READABLE_VERSIONS', frozenset({1, 2}))

    for version in (1, 2):
        read = unpack(a_payload(**{ENVELOPE_KEY: version}))
        assert read.function == 'send_message', f'version {version} was refused by a reader that reads it'


def test_a_version_this_reader_does_not_have_is_told_apart_from_a_newer_one():
    """The two get different answers, and the difference decides the message's fate."""
    with pytest.raises(UnknownEnvelopeVersionError):
        unpack(a_payload(**{ENVELOPE_KEY: ENVELOPE_VERSION + 1}))
    with pytest.raises(MalformedEnvelopeError):
        unpack(a_payload(**{ENVELOPE_KEY: 0}))


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_message_reaches_the_bot_it_names():
    """One queue, several bots: the envelope is what says which of them a message is for."""
    handled = []
    served = {123456: lambda **call: handled.append(('one', call['text']))}
    delivery = get_delivery(
        handler=lambda **call: handled.append(('default', call['text'])),
        route=lambda bot_id: served[bot_id],
    )

    assert isinstance(delivery, BlpopDelivery)
    assert delivery.dispatch(_bytes(a_payload(bot=123456, kwargs={'chat_id': 1, 'text': 'named'})))
    assert handled == [('one', 'named')]


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_message_naming_no_bot_goes_to_the_process_own():
    """Which is every 4.x payload, and the right answer for a deployment that has one bot."""
    handled = []
    delivery = get_delivery(
        handler=lambda **call: handled.append(('default', call['text'])),
        route=lambda bot_id: pytest.fail(f'routed a payload that named no bot: {bot_id}'),
    )

    assert delivery.dispatch(_bytes(a_payload(kwargs={'chat_id': 1, 'text': 'unnamed'})))
    assert handled == [('default', 'unnamed')]


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_consumer_that_cannot_route_delivers_through_its_own_bot():
    """Which is what a 4.1 consumer does with a 5.0 payload, and why the upgrade has an order.

    A consumer with no route is exactly that consumer: it reads the keys it knows and hands
    the message to the one bot it has. For a deployment with one bot that is the right answer
    and nothing is lost. For one with two it is the *wrong bot* — the wrong token, and a chat
    it may not be in — with nothing raised and nothing dropped to notice it by. So the pages
    ask for every consumer to be at 5.0 before a second bot starts queueing, and this is the
    behaviour that makes them ask.
    """
    handled = []
    delivery = get_delivery(handler=lambda **call: handled.append(call['text']))

    assert delivery.dispatch(_bytes(a_payload(bot=654321, kwargs={'chat_id': 1, 'text': 'for support'})))
    assert handled == ['for support'], 'a consumer without a route refused a payload it should deliver'


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_message_for_a_bot_this_process_does_not_serve_is_left_in_flight(caplog):
    """Not acknowledged, and not delivered by somebody else's bot either.

    Falling back to the default bot would send it under the wrong token, to a chat that bot
    may not even be in. Acknowledging would destroy a message a correctly configured process
    can still deliver. So it stays where a reclaim can find it, which is the same answer an
    envelope from a newer version gets.
    """
    handled = []

    def refuse(bot_id):
        msg = f'no bot with the identity {bot_id}'
        raise LookupError(msg)

    delivery = get_delivery(handler=lambda **call: handled.append(call), route=refuse)

    with caplog.at_level('ERROR', logger='django_aiogram'):
        acknowledged = delivery.dispatch(_bytes(a_payload(bot=999999)))

    assert acknowledged is False, 'a message for an unserved bot was acknowledged'
    assert handled == [], 'it was delivered by a bot it was not addressed to'
    assert any(record.tg_bot_id == 999999 for record in caplog.records if hasattr(record, 'tg_bot_id'))


def test_two_bots_sending_through_one_queue_are_told_apart_end_to_end():
    """The whole path: two real sends, one queue, and each message delivered by its own bot.

    Everything above tests a step. This is the claim a project reads — that a second bot can
    share a queue and still be the one that sends — and it is asserted through `bot.send()`
    rather than through a payload built by hand.
    """
    from django_aiogram.runtime import groups
    from django_aiogram.runtime.registry import bots

    delivered = []
    with override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS, TELEGRAM_BOTS=BOTS):
        bots['default'].enqueue('send_message', chat_id=1, text='from the default')
        bots['support'].enqueue('send_message', chat_id=2, text='from support')

        held = groups.live_groups()
        assert len(held) == 1, f'the case needs them sharing one queue, got {len(held)}'
        queued = list(held[0].broker.messages)
        assert len(queued) == 2

        delivery = get_delivery(
            handler=lambda **call: delivered.append(('default', call['text'])),
            route=lambda bot_id: lambda **call: delivered.append((bot_id, call['text'])),
        )
        for message in queued:
            assert delivery.dispatch(message), 'a message was left in flight'

    assert sorted(delivered) == sorted([(123456, 'from the default'), (654321, 'from support')]), delivered


class Older(BlpopDelivery):
    """A project's own consumer, written before there was a route to take."""

    def __init__(self, handler):
        """Take the handler and nothing else, which is all `DELIVERY` ever promised."""
        super().__init__(handler)


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'DELIVERY': 'tests.test_addressing.Older'})
def test_a_consumer_written_before_the_route_is_still_built():
    """`DELIVERY` is a documented seam and what it asked for was `run()`, not a signature.

    A project with one bot has nothing to route, so a consumer that takes only the handler is
    correct and must keep working — the alternative is a startup `TypeError` on an upgrade
    that changed nothing for them.
    """
    built = get_delivery(handler=lambda **call: None)

    assert isinstance(built, Older)
    assert built.route is None


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'DELIVERY': 'tests.test_addressing.Older'})
def test_a_consumer_that_cannot_route_is_refused_where_routing_matters():
    """The other side of it: silence here would be a message sent under the wrong token.

    Refused at startup rather than at the first addressed message, and named, because a
    consumer that cannot route is a configuration problem rather than a runtime one.
    """
    with pytest.raises(DeliveryNotConfiguredError, match='takes no `route`'):
        get_delivery(handler=lambda **call: None, route=lambda bot_id: None)


def _bytes(payload):
    """Serialize one payload the way a producer would."""
    from django_aiogram.wire.serializers import get_serializer

    return get_serializer().dumps(payload)
