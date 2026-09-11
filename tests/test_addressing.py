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


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'TOKEN': TOKEN})
def test_a_consumer_with_no_route_still_only_delivers_what_is_its_own():
    """A process serving one bot is handed no route, and two of them can share a queue.

    So "no route" cannot mean "deliver anything": a message naming the other process's bot
    would go out under this bot's token, into a chat it may not be in, and be acknowledged —
    gone. Only a payload naming this bot, or naming none, is its to deliver.

    This is also what keeps the upgrade hazard on the pages a *4.1* hazard rather than one
    this release has: a 4.1 consumer cannot read the field at all, and no case here can be
    that consumer.
    """
    handled = []
    delivery = get_delivery(handler=lambda **call: handled.append(call['text']))

    assert delivery.dispatch(_bytes(a_payload(bot=123456, kwargs={'chat_id': 1, 'text': 'mine'})))
    assert handled == ['mine'], 'a message for the bot this process serves was refused'

    foreign = _bytes(a_payload(bot=654321, kwargs={'chat_id': 1, 'text': 'not mine'}))
    assert delivery.dispatch(foreign) is False
    assert handled == ['mine'], "a message for another process's bot was delivered by this one"


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


class KeywordOnly(BlpopDelivery):
    """A consumer taking the route as a keyword-only argument, which is a legal way to write it."""

    def __init__(self, handler, *, route=None):
        """Take the pair, with the route reachable only by name."""
        super().__init__(handler, route)


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'DELIVERY': 'tests.test_addressing.KeywordOnly'})
def test_a_keyword_only_route_is_passed_by_name():
    """`accepts_keyword` accepts a keyword-only parameter, so the call that follows has to be one.

    Positionally this is a `TypeError` at startup, on a class the setting is documented to
    accept — the same shape of break as the one-argument consumer below, reached from the
    other side.
    """

    def route(bot_id):
        return None

    built = get_delivery(handler=lambda **call: None, route=route)

    assert isinstance(built, KeywordOnly)
    assert built.route is route


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


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'DELIVERY': 'tests.test_addressing.Older'})
def test_a_consumer_that_cannot_be_told_its_queue_is_refused_where_that_matters():
    """The same contract as the route, one release later and for the queue.

    Silently built, it would take every message from the process's own queue while the
    container believes it is serving another — so the queue asked for is never consumed and
    the one that is looks busier than it should.
    """
    with pytest.raises(DeliveryNotConfiguredError, match='takes no `settings`'):
        get_delivery(handler=lambda **call: None, settings={**SETTINGS, 'QUEUE': 'vip'})


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'DELIVERY': 'tests.test_addressing.Older'})
def test_a_consumer_written_before_queues_is_still_built_for_the_process_own():
    """Which is every container that serves one queue, and it must need no edit at all."""
    built = get_delivery(handler=lambda **call: None)

    assert isinstance(built, Older)
    assert built.settings is None


class QueuedOnly(BlpopDelivery):
    """A consumer that can be told its queue and has nothing to route."""

    def __init__(self, handler, *, settings=None):
        """Take the queue's settings, and no route."""
        super().__init__(handler, None, settings)


@override_settings(
    TELEGRAM_BOT_DEFAULTS={
        **SETTINGS,
        'DELIVERY': 'tests.test_addressing.QueuedOnly',
        # declared on the process's settings, because `QUEUES` is the deployment's word and a
        # bot only chooses from it
        'QUEUES': ('vip',),
    }
)
def test_a_consumer_taking_only_the_queue_is_still_told_which_queue():
    """The two arguments are separate questions, so one may be accepted and the other not.

    Judged together, a class taking `settings` and not `route` fell through to the
    one-argument call: built without the queue it was asked for, reading the process's own
    while the container believed it was serving another. Silently, and on every message.
    """
    # through `settings_for`, which is what the command hands a consumer: a resolved mapping
    # rather than a fragment, because `Delivery` reads its budget out of it
    from django_aiogram.runtime.queues import settings_for

    served = settings_for('vip')

    built = get_delivery(handler=lambda **call: None, settings=served)

    assert isinstance(built, QueuedOnly)
    assert built.settings is served, 'the queue it was asked for did not reach it'


def test_a_write_built_positionally_still_means_what_it_says():
    """`Queueing` and `Event` both grew a `bot_id`, and both are built positionally somewhere.

    A field inserted in the middle of a dataclass rebinds every argument after it — here
    `details` would have become the identity, and the feed would have attributed rows to a
    list. So the order is the contract, and this is what says so.
    """
    import uuid as _uuid

    from django_aiogram.producer.queueing import Queueing

    identifier = _uuid.uuid4()
    write = Queueing([b'payload'], [(identifier, {'chat_id': 1})], 1234.0, [{'text': 'hi'}])

    assert write.details == [{'text': 'hi'}]
    assert write.bot_id is None


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_bot_identity_off_the_wire_cannot_fail_the_batch_of_rows_it_travelled_in(monkeypatch):
    """An envelope is untrusted input, and a Python integer has no width.

    One wider than the feed's column fails the whole batch of rows rather than its own, so
    the consumer narrows it the way it narrows every other number off the wire.
    """
    from django_aiogram.consumer import delivery as delivery_module

    kept = []
    monkeypatch.setattr(delivery_module.recorder, 'record', kept.append)
    monkeypatch.setattr(type(delivery_module.recorder), 'active', property(lambda self: True))
    handled = []
    # a route that answers for whatever the envelope names: the point here is the number, not
    # whether this process serves that bot
    delivery = get_delivery(
        handler=lambda **call: handled.append(call),
        route=lambda _bot_id: lambda **call: handled.append(call),
    )

    assert delivery.dispatch(_bytes(a_payload(bot=2**63, kwargs={'chat_id': 1, 'text': 'wide'})))

    assert kept, 'nothing was recorded at all'
    assert all(event.bot_id is None for event in kept), [event.bot_id for event in kept]
