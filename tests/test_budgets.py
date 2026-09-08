"""Two in-flight budgets: one over the queue, one over each bot on it.

One bound over the whole queue is what turns a single slow client into everybody's outage --
their sends fill it and every other bot waits behind them. So a bot has a bound of its own,
and a message taken for a bot already at it is held by the consumer rather than blocking the
read or being released to a transport that would strand it.
"""

import uuid

from django.test import override_settings

from django_aiogram.consumer.delivery import BlpopDelivery
from django_aiogram.wire.envelope import pack
from django_aiogram.wire.serializers import get_serializer

SETTINGS = {'BROKER': 'django_aiogram.testing.InMemoryBroker', 'FSM_STORAGE': 'memory'}


class Deferring:
    """A handler that takes `on_complete` and `on_refused`, like `send_raw` does.

    Both, because the two give the slot back through different paths: a send that finished and
    a send the producer would not take. A fake taking only the first leaves the second's
    accounting untested, and it is the one nothing else in the suite reaches.
    """

    def __init__(self):
        """Hold the callbacks nobody has called yet, with the text each belongs to."""
        self.pending = []
        self.refusals = {}

    def __call__(
        self,
        function=None,
        correlation_id=None,
        queued_at=0.0,
        on_complete=None,
        on_refused=None,
        **kwargs,
    ):
        """Record the send and keep both of its callbacks for the case to call."""
        self.pending.append((kwargs.get('text'), on_complete))
        self.refusals[kwargs.get('text')] = on_refused


def a_message(bot_id, text):
    """One serialized send for one bot."""
    return get_serializer().dumps(pack('send_message', {'chat_id': 1, 'text': text}, uuid.uuid4(), 0.0, bot_id))


def routed(handler):
    """A route that hands every bot the same handler, which is what a container does."""
    return lambda bot_id: handler


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'MAX_IN_FLIGHT_PER_BOT': 1, 'MAX_IN_FLIGHT': 10})
def test_a_bot_at_its_own_budget_does_not_hold_up_the_others():
    """The claim the second budget exists for, and the one a single bound cannot make."""
    handler = Deferring()
    delivery = BlpopDelivery(handler=handler, route=routed(handler))

    assert delivery.dispatch(a_message(123456, 'first for the slow bot')) is False
    assert delivery.dispatch(a_message(123456, 'second for the slow bot')) is False
    assert delivery.dispatch(a_message(654321, 'for the other bot')) is False

    handed = [text for text, _ in handler.pending]
    assert handed == ['first for the slow bot', 'for the other bot'], handed


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'MAX_IN_FLIGHT_PER_BOT': 1, 'MAX_IN_FLIGHT': 10})
def test_a_held_message_is_handed_over_when_that_bot_has_room():
    """Held, not dropped and not released: the send happens as soon as a slot comes back."""
    handler = Deferring()
    delivery = BlpopDelivery(handler=handler, route=routed(handler))

    delivery.dispatch(a_message(123456, 'first'))
    delivery.dispatch(a_message(123456, 'waiting'))
    assert [text for text, _ in handler.pending] == ['first']

    # the first send finishes, which is the only thing that gives that bot a slot back
    handler.pending[0][1]()
    delivery.collect()

    assert [text for text, _ in handler.pending] == ['first', 'waiting']


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'MAX_IN_FLIGHT_PER_BOT': 1, 'MAX_IN_FLIGHT': 10})
def test_a_held_message_occupies_one_of_the_queue_slots():
    """So `MAX_IN_FLIGHT` bounds how many can wait, rather than nothing bounding it."""
    handler = Deferring()
    delivery = BlpopDelivery(handler=handler, route=routed(handler))

    delivery.dispatch(a_message(123456, 'sent'))
    delivery.dispatch(a_message(123456, 'waiting'))

    assert delivery._in_flight == 2, delivery._in_flight


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'MAX_IN_FLIGHT_PER_BOT': 1, 'MAX_IN_FLIGHT': 10})
def test_the_counts_come_back_to_nothing():
    """A budget that drifts is a consumer that stops reading while nothing is in flight.

    Both counts, and both directions: what is taken has to be given back exactly once,
    whether the send finished or the producer refused it.
    """
    handler = Deferring()
    delivery = BlpopDelivery(handler=handler, route=routed(handler))

    delivery.dispatch(a_message(123456, 'sent'))
    delivery.dispatch(a_message(123456, 'waiting'))
    handler.pending[0][1]()
    delivery.collect()
    handler.pending[1][1]()
    delivery.collect()

    assert delivery._in_flight == 0, delivery._in_flight
    assert delivery._sending == {}, delivery._sending


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'MAX_IN_FLIGHT': 10})
def test_no_per_bot_budget_is_the_behaviour_that_shipped_before():
    """Zero is the default, and it has to mean what it always meant: only the queue's bound."""
    handler = Deferring()
    delivery = BlpopDelivery(handler=handler, route=routed(handler))

    for number in range(5):
        delivery.dispatch(a_message(123456, f'number {number}'))

    assert len(handler.pending) == 5
    assert not delivery._parked, 'a message was held with no per-bot budget set'


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'MAX_IN_FLIGHT_PER_BOT': 1, 'MAX_IN_FLIGHT': 10})
def test_a_held_message_is_not_acknowledged_while_it_waits():
    """A crash while it waits has to leave it where every other in-flight message is.

    Acknowledged on being parked, it would be gone: nothing sent it. `dispatch` says `False`,
    which is the same answer it gives a message for a bot this process does not serve.
    """
    handler = Deferring()
    delivery = BlpopDelivery(handler=handler, route=routed(handler))

    delivery.dispatch(a_message(123456, 'sent'))

    assert delivery.dispatch(a_message(123456, 'waiting')) is False


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'MAX_IN_FLIGHT_PER_BOT': 1, 'MAX_IN_FLIGHT': 10})
def test_a_bot_still_at_its_budget_stays_held_and_the_walk_carries_on():
    """The park is one queue, so stopping at the first saturated bot would be the blocking
    the per-bot budget exists to prevent — the messages behind it are for other bots.
    """
    handler = Deferring()
    delivery = BlpopDelivery(handler=handler, route=routed(handler))

    delivery.dispatch(a_message(123456, 'slow one: sent'))
    delivery.dispatch(a_message(123456, 'slow one: waiting'))
    delivery.dispatch(a_message(654321, 'other one: sent'))
    delivery.dispatch(a_message(654321, 'other one: waiting'))

    # only the other bot's send finishes, so the slow bot is still at its budget
    handler.pending[1][1]()
    delivery.collect()

    handed = [text for text, _ in handler.pending]
    assert handed == [
        'slow one: sent',
        'other one: sent',
        'other one: waiting',
    ], handed


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'MAX_IN_FLIGHT_PER_BOT': 1, 'MAX_IN_FLIGHT': 10})
def test_a_refused_send_gives_both_slots_back_and_the_message_is_not_acknowledged():
    """The other way a slot comes back, and the one nothing else reaches.

    A producer that refuses the send outright -- shutting down, or over its own bound -- has
    not sent anything, so the message must stay for a redelivery while the budget it was
    holding comes back. Held per bot as well as per queue, or that bot's budget would leak a
    slot per refusal until it could serve nobody.
    """
    handler = Deferring()
    delivery = BlpopDelivery(handler=handler, route=routed(handler))

    delivery.dispatch(a_message(123456, 'refused'))
    delivery.dispatch(a_message(123456, 'waiting'))

    acknowledged = []
    delivery.acknowledge = acknowledged.append
    handler.refusals['refused']()
    delivery.collect()

    assert [text for text, _ in handler.pending] == ['refused', 'waiting'], 'the held message never went'
    assert acknowledged == [], 'a message nothing sent was acknowledged'

    handler.refusals['waiting']()
    delivery.collect()

    assert delivery._in_flight == 0, delivery._in_flight
    assert delivery._sending == {}, delivery._sending


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'MAX_IN_FLIGHT_PER_BOT': -1})
def test_a_negative_per_bot_budget_is_reported_rather_than_read_as_none():
    """`max(0, ...)` in the consumer reads it as *no bound*, which is the opposite of asking.

    So the check has to be the thing that says so: silently, the setting a project added to
    stop one client filling the queue would be the behaviour it was added to leave.
    """
    from django_aiogram.config.checks import check_settings

    reported = [message for message in check_settings() if str(message.id).endswith('E060')]

    assert reported, 'a negative per-bot budget was accepted'


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'MAX_IN_FLIGHT_PER_BOT': 2, 'MAX_IN_FLIGHT': 10})
def test_two_held_messages_do_not_reserve_the_budget_they_are_waiting_for():
    """A held message waits for one of this bot's sends to end, so it may not count as one.

    Counted, a budget of two with two sends and two held messages sits at two once both sends
    finish: at the budget, with nothing running that could take it below again. Both held
    messages are re-held for ever and that bot is stalled for the life of the process --
    measured before this, and it is the shape a reservation nothing can release always has.
    """
    handler = Deferring()
    delivery = BlpopDelivery(handler=handler, route=routed(handler))

    delivery.dispatch(a_message(123456, 'first'))
    delivery.dispatch(a_message(123456, 'second'))
    delivery.dispatch(a_message(123456, 'third'))
    delivery.dispatch(a_message(123456, 'fourth'))
    assert [text for text, _ in handler.pending] == ['first', 'second']

    handler.pending[0][1]()
    handler.pending[1][1]()
    delivery.collect()

    handed = [text for text, _ in handler.pending]
    assert handed == ['first', 'second', 'third', 'fourth'], handed

    for _, finished in handler.pending[2:]:
        finished()
    delivery.collect()

    assert delivery._in_flight == 0, delivery._in_flight
    assert delivery._sending == {}, delivery._sending


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'MAX_IN_FLIGHT_PER_BOT': 1, 'MAX_IN_FLIGHT': 10})
def test_a_send_that_raised_gives_its_bot_its_slot_back():
    """A handler that raises has not deferred anything, so nothing else will report it.

    The queue's slot came back already; this is the bot's, and a held message behind it is
    handed over on the same turn rather than waiting for an unrelated send to finish.
    """
    calls = []

    def raising(function=None, correlation_id=None, queued_at=0.0, on_complete=None, on_refused=None, **kwargs):
        calls.append(kwargs.get('text'))
        if kwargs.get('text') == 'raises':
            msg = 'this handler failed'
            raise RuntimeError(msg)

    delivery = BlpopDelivery(handler=raising, route=lambda bot_id: raising)

    delivery.dispatch(a_message(123456, 'raises'))

    assert delivery._sending == {}, delivery._sending

    delivery.dispatch(a_message(123456, 'after it'))

    assert calls == ['raises', 'after it'], 'the next message waited on a send that had failed'
    assert delivery._sending == {123456: 1}, delivery._sending
