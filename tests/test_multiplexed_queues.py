"""One consumer, several queues, one connection -- and a budget that is still per queue.

A container serving twenty client queues held twenty connections and twenty threads, because a
consumer was one queue's. Three of the four transports can read several queues over the
connection they already have, and these cases are what that has to keep true: every queue is
read, each message says which queue it came off, and the bound that stops one client's backlog
becoming everybody's is still applied per queue rather than to the lot.

Against Redis Streams over `fakeredis`, which is the multiplexing transport this suite can run
without a server. RabbitMQ's half of the same contract is in `test_broker_conformance.py`,
which runs against a real broker in CI.
"""

import uuid

import pytest
from django.test import override_settings

from django_aiogram.broker.exceptions import QueueMultiplexingUnavailableError
from django_aiogram.broker.registry import get_broker
from django_aiogram.consumer.delivery import BlpopDelivery
from django_aiogram.consumer.serving import lanes
from django_aiogram.eventlog.recorder import recorder
from django_aiogram.runtime.queues import settings_for
from django_aiogram.wire.envelope import pack
from django_aiogram.wire.serializers import get_serializer

STREAMS = {
    'BROKER': 'django_aiogram.broker.redis_streams.RedisStreamsBroker',
    'REDIS_STREAM_KEY': 'TELEGRAM_BOT_STREAM',
    'FSM_STORAGE': 'memory',
    'TOKEN': '123456:AAaa',
    'QUEUES': ('vip', 'bulk'),
}
MEMORY = {
    'BROKER': 'django_aiogram.testing.InMemoryBroker',
    'FSM_STORAGE': 'memory',
    'TOKEN': '123456:AAaa',
    'QUEUES': ('vip', 'bulk'),
}


class Deferring:
    """A handler shaped like `send_raw`: it takes `on_complete` and calls nothing itself."""

    def __init__(self):
        """Hold the sends nobody has finished yet."""
        self.pending = []

    def __call__(self, function=None, correlation_id=None, queued_at=0.0, on_complete=None, **kwargs):
        """Record the send and keep the callback for the case to call."""
        self.pending.append((kwargs.get('text'), on_complete))


def routed(handler):
    """A route that hands every bot the same handler, which is what a container does."""
    return lambda bot_id: handler


def a_message(text):
    """One serialized send, for the one bot these cases configure."""
    return get_serializer().dumps(pack('send_message', {'chat_id': 1, 'text': text}, uuid.uuid4(), 0.0, 123456))


def publish(queue, text):
    """Queue one message on one queue, through the transport that queue resolves to."""
    get_broker(settings_for(queue)).publish([a_message(text)])


def consuming(handler, queues=('vip', 'bulk')):
    """A consumer serving these queues, built the way the command builds one."""
    return BlpopDelivery(
        handler=handler,
        route=routed(handler),
        settings=settings_for(queues[0]),
        queues=queues,
    )


@override_settings(TELEGRAM_BOT_DEFAULTS=STREAMS)
def test_a_consumer_serving_two_queues_delivers_from_both(redis_server):
    """The case the issue names: two queues, one connection, and a message from each.

    One transport object is what "one connection" means here -- the consumer holds exactly the
    broker it was built with, and both messages come through it.
    """
    handler = Deferring()
    delivery = consuming(handler)
    publish('vip', 'to vip')
    publish('bulk', 'to bulk')

    delivery.consume_pending()

    assert sorted(text for text, _ in handler.pending) == ['to bulk', 'to vip']
    assert delivery.queues == ('vip', 'bulk')


@override_settings(TELEGRAM_BOT_DEFAULTS={**STREAMS, 'MAX_IN_FLIGHT': 1})
def test_the_budget_is_kept_per_queue_under_one_consumer(redis_server):
    """One queue at its bound does not stop the other being read, which is the whole point.

    `MAX_IN_FLIGHT` is 1 here, so the first message on each queue takes that queue's only slot.
    A bound applied to the *consumer* would have stopped after one message in total, and the
    second queue would have waited behind a client it has nothing to do with.
    """
    handler = Deferring()
    delivery = consuming(handler)
    publish('vip', 'first for vip')
    publish('vip', 'second for vip')
    publish('bulk', 'for bulk')

    delivery.consume_pending()

    assert sorted(text for text, _ in handler.pending) == ['first for vip', 'for bulk']
    assert delivery.at_capacity('vip') is True
    assert delivery.at_capacity('bulk') is True
    assert delivery.in_flight('vip') == 1, delivery.in_flight('vip')
    assert delivery.in_flight('bulk') == 1, delivery.in_flight('bulk')


@override_settings(TELEGRAM_BOT_DEFAULTS={**STREAMS, 'MAX_IN_FLIGHT': 1})
def test_the_counts_come_back_to_nothing_on_every_queue(redis_server):
    """Each queue's slot is given back by its own send finishing, and by nothing else.

    Asserted per queue rather than on the total: a completion charged to the wrong queue leaves
    one count negative and another still held, and those add up to the zero a total would
    report.
    """
    handler = Deferring()
    delivery = consuming(handler)
    publish('vip', 'for vip')
    publish('bulk', 'for bulk')
    delivery.consume_pending()

    for _text, finished in handler.pending:
        finished()
    delivery.collect()

    assert delivery.in_flight('vip') == 0, delivery.in_flight('vip')
    assert delivery.in_flight('bulk') == 0, delivery.in_flight('bulk')
    assert delivery.at_capacity() is False


@override_settings(TELEGRAM_BOT_DEFAULTS={**STREAMS, 'MAX_IN_FLIGHT': 1})
def test_a_queue_at_its_budget_is_not_read_from_while_another_still_is(redis_server):
    """Not read rather than read and given back: a release would spin against the backlog."""
    handler = Deferring()
    delivery = consuming(handler)
    publish('vip', 'holds the vip slot')
    delivery.consume_pending()

    assert delivery.at_capacity('vip') is True
    assert delivery.readable() == ('bulk',), delivery.readable()


@override_settings(TELEGRAM_BOT_DEFAULTS=STREAMS)
def test_a_message_says_which_queue_it_came_off(redis_server):
    """What the budget is counted with, and the one thing a multiplexed read must add."""
    publish('bulk', 'for bulk')
    broker = get_broker(settings_for('vip'))

    taken = broker.take_nowait(('vip', 'bulk'))

    assert taken is not None
    assert taken.queue == 'bulk'


@override_settings(TELEGRAM_BOT_DEFAULTS=STREAMS)
def test_queues_sharing_a_connection_are_one_lane(redis_server):
    """The grouping a container serves by: one consumer for the queues one connection reads."""
    grouped = lanes(['vip', 'bulk'])

    assert list(grouped.values()) == [('vip', 'bulk')], grouped
    # keyed by the profile the two share rather than by a name, so a queue cannot be mistaken
    # for a connection and two connections cannot be mistaken for each other
    assert [kind for kind, _held in grouped] == ['connection']


@override_settings(TELEGRAM_BOT_DEFAULTS=MEMORY)
def test_a_transport_that_reads_one_queue_gets_a_lane_each():
    """Which is every deployment before this, and the Redis list's answer for good."""
    grouped = lanes(['vip', 'bulk'])

    assert grouped == {('queue', 'vip'): ('vip',), ('queue', 'bulk'): ('bulk',)}


@override_settings(TELEGRAM_BOT_DEFAULTS=MEMORY)
def test_a_consumer_on_a_transport_that_cannot_multiplex_refuses_the_set():
    """Refused where it is built, not at the first read: a container must not be wrong about
    how many backlogs it is serving.
    """
    handler = Deferring()

    with pytest.raises(QueueMultiplexingUnavailableError, match='several queues'):
        consuming(handler)


@override_settings(TELEGRAM_BOT_DEFAULTS=STREAMS)
def test_a_queue_that_arrives_is_served_without_stopping_the_others(redis_server):
    """A client arriving must not pause the other nineteen for the length of a join."""
    handler = Deferring()
    delivery = consuming(handler, queues=('vip',))

    delivery.serve(('vip', 'bulk'))
    publish('bulk', 'for the new queue')
    publish('vip', 'for the one already served')
    delivery.consume_pending()

    # both halves: the queue that arrived is read, and the queue that was already being served
    # is still read -- a `serve` that replaced the set rather than widening it would pass on the
    # first assertion alone
    assert sorted(text for text, _ in handler.pending) == ['for the new queue', 'for the one already served']
    assert delivery.in_flight('bulk') == 1
    assert delivery.in_flight('vip') == 1


@override_settings(TELEGRAM_BOT_DEFAULTS={**STREAMS, 'EVENT_LOG': True})
def test_the_feed_says_which_queue_a_message_came_off(redis_server, monkeypatch):
    """One consumer reads several, so the row cannot say the one it was built from.

    A container serving twenty clients writes every consumed message into one feed, and a row
    naming the lane's first queue for all of them is a row an operator cannot use.
    """
    written = []
    monkeypatch.setattr(recorder, 'record', written.append)
    handler = Deferring()
    delivery = consuming(handler)
    publish('bulk', 'off the bulk queue')

    delivery.consume_pending()

    queues = [event.detail.get('queue') for event in written]
    assert queues == ['bulk'], queues


@override_settings(TELEGRAM_BOT_DEFAULTS=STREAMS)
def test_a_lane_that_shrinks_to_one_queue_goes_on_reading_that_queue(redis_server):
    """The queue that is left is not necessarily the one this consumer's settings name.

    A lane built for `vip` and `bulk` is built from `vip`'s settings, so its transport is
    addressed at `vip`. When `vip` goes away the consumer serves `bulk` alone -- and a read
    that stopped naming the queue then would take from `vip`, which nobody asked for, while
    `bulk`'s backlog sat there with a consumer that believed it was serving it.
    """
    handler = Deferring()
    delivery = consuming(handler)
    delivery.serve(('bulk',))
    publish('bulk', 'the queue that is left')
    publish('vip', 'the queue that went away')

    delivery.consume_pending()

    assert [text for text, _ in handler.pending] == ['the queue that is left']


@override_settings(TELEGRAM_BOT_DEFAULTS=MEMORY)
def test_a_broker_that_predates_queue_sets_is_never_handed_one():
    """A `Broker` written before 5.0 declares `take(self, timeout)`, and that has to keep working.

    Passing `None` is not the same as passing nothing: `take(timeout, None)` is a `TypeError`
    out of the consumer's own loop, which is a container that stops delivering rather than one
    that delivers through a transport somebody wrote. So a consumer serving one queue calls the
    signature that existed before the argument did.
    """

    class Older:
        """The three methods as they were, with no room for a queue set."""

        CALL_TIMEOUT_OPTION = ''
        MULTIPLEXES = False

        def __init__(self):
            """Record what was asked of it, which is what the case reads."""
            self.calls = []

        @property
        def call_ceiling(self):
            """Whatever the consumer's arithmetic needs; the number is not what is under test."""
            return 5

        def addressed(self):
            """Name the one queue it reads, the way the consumer asks for it."""
            return 'older'

        def reclaim(self):
            """Put nothing back, and say so."""
            self.calls.append('reclaim')
            return 0

        def take_nowait(self):
            """Answer with nothing, having been called the way it declares."""
            self.calls.append('take_nowait')

        def ack(self, handle):
            """Settle nothing; no message ever reaches this."""

    handler = Deferring()
    broker = Older()
    delivery = BlpopDelivery(handler=handler, route=routed(handler))
    delivery.broker = broker
    delivery.queues = (broker.addressed(),)
    delivery._in_flight = {broker.addressed(): 0}
    delivery._asked = None

    delivery.reclaim()
    delivery.consume_pending()

    assert broker.calls == ['reclaim', 'take_nowait'], broker.calls


@override_settings(TELEGRAM_BOT_DEFAULTS=STREAMS)
def test_the_books_of_a_queue_that_went_away_are_dropped_once_it_has_settled(redis_server):
    """A container whose clients come and go must not keep a counter per queue that ever was.

    Both halves, because only one of them is about memory: a queue with a send still in flight
    keeps its entry -- that slot has to be given back somewhere -- and one that has settled is
    forgotten by the next read.
    """
    handler = Deferring()
    delivery = consuming(handler)
    publish('bulk', 'still going when the queue leaves')
    delivery.consume_pending()

    delivery.serve(('vip',))
    delivery.consume_pending()

    assert delivery.in_flight('bulk') == 1, 'a send in flight lost the books its slot comes back to'

    for _text, finished in handler.pending:
        finished()
    delivery.collect()
    delivery.consume_pending()

    assert delivery.in_flight('bulk') == 0
    assert 'bulk' not in delivery._in_flight, 'the books of a queue nothing serves are kept for ever'


@override_settings(TELEGRAM_BOT_DEFAULTS=STREAMS)
def test_the_broker_is_told_the_set_a_consumer_is_for_rather_than_what_it_reads_now(redis_server):
    """The two questions a lane asks, and the reason they are asked separately.

    A `take` naming fewer queues means one of them is at its budget; on Kafka a narrower
    subscription is a group rebalance, so the set that moves the subscription has to be the one
    that moves when a queue is added or taken away. It is told at construction and again on
    `serve`, and never on a capacity change.
    """
    told: list[tuple[str, ...]] = []
    handler = Deferring()
    delivery = consuming(handler)
    delivery.broker.serving = lambda queues=None: told.append(tuple(queues or ()))

    delivery._tell_the_broker()
    delivery.serve(('vip', 'bulk', 'later'))
    publish('vip', 'fills the vip budget')
    delivery.consume_pending()

    assert told == [('vip', 'bulk'), ('vip', 'bulk', 'later')], told
