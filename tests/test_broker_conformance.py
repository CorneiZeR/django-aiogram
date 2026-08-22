"""One suite, run against every transport there is.

The point of the contract is that `Delivery` can be written once. That only holds if every
broker answers the same questions the same way, and the only way to know is to ask all of
them the same questions — so a transport is added here on the day it is added to the package.
A broker that cannot pass this is not a broker.

Parametrised over `SHIPPED`, so a new entry in the registry is covered without anybody
remembering to come back here. What each case needs to *run* differs — Redis wants a server,
Kafka wants a broker — so a fixture per transport supplies that, and a transport with no
fixture yet is skipped loudly rather than silently passing.
"""

import pytest
from django.test import override_settings

from django_aiogram.broker.base import Broker
from django_aiogram.broker.registry import SHIPPED
from django_aiogram.wire.serializers import JsonSerializer

SETTINGS = {'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost:6379/0', 'RATE_LIMIT': None}


def payload(chat_id: int) -> bytes:
    """One message, in the shape a producer writes."""
    return JsonSerializer().dumps({'function': 'send_message', 'chat_id': chat_id})


@pytest.fixture(params=sorted(SHIPPED), ids=lambda path: path.rsplit('.', 1)[-1])
def broker(request, redis_server):
    """One instance of each shipped transport, ready to publish and take.

    `redis_server` for the Redis ones; a transport that needs something else grows its own
    branch here, and the assertions below do not change — which is the whole point.
    """
    from django.utils.module_loading import import_string

    path = request.param
    if 'redis' not in path:
        pytest.skip(f'no fixture yet for {path}: add one rather than skipping the contract')
    with override_settings(TELEGRAM_BOT=SETTINGS):
        yield import_string(path)()


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_published_message_can_be_taken(broker: Broker):
    """Publish then take, which is the whole of what a transport is for."""
    broker.publish([payload(7)])

    taken = broker.take_nowait()

    assert taken is not None, 'nothing came back from a queue that was just written to'
    assert taken.payload == payload(7)


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_publishing_nothing_queues_nothing_and_raises_nothing(broker: Broker):
    """The transports disagree by nature, so the contract has to decide.

    A batching producer accepts an empty batch quietly; `RPUSH key` with no values is a
    syntax error to Redis — measured, `wrong number of arguments for 'rpush' command`. A
    caller holding a list that turned out empty should not have to know which transport it
    is talking to, so the answer is: nothing happens.

    Not reachable through this package's own producers — `_chunks` yields no chunk for an
    empty iterable, so the loop body never runs — which is exactly why it belongs here
    rather than in a producer test. The contract accepts a `Sequence[bytes]` from anyone.
    """
    before = broker.depth()

    broker.publish([])

    assert broker.depth() == before, 'publishing nothing changed the queue'


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_an_empty_queue_answers_none_rather_than_blocking(broker: Broker):
    """`take_nowait` on nothing is `None`, not an exception and not a wait."""
    assert broker.take_nowait() is None


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_an_acknowledged_message_does_not_come_back(broker: Broker):
    """What `ack` means, stated as the only thing that can be checked from outside."""
    broker.publish([payload(1)])
    taken = broker.take_nowait()
    assert taken is not None

    broker.ack(taken.handle)

    assert broker.inflight_depth() == 0, 'the message is still in flight after being settled'
    assert broker.reclaim() in (0, None), 'a settled message was reclaimed'


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_released_message_comes_back(broker: Broker):
    """The difference between refused and delivered, which is why `release` exists.

    A broker where leaving a message alone already means "redeliver it" implements this as a
    no-op — and then reclaiming is what brings it back, which is the same promise reached the
    other way. Either is conformant; losing the message is not.
    """
    broker.publish([payload(2)])
    taken = broker.take_nowait()
    assert taken is not None

    broker.release(taken.handle)
    broker.reclaim()

    again = broker.take_nowait()
    assert again is not None, 'a released message was lost'
    assert again.payload == payload(2)


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_depth_counts_what_is_waiting(broker: Broker):
    """Two published, two waiting; one taken, one waiting."""
    broker.publish([payload(3), payload(4)])
    assert broker.depth() == 2

    broker.take_nowait()

    assert broker.depth() == 1


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_what_is_taken_and_unsettled_is_in_flight(broker: Broker):
    """The count `MAX_IN_FLIGHT` is compared against, from the transport's own books."""
    broker.publish([payload(5)])
    assert broker.inflight_depth() == 0

    taken = broker.take_nowait()

    assert taken is not None
    assert broker.inflight_depth() == 1, 'a taken message is not counted as in flight'


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_handle_is_opaque_and_round_trips(broker: Broker):
    """`Delivery` passes a handle back unread, so the broker must accept its own.

    Asserted by settling with the handle exactly as given, not by looking at it: what it
    names is the transport's business, and a test that reads it would be pinning one
    transport's answer as the contract.
    """
    broker.publish([payload(6)])
    taken = broker.take_nowait()
    assert taken is not None

    broker.ack(taken.handle)

    assert broker.take_nowait() is None, 'the message survived being settled by its handle'


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_broker_says_whether_a_kill_loses_a_message(broker: Broker):
    """Whatever the answer, there has to be one — a deployment refuses on it."""
    assert isinstance(broker.crash_safe, bool)


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_broker_says_whether_it_needs_a_worker_name(broker: Broker):
    """True only where the transport cannot say which consumer holds a message."""
    assert isinstance(broker.needs_identity, bool)


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_liveness_answers_without_a_consumer_running(broker: Broker):
    """A probe asks this of a process that may never have consumed anything."""
    liveness = broker.liveness()

    assert isinstance(liveness.reported, bool)
    assert liveness.age is None or liveness.age >= 0
