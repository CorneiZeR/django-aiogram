"""What closes the transport, and when.

`Broker.close()` describes itself as "called once, at shutdown". Nothing called it there: the
only path to `registry.close_broker` was the `setting_changed` receiver, which fires in a test
suite and never in a deployment. `bot.close()` tore down the loop, the aiogram session and the
FSM storage and left the transport alone, so the two long-running commands did not reach it
either, and a web process that only queues had no shutdown path at all.

What that cost is per transport and worst on Kafka: the producer was never flushed, so the
warning it raises about messages accepted locally and never delivered could only fire in tests;
and the consumer never left its group, so a restarted bot container waits out the session
timeout before the coordinator reassigns its partitions — delivering nothing meanwhile, with a
full queue and a healthy-looking worker.

Two mechanisms, because one is not enough. `bot.close()` releases it deterministically, which
covers the commands. `atexit` covers everything else, in the same shape `EventRecorder` has used
for its writer all along.

Neither promises a thread — `atexit` runs on the main one at interpreter shutdown, `bot.close()`
wherever its caller is — so each transport's `close()` already restricts what it touches from a
thread that did not open it: pika asks the owner through `add_callback_threadsafe`, librdkafka
flushes the process producer and closes only the calling thread's consumer.
"""

import atexit

import pytest
from django.test import override_settings

from django_aiogram import TelegramBot
from django_aiogram.broker import registry

SETTINGS = {
    'TOKEN': '42:x',
    'REDIS_URL': 'redis://localhost:6379/0',
    'BROKER': 'django_aiogram.broker.redis_list.RedisListBroker',
}


class Closeable:
    """A broker that records being closed, and nothing else."""

    def __init__(self):
        self.closed = 0

    def close(self):
        self.closed += 1


@pytest.fixture
def fresh_registry(monkeypatch):
    """A registry with no broker and no armed hook, restored afterwards.

    Both are process-global, so a test that armed the hook would otherwise decide the answer
    for every test after it — which is the shape of pass that means nothing.
    """
    monkeypatch.setattr(registry, '_broker', None)
    monkeypatch.setattr(registry, '_exit_hook_armed', False)
    return registry


@pytest.fixture
def armed(monkeypatch):
    """Everything handed to `atexit.register`, without registering any of it for real."""
    calls = []
    monkeypatch.setattr(atexit, 'register', lambda hook, *a, **k: calls.append(hook) or hook)
    return calls


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_building_the_broker_arms_an_exit_hook(fresh_registry, armed, redis_server):
    """The hook is what makes `close()`'s own docstring true for a process that never closes."""
    fresh_registry.get_broker()

    assert fresh_registry.close_broker in armed, (
        f'nothing armed close_broker at exit; atexit was handed {[getattr(h, "__name__", h) for h in armed]}'
    )


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_hook_is_armed_once_however_often_the_broker_is_rebuilt(fresh_registry, armed, redis_server):
    """A settings change replaces the broker, and must not stack another callback.

    Harmless if it did — `close_broker` is idempotent — but a list of duplicates is how a
    process ends up closing something twice and reporting the second failure.
    """
    fresh_registry.get_broker()
    fresh_registry.close_broker()
    fresh_registry.get_broker()
    fresh_registry.close_broker()
    fresh_registry.get_broker()

    assert armed.count(fresh_registry.close_broker) == 1, f'armed {armed.count(fresh_registry.close_broker)} times'


def test_closing_the_bot_closes_the_transport(fresh_registry, monkeypatch):
    """`start_tgbot` joins the consumer before this, so nothing is taking from the broker.

    Asserted through `bot.close()` rather than through `close_broker` directly, because the
    defect was that this path did not reach it — the transport survived the teardown of
    everything around it.
    """
    broker = Closeable()
    monkeypatch.setattr(fresh_registry, '_broker', broker)

    TelegramBot().close()

    assert broker.closed == 1, 'closing the bot left the transport open'
    assert fresh_registry._broker is None, 'the closed broker is still cached'


def test_closing_twice_is_not_an_error(fresh_registry, monkeypatch):
    """Both mechanisms can reach it — `close()` and then `atexit` — so it has to be idempotent."""
    broker = Closeable()
    monkeypatch.setattr(fresh_registry, '_broker', broker)

    fresh_registry.close_broker()
    fresh_registry.close_broker()

    assert broker.closed == 1, 'the second close reached a broker that was already released'


def test_closing_with_no_broker_built_does_nothing(fresh_registry):
    """The common case at exit: a process that imported the package and never sent."""
    fresh_registry.close_broker()

    assert fresh_registry._broker is None


def test_a_transport_that_fails_to_close_does_not_wedge_the_bot(fresh_registry, monkeypatch):
    """The flags gate sending, so leaving them set retires the bot for the life of the process.

    `close_broker` propagates on purpose — a caller should hear that a queue could not be
    released — and an exception escaping a `finally` skips whatever follows it in the same
    block. Closing came first there, so a Kafka flush that raised took the resets with it and
    every later send was refused, with the failure long since logged and forgotten.
    """

    class Stubborn:
        def close(self):
            msg = 'the broker could not be released'
            raise RuntimeError(msg)

    monkeypatch.setattr(fresh_registry, '_broker', Stubborn())
    instance = TelegramBot()

    with pytest.raises(RuntimeError, match='could not be released'):
        instance.close()

    assert instance._closing is False, 'a failed close left the bot refusing every later send'
    assert instance._draining is False, 'and draining, which refuses them differently'


def test_a_skipped_teardown_leaves_the_transport_alone(fresh_registry, monkeypatch, caplog):
    """`close()` refuses a running loop and expects to be called again, so nothing may be released.

    The transport is process-global and the loop that is still running is the one polling, so
    closing it here takes the queue out from under a live consumer. On Kafka the rebuild joins
    the group a second time while the first member still holds the partitions — which is the
    stall this whole change exists to remove, reintroduced by the fix for it.
    """

    class Running:
        def is_running(self):
            return True

        def is_closed(self):
            return False

    broker = Closeable()
    monkeypatch.setattr(fresh_registry, '_broker', broker)
    instance = TelegramBot()
    monkeypatch.setattr(instance, '_loop', Running())

    with caplog.at_level('WARNING', logger='django_aiogram'):
        instance.close()

    assert broker.closed == 0, 'a skipped teardown released the queue a live consumer is reading'
    assert any('skipping close' in record.getMessage() for record in caplog.records), (
        'the skip was not the path taken, so this case proves nothing'
    )
    assert instance._closing is False, 'and the retry it asks for must still be possible'
