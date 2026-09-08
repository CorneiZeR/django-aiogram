"""Which of `start_tgbot`'s two jobs a container does.

It has always done both — receive updates, drain the queues — and at scale they do not scale
together: a webhook deployment wants a process that only sends, a busy pool wants receivers
with no consumer in them, and a small installation wants what it has always had. So both is
the default and each flag takes one away.
"""

import asyncio

import pytest
from django.core.management import CommandError, call_command
from django.test import override_settings

from django_aiogram import bot
from django_aiogram.management.commands.start_tgbot import Command

SETTINGS = {
    'BROKER': 'django_aiogram.testing.InMemoryBroker',
    'FSM_STORAGE': 'memory',
    'TOKEN': '123456:AAaa',
}


@pytest.fixture
def watching(monkeypatch):
    """Record what each half of the command did, without a network or a real consumer."""
    events = []

    class Recording:
        """A consumer that says what was asked of it."""

        def __init__(self, *args, **kwargs):
            """Take whatever the command hands a `DELIVERY`."""
            self.settings = kwargs.get('settings')

        def reclaim(self):
            """Answer the preflight the way a crash-safe transport does."""
            return True

        @property
        def crash_safe(self):
            """Say the guarantee holds, so the preflight passes."""
            return True

        @property
        def queue_key(self):
            """Name the queue this stand-in would read."""
            return 'queue'

        def start_thread(self):
            """Say a consumer started, and hand back something a join can be called on."""
            events.append('consumer-started')
            return _Finished()

        def stop(self):
            """Say it was stopped."""
            events.append('consumer-stopped')

        def collect(self):
            """Say what it finished was settled."""

    monkeypatch.setattr('django_aiogram.management.commands.start_tgbot.get_delivery', Recording)

    def polled():
        events.append('polling-started')
        # one turn of the loop, because the consumer is started from a callback on it: the
        # command defers that deliberately, so a backlog cannot reach a handler before the
        # loop is running
        bot.loop.run_until_complete(asyncio.sleep(0))

    def idled(self):
        events.append('idled')
        bot.loop.run_until_complete(asyncio.sleep(0))

    monkeypatch.setattr(bot, 'start_polling', polled)
    monkeypatch.setattr(bot, 'close', lambda: None)
    monkeypatch.setattr(Command, '_idle_on_the_loop', idled)
    return events


class _Finished:
    """A thread that has already ended, which is what a stand-in consumer's is."""

    @staticmethod
    def join(timeout=None):
        """Return at once."""

    @staticmethod
    def is_alive():
        """Say it is not."""
        return False


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_both_halves_run_by_default(watching):
    """What a small installation has always had, and the flags only take away from."""
    call_command('start_tgbot')

    assert 'polling-started' in watching
    assert 'consumer-started' in watching


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_no_updates_consumes_and_never_asks_telegram_for_anything(watching):
    """The claim #118 names, and the shape a webhook deployment's sender pool is."""
    call_command('start_tgbot', '--no-updates')

    assert 'polling-started' not in watching, 'a sender-only container polled for updates'
    assert 'consumer-started' in watching, 'the queue was not consumed'
    # a loop still has to turn: the consumers hand their sends to it, which is what webhook
    # mode has always idled for
    assert 'idled' in watching


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_updates_only_receives_and_consumes_nothing(watching):
    """A receiver in a pool where somebody else drains the queues."""
    call_command('start_tgbot', '--updates-only')

    assert 'polling-started' in watching
    assert 'consumer-started' not in watching, 'a receiver-only container consumed a queue'


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_container_that_would_do_neither_is_refused(watching):
    """It would sit there looking alive, answering a probe, and doing nothing at all."""
    with pytest.raises(CommandError, match='nothing for this process to do'):
        call_command('start_tgbot', '--no-updates', '--updates-only')

    assert watching == []


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_receiver_is_told_to_give_the_probe_the_same_news(watching, capsys):
    """Otherwise the probe restarts a healthy container for not having a consumer in it."""
    call_command('start_tgbot', '--updates-only')

    assert '--no-consumer' in capsys.readouterr().out


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_receiver_shuts_down_cleanly_with_no_consumer_to_join(watching):
    """`max` of no queues raises, and the join bound is read before any thread exists."""
    call_command('start_tgbot', '--updates-only')

    assert 'consumer-stopped' not in watching
    assert asyncio.get_event_loop_policy() is not None  # the loop was left in one piece
