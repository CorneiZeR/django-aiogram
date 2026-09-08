"""The consumers a container is running, made to match the queues it should be serving.

A container told a *pool* is told a label, and which queues carry it is a table's answer that
changes while the container runs. Read once, a queue created for a client an hour after the
deploy would wait for a redeploy — which is what selecting by pool exists to avoid.
"""

import threading

import pytest
from django.db import OperationalError
from django.test import override_settings

from django_aiogram.consumer.serving import Consumers
from django_aiogram.models import TelegramQueue
from django_aiogram.runtime.queues import served_by

pytestmark = pytest.mark.django_db

MEMORY = {'BROKER': 'django_aiogram.testing.InMemoryBroker', 'FSM_STORAGE': 'memory', 'TOKEN': '123456:AAaa'}


class Fake:
    """A consumer that records what was asked of it, without a transport or a thread."""

    def __init__(self, queue, log):
        """Remember which queue this one is for, and where to say what happened."""
        self.queue = queue
        self.log = log
        self.thread = None

    def start_thread(self):
        """Hand back something a join can be called on, and say we started."""
        self.log.append(('started', self.queue))
        self.thread = _Thread()
        return self.thread

    def stop(self):
        """Say we were asked to stop."""
        self.log.append(('stopped', self.queue))

    def collect(self):
        """Say we were asked to settle what our sends finished."""
        self.log.append(('collected', self.queue))


class _Thread:
    """A thread that has already finished, which is what a fake consumer's is."""

    @staticmethod
    def join(timeout=None):
        """Return at once."""

    @staticmethod
    def is_alive():
        """Say it is not."""
        return False


def watching():
    """A `Consumers` over fake consumers, and the log they write to."""
    log = []
    return Consumers(build=lambda queue: Fake(queue, log), join_timeout=1.0), log


def test_a_queue_added_to_the_pool_is_served_without_a_restart():
    """The claim `Deployment.md` makes about pools, which a startup snapshot cannot keep."""
    TelegramQueue.objects.create(name='client-1', pool='vip')
    consumers, log = watching()

    with override_settings(TELEGRAM_BOT_DEFAULTS=MEMORY):
        consumers.reconcile(served_by([], ['vip']))
        assert log == [('started', 'client-1')]

        TelegramQueue.objects.create(name='client-2', pool='vip')
        consumers.reconcile(served_by([], ['vip']))

    assert log == [('started', 'client-1'), ('started', 'client-2')]
    assert sorted(consumers.running) == ['client-1', 'client-2']


def test_a_queue_that_left_the_pool_stops_being_consumed():
    """The other direction: a client moved to another pool is not this container's any more."""
    row = TelegramQueue.objects.create(name='client-1', pool='vip')
    TelegramQueue.objects.create(name='client-2', pool='vip')
    consumers, log = watching()

    with override_settings(TELEGRAM_BOT_DEFAULTS=MEMORY):
        consumers.reconcile(served_by([], ['vip']))
        row.pool = 'bulk'
        row.save()
        consumers.reconcile(served_by([], ['vip']))

    assert ('stopped', 'client-1') in log
    assert ('collected', 'client-1') in log, 'what the stopped consumer had in flight was left unsettled'
    assert list(consumers.running) == ['client-2']


def test_a_pass_that_cannot_read_the_queues_changes_nothing(monkeypatch, caplog):
    """A database blinking must not stop a container consuming, as with the bot providers.

    Through the watcher rather than through `served_by` alone: what is being tested is what
    the *thread* does with the failure, and a case that only proves the read raises passes
    just as well if the thread goes on to reconcile an empty set.
    """
    from django_aiogram.management.commands.start_tgbot import Command

    TelegramQueue.objects.create(name='client-1', pool='vip')
    consumers, log = watching()
    reads = []

    def refuse(*args, **kwargs):
        reads.append('asked')
        msg = 'the database is not reachable'
        raise OperationalError(msg)

    with override_settings(TELEGRAM_BOT_DEFAULTS=MEMORY):
        consumers.reconcile(served_by([], ['vip']))
        monkeypatch.setattr('django_aiogram.management.commands.start_tgbot.served_by', refuse)
        monkeypatch.setattr(Command, '_refresh_interval', staticmethod(lambda: 0.01))

        shutting_down = threading.Event()
        watcher = threading.Thread(
            target=Command()._watch_the_queues,
            args=(consumers, {'queues': '', 'pools': 'vip'}, shutting_down),
            daemon=True,
        )
        with caplog.at_level('ERROR', logger='django_aiogram'):
            watcher.start()
            for _ in range(200):
                if reads:
                    break
                watcher.join(0.01)
            shutting_down.set()
            watcher.join(timeout=2)

    assert reads, 'the watcher never read the queues'
    assert list(consumers.running) == ['client-1'], 'a failed read took the consumer down'
    assert log == [('started', 'client-1')]
    assert 'could not re-read the queues to serve' in caplog.text


def test_one_queue_that_cannot_be_consumed_does_not_stop_the_others(caplog):
    """A misconfigured queue is one queue: the container keeps serving the rest.

    And the next pass tries it again, because nothing here remembers a failure -- level
    triggered, like every other pass in this package.
    """
    log = []
    attempts = []

    def build(queue):
        # recorded before the refusal, because the claim is that the queue is *tried* again:
        # asserted on what started, a pass that stopped trying reads exactly the same
        attempts.append(queue)
        if queue == 'broken':
            msg = 'this queue cannot be consumed'
            raise RuntimeError(msg)
        return Fake(queue, log)

    consumers = Consumers(build=build, join_timeout=1.0)

    with caplog.at_level('ERROR', logger='django_aiogram'):
        consumers.reconcile(['broken', 'fine'])

    assert log == [('started', 'fine')]
    assert list(consumers.running) == ['fine']
    assert any(record.tg_queue == 'broken' for record in caplog.records if hasattr(record, 'tg_queue'))

    consumers.reconcile(['broken', 'fine'])

    assert attempts == ['broken', 'fine', 'broken'], f'the failed queue was not tried again: {attempts}'
    assert log == [('started', 'fine')], 'the queue that was already running was built twice'


def test_a_consumer_built_and_never_started_is_still_stopped_and_settled():
    """A container killed during startup: the callback that starts them never ran.

    A built consumer holds a transport and has already reclaimed, so leaving it unstopped
    strands what it took.
    """
    log = []
    never = Fake('vip', log)
    consumers = Consumers(build=lambda queue: Fake(queue, log), join_timeout=1.0, ready={'vip': never})

    consumers.stop()
    consumers.collect()

    assert log == [('stopped', 'vip'), ('collected', 'vip')]


def test_a_consumer_whose_thread_will_not_start_is_settled_rather_than_dropped():
    """It has already reclaimed, and only it can settle what it took.

    Dropped on the way out of the failure, its in-flight list is unreachable: nothing in this
    process can acknowledge those messages, and the next container sends them again.
    """
    log = []

    class WillNotStart(Fake):
        """A consumer whose thread refuses to start, after it has reclaimed."""

        def start_thread(self):
            """Refuse, the way a thread limit or a broken transport would."""
            msg = 'no thread for this one'
            raise RuntimeError(msg)

    consumers = Consumers(build=lambda queue: WillNotStart(queue, log), join_timeout=1.0)
    consumers.reconcile(['vip'])

    assert log == [('stopped', 'vip'), ('collected', 'vip')], log
    assert consumers.running == {}


def test_a_pass_that_finishes_after_the_shutdown_starts_nothing():
    """A pass can be inside a database read when the shutdown begins.

    Coming back afterwards, it would start a daemon consumer behind the joins -- doing
    transport work while `bot.close()` runs, with nothing left to stop it. So a stop is
    terminal: every later pass returns without building anything.
    """
    consumers, log = watching()

    consumers.reconcile(['client-1'])
    consumers.stop()
    consumers.reconcile(['client-1', 'client-2'])

    assert log == [('started', 'client-1'), ('stopped', 'client-1')], log
    assert consumers.running == {}


def test_a_startup_that_fails_part_way_settles_what_it_had_already_built():
    """A refusal on the second queue must not strand what the first one reclaimed.

    `REQUIRE_CRASH_SAFE` is the refusal that does this, and it is meant to stop the container
    — but a consumer that was built has already reclaimed, and only it can acknowledge what it
    took. Asserted on the command's own startup path, since that is where the building is.
    """
    from django.core.management import call_command

    log = []

    def refusing_delivery(handler, route=None, settings=None):
        queue = (settings or {}).get('QUEUE', '')
        if queue == 'client-2':
            msg = 'this queue refuses to be served'
            raise RuntimeError(msg)
        return Fake(queue, log)

    TelegramQueue.objects.create(name='client-1', pool='vip')
    TelegramQueue.objects.create(name='client-2', pool='vip')

    with override_settings(TELEGRAM_BOT_DEFAULTS=MEMORY):
        import django_aiogram.management.commands.start_tgbot as command_module

        original = command_module.get_delivery
        command_module.get_delivery = refusing_delivery
        try:
            with pytest.raises(RuntimeError, match='refuses to be served'):
                call_command('start_tgbot', '--pools', 'vip')
        finally:
            command_module.get_delivery = original

    assert log == [('stopped', 'client-1'), ('collected', 'client-1')], log


def test_a_consumer_whose_thread_outlived_the_join_is_not_settled_from_here(caplog):
    """`collect` drains the queue that consumer's own thread writes to, and calls the transport.

    Called while the thread is still inside `run`, that is two threads settling one in-flight
    list and two callers on one connection: the count drifts and the broker is reached across
    the boundary every Redis call in this package is kept on one side of.
    """
    log = []

    stuck = StillTurning('vip', log)
    consumers = Consumers(build=lambda queue: stuck, join_timeout=0.01)
    consumers.reconcile(['vip'])

    with caplog.at_level('WARNING', logger='django_aiogram'):
        consumers.reconcile([])

    assert log == [('started', 'vip'), ('stopped', 'vip')], 'the live thread was collected anyway'
    assert 'the delivery consumer did not stop in time' in caplog.text

    # and once that thread has gone, the next reach settles it: nothing else in this process
    # would, so the consumer is kept rather than dropped
    stuck.thread.alive = False
    consumers.collect()

    assert ('collected', 'vip') in log, 'what it held was never settled once its thread had gone'


class _Alive:
    """A thread that never finishes until a case says it has."""

    def __init__(self):
        """Start out still turning, which is what a blocking read looks like."""
        self.alive = True

    def join(self, timeout=None):
        """Return without waiting; whether it ended is `alive`'s answer."""

    def is_alive(self):
        """Say whether this thread is still turning."""
        return self.alive


def test_a_queue_that_comes_back_before_its_old_consumer_has_gone_waits(caplog):
    """Two consumers on one queue would both take from it and both settle it.

    The shape is a queue leaving the set, its stop timing out, and the queue coming back on
    the next pass: absent from the running set, it looks like a queue that needs starting.
    """
    log = []
    stuck = None

    def build(queue):
        nonlocal stuck
        stuck = StillTurning(queue, log)
        return stuck

    consumers = Consumers(build=build, join_timeout=0.01)
    consumers.reconcile(['vip'])
    consumers.reconcile([])

    with caplog.at_level('WARNING', logger='django_aiogram'):
        consumers.reconcile(['vip'])

    assert log == [('started', 'vip'), ('stopped', 'vip')], 'a second consumer was started for one queue'
    assert 'not starting a queue whose previous consumer is still running' in caplog.text

    stuck.thread.alive = False
    consumers.reconcile(['vip'])

    assert log[-1] == ('started', 'vip'), 'the replacement never started once the old thread had gone'


class StillTurning(Fake):
    """A consumer whose thread does not stop when it is asked to."""

    def start_thread(self):
        """Hand back a thread that never finishes until a case says it has."""
        self.log.append(('started', self.queue))
        self.thread = _Alive()
        return self.thread
