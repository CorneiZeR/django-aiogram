"""The consumers a container is running, made to match the queues it should be serving.

A container told a *pool* is told a label, and which queues carry it is a table's answer that
changes while the container runs. Read once, a queue created for a client an hour after the
deploy would wait for a redeploy — which is what selecting by pool exists to avoid.
"""

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


def test_a_pass_that_cannot_read_the_queues_changes_nothing(monkeypatch):
    """A database blinking must not stop a container consuming, as with the bot providers."""
    TelegramQueue.objects.create(name='client-1', pool='vip')
    consumers, log = watching()

    with override_settings(TELEGRAM_BOT_DEFAULTS=MEMORY):
        consumers.reconcile(served_by([], ['vip']))

        def refuse(*args, **kwargs):
            msg = 'the database is not reachable'
            raise OperationalError(msg)

        monkeypatch.setattr(TelegramQueue.objects, 'filter', refuse)
        with pytest.raises(OperationalError):
            served_by([], ['vip'])

    assert list(consumers.running) == ['client-1'], 'a failed read took the consumer down'
    assert log == [('started', 'client-1')]


def test_one_queue_that_cannot_be_consumed_does_not_stop_the_others(caplog):
    """A misconfigured queue is one queue: the container keeps serving the rest.

    And the next pass tries it again, because nothing here remembers a failure -- level
    triggered, like every other pass in this package.
    """
    log = []

    def build(queue):
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
    assert log == [('started', 'fine')], 'the failed queue was not tried again by the next pass'


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
