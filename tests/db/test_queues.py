"""Queues declared by rows rather than by settings, which is the run-time half.

A deployment whose clients arrive while it runs cannot list its queues in `settings.py`, so
the table declares them too — and the refusal has to read both or a queue created through a
project's own interface would be refused as a typo.
"""

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.db import OperationalError
from django.test import override_settings

from django_aiogram.config.checks import check_settings
from django_aiogram.models import TelegramQueue
from django_aiogram.runtime.queues import declared, refuse_undeclared

pytestmark = pytest.mark.django_db

MEMORY = {'BROKER': 'django_aiogram.testing.InMemoryBroker', 'FSM_STORAGE': 'memory'}


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUES': ('bulk',)})
def test_a_row_declares_a_queue_the_settings_never_named():
    """A client connected through a project's interface gets a queue nobody deployed."""
    TelegramQueue.objects.create(name='client-42', pool='vip')

    assert declared() == {'bulk', 'client-42'}
    refuse_undeclared({'QUEUE': 'client-42'})
    with pytest.raises(ImproperlyConfigured):
        refuse_undeclared({'QUEUE': 'client-43'})


@override_settings(TELEGRAM_BOT_DEFAULTS=MEMORY)
def test_a_table_that_cannot_be_read_refuses_nothing(monkeypatch, caplog):
    """The refusal is there to catch a typo, and a database blinking is not evidence of one.

    A bot that has served its own queue for a year must not stop because the row confirming
    the name is momentarily unreachable — the same rule this package applies to a provider
    that could not look.
    """
    TelegramQueue.objects.create(name='client-42')

    def refuse(*args, **kwargs):
        # the failure a database that is down raises, which is what makes the answer unknown.
        # A `RuntimeError` here would mean something else entirely -- no table to read -- and
        # that state refuses, because then the settings are the whole declaration
        msg = 'the database is not reachable'
        raise OperationalError(msg)

    monkeypatch.setattr(TelegramQueue.objects, 'values_list', refuse)

    with caplog.at_level('WARNING', logger='django_aiogram'):
        refuse_undeclared({'QUEUE': 'client-42'})

    assert any('could not read the declared queues' in record.getMessage() for record in caplog.records)


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUES': ('vip',), 'QUEUE': 'viip'})
def test_a_queue_nothing_declares_is_reported_at_boot():
    """Boot is where a typo in a queue name is cheap; the first send is where it is not.

    Here rather than in the non-database suite deliberately: `E059` is silent where the table
    cannot be read, and a suite with no database is one of the states that is silent in.
    """
    reported = [message for message in check_settings() if str(message.id).endswith('E059')]

    assert reported, 'a bot publishing to an undeclared queue was not reported'
    assert "'viip'" in reported[0].msg
    assert 'vip' in (reported[0].hint or ''), reported[0].hint


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUES': ('vip',), 'QUEUE': 'vip'})
def test_a_declared_queue_is_not_reported():
    """The other half, and the one a correctly configured deployment needs."""
    assert [message for message in check_settings() if str(message.id).endswith('E059')] == []


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUE': 'client-42'})
def test_a_queue_declared_only_by_a_row_is_not_reported():
    """Which is the whole point of reading the table: it is where a run-time queue exists."""
    TelegramQueue.objects.create(name='client-42')

    assert [message for message in check_settings() if str(message.id).endswith('E059')] == []


@override_settings(TELEGRAM_BOT_DEFAULTS={**MEMORY, 'QUEUES': ('bulk',)})
def test_no_table_at_all_is_not_an_outage_and_still_refuses(monkeypatch, caplog):
    """A deployment with no database has no table to know better than its settings.

    Read as an outage, a project that configures its queues in `settings.py` and never
    migrates would get no refusal at all — which is the state this whole mechanism is for.
    """

    def missing(*args, **kwargs):
        msg = 'this process has no database'
        raise RuntimeError(msg)

    monkeypatch.setattr(TelegramQueue.objects, 'values_list', missing)

    with caplog.at_level('WARNING', logger='django_aiogram'), pytest.raises(ImproperlyConfigured):
        refuse_undeclared({'QUEUE': 'client-42'})

    assert not [record for record in caplog.records if 'could not read' in record.getMessage()], (
        'a process with no database reported an outage'
    )
