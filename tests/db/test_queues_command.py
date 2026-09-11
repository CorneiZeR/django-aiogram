"""`tgbot_queues`: what is declared, how deep it is, and whether anybody is reading it.

A queue nobody consumes fills up and every message in it is delivered *eventually*, which
looks exactly like a slow bot until somebody asks. This is where it is asked.
"""

import json
from io import StringIO

import pytest
from django.core.management import call_command
from django.test import override_settings

from django_aiogram.models import TelegramQueue

pytestmark = pytest.mark.django_db

SETTINGS = {
    'BROKER': 'django_aiogram.testing.InMemoryBroker',
    'FSM_STORAGE': 'memory',
}


def listed(**options):
    """Run the command in JSON and return what it said, row by row."""
    out = StringIO()
    call_command('tgbot_queues', json=True, stdout=out, stderr=StringIO(), **options)
    return [json.loads(line) for line in out.getvalue().splitlines() if line.strip()]


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_the_declared_queues_are_listed_with_their_pools():
    """The pool is the deployment axis: a container is started with pools, not queues."""
    TelegramQueue.objects.create(name='client-a', pool='vip')
    TelegramQueue.objects.create(name='client-b', pool='default')

    rows = {row['queue']: row for row in listed()}

    assert rows['client-a']['pool'] == 'vip'
    assert rows['client-b']['pool'] == 'default'


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'QUEUES': ('from-settings',)})
def test_a_queue_declared_in_the_settings_is_listed_too():
    """A deployment that names its queues in `settings.py` has no table row to find them by.

    Its pool is empty rather than `default`: there is no row to give it one, and calling it
    the default pool would be inventing a fact a container could be started against.
    """
    (row,) = [found for found in listed() if found['queue'] == 'from-settings']

    assert row['pool'] == '—'


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_the_listing_can_be_narrowed_by_queue_and_by_pool():
    """Because a deployment with five hundred clients has five hundred queues."""
    TelegramQueue.objects.create(name='client-a', pool='vip')
    TelegramQueue.objects.create(name='client-b', pool='default')

    assert {row['queue'] for row in listed(queues=['client-a'])} == {'client-a'}
    assert {row['queue'] for row in listed(pools=['default'])} == {'client-b'}


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_nothing_is_asked_of_the_transport_when_the_depth_is_not_wanted():
    """`--no-depth` is the form that answers over no network at all."""
    TelegramQueue.objects.create(name='client-a', pool='vip')

    (row,) = listed(no_depth=True)

    assert row['depth'] == '?'
    assert row['consumer'] == '?'


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_transport_that_cannot_be_reached_is_not_an_empty_queue():
    """Printing `0` for it is the one answer that sends an operator looking in the wrong place."""
    TelegramQueue.objects.create(name='client-a', pool='vip')

    with override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'BROKER': 'no.such.module.Broker'}):
        (row,) = listed()

    assert row['depth'] == '?', row


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_transport_that_will_not_answer_is_not_an_empty_queue(monkeypatch):
    """The other half: the broker builds, and then the read itself fails.

    A connection refused mid-listing must read as *unknown* rather than as zero — the case
    above only covers a transport that could not be built at all.
    """
    TelegramQueue.objects.create(name='client-a', pool='vip')

    class Silent:
        """A transport that builds and then refuses every question."""

        def configured(self, _settings):
            """Answer as a configured broker does."""
            return self

        def depth(self):
            """Refuse the way a socket does."""
            msg = 'connection refused'
            raise ConnectionError(msg)

        def inflight_depth(self, worker=None):
            """The same."""
            msg = 'connection refused'
            raise ConnectionError(msg)

        def liveness(self):
            """And the same again."""
            msg = 'connection refused'
            raise ConnectionError(msg)

    monkeypatch.setattr('django_aiogram.broker.registry.broker_class', lambda *_a, **_k: Silent())

    (row,) = listed()

    assert row['depth'] == '?', row
    assert row['in_flight'] == '?', row
    assert row['consumer'] == '?', row


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_deployment_with_no_queues_says_so():
    """Rather than a header with nothing under it, which reads as a broken command."""
    out = StringIO()
    call_command('tgbot_queues', stdout=out, stderr=StringIO())

    assert 'no queues are declared' in out.getvalue()
