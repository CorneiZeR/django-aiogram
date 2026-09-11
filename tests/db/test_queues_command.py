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
def test_nothing_is_asked_of_the_transport_when_the_depth_is_not_wanted(monkeypatch):
    """`--no-depth` is the form that answers over no network at all.

    Asserted by making the transport itself the failure: reading `?` in the output proves
    nothing, because a command that *did* ask and then could not answer prints the same
    thing.
    """
    TelegramQueue.objects.create(name='client-a', pool='vip')

    touched = []

    def note(*_args, **_kwargs):
        """Record that something reached for a transport, and answer nothing useful.

        Recorded rather than raised: the command turns a transport that refuses into `?`,
        which is the same output `--no-depth` produces — so an exception here would be
        swallowed and the case would pass for the wrong reason.
        """
        touched.append('asked')
        msg = 'not reachable'
        raise ConnectionError(msg)

    monkeypatch.setattr('django_aiogram.broker.registry.broker_class', note)
    monkeypatch.setattr('django_aiogram.runtime.queues.settings_for', note)

    (row,) = listed(no_depth=True)

    assert touched == [], 'the transport was asked although --no-depth was given'
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

    # the whole line: the unreadable-table message starts with the same words, so a substring
    # match would pass for the answer this case exists to tell apart
    assert out.getvalue().strip() == 'no queues are declared', out.getvalue()


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_queue_table_that_cannot_be_read_is_not_a_deployment_with_no_queues(monkeypatch):
    """Two different answers again: *none declared* and *nobody could look*.

    Saying the first for the second sends an operator to their settings instead of to the
    database that refused.
    """
    from django.db import DatabaseError

    def refuse(*_args, **_kwargs):
        """Refuse the way an unmigrated database does."""
        msg = 'no such table'
        raise DatabaseError(msg)

    monkeypatch.setattr('django_aiogram.models.TelegramQueue.objects.values_list', refuse)
    out, err = StringIO(), StringIO()

    call_command('tgbot_queues', stdout=out, stderr=err)

    assert 'the table could not be read' in out.getvalue(), out.getvalue()
    assert 'could not read the queue table' in err.getvalue()


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_filter_that_matches_nothing_is_not_a_deployment_with_no_queues():
    """Three reasons for an empty listing, and one message for all of them misdirects two."""
    TelegramQueue.objects.create(name='client-a', pool='vip')
    out = StringIO()

    call_command('tgbot_queues', queue=['client-z'], stdout=out, stderr=StringIO())

    assert 'no declared queue matches client-z' in out.getvalue(), out.getvalue()


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_filter_with_an_unreadable_table_overstates_neither(monkeypatch):
    """Both halves at once, because either alone says more than is known.

    The settings did not match, and what the table declares is nobody's guess — so a flat
    "no declared queue matches" would be a definite answer about rows nothing could read.
    """
    from django.db import DatabaseError

    def refuse(*_args, **_kwargs):
        """Refuse the way an unmigrated database does."""
        msg = 'no such table'
        raise DatabaseError(msg)

    monkeypatch.setattr('django_aiogram.models.TelegramQueue.objects.values_list', refuse)
    out = StringIO()

    call_command('tgbot_queues', queue=['client-z'], stdout=out, stderr=StringIO())

    said = out.getvalue()
    assert 'nothing in the settings matches client-z' in said, said
    assert 'the queue table could not be read' in said, said
