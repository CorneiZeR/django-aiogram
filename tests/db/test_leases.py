"""Which container polls which bot, and what happens when one of them stops asking.

`getUpdates` is exclusive, so this is the mechanism that keeps two containers off one token:
a row per bot, claimed by compare-and-set, believed for as long as its lease. Every case here
plays two containers by giving each pass its own `WORKER_NAME`, which is what a lease is held
under.
"""

import contextlib
import datetime

import pytest
from django.test import override_settings
from django.utils import timezone

from django_aiogram.models import TelegramBot, TelegramBotLease
from django_aiogram.runtime.leases import claim, release
from django_aiogram.runtime.supervisor import Supervisor

pytestmark = pytest.mark.django_db

FROM_DB = ('django_aiogram.runtime.providers.from_database',)
TOKENS = ('123456:AAaa', '654321:BBbb', '111111:CCcc')


@contextlib.contextmanager
def container(name, **settings):
    """Run a pass as one named container, which is what a lease is held under."""
    with override_settings(TELEGRAM_BOT_DEFAULTS={'WORKER_NAME': name, 'BOT_PROVIDERS': FROM_DB, **settings}):
        yield


def rows(*tokens):
    """Configure the bots these tokens name, in the table a provider reads."""
    for token in tokens:
        TelegramBot.objects.create(bot_id=int(token.split(':')[0]), token=token)


def watching():
    """A supervisor that serves bots exclusively and records what it started and stopped."""
    started, stopped = [], []
    supervisor = Supervisor(start=lambda record: started.append(record.bot_id), stop=stopped.append, exclusive=True)
    return supervisor, started, stopped


def test_two_containers_over_one_bot_leave_exactly_one_polling():
    """The case #116 names: the loser serves nothing rather than both getting a 409."""
    rows(TOKENS[0])
    first, started_first, _ = watching()
    second, started_second, _ = watching()

    with container('one'):
        first.reconcile()
    with container('two'):
        second.reconcile()

    assert started_first == [123456]
    assert started_second == [], 'both containers polled one token'
    assert second.running == {}
    assert TelegramBotLease.objects.get(bot_id=123456).holder == 'one'


def test_two_containers_over_several_bots_split_them():
    """Nothing coordinates them: each takes what is free, and a claim is a row nobody shares."""
    rows(*TOKENS)
    first, started_first, _ = watching()
    second, started_second, _ = watching()

    with container('one', MAX_BOTS_PER_WORKER=2):
        first.reconcile()
    with container('two', MAX_BOTS_PER_WORKER=2):
        second.reconcile()

    assert len(started_first) == 2, 'the ceiling was not applied'
    assert len(started_second) == 1
    assert set(started_first) | set(started_second) == {123456, 654321, 111111}
    assert not set(started_first) & set(started_second), 'one bot was served twice'


def test_a_container_that_stopped_renewing_loses_its_bots_to_the_other():
    """Which is failover, and it is the same mechanism as the split above.

    The lease is aged by hand rather than waited out: what the case is about is that an
    expired lease is takeable, and a sleep of `BOT_LEASE_SECONDS` would say nothing more.
    """
    rows(TOKENS[0])
    first, _, _ = watching()
    second, started_second, _ = watching()

    with container('one'):
        first.reconcile()
    TelegramBotLease.objects.filter(bot_id=123456).update(expires_at=timezone.now() - datetime.timedelta(seconds=1))
    with container('two'):
        second.reconcile()

    assert started_second == [123456]
    assert TelegramBotLease.objects.get(bot_id=123456).holder == 'two'


def test_a_bot_whose_lease_was_taken_is_stopped_by_the_process_that_lost_it():
    """The half a renewal alone would miss: it is serving a bot somebody else now polls."""
    rows(TOKENS[0])
    first, _, stopped_first = watching()

    with container('one'):
        first.reconcile()
    TelegramBotLease.objects.filter(bot_id=123456).update(
        holder='two',
        expires_at=timezone.now() + datetime.timedelta(seconds=90),
    )
    with container('one'):
        first.reconcile()

    assert stopped_first == [123456], 'a bot another container holds was left polling here'
    assert first.running == {}


def test_a_bot_already_held_keeps_its_lease_when_the_set_grows_past_the_ceiling():
    """Otherwise a new client's bot arriving would take an existing one off the air.

    The arriving bot has the *lower* identity, and that is the case rather than a detail: the
    provider reads the table in identity order, so a pass that simply claimed what it was
    given would reach the new one first, spend the only lease on it and stop the bot it was
    already serving.
    """
    rows(TOKENS[0])
    first, started, stopped = watching()

    with container('one', MAX_BOTS_PER_WORKER=1):
        first.reconcile()
        rows(TOKENS[2])
        first.reconcile()

    assert started == [123456]
    assert stopped == [], 'the bot it was already serving was dropped for a newly arrived one'


def test_a_clean_shutdown_hands_the_bots_over_at_once():
    """A lease lapses on its own, and a client's bot answering nothing for 90s is the cost."""
    rows(TOKENS[0])
    first, _, stopped = watching()
    second, started_second, _ = watching()

    with container('one'):
        first.reconcile()
        first.released()

    assert stopped == [123456]
    assert not TelegramBotLease.objects.exists()

    with container('two'):
        second.reconcile()
    assert started_second == [123456], 'the released bot waited out a lease anyway'


def test_a_release_leaves_a_lease_somebody_else_took_alone():
    """Deleting it would put two pollers on one token, which is what the lease prevents."""
    rows(TOKENS[0])
    first, _, _ = watching()

    with container('one'):
        first.reconcile()
    TelegramBotLease.objects.filter(bot_id=123456).update(holder='two')

    with container('one'):
        released = release([123456])

    assert released == 0
    assert TelegramBotLease.objects.get(bot_id=123456).holder == 'two'


def test_leases_that_cannot_be_read_keep_the_bots_already_held(monkeypatch, caplog):
    """The rule the supervisor already applies to a provider that could not look."""
    rows(TOKENS[0], TOKENS[1])
    first, started, stopped = watching()
    asked = []

    with container('one'):
        first.reconcile()
        assert sorted(started) == [123456, 654321]

        def refuse(wanted):
            # recorded, because a patch that did not take hold would leave the claim working
            # and nothing stopped either -- which is the assertion below, passing for the
            # wrong reason
            asked.append(sorted(wanted))
            msg = 'the database is not reachable'
            raise RuntimeError(msg)

        monkeypatch.setattr('django_aiogram.runtime.leases.claim', refuse)
        with caplog.at_level('ERROR', logger='django_aiogram'):
            first.reconcile()

    assert asked == [[123456, 654321]], 'the claim under test was never reached'
    assert any('could not read the bot leases' in record.getMessage() for record in caplog.records)
    assert stopped == [], 'a database that blinked took the bots off the air'
    assert sorted(first.running) == [123456, 654321]


def test_a_claim_names_the_process_that_asked():
    """A stale lease has to name something an operator can go and look at."""
    with container('one'):
        assert claim([123456]) == (123456,)

    row = TelegramBotLease.objects.get(bot_id=123456)
    assert row.holder == 'one'
    assert row.expires_at > timezone.now()
