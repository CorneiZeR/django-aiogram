"""Bringing what is running into line with what is configured, and what a bad read may not do.

Every case here is one sentence from the module's own docstring: a failed read is not a
removal, one bot's failure is its own, and a pass reads the whole desired set rather than a
change somebody sent.
"""

import pytest
from aiogram.exceptions import TelegramConflictError, TelegramUnauthorizedError
from aiogram.methods import GetMe
from django.test import override_settings

from django_aiogram.runtime.lifecycle import Fate
from django_aiogram.runtime.supervisor import Supervisor, serving

TOKEN = '123456:AAone'
OTHER = '654321:BBtwo'


@pytest.fixture(autouse=True)
def _no_row_to_write(monkeypatch):
    """Nothing here has a database, and what the row says is `tests/db/test_lifecycle.py`.

    Silenced rather than left to fail: writing the state is best effort by design, so a case
    about the pass would otherwise assert against a log full of the blocker's refusals.
    """
    monkeypatch.setattr('django_aiogram.runtime.lifecycle._write', lambda *args, **kwargs: None)


class Ticking:
    """A clock a case moves by hand, because a quarantine is measured against one."""

    def __init__(self):
        """Start at zero, which is as good a moment as any."""
        self.now = 0.0

    def __call__(self):
        """Answer with the moment this clock has been moved to."""
        return self.now


def watching(refuse=None):
    """A supervisor recording what it was asked to start and stop.

    `refuse` maps an identity to what starting it raises, which is how a case makes one bot
    unservable without touching the others.
    """
    refuse = refuse or {}
    started, stopped, attempted = [], [], []

    def start(record):
        # recorded before the refusal, and separately from `started`: a case about a bot that
        # is *not tried again* cannot assert on the list only successes reach, which would
        # read the same whether the retry happened or not
        attempted.append(record.bot_id)
        if record.bot_id in refuse:
            raise refuse[record.bot_id]
        started.append(record.bot_id)

    supervisor = Supervisor(start=start, stop=stopped.append, clock=Ticking())
    # the mapping itself, so a case can stop refusing: a conflict is a failure that clears
    # without anybody editing a configuration, which is what tells it apart from the rest
    supervisor.refuse = refuse
    supervisor.attempted = attempted
    return supervisor, started, stopped


def test_a_pass_starts_what_is_configured():
    """The ordinary case, and the one every other case is a deviation from."""
    supervisor, started, _ = watching()

    with override_settings(TELEGRAM_BOTS={'a': {'TOKEN': TOKEN}, 'b': {'TOKEN': OTHER}}):
        supervisor.reconcile()

    assert sorted(started) == [123456, 654321]
    assert sorted(supervisor.running) == [123456, 654321]


def test_a_provider_that_cannot_look_does_not_deregister_anything(monkeypatch, caplog):
    """The rule this module exists for: a database blinking must not take the bots off the air.

    Read the other way round -- an empty answer meaning "nothing is configured" -- twenty bots
    would stop on a connection error and start again on the next pass, which is an outage the
    deployment did not have.
    """
    supervisor, started, stopped = watching()
    with override_settings(TELEGRAM_BOTS={'a': {'TOKEN': TOKEN}}):
        supervisor.reconcile()
    assert started == [123456]

    def refuse():
        msg = 'the database is not answering'
        raise OSError(msg)

    # patched where it is *used*: `supervisor` imports the name, so it holds its own
    # reference and patching the provider module would leave the supervisor unaffected
    monkeypatch.setattr('django_aiogram.runtime.supervisor.desired', refuse)
    with caplog.at_level('ERROR', logger='django_aiogram'):
        supervisor.reconcile()

    assert stopped == [], 'a failed read stopped a bot'
    assert sorted(supervisor.running) == [123456]
    assert any('could not read the configured bots' in record.getMessage() for record in caplog.records)


def test_a_bot_that_is_no_longer_configured_is_stopped():
    """A client who disconnected their bot, which is a row gone rather than an event."""
    supervisor, _, stopped = watching()
    with override_settings(TELEGRAM_BOTS={'a': {'TOKEN': TOKEN}, 'b': {'TOKEN': OTHER}}):
        supervisor.reconcile()

    with override_settings(TELEGRAM_BOTS={'a': {'TOKEN': TOKEN}}):
        supervisor.reconcile()

    assert stopped == [654321]
    assert sorted(supervisor.running) == [123456]


def test_a_bot_whose_settings_moved_is_rebuilt():
    """A rotated token is the same bot with a different credential, and the old one is running.

    Stopped before it is started, so nothing is briefly served twice — which for polling would
    be two processes on one token and a 409 from Telegram.
    """
    order = []
    supervisor = Supervisor(
        start=lambda record: order.append(('start', record['TOKEN'])),
        stop=lambda identity: order.append(('stop', identity)),
        clock=Ticking(),
    )
    with override_settings(TELEGRAM_BOTS={'a': {'TOKEN': TOKEN}}):
        supervisor.reconcile()
    with override_settings(TELEGRAM_BOTS={'a': {'TOKEN': '123456:AArotated'}}):
        supervisor.reconcile()

    assert order == [('start', TOKEN), ('stop', 123456), ('start', '123456:AArotated')]


def test_one_bot_that_cannot_be_served_does_not_take_the_others_with_it(caplog):
    """A supervisor that raised would be an outage caused by one client's bad token."""
    supervisor, started, _ = watching({123456: RuntimeError('this token is refused')})

    with (
        override_settings(TELEGRAM_BOTS={'a': {'TOKEN': TOKEN}, 'b': {'TOKEN': OTHER}}),
        caplog.at_level('ERROR', logger='django_aiogram'),
    ):
        supervisor.reconcile()

    assert started == [654321], 'the healthy bot was not served'
    assert supervisor.quarantined[123456].reason == 'RuntimeError'
    assert 123456 not in supervisor.running


def test_a_quarantined_bot_waits_before_it_is_tried_again_and_the_wait_grows():
    """A token Telegram has refused will not start working, and a retry per pass is a log nobody reads."""
    supervisor, _, _ = watching({123456: RuntimeError('refused')})
    clock = supervisor.clock

    with override_settings(TELEGRAM_BOTS={'a': {'TOKEN': TOKEN}}):
        supervisor.reconcile()
        first = supervisor.quarantined[123456]

        supervisor.reconcile()
        assert supervisor.quarantined[123456].attempts == 1, 'it was tried again inside its own wait'

        clock.now = first.until
        supervisor.reconcile()
        second = supervisor.quarantined[123456]

    assert second.attempts == 2
    assert second.until - clock.now > first.until, 'the second wait is no longer than the first'


def test_a_corrected_bot_is_tried_at_once_rather_than_waiting_out_its_backoff():
    """An operator who fixes a token watches nothing happen for up to five minutes otherwise.

    The quarantine was earned by a configuration that is no longer the one being asked for, so
    the wait it carries is about a problem that may already be gone.
    """
    refused = {123456: RuntimeError('this token is refused')}
    supervisor, started, _ = watching(refused)

    with override_settings(TELEGRAM_BOTS={'a': {'TOKEN': TOKEN}}):
        supervisor.reconcile()
    assert 123456 in supervisor.quarantined

    refused.clear()
    with override_settings(TELEGRAM_BOTS={'a': {'TOKEN': '123456:AAcorrected'}}):
        supervisor.reconcile()

    assert started == [123456], 'the corrected bot waited out a backoff the old token earned'
    assert supervisor.quarantined == {}


def test_a_bot_that_has_failed_for_ever_still_has_a_wait_that_is_a_number():
    """`2 ** attempts` stops being a float long before a long-lived process stops running.

    The overflow would escape into the pass and stop every bot after this one — which is the
    promise this class exists to keep, broken by the arithmetic that keeps the retries rare.
    """
    from django_aiogram.runtime.supervisor import _LONGEST_WAIT, Quarantined

    assert Quarantined(reason='x', until=0.0, attempts=1_025).wait() == _LONGEST_WAIT
    assert Quarantined(reason='x', until=0.0, attempts=10_000).wait() == _LONGEST_WAIT


def test_a_quarantine_is_forgotten_when_the_bot_goes_away():
    """Otherwise a client who disconnects a broken bot and reconnects it waits out an old backoff."""
    supervisor, _, _ = watching({123456: RuntimeError('refused')})
    with override_settings(TELEGRAM_BOTS={'a': {'TOKEN': TOKEN}}):
        supervisor.reconcile()
    assert 123456 in supervisor.quarantined

    with override_settings(TELEGRAM_BOTS={'b': {'TOKEN': OTHER}}):
        supervisor.reconcile()

    assert supervisor.quarantined == {}


def test_a_bot_that_refuses_to_stop_is_dropped_anyway(caplog):
    """The alternative is a bot this process believes it serves and does not."""

    def refuse(identity):
        msg = f'{identity} will not stop'
        raise RuntimeError(msg)

    supervisor = Supervisor(start=lambda record: None, stop=refuse, clock=Ticking())
    with override_settings(TELEGRAM_BOTS={'a': {'TOKEN': TOKEN}}):
        supervisor.reconcile()

    with (
        override_settings(TELEGRAM_BOTS={}),
        caplog.at_level('ERROR', logger='django_aiogram'),
    ):
        supervisor.reconcile()

    assert supervisor.running == {}, 'a bot that refused to stop is still recorded as running'
    assert any('refused to stop' in record.getMessage() for record in caplog.records)


@override_settings(TELEGRAM_BOT_DEFAULTS={'BOT_REFRESH_INTERVAL': 'often'})
def test_an_unreadable_interval_falls_back_rather_than_raising(caplog):
    """A pass that cannot be scheduled is a deployment serving nothing."""
    supervisor, _, _ = watching()

    with caplog.at_level('WARNING', logger='django_aiogram'):
        assert supervisor.interval() == 30.0

    assert any('BOT_REFRESH_INTERVAL' in record.getMessage() for record in caplog.records)


def test_the_process_supervisor_is_reachable_for_a_push():
    """A control message has to find it, and there is one per process."""
    supervisor, _, _ = watching()

    assert serving(supervisor) is supervisor
    assert serving() is supervisor


def test_a_revoked_token_is_not_tried_again_and_the_other_bots_keep_running():
    """The case #115 names: nothing but a new token fixes a 401, so nothing retries it.

    Retried on the ordinary backoff it would be one request per bot every five minutes for
    as long as the container runs, all of them failing for the reason the first one did --
    and the bot next to it must not notice any of that.
    """
    revoked = TelegramUnauthorizedError(method=GetMe(), message='Unauthorized')
    supervisor, started, _ = watching({123456: revoked})

    with override_settings(TELEGRAM_BOTS={'a': {'TOKEN': TOKEN}, 'b': {'TOKEN': OTHER}}):
        supervisor.reconcile()
        assert started == [654321], 'the healthy bot was taken down with the broken one'
        assert supervisor.quarantined[123456].fate is Fate.REVOKED
        assert supervisor.quarantined[123456].until is None, 'a revoked token was given a clock'

        supervisor.clock.now += 10_000
        supervisor.reconcile()

    assert supervisor.attempted == [123456, 654321], 'a revoked token was tried again by a timer'


def test_a_conflict_is_waited_out_rather_than_left_to_a_person():
    """It is what a deploy looks like while the old process is still polling its updates."""
    conflict = TelegramConflictError(method=GetMe(), message='terminated by other getUpdates')
    supervisor, started, _ = watching({123456: conflict})

    with override_settings(TELEGRAM_BOTS={'a': {'TOKEN': TOKEN}}):
        supervisor.reconcile()
        held = supervisor.quarantined[123456]
        assert held.fate is Fate.CONFLICT
        assert held.until == supervisor.clock.now + held.wait()

        supervisor.refuse.clear()
        supervisor.clock.now += held.wait()
        supervisor.reconcile()

    assert started == [123456], 'a conflict that cleared itself was never tried again'


def test_a_rotated_token_is_torn_down_and_rebuilt_under_the_same_identity():
    """Identity is the number in front of the colon, so a rotation is not a new bot.

    The old `Bot` was built from the old token and cannot be reused, so the order is the one
    `reconcile` promises: stopped, then started, and never served twice at once.
    """
    supervisor, started, stopped = watching()

    with override_settings(TELEGRAM_BOTS={'a': {'TOKEN': TOKEN}}):
        supervisor.reconcile()
    with override_settings(TELEGRAM_BOTS={'a': {'TOKEN': '123456:AArotated'}}):
        supervisor.reconcile()

    assert started == [123456, 123456]
    assert stopped == [123456], 'the bot built from the old token was left running'
    assert supervisor.running[123456]['TOKEN'] == '123456:AArotated'
