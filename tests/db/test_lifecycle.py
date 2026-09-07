"""What a quarantine leaves behind for a person, and what a project is told about it.

The supervisor's own cases run without a database — `tests/test_supervisor.py` — because a
supervisor has to. These are the other half: the row an admin page reads, and the signal a
project connects to reach the client whose bot stopped working.
"""

import uuid

import pytest
from aiogram.exceptions import TelegramUnauthorizedError
from aiogram.methods import GetMe
from django.db.models import QuerySet
from django.test import override_settings
from django.utils import timezone

from django_aiogram.models import TelegramBot, TelegramEvent, TelegramScheduledSend
from django_aiogram.runtime import lifecycle
from django_aiogram.runtime.lifecycle import Fate
from django_aiogram.runtime.supervisor import Supervisor

pytestmark = pytest.mark.django_db

FROM_DB = ('django_aiogram.runtime.providers.from_database',)


def listening(signal):
    """Collect what one lifecycle signal was sent, in the order it arrived."""
    heard = []

    def receiver(**kwargs):
        heard.append(kwargs)

    signal.connect(receiver, weak=False)
    return heard, lambda: signal.disconnect(receiver)


def test_a_quarantine_is_written_where_a_person_can_read_it():
    """The admin is where somebody finds out why a client's bot stopped answering."""
    TelegramBot.objects.create(bot_id=123456, token='123456:AAaa')
    until = timezone.now()

    lifecycle.remember(123456, Fate.CONFLICT, 'TelegramConflictError', until)

    row = TelegramBot.objects.get(bot_id=123456)
    assert row.quarantine_reason == 'conflict: TelegramConflictError'
    assert row.quarantined_until == until


def test_a_revoked_token_is_written_with_no_moment_to_try_again():
    """`None` is the difference an operator has to be able to see: this one needs them."""
    TelegramBot.objects.create(bot_id=123456, token='123456:AAaa')

    lifecycle.remember(123456, Fate.REVOKED, 'TelegramUnauthorizedError', None)

    row = TelegramBot.objects.get(bot_id=123456)
    assert row.quarantine_reason == 'revoked: TelegramUnauthorizedError'
    assert row.quarantined_until is None


def test_writing_a_state_is_not_a_configuration_change():
    """`updated_at` is the watermark a supervisor reconciles from, and this did not move it.

    A state write that bumped it would make every quarantine look like an edited row: the
    table would be re-read on the next pass in every container, and the reason it was re-read
    would be the container's own writing.
    """
    row = TelegramBot.objects.create(bot_id=123456, token='123456:AAaa')

    lifecycle.remember(123456, Fate.TRANSIENT, 'TelegramNetworkError', timezone.now())

    assert TelegramBot.objects.get(bot_id=123456).updated_at == row.updated_at


def test_a_bot_configured_in_settings_leaves_no_row_behind():
    """It has none, and a row written here would be a bot the table claims to configure."""
    lifecycle.remember(123456, Fate.REVOKED, 'TelegramUnauthorizedError', None)

    assert not TelegramBot.objects.exists()


def test_a_project_is_told_which_bot_stopped_and_why():
    """A revoked token is the client's problem to fix, and nothing in a container can fix it.

    So the signal carries what an outbound message to that client needs: which bot, what
    happened, and whether anything is going to retry it.
    """
    heard, disconnect = listening(lifecycle.bot_quarantined)
    try:
        lifecycle.remember(123456, Fate.REVOKED, 'TelegramUnauthorizedError', None)
    finally:
        disconnect()

    (said,) = heard
    assert said['bot_id'] == 123456
    assert said['fate'] is Fate.REVOKED
    assert said['reason'] == 'TelegramUnauthorizedError'
    assert said['until'] is None


def test_a_bot_that_starts_working_again_is_cleared_and_said_so():
    """Through a pass rather than by hand: the clearing is the supervisor's, not a caller's."""
    TelegramBot.objects.create(bot_id=123456, token='123456:AAaa')
    refused = {123456: TelegramUnauthorizedError(method=GetMe(), message='Unauthorized')}

    def start(record):
        if record.bot_id in refused:
            raise refused[record.bot_id]

    supervisor = Supervisor(start=start, stop=lambda identity: None)
    with override_settings(TELEGRAM_BOT_DEFAULTS={'BOT_PROVIDERS': FROM_DB}):
        supervisor.reconcile()
        assert TelegramBot.objects.get(bot_id=123456).quarantine_reason.startswith('revoked')

        refused.clear()
        # saved rather than `QuerySet.update`d, and that is the API a project has to use:
        # `update` moves no `auto_now` column and sends no signal, so a supervisor would
        # neither be pushed at nor see its watermark move
        rotated = TelegramBot.objects.get(bot_id=123456)
        rotated.token = '123456:AArotated'
        rotated.save()
        heard, disconnect = listening(lifecycle.bot_recovered)
        try:
            supervisor.reconcile()
        finally:
            disconnect()

    row = TelegramBot.objects.get(bot_id=123456)
    assert row.quarantine_reason == '', 'a bot being served again still reads as quarantined'
    assert row.quarantined_until is None
    assert heard == [{'signal': lifecycle.bot_recovered, 'sender': None, 'bot_id': 123456}]


def test_a_rotated_token_keeps_the_bot_history_and_its_pending_sends():
    """Identity is the number in front of the colon, so a rotation is not a new bot.

    The rows that name a bot name it by that number, and a rotation touches one column of one
    row — which is the whole reason the identity is not the token.
    """
    row = TelegramBot.objects.create(bot_id=123456, token='123456:AAaa')
    TelegramEvent.objects.create(kind='outbound.sent', bot_id=123456, correlation_id=uuid.uuid4())
    TelegramScheduledSend.objects.create(
        bot_id=123456,
        correlation_id=uuid.uuid4(),
        function='send_message',
        payload=b'{}',
        due_at=timezone.now(),
    )

    row.token = '123456:AArotated'
    row.save()

    assert TelegramEvent.objects.filter(bot_id=123456).count() == 1
    assert TelegramScheduledSend.objects.filter(bot_id=123456).count() == 1


def test_a_state_a_database_refused_is_written_by_the_next_pass(monkeypatch):
    """The pass that follows does not write it again, so something has to.

    Both ends of this are steady states: a bot that started working is running and unchanged,
    and a revoked token is held with no clock, so no later pass touches either. A write lost
    there would leave the admin saying the opposite of the truth for as long as the container
    runs -- a client's bot reading as quarantined while it answers.
    """
    TelegramBot.objects.create(bot_id=123456, token='123456:AAaa')
    refused = {123456: TelegramUnauthorizedError(method=GetMe(), message='Unauthorized')}

    def start(record):
        if record.bot_id in refused:
            raise refused[record.bot_id]

    supervisor = Supervisor(start=start, stop=lambda identity: None)
    with override_settings(TELEGRAM_BOT_DEFAULTS={'BOT_PROVIDERS': FROM_DB}):
        supervisor.reconcile()
        assert TelegramBot.objects.get(bot_id=123456).quarantine_reason.startswith('revoked')

        refused.clear()
        rotated = TelegramBot.objects.get(bot_id=123456)
        rotated.token = '123456:AArotated'
        rotated.save()

        # `update` alone, and that precision is the case: breaking the manager would break
        # the provider's read too, and the quarantine would then be kept by the rule about a
        # failed read rather than by anything this is about
        monkeypatch.setattr(QuerySet, 'update', _refusing)
        supervisor.reconcile()
        monkeypatch.undo()
        assert supervisor.running == {123456: supervisor.running[123456]}, 'the case needs the bot started'
        assert TelegramBot.objects.get(bot_id=123456).quarantine_reason.startswith('revoked'), (
            'the case needs the write to have failed'
        )

        supervisor.reconcile()

    assert TelegramBot.objects.get(bot_id=123456).quarantine_reason == '', 'the refused write was never retried'


def _refusing(*args, **kwargs):
    """Stand in for a write whose database is not there."""
    msg = 'the database is not reachable'
    raise RuntimeError(msg)


def test_a_fresh_process_tries_a_revoked_bot_once_rather_than_never():
    """A quarantine is a process's, and a restart deliberately does not inherit one.

    The row cannot say whether the token in it is still the one that was refused, so a
    hydrated quarantine would outlive a token corrected while the container was down -- until
    somebody edited the row a second time to move its watermark. One refused request per bot
    per process start is the cheaper end of that trade, and *once* is the part that matters.
    """
    TelegramBot.objects.create(bot_id=123456, token='123456:AAaa')
    tried = []

    def start(record):
        tried.append(record.bot_id)
        raise TelegramUnauthorizedError(method=GetMe(), message='Unauthorized')

    with override_settings(TELEGRAM_BOT_DEFAULTS={'BOT_PROVIDERS': FROM_DB}):
        first = Supervisor(start=start, stop=lambda identity: None)
        first.reconcile()
        first.reconcile()
        assert tried == [123456], 'a revoked token was tried again inside one process'

        second = Supervisor(start=start, stop=lambda identity: None)
        second.reconcile()
        second.reconcile()

    assert tried == [123456, 123456], 'a restart either inherited the quarantine or retried on a timer'


def test_a_state_write_happens_under_the_lock_that_chose_it(monkeypatch):
    """Otherwise a retry can land after a newer state and leave the row saying the older one.

    Asserted on the lock rather than with threads, and for the reason a threaded case would
    be worse: the interleaving that loses the newer write needs the retry to be chosen, then
    descheduled, then run, which a test cannot ask for. What it can pin is the property that
    makes the interleaving impossible -- the row is written while the lock that read what was
    held is still held.
    """
    TelegramBot.objects.create(bot_id=123456, token='123456:AAaa')
    under = []

    original = QuerySet.update

    def watching(self, **fields):
        under.append(lifecycle._lock.locked())
        return original(self, **fields)

    monkeypatch.setattr(QuerySet, 'update', watching)
    lifecycle.remember(123456, Fate.CONFLICT, 'TelegramConflictError', None)

    assert under == [True], 'the write ran with the lock released, so a stale retry could follow it'
