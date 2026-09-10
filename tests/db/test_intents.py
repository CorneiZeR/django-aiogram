"""What the admin asks for, and the pass that answers it.

The split exists because an HTTP request must not talk to Telegram: five hundred selected bots
would be five hundred round trips inside one request, and a timeout half way through would
leave nobody able to say which of them happened. So the page writes the asking and this is
what carries it out.
"""

from datetime import timedelta

import pytest
from django.core.management import call_command
from django.test import override_settings
from django.utils import timezone

from django_aiogram.config.enums import BotIntent
from django_aiogram.models import TelegramBot
from django_aiogram.runtime import intents, providers

pytestmark = pytest.mark.django_db

SETTINGS = {
    'BROKER': 'django_aiogram.testing.InMemoryBroker',
    'FSM_STORAGE': 'memory',
    'BOT_PROVIDERS': ('django_aiogram.runtime.providers.from_database',),
}
TOKEN = '123456:AAaa'


@pytest.fixture(autouse=True)
def _no_read_kept():
    """The providers cache by watermark, and these cases rewrite rows under it."""
    providers.forget()
    yield
    providers.forget()


def asked(intent, **kwargs):
    """One bot waiting on an intent, the way an admin action leaves it."""
    fields = {'bot_id': 123456, 'token': TOKEN, 'intent': intent, 'intent_asked_at': timezone.now()}
    fields.update(kwargs)
    return TelegramBot.objects.create(**fields)


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_an_answer_is_written_back_to_the_row(monkeypatch):
    """Which is the whole contract: the page asked, and this is where a person reads the answer."""
    row = asked(BotIntent.CHECK.value)
    monkeypatch.setattr(intents, '_performed', lambda intent, record: 'ok: @a_bot (123456)')

    assert intents.carry_out(providers.desired()) == 1

    row.refresh_from_db()
    assert row.intent == '', 'the asking was left outstanding'
    assert row.intent_result == 'ok: @a_bot (123456)'
    assert row.intent_done_at is not None


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_an_intent_that_moved_between_the_read_and_the_claim_is_left_to_whoever_asked(monkeypatch):
    """Two containers read this table, and each carrying-out is a call to Telegram.

    Claimed with a compare-and-set on the value that was read — the claim the leases and the
    replay rows use, and the only one atomic on every database this package supports. Asserted
    through a *changed* row rather than through two passes: a second pass finds the column
    already empty whether the claim compares or not, so it passes either way.
    """
    row = asked(BotIntent.CHECK.value)
    calls = []
    monkeypatch.setattr(intents, '_performed', lambda intent, record: calls.append(intent) or 'ok')
    (record,) = providers.desired()

    # what a second container's claim, or an operator asking for something else, leaves behind
    TelegramBot.objects.filter(bot_id=123456).update(intent=BotIntent.SET_WEBHOOK.value)

    assert intents._one(123456, BotIntent.CHECK.value, record) is False
    assert calls == [], 'the intent was carried out against a row that had moved'
    row.refresh_from_db()
    assert row.intent == BotIntent.SET_WEBHOOK.value, 'the newer asking was swallowed'


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_one_bots_failure_is_its_own(monkeypatch):
    """A token Telegram refuses is one line in one row, and the pass carries on."""
    asked(BotIntent.CHECK.value)
    asked(BotIntent.CHECK.value, bot_id=654321, token='654321:BBbb')

    def refuse(intent, record):
        """Fail for the first bot only, the way one revoked token does."""
        if record.bot_id == 123456:
            msg = 'Unauthorized'
            raise RuntimeError(msg)
        return 'ok'

    monkeypatch.setattr(intents, '_performed', refuse)

    assert intents.carry_out(providers.desired()) == 2

    first, second = (TelegramBot.objects.get(bot_id=identity) for identity in (123456, 654321))
    assert 'RuntimeError' in first.intent_result
    assert second.intent_result == 'ok', 'the healthy bot was not answered'


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_what_is_written_back_carries_no_token(monkeypatch):
    """aiogram puts the API URL into its messages, and the URL carries the credential."""
    token = '123456:AAFakeTokenThatLooksExactlyLikeARealOne'
    asked(BotIntent.CHECK.value, token=token)

    def refuse(intent, record):
        """Fail the way aiogram does, with the request in the message."""
        msg = f'POST https://api.telegram.org/bot{token}/getMe: Unauthorized'
        raise RuntimeError(msg)

    monkeypatch.setattr(intents, '_performed', refuse)

    intents.carry_out(providers.desired())

    said = TelegramBot.objects.get(bot_id=123456).intent_result
    # the marker first: an empty column has no token in it either, and this case would pass
    # for a failure that never reached the redaction at all
    assert said.startswith('RuntimeError:'), said
    assert token not in said


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_bot_switched_off_can_still_be_asked_to_drop_its_webhook(monkeypatch):
    """Which is exactly what a bot somebody just switched off is waiting for.

    `desired` leaves the switched-off ones out — they are not served — so a command that read
    only that would leave Telegram posting updates at a URL answering 404 for ever.
    """
    asked(BotIntent.DELETE_WEBHOOK.value, enabled=False)
    monkeypatch.setattr(intents, '_performed', lambda intent, record: 'webhook deleted')

    call_command('tgbot_intents')

    assert TelegramBot.objects.get(bot_id=123456).intent_result == 'webhook deleted'


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_an_intent_this_version_does_not_know_says_so_rather_than_raising():
    """A row written by a newer admin against an older container is a rolling deploy."""
    asked('teleport')

    assert intents.carry_out(providers.desired()) == 1

    assert 'unknown intent' in TelegramBot.objects.get(bot_id=123456).intent_result


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_an_intent_whose_worker_died_is_carried_out_by_the_next_pass(monkeypatch):
    """The asking stays in the row while a claim is held, so a killed worker loses the work.

    Clearing the column at claim time was simpler and lost the request: the only record of
    what somebody asked for went with the process that died holding it.
    """
    row = asked(BotIntent.CHECK.value)

    def die(intent, record):
        """Take the claim and never answer, the way a container killed mid-pass does."""
        msg = 'killed'
        raise KeyboardInterrupt(msg)

    monkeypatch.setattr(intents, '_performed', die)
    with pytest.raises(KeyboardInterrupt):
        intents.carry_out(providers.desired())

    row.refresh_from_db()
    assert row.intent == BotIntent.CHECK.value, 'the asking was thrown away with the worker'

    # the claim lapses, and the next pass takes it
    row.intent_claimed_at = timezone.now() - timedelta(seconds=intents.CLAIM_SECONDS + 1)
    row.save(update_fields=['intent_claimed_at'])
    providers.forget()
    monkeypatch.setattr(intents, '_performed', lambda intent, record: 'ok')

    assert intents.carry_out(providers.desired()) == 1

    row.refresh_from_db()
    assert row.intent_result == 'ok'
    assert row.intent == ''


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_live_claim_keeps_another_pass_off_the_same_intent(monkeypatch):
    """Two containers read this table, and each carrying-out is a call to Telegram."""
    asked(BotIntent.CHECK.value)
    calls = []
    monkeypatch.setattr(intents, '_performed', lambda intent, record: calls.append(intent) or 'ok')
    (record,) = providers.desired()

    TelegramBot.objects.filter(bot_id=123456).update(intent_claim='someone-else', intent_claimed_at=timezone.now())

    assert intents.carry_out([record]) == 0
    assert calls == [], 'a bot another container is holding was called anyway'


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_an_answer_from_a_lapsed_claim_does_not_replace_a_newer_one(monkeypatch):
    """A worker slow enough to finish after its lease must not answer a later question.

    Two `check` requests for one bot look identical in the row, so the write has to be
    conditional on the claim rather than on the identity alone.
    """
    row = asked(BotIntent.CHECK.value)
    (record,) = providers.desired()

    def overtaken(intent, held):
        """Answer slowly: while this runs, the intent is re-asked and answered by another."""
        TelegramBot.objects.filter(bot_id=123456).update(
            intent='',
            intent_claim='',
            intent_claimed_at=None,
            intent_result='ok: the newer answer',
            intent_done_at=timezone.now(),
        )
        return 'ok: the older answer'

    monkeypatch.setattr(intents, '_performed', overtaken)

    assert intents.carry_out([record]) == 0, 'a lapsed claim reported success'

    row.refresh_from_db()
    assert row.intent_result == 'ok: the newer answer', 'the older worker overwrote a newer answer'
