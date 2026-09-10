"""What the admin asks for, and the pass that answers it.

The split exists because an HTTP request must not talk to Telegram: five hundred selected bots
would be five hundred round trips inside one request, and a timeout half way through would
leave nobody able to say which of them happened. So the page writes the asking and this is
what carries it out.
"""

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

    assert token not in TelegramBot.objects.get(bot_id=123456).intent_result


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
