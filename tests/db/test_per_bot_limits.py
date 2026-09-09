"""Each bot paced by its own numbers, and a change in a row taking effect without a restart.

Telegram meters the token, so per-bot budgets are the correct semantics rather than a
refinement: a noisy client throttled below the shared default, a client with paid
broadcasting above it. What this file is about is that the numbers come from that bot's own
resolved settings, and that a row moving is noticed.
"""

import pytest
from django.test import override_settings

from django_aiogram.models import TelegramBot
from django_aiogram.producer.throttling import get_rate_limiter, reset_rate_limiters

pytestmark = pytest.mark.django_db

FROM_DB = ('django_aiogram.runtime.providers.from_database',)
SETTINGS = {
    'BROKER': 'django_aiogram.testing.InMemoryBroker',
    'FSM_STORAGE': 'memory',
    'BOT_PROVIDERS': FROM_DB,
    'RATE_LIMIT': {'overall_per_second': 30},
}


@pytest.fixture(autouse=True)
def _no_limiters_left_behind():
    """The registry outlives a case, and a limiter left behind decides the next one's answer."""
    reset_rate_limiters()
    yield
    reset_rate_limiters()


def bot_for(bot_id):
    """The bot object one row describes."""
    from django_aiogram.runtime.providers import desired
    from django_aiogram.runtime.registry import bots

    found = next(record for record in desired() if record.bot_id == bot_id)
    return bots.for_record(found)


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_two_bots_with_different_budgets_pace_independently():
    """The claim #120 names: a client's own row decides their rate, not the shared default."""
    TelegramBot.objects.create(bot_id=123456, token='123456:AAaa', overrides={'RATE_LIMIT': {'overall_per_second': 1}})
    TelegramBot.objects.create(bot_id=654321, token='654321:BBbb', overrides={'RATE_LIMIT': {'overall_per_second': 5}})

    slow = bot_for(123456).rate_limiter
    quick = bot_for(654321).rate_limiter

    assert slow is not quick, 'two bots shared one budget'
    assert slow._overall.rate == 1, slow._overall.rate
    assert quick._overall.rate == 5, quick._overall.rate


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_bot_without_its_own_numbers_takes_the_shared_ones():
    """Which is most bots: a budget is a decision, and most clients need no decision."""
    TelegramBot.objects.create(bot_id=123456, token='123456:AAaa')

    assert bot_for(123456).rate_limiter._overall.rate == 30


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_budget_changed_in_a_row_takes_effect_without_a_restart():
    """The rates live in rows since 5.0, and a row moves without `setting_changed` firing.

    So a registry that only cleared on that signal would pace a client against the numbers
    their previous plan had — for as long as the process ran.
    """
    row = TelegramBot.objects.create(
        bot_id=123456,
        token='123456:AAaa',
        overrides={'RATE_LIMIT': {'overall_per_second': 1}},
    )
    assert bot_for(123456).rate_limiter._overall.rate == 1

    row.overrides = {'RATE_LIMIT': {'overall_per_second': 9}}
    row.save()

    assert bot_for(123456).rate_limiter._overall.rate == 9, 'the old plan was still pacing them'


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_the_same_token_asked_twice_is_one_limiter():
    """Telegram meters the token, so two objects holding one must not have a budget each."""
    TelegramBot.objects.create(bot_id=123456, token='123456:AAaa')
    found = bot_for(123456).settings

    assert get_rate_limiter('123456:AAaa', found) is get_rate_limiter('123456:AAaa', found)


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_bot_whose_limits_are_switched_off_keeps_no_limiter():
    """`RATE_LIMIT: {}` is a legitimate "do not pace this bot", and it has to be *now*.

    A limiter left in the registry would go on pacing a bot nothing asked to pace, which is
    the opposite of what the row now says.
    """
    row = TelegramBot.objects.create(
        bot_id=123456,
        token='123456:AAaa',
        overrides={'RATE_LIMIT': {'overall_per_second': 1}},
    )
    assert bot_for(123456).rate_limiter is not None

    row.overrides = {'RATE_LIMIT': {}}
    row.save()

    assert bot_for(123456).rate_limiter is None, 'a bot asked not to be paced was still paced'
    # and the entry is gone rather than left unread: a registry that kept one per bot whose
    # limits were ever switched off would grow by one per client for the life of the process
    from django_aiogram.producer.throttling import _registry

    assert '123456:AAaa' not in _registry._limiters, 'the limiter was kept, unread'
