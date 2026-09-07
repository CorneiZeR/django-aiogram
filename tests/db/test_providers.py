"""Where a supervisor gets its bots from, when they live in the database.

The settings provider is covered without one — `tests/test_bots.py` — so everything here is
about the table: the three levels resolving through a profile, and what a reader is allowed
to say about what it found.
"""

import pytest
from django.test import override_settings

from django_aiogram.models import TelegramBot, TelegramBotProfile
from django_aiogram.runtime import providers

pytestmark = pytest.mark.django_db

FROM_DB = ('django_aiogram.runtime.providers.from_database',)
BOTH = ('django_aiogram.runtime.providers.from_settings', 'django_aiogram.runtime.providers.from_database')


def test_a_row_resolves_through_its_profile_and_its_own_overrides():
    """Three levels, as rows, and each value says which of them decided it."""
    profile = TelegramBotProfile.objects.create(name='vip', overrides={'MAX_IN_FLIGHT': 4, 'MAX_RETRIES': 2})
    TelegramBot.objects.create(bot_id=123456, token='123456:AAaa', profile=profile, overrides={'MAX_RETRIES': 7})

    (found,) = providers.from_database()

    assert found.alias == '123456', 'a row has no alias, so the identity written out is the name'
    assert found.bot_id == 123456
    assert found['TOKEN'] == '123456:AAaa'
    assert found['MAX_IN_FLIGHT'] == 4, 'the profile has to reach the bot'
    assert found['MAX_RETRIES'] == 7, "and the bot's own has to win over it"
    assert found.label('MAX_IN_FLIGHT') == "TelegramBotProfile(vip).overrides['MAX_IN_FLIGHT']"
    assert found.label('MAX_RETRIES') == "TelegramBot(123456).overrides['MAX_RETRIES']"


def test_a_row_without_a_profile_takes_the_shared_defaults():
    """Which is the common shape: one profile is a decision, and most bots need none."""
    TelegramBot.objects.create(bot_id=123456, token='123456:AAaa')

    with override_settings(TELEGRAM_BOT_DEFAULTS={'MAX_RETRIES': 3}):
        (found,) = providers.from_database()

    assert found['MAX_RETRIES'] == 3
    assert found.label('MAX_RETRIES') == "TELEGRAM_BOT_DEFAULTS['MAX_RETRIES']"


def test_a_switched_off_bot_is_not_offered():
    """A person decided not to serve it, which is not the same as a quarantine."""
    TelegramBot.objects.create(bot_id=123456, token='123456:AAaa', enabled=False)

    assert providers.from_database() == ()


@override_settings(TELEGRAM_BOT_DEFAULTS={'BOT_PROVIDERS': BOTH}, TELEGRAM_BOTS={'support': {'TOKEN': '123456:AAaa'}})
def test_the_first_source_to_name_an_identity_keeps_it(caplog):
    """One bot in two sources is one bot, and the order decides which configuration wins.

    Reported rather than silently resolved: two sources disagreeing about a token is something
    a person has to fix, and the quiet answer would be a bot sending with whichever row was
    read second.
    """
    TelegramBot.objects.create(bot_id=123456, token='123456:BBbb', overrides={'MAX_RETRIES': 9})

    with caplog.at_level('WARNING', logger='django_aiogram'):
        found = providers.desired()

    assert len(found) == 1
    assert found[0].alias == 'support', 'the settings are named first, so they keep the identity'
    assert found[0]['TOKEN'] == '123456:AAaa'
    assert any('two sources configure one bot' in record.getMessage() for record in caplog.records)


@override_settings(TELEGRAM_BOT_DEFAULTS={'BOT_PROVIDERS': FROM_DB}, TELEGRAM_BOTS=None)
def test_a_token_with_no_identity_is_left_out_and_said_once(caplog):
    """Nothing could address it, and serving it would queue messages that name no bot."""
    TelegramBot.objects.create(bot_id=123456, token='not-a-token')

    with caplog.at_level('WARNING', logger='django_aiogram'):
        found = providers.desired()

    assert found == ()
    assert any('carries no identity' in record.getMessage() for record in caplog.records)


@override_settings(TELEGRAM_BOT_DEFAULTS={'BOT_PROVIDERS': ('no.such.provider',)})
def test_a_provider_that_cannot_be_imported_is_refused():
    """A source nobody reads is a set of bots nobody serves, and it must not look healthy."""
    with pytest.raises(ImportError):
        providers.desired()
