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
    """Nothing could address it, and serving it would queue messages that name no bot.

    Counted rather than looked for: `any` passes on a log that repeats the same bot for every
    provider in the chain, which is the shape a reader of this name would not expect.
    """
    TelegramBot.objects.create(bot_id=123456, token='not-a-token')

    with caplog.at_level('WARNING', logger='django_aiogram'):
        found = providers.desired()

    assert found == ()
    said = [record for record in caplog.records if 'carries no identity' in record.getMessage()]
    assert len(said) == 1, f'said {len(said)} times, and the promise is once per read'


@override_settings(TELEGRAM_BOT_DEFAULTS={'BOT_PROVIDERS': ('no.such.provider',)})
def test_a_provider_that_cannot_be_imported_is_refused():
    """A source nobody reads is a set of bots nobody serves, and it must not look healthy."""
    with pytest.raises(ImportError):
        providers.desired()


@override_settings(TELEGRAM_BOT_DEFAULTS={'BOT_PROVIDERS': FROM_DB})
def test_an_unchanged_table_is_answered_without_reading_the_rows(django_assert_num_queries):
    """Every container runs a pass every few seconds, and most passes have nothing to do.

    So the rows are read once and the watermark -- one aggregate per table -- is what the next
    pass pays for. Without it a hundred bots are resolved through their profiles every few
    seconds in every container, which is three layers of arithmetic per row for an answer that
    has not moved.
    """
    profile = TelegramBotProfile.objects.create(name='vip', overrides={'MAX_RETRIES': 2})
    TelegramBot.objects.create(bot_id=123456, token='123456:AAaa', profile=profile)

    with django_assert_num_queries(3):  # two aggregates and the rows
        first = providers.desired()
    with django_assert_num_queries(2):  # the aggregates alone
        again = providers.desired()

    assert first == again


@override_settings(TELEGRAM_BOT_DEFAULTS={'BOT_PROVIDERS': FROM_DB})
def test_a_changed_row_is_seen_on_the_next_pass():
    """The other half, and the one that matters more: a cache is only allowed if it is right.

    A token edited in the admin has to reach the container, so the watermark has to move when
    a row does -- and `updated_at` is what moves it.
    """
    row = TelegramBot.objects.create(bot_id=123456, token='123456:AAaa')
    assert providers.desired()[0]['TOKEN'] == '123456:AAaa'

    row.token = '123456:AArotated'
    row.save()

    assert providers.desired()[0]['TOKEN'] == '123456:AArotated', 'the watermark did not move with the row'


@override_settings(TELEGRAM_BOT_DEFAULTS={'BOT_PROVIDERS': FROM_DB})
def test_a_deleted_row_is_seen_on_the_next_pass():
    """A delete moves no timestamp, so the count is in the watermark as well as the maximum.

    The older of the two is the one deleted here, and that is the whole case: deleting the
    *newer* one lowers `max(updated_at)` and would be noticed by a watermark that never
    counted anything. Measured -- with the count dropped from the watermark this case passes
    and this one fails.
    """
    older = TelegramBot.objects.create(bot_id=123456, token='123456:AAaa')
    TelegramBot.objects.create(bot_id=654321, token='654321:BBbb')
    assert len(providers.desired()) == 2

    older.delete()

    assert [found.bot_id for found in providers.desired()] == [654321]


@override_settings(TELEGRAM_BOT_DEFAULTS={'BOT_PROVIDERS': FROM_DB})
def test_an_empty_read_after_a_full_one_is_held_back_once(caplog):
    """Twenty bots do not usually vanish at once; a query reaching the wrong database does.

    So the first empty answer keeps what the provider last said -- the same rule as a provider
    that raised, for an answer that reads the same way -- and the second is honoured, so a
    deployment that really removed its last bot converges one pass later.
    """
    TelegramBot.objects.create(bot_id=123456, token='123456:AAaa')
    assert len(providers.desired()) == 1

    TelegramBot.objects.all().delete()

    with caplog.at_level('WARNING', logger='django_aiogram'):
        held = providers.desired()
    assert [found.bot_id for found in held] == [123456], 'one empty read deregistered the bot'
    assert any('read no bots at all' in record.getMessage() for record in caplog.records)

    assert providers.desired() == (), 'the second empty read was held back too'
