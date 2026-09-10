"""What a row holds once the storage seam is in front of it, and the rewrapping walk.

The seam itself is `tests/test_token_storage.py`; this is about the rows: that a provider
reads a wrapped column, that one bad row costs one bot, and that a rotation can be walked
through without anything being down.
"""

from io import StringIO

import pytest
from django.core.management import call_command
from django.test import override_settings

from django_aiogram.models import TelegramBot
from django_aiogram.runtime import providers
from django_aiogram.tokens import store_token

pytestmark = pytest.mark.django_db

Fernet = pytest.importorskip('cryptography.fernet').Fernet

TOKEN = '123456:AAaa'
OTHER = '654321:BBbb'
FIRST = Fernet.generate_key().decode()
SECOND = Fernet.generate_key().decode()
CRYPTO = 'django_aiogram.crypto.FernetTokenStorage'


def encrypting(*keys):
    """The settings of a deployment whose tokens are encrypted under these keys, newest first."""
    return override_settings(
        TELEGRAM_BOT_DEFAULTS={
            'BROKER': 'django_aiogram.testing.InMemoryBroker',
            'FSM_STORAGE': 'memory',
            'BOT_PROVIDERS': ('django_aiogram.runtime.providers.from_database',),
            'TOKEN_STORAGE': CRYPTO,
            'TOKEN_ENCRYPTION_KEYS': keys,
        }
    )


@pytest.fixture(autouse=True)
def _no_read_kept():
    """The providers cache their read by watermark, and a case rewriting rows outruns it."""
    providers.forget()
    yield
    providers.forget()


def test_a_provider_reads_the_token_through_the_storage():
    """The row holds a ciphertext and the record holds a token; nothing else changes."""
    with encrypting(FIRST):
        row = TelegramBot.objects.create(bot_id=123456, token=store_token(TOKEN))
        row.refresh_from_db()
        assert TOKEN not in row.token, 'the column held the token in plain'

        (found,) = providers.from_database()

        assert found['TOKEN'] == TOKEN
        assert found.bot_id == 123456


def test_one_unreadable_row_costs_one_bot():
    """A key dropped before its rows were rewrapped must not take the healthy bots off the air.

    Which is the read-side twin of the supervisor's rule: a read that could not see everything
    has not said there is nothing to serve.
    """
    with encrypting(FIRST):
        TelegramBot.objects.create(bot_id=123456, token=store_token(TOKEN))
    with encrypting(SECOND):
        TelegramBot.objects.create(bot_id=654321, token=store_token(OTHER))
        providers.forget()

        found = providers.from_database()

    assert [record.bot_id for record in found] == [654321], 'the unreadable row was not the only one lost'


def test_rewrapping_moves_a_plain_table_to_ciphertext():
    """Turning the storage on: the settings change, then one walk, and no restart between."""
    TelegramBot.objects.create(bot_id=123456, token=TOKEN)
    with encrypting(FIRST):
        call_command('tgbot_rewrap_tokens')

        row = TelegramBot.objects.get(bot_id=123456)
        assert TOKEN not in row.token
        providers.forget()
        (found,) = providers.from_database()
        assert found['TOKEN'] == TOKEN


def test_a_rotation_is_readable_at_every_step():
    """Add the new key in front, run this, drop the old one — with the bots up throughout."""
    with encrypting(FIRST):
        TelegramBot.objects.create(bot_id=123456, token=store_token(TOKEN))
    with encrypting(SECOND, FIRST):
        providers.forget()
        assert providers.from_database()[0]['TOKEN'] == TOKEN, 'the rows stopped reading before the rewrap'

        call_command('tgbot_rewrap_tokens')

    with encrypting(SECOND):
        providers.forget()
        assert providers.from_database()[0]['TOKEN'] == TOKEN, 'dropping the old key lost the rows'


def test_a_dry_run_writes_nothing_and_an_unchanged_row_is_left_alone():
    """`updated_at` is the watermark every supervisor polls, so a walk must not bump the table."""
    TelegramBot.objects.create(bot_id=123456, token=TOKEN)
    before = TelegramBot.objects.get(bot_id=123456).updated_at

    with encrypting(FIRST):
        call_command('tgbot_rewrap_tokens', '--dry-run')
        assert TelegramBot.objects.get(bot_id=123456).token == TOKEN, 'a dry run wrote'

    call_command('tgbot_rewrap_tokens')

    assert TelegramBot.objects.get(bot_id=123456).updated_at == before, 'a row nothing changed was written back'


def test_an_unreadable_row_is_reported_and_left_as_it_is():
    """Writing unreadable bytes back as a token would turn a recoverable mistake into a loss.

    Reported as well as left alone: a walk that skipped it quietly would say it finished, and
    the operator who dropped a key too early would find out from a bot that stopped answering.
    """
    with encrypting(FIRST):
        TelegramBot.objects.create(bot_id=123456, token=store_token(TOKEN))
    wrapped = TelegramBot.objects.get(bot_id=123456).token
    said = StringIO()

    with encrypting(SECOND):
        call_command('tgbot_rewrap_tokens', stderr=said)

    assert TelegramBot.objects.get(bot_id=123456).token == wrapped
    assert '123456' in said.getvalue(), said.getvalue()


def test_a_token_rotated_under_the_walk_is_not_replaced_by_a_rewrapped_copy(monkeypatch):
    """An admin rotating a token between the read and the write must win, and be told.

    The row is read, then wrapped, then written, and an operator saving a new credential in
    between would otherwise have it overwritten by a rewrapped copy of the token they were
    replacing -- with the bot then serving under a token nobody has any more.
    """
    TelegramBot.objects.create(bot_id=123456, token=TOKEN)
    rotated = '123456:AArotated'

    def store_and_race(token):
        """Wrap it, the way the command does, with a rotation landing at the same moment."""
        TelegramBot.objects.filter(bot_id=123456).update(token=rotated)
        return store_token(token)

    said = StringIO()
    with encrypting(FIRST):
        monkeypatch.setattr('django_aiogram.management.commands.tgbot_rewrap_tokens.store_token', store_and_race)
        call_command('tgbot_rewrap_tokens', stderr=said)

    assert TelegramBot.objects.get(bot_id=123456).token == rotated, 'the rewrap overwrote a newer token'
    assert '123456' in said.getvalue(), said.getvalue()
