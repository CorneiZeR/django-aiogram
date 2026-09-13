"""The seam a token is stored through, and what each end of it promises.

Without a database, because the seam has nothing to do with one: what a column holds is
decided here, and `tests/db/test_token_storage.py` is where the rows are.
"""

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.test import override_settings

from django_aiogram.tokens import (
    PlainTokenStorage,
    TokenStorage,
    TokenUnreadableError,
    get_token_storage,
    read_token,
    store_token,
)

TOKEN = '123456:AAaa'


class Reversing(TokenStorage):
    """A storage that is obviously not the plain one, for asserting the seam is used."""

    needs_keys = False

    def store(self, token):
        """Return the token backwards."""
        return token[::-1]

    def read(self, stored):
        """Return it the right way round again."""
        return stored[::-1]


class Unreadable(TokenStorage):
    """A storage that refuses every read, the way a rotated-away key does."""

    def store(self, token):
        """Return something nothing can read back."""
        return 'wrapped'

    def read(self, stored):
        """Refuse, carrying nothing of the value."""
        msg = 'no key reads it'
        raise TokenUnreadableError(msg)


def test_the_default_stores_the_token_as_it_was_given():
    """Which is the documented answer, not an oversight: the database is the trust boundary."""
    assert isinstance(get_token_storage(), PlainTokenStorage)
    assert store_token(TOKEN) == TOKEN
    assert read_token(TOKEN) == TOKEN


@override_settings(TELEGRAM_BOT_DEFAULTS={'TOKEN_STORAGE': 'tests.test_token_storage.Reversing'})
def test_both_directions_go_through_the_configured_storage():
    """A project that turned encryption on must have no plaintext path left."""
    assert store_token(TOKEN) == TOKEN[::-1]
    assert read_token(TOKEN[::-1]) == TOKEN


@override_settings(TELEGRAM_BOT_DEFAULTS={'TOKEN_STORAGE': 'tests.test_token_storage.Reversing'})
def test_a_settings_change_reaches_the_storage_a_process_already_built():
    """The storage is cached per process, and a suite that overrides the setting has to win."""
    assert store_token(TOKEN) == TOKEN[::-1]
    with override_settings(TELEGRAM_BOT_DEFAULTS={}):
        assert store_token(TOKEN) == TOKEN
    assert store_token(TOKEN) == TOKEN[::-1]


def test_an_empty_column_is_empty_rather_than_unreadable():
    """A row written without a token is a bot somebody is still configuring."""
    with override_settings(TELEGRAM_BOT_DEFAULTS={'TOKEN_STORAGE': 'tests.test_token_storage.Unreadable'}):
        assert read_token('') == ''
        with pytest.raises(TokenUnreadableError):
            read_token('wrapped')


@pytest.mark.parametrize(
    ('path', 'says'),
    [
        ('tests.test_token_storage.NotThere', 'cannot be imported'),
        ('tests.test_token_storage.TOKEN', 'not a TokenStorage subclass'),
        # the base class is a `TokenStorage` subclass by every test but the one that matters:
        # instantiating it raises `TypeError`, and a `TypeError` here is a traceback out of
        # `manage.py check` where `E062` should have been a finding
        ('django_aiogram.tokens.TokenStorage', 'which is abstract'),
    ],
)
def test_a_storage_that_cannot_be_built_is_refused_by_name(path, says):
    """Refused rather than fallen back to the plain one.

    A project that asked for encryption and got the default would be writing plaintext under a
    setting that says otherwise, which is the one outcome this seam must never have.
    """
    with (
        override_settings(TELEGRAM_BOT_DEFAULTS={'TOKEN_STORAGE': path}),
        pytest.raises(ImproperlyConfigured) as refused,
    ):
        get_token_storage()

    assert says in str(refused.value)
    assert path in str(refused.value)
