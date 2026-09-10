"""The encrypting storage: what the column holds, and what a rotation can walk through."""

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.test import override_settings

from django_aiogram.crypto import FernetTokenStorage
from django_aiogram.tokens import TokenUnreadableError

Fernet = pytest.importorskip('cryptography.fernet').Fernet

TOKEN = '123456:AAaa'
FIRST = Fernet.generate_key().decode()
SECOND = Fernet.generate_key().decode()
CRYPTO = 'django_aiogram.crypto.FernetTokenStorage'


def configured(*keys):
    """The settings for a deployment whose keys are these, newest first."""
    return override_settings(TELEGRAM_BOT_DEFAULTS={'TOKEN_STORAGE': CRYPTO, 'TOKEN_ENCRYPTION_KEYS': keys})


def test_the_column_holds_no_readable_token():
    """#122's acceptance, and the only claim that matters here."""
    with configured(FIRST):
        stored = FernetTokenStorage().store(TOKEN)

        assert TOKEN not in stored
        assert stored != TOKEN
        assert FernetTokenStorage().read(stored) == TOKEN


def test_a_plain_column_from_before_it_was_turned_on_still_reads():
    """Which is what makes turning encryption on a settings change rather than an outage."""
    with configured(FIRST):
        assert FernetTokenStorage().read(TOKEN) == TOKEN


def test_a_key_added_in_front_reads_what_the_old_one_wrote():
    """The rotation this is shaped for: add, deploy, rewrap, drop — with nothing down."""
    with configured(FIRST):
        old = FernetTokenStorage().store(TOKEN)
    with configured(SECOND, FIRST):
        assert FernetTokenStorage().read(old) == TOKEN, 'the old rows stopped being readable mid-rotation'
        fresh = FernetTokenStorage().store(TOKEN)
    with configured(SECOND):
        assert FernetTokenStorage().read(fresh) == TOKEN, 'the rewrapped rows were written under the wrong key'
        with pytest.raises(TokenUnreadableError):
            FernetTokenStorage().read(old)


def test_an_unreadable_value_says_nothing_about_itself():
    """A ciphertext in a log is a ciphertext in whatever ships the logs."""
    with configured(FIRST):
        wrapped = FernetTokenStorage().store(TOKEN)
    with configured(SECOND), pytest.raises(TokenUnreadableError) as refused:
        FernetTokenStorage().read(wrapped)

    assert wrapped not in str(refused.value)
    assert 'TOKEN_ENCRYPTION_KEYS' in str(refused.value)


def test_one_key_may_be_written_as_a_bare_string():
    """The shape a project reaches for first, and a ring of single characters otherwise."""
    with override_settings(TELEGRAM_BOT_DEFAULTS={'TOKEN_STORAGE': CRYPTO, 'TOKEN_ENCRYPTION_KEYS': FIRST}):
        assert FernetTokenStorage().read(FernetTokenStorage().store(TOKEN)) == TOKEN


@pytest.mark.parametrize(('keys', 'says'), [((), 'is empty'), (('not-a-key',), 'not a Fernet key')])
def test_keys_it_cannot_use_are_refused_at_boot(keys, says):
    """In `__init__`, so `manage.py check` reports it rather than the first row read does.

    Refused rather than generated: a storage that made up a key would encrypt every row under
    something no restart can reproduce, and the tokens would be gone.
    """
    with configured(*keys), pytest.raises(ImproperlyConfigured) as refused:
        FernetTokenStorage()

    assert says in str(refused.value)


@override_settings(TELEGRAM_BOT_DEFAULTS={'TOKEN_STORAGE': CRYPTO, 'TOKEN_ENCRYPTION_KEYS': 5})
def test_a_key_setting_there_is_no_walking_is_a_configuration_error():
    """`E063` reports the shape; this is the same refusal for a process started without checks.

    A raw `TypeError` out of the storage would reach a send or a webhook request as a
    traceback about iteration, which says nothing about the setting that is wrong.
    """
    with pytest.raises(ImproperlyConfigured) as refused:
        FernetTokenStorage()

    assert 'TOKEN_ENCRYPTION_KEYS' in str(refused.value)
