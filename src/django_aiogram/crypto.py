"""The encrypting token storage, behind the ``[crypto]`` extra.

Off by default and imported only where ``TOKEN_STORAGE`` names it, which is the whole reason
it is a separate module: a base install must never import ``cryptography``, and
``tests/test_lazy_init.py`` is what notices when something makes it.

Fernet, and through ``MultiFernet`` rather than a single key, because the requirement is a
rotation with nothing down: the keys are listed newest first, the first one encrypts, and
every one of them decrypts. So a deployment adds the new key in front, restarts, walks the
table with ``manage.py tgbot_rewrap_tokens``, and only then drops the old key -- with rows
readable at every step.

**A value that is not ours is read back as it is.** A column holding a plain token -- every
row written before the storage was turned on -- is returned unchanged rather than refused, so
switching encryption on is a settings change followed by a rewrap rather than an outage. The
prefix is what tells the two apart.
"""

from typing import TYPE_CHECKING

from django.core.exceptions import ImproperlyConfigured

from django_aiogram.config.settings import conf, setting_label
from django_aiogram.tokens import TokenStorage, TokenUnreadableError

if TYPE_CHECKING:
    from cryptography.fernet import MultiFernet

__all__ = ('FernetTokenStorage',)


class FernetTokenStorage(TokenStorage):
    """Keep the token as a Fernet ciphertext, under the newest key that can read it."""

    needs_keys = True

    #: what marks a column value as this storage's own, so a plain token from before the
    #: rewrap is recognised as one instead of failing to decrypt
    PREFIX = 'fernet:'

    def __init__(self) -> None:
        """Build the key ring, or refuse naming the setting that is missing.

        In ``__init__`` rather than on first use: this is reached from ``manage.py check``
        through the storage seam, and a deployment finds out at boot rather than on the first
        send that its keys are unusable.
        """
        self._fernet = _key_ring()

    def store(self, token: str) -> str:
        """Return the ciphertext for ``token``, under the first key."""
        return self.PREFIX + self._fernet.encrypt(token.encode()).decode()

    def read(self, stored: str) -> str:
        """Return the token a column holds, whichever key wrote it.

        A value without the prefix is a plain token from before this storage was configured,
        and comes back as it is: the rewrap is what makes the column ciphertext, and until it
        has run the bots have to keep working.
        """
        if not stored.startswith(self.PREFIX):
            return stored
        # deferred with the rest of the driver: importing this module must not be what makes
        # a base install pay for `cryptography`
        from cryptography.fernet import InvalidToken  # noqa: PLC0415 - as above

        try:
            return self._fernet.decrypt(stored.removeprefix(self.PREFIX).encode()).decode()
        except InvalidToken as error:
            # the value is deliberately not in the message: an unreadable column is reported
            # into a log, and a ciphertext in a log is a ciphertext in whatever ships the logs
            msg = (
                'a stored token could not be decrypted: no key in '
                f'{setting_label(None, "TOKEN_ENCRYPTION_KEYS")} reads it. '
                'A key that was dropped before the rows were rewrapped is the usual reason.'
            )
            raise TokenUnreadableError(msg) from error


def _key_ring() -> 'MultiFernet':
    """Build the ``MultiFernet`` the configured keys describe, or refuse.

    Refused rather than defaulted: a storage that quietly generated a key would encrypt every
    row under something no restart can reproduce, and the tokens would be gone.
    """
    named = setting_label(None, 'TOKEN_ENCRYPTION_KEYS')
    try:
        from cryptography.fernet import Fernet, MultiFernet  # noqa: PLC0415 - see `read`
    except ImportError as error:
        msg = (
            "FernetTokenStorage needs the 'cryptography' package, which is not installed. "
            'Install it with: pip install "django-aiogram[crypto]"'
        )
        raise ImproperlyConfigured(msg) from error

    keys = conf['TOKEN_ENCRYPTION_KEYS']
    if isinstance(keys, str):
        # one key written as a bare string rather than as a list of one: taken, because the
        # alternative is a ring of single characters and a refusal nobody can read
        keys = (keys,)
    listed = [str(key).strip() for key in keys or () if str(key).strip()]
    if not listed:
        msg = f'FernetTokenStorage is configured but {named} is empty. It holds the keys, newest first.'
        raise ImproperlyConfigured(msg)
    try:
        return MultiFernet([Fernet(key) for key in listed])
    except (TypeError, ValueError) as error:
        msg = f'{named} holds something that is not a Fernet key: {error}'
        raise ImproperlyConfigured(msg) from error
