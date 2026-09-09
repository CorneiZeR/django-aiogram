"""Where a bot's credential is kept, and the one seam every read and write goes through.

Since 5.0 a token can live in a row rather than in an environment variable, and a row is
dumped, replicated and backed up by people who are not the ones who own the bot. Some
deployments answer that with database access control and the admin permission; some have to
answer it with encryption at rest. So the storage is a *seam*: ``TOKEN_STORAGE`` names a
class, the shipped default keeps the value as it was given, and the encrypting one lives
behind the ``[crypto]`` extra -- because a package that made ``cryptography`` a base
dependency would charge every project for a decision only some of them made.

**Both directions go through here.** A provider reading a row, the admin writing one, the
rewrapping command: each calls :func:`read_token` or :func:`store_token`, so a project that
turns encryption on has no plaintext path left behind.

**Reading is allowed to fail, and it says so with its own exception.** A key that was rotated
away, a column somebody edited by hand: that is one bot's problem, and
:class:`TokenUnreadableError` is what lets the caller leave the other nineteen alone.
"""

import threading
from abc import ABC, abstractmethod
from typing import Any

from django.core.exceptions import ImproperlyConfigured
from django.core.signals import setting_changed
from django.dispatch import receiver
from django.utils.module_loading import import_string

from django_aiogram.config.bots import BOTS_SETTINGS_NAME
from django_aiogram.config.settings import SETTINGS_NAME, conf, setting_label

__all__ = (
    'PlainTokenStorage',
    'TokenStorage',
    'TokenUnreadableError',
    'get_token_storage',
    'read_token',
    'storage_class',
    'store_token',
)


class TokenUnreadableError(Exception):
    """A stored value this storage cannot turn back into a token.

    Its message never carries the value: an unreadable column is reported into a log, and a
    ciphertext in a log is a ciphertext in whatever ships the logs.
    """


class TokenStorage(ABC):
    """How a token is turned into a column value and back.

    Two methods and no state a process keeps between them -- one instance serves every bot,
    and it is built once per process. An implementation that has to be told something reads
    its own settings in ``__init__``, where a refusal reaches ``manage.py check``.
    """

    #: whether ``TOKEN_ENCRYPTION_KEYS`` has to hold something for this storage to work, so
    #: the check about the keys asks the storage rather than matching on its dotted path
    needs_keys = False

    @abstractmethod
    def store(self, token: str) -> str:
        """Return what the column should hold for ``token``."""

    @abstractmethod
    def read(self, stored: str) -> str:
        """Return the token a column value holds, or raise :class:`TokenUnreadableError`."""


class PlainTokenStorage(TokenStorage):
    """The shipped default: the column holds the token as it was given.

    Not a placeholder for the encrypting one -- it is the right answer wherever the database
    is already the trust boundary, and it is what keeps a base install free of
    ``cryptography``. What protects the token then is the database's own access control and
    the admin permission on the column.
    """

    def store(self, token: str) -> str:
        """Return the token unchanged."""
        return token

    def read(self, stored: str) -> str:
        """Return the column value unchanged."""
        return stored


#: the one storage a process uses, built on first ask. Rebuilt when the settings move, which
#: is what makes an `override_settings` in a suite reach it
_storage: TokenStorage | None = None
_lock = threading.Lock()


def get_token_storage() -> TokenStorage:
    """Return the storage ``TOKEN_STORAGE`` names, built once for this process."""
    global _storage  # noqa: PLW0603 - one storage per process, like the settings that name it
    with _lock:
        if _storage is None:
            _storage = _built(str(conf['TOKEN_STORAGE']))
        return _storage


def storage_class(path: str) -> 'type[TokenStorage]':
    """Import one storage class, or refuse naming what was wrong with the path.

    Apart from building one because a check asks the class rather than an instance: whether a
    storage needs keys is a property of the class, and asking it that must not depend on the
    keys being there.
    """
    named = setting_label(None, 'TOKEN_STORAGE')
    try:
        resolved = import_string(path)
    # `ValueError` for a path with an empty module part, which `import_module('')` raises
    # rather than `ImportError` -- see `broker.registry.broker_class` for the same catch
    except (ImportError, ValueError) as error:
        msg = f'{named} is {path!r}, which cannot be imported: {error}'
        raise ImproperlyConfigured(msg) from error
    if not (isinstance(resolved, type) and issubclass(resolved, TokenStorage)):
        msg = f'{named} is {path!r}, which is not a TokenStorage subclass.'
        raise ImproperlyConfigured(msg)
    return resolved


def _built(path: str) -> TokenStorage:
    """Instantiate the storage the path names, or refuse.

    Refused rather than fallen back to the plain one: a project that asked for encryption and
    got the default would be writing plaintext under a setting that says otherwise.
    """
    return storage_class(path)()


def store_token(token: str) -> str:
    """Return what a row should hold for ``token``."""
    return get_token_storage().store(token)


def read_token(stored: str) -> str:
    """Return the token a row holds, or raise :class:`TokenUnreadableError`.

    An empty column is empty rather than unreadable: a row written without a token is a bot
    somebody is in the middle of configuring, and every check about a missing token already
    reports it as missing.
    """
    if not stored:
        return stored
    return get_token_storage().read(stored)


@receiver(setting_changed, dispatch_uid='django_aiogram.tokens')
def _forget_the_storage(**kwargs: Any) -> None:
    """Drop the storage when the settings it was built from change."""
    global _storage  # noqa: PLW0603 - as above
    if kwargs.get('setting') in {SETTINGS_NAME, BOTS_SETTINGS_NAME}:
        with _lock:
            _storage = None
