"""Which bots a project configures, and how each one resolves its own settings.

A bot is a section under ``TELEGRAM_BOTS``. Anything it leaves out comes from
``TELEGRAM_BOT_DEFAULTS``, anything left out there from :mod:`django_aiogram.config.defaults`,
and a project that names no sections at all has one bot called ``default`` resolving
entirely from those two.

Its identity is the number in front of the colon in its token, which is the one thing about
a bot that holds still: an alias is a name a project may change and a token is a credential a
project may rotate, and a rotated token keeps the same identity.

Nothing here reads Django settings at import time, for the reason
:mod:`django_aiogram.config.settings` gives.
"""

import re
import threading
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any

from django.conf import settings as django_settings
from django.core.exceptions import ImproperlyConfigured
from django.core.signals import setting_changed

from django_aiogram.config.defaults import DEFAULTS, PROCESS_SCOPED
from django_aiogram.config.settings import ENV_PREFIX, MISSING, SETTINGS_NAME, conf, from_env

BOTS_SETTINGS_NAME = 'TELEGRAM_BOTS'

#: the alias a project gets when it configures no sections, and the one ``django_aiogram.bot``
#: resolves to
DEFAULT_ALIAS = 'default'

#: an alias is uppercased into an environment variable, so the set it is drawn from has to
#: survive that: `foo` and `FOO` are one variable, and so are `foo-bar` and `foo_bar`. Held to
#: what uppercasing cannot merge, which is lower case and underscores
_ALIAS = re.compile(r'^[a-z0-9][a-z0-9_]*$')

#: enough of a token to read the identity out of it and to refuse a string that is not one.
#: Deliberately not the exact shape Telegram issues: the secret half has changed length before
_TOKEN = re.compile(r'^(\d+):(\S+)$')


def parse_bot_id(token: object) -> int | None:
    """Return the identity in a token, or ``None`` when the string is not one."""
    found = _TOKEN.match(str(token or '').strip())
    return int(found.group(1)) if found else None


def env_prefix(alias: str) -> str:
    """Return the environment prefix one bot's settings are read under.

    Injective over the aliases :data:`_ALIAS` allows, which is the whole reason that pattern is
    as narrow as it is: two aliases sharing a prefix would have one variable configure both.
    """
    return f'{ENV_PREFIX}{alias.upper()}_'


@dataclass(frozen=True)
class BotRecord(Mapping[str, Any]):
    """One configured bot: its alias, its resolved settings, and where each came from.

    A ``Mapping`` so a caller reads it like ``conf`` — the checks do, and so will the client
    once it stops reading process-wide settings.
    """

    alias: str
    resolved: Mapping[str, Any]
    #: key to the label naming where its value came from, so a finding sends the reader to the
    #: file that holds it rather than to whichever place could have held it
    origins: Mapping[str, str]
    #: whether a ``TELEGRAM_BOTS`` section declared this bot, as against the implicit one
    declared: bool
    #: whether a *provider* described this bot -- a row rather than any settings section.
    #: Kept apart from `declared`, which is about sections and the implicit default: this one
    #: decides whether the bot may re-resolve its own settings by alias, and it may not. A
    #: row's alias is its identity written out, and a section may legally be *named* `123456`
    provided: bool = False

    @property
    def bot_id(self) -> int | None:
        """The identity in this bot's token, or ``None`` when it has none to read."""
        return parse_bot_id(self.resolved.get('TOKEN'))

    def label(self, key: str) -> str:
        """Name the setting the way the project wrote it, for a message about ``key``."""
        return self.origins.get(key, f"{SETTINGS_NAME}['{key}']")

    def section_label(self) -> str:
        """Name the dict this bot is configured in, for a finding about no single setting."""
        return f"{BOTS_SETTINGS_NAME}['{self.alias}']" if self.declared else SETTINGS_NAME

    def __getitem__(self, key: str) -> Any:  # noqa: ANN401 - a setting holds whatever the project put there
        """Return one resolved setting."""
        return self.resolved[key]

    def __iter__(self) -> Iterator[str]:
        """Iterate over the resolved setting names."""
        return iter(self.resolved)

    def __len__(self) -> int:
        """Return how many settings are resolved."""
        return len(self.resolved)

    def __repr__(self) -> str:
        """Name the bot without printing its settings, which hold the token."""
        return f'<BotRecord {self.alias} bot_id={self.bot_id}>'


def sections() -> Mapping[str, Any]:
    """Read ``TELEGRAM_BOTS``, refusing a shape whose keys could not be aliases.

    Loudly, like the package-wide settings: a list here answers ``False`` to every membership
    test, so every bot would resolve as though it had been configured with nothing at all.
    """
    raw = getattr(django_settings, BOTS_SETTINGS_NAME, None)
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        msg = f'{BOTS_SETTINGS_NAME} must be a mapping of alias to settings, got {type(raw).__name__}.'
        raise ImproperlyConfigured(msg)
    for alias, section in raw.items():
        if not isinstance(alias, str) or not _ALIAS.match(alias):
            msg = (
                f'{BOTS_SETTINGS_NAME} has an alias that is not a name: {alias!r}. '
                'An alias is lower case letters, digits and underscores, starting with a letter '
                'or a digit: it is uppercased into an environment variable, and anything wider '
                'would let two bots share one.'
            )
            raise ImproperlyConfigured(msg)
        if not isinstance(section, Mapping):
            msg = f'{BOTS_SETTINGS_NAME} has a section that is not a mapping: {alias!r} holds {type(section).__name__}.'
            raise ImproperlyConfigured(msg)
    return raw


def resolve(
    alias: str,
    *layers: tuple[str, Mapping[str, Any]],
    declared: bool = True,
    provided: bool = False,
) -> BotRecord:
    """Resolve one bot from the shared settings, its own environment, and the layers over them.

    A layer is ``(where, mapping)``: what to call it in a finding, and what it says. Later
    layers win, so the settings section is one layer and a profile row plus a bot row are two --
    which is what lets `runtime.providers` build a record for a bot that lives in the database
    without a second copy of this arithmetic.

    Precedence is defaults, then this bot's environment, then the layers in order. The
    environment sits *below* them deliberately: a layer is configuration somebody wrote for
    this bot, and a variable is the fallback for what nobody did.

    A setting in :data:`~django_aiogram.config.defaults.PROCESS_SCOPED` is skipped whatever a
    layer says. Those belong to the process -- the event log's writer is one per process, and
    router discovery happens once -- so honouring a per-bot value would mean the last bot
    resolved decided for everybody. `E053` reports the attempt.
    """
    resolved = dict(conf.resolved)
    origins: dict[str, str] = {}
    prefix = env_prefix(alias)
    # what the layers already decide, so the environment is not read for it at all. Not an
    # optimisation: `from_env` refuses a variable it cannot coerce, and a value nothing would
    # have used must not be what takes a bot down
    spoken_for = {key for _, said in layers for key in said}
    for key, default in DEFAULTS.items():
        if key in PROCESS_SCOPED or key in spoken_for:
            continue
        value = from_env(key, default, prefix=prefix)
        if value is not MISSING:
            resolved[key] = value
            origins[key] = f'{prefix}{key}'
    for where, said in layers:
        for key, value in said.items():
            # a transport's own options are not in the package-wide table, and they are kept
            # rather than dropped so `W010` can call an unknown one a typo
            if key in PROCESS_SCOPED:
                continue
            resolved[key] = value
            origins[key] = f"{where}['{key}']"
    return BotRecord(
        alias=alias,
        resolved=resolved,
        origins=origins,
        declared=declared,
        provided=provided,
    )


def _from_settings(alias: str, section: Mapping[str, Any], *, declared: bool) -> BotRecord:
    """Resolve one bot the way a ``TELEGRAM_BOTS`` section describes it."""
    return resolve(alias, (f"{BOTS_SETTINGS_NAME}['{alias}']", section), declared=declared)


class _Registry:
    """The resolved bots, built once and dropped when the settings change."""

    def __init__(self) -> None:
        """Start with nothing resolved; the first read does the work."""
        self._cache: tuple[BotRecord, ...] | None = None
        self._lock = threading.Lock()

    def all(self) -> tuple[BotRecord, ...]:
        """Every configured bot, in the order the project declared them."""
        cache = self._cache
        if cache is None:
            with self._lock:
                declared_sections = sections()
                declared = bool(declared_sections)
                if not declared:
                    declared_sections = {DEFAULT_ALIAS: {}}
                cache = self._cache = tuple(
                    _from_settings(alias, section, declared=declared) for alias, section in declared_sections.items()
                )
        return cache

    def reset(self) -> None:
        """Drop the cache, so the next read picks up changed settings.

        Under the same lock `all()` resolves beneath. Without it a reset landing between the
        read of the settings and the assignment is lost: the records built from the settings
        that have just changed are stored anyway, and every later read is served from them.
        """
        with self._lock:
            self._cache = None


_registry = _Registry()


def records() -> tuple[BotRecord, ...]:
    """Every bot this project configures."""
    return _registry.all()


def defaults_record() -> BotRecord:
    """Return the shared settings as a record, for what one process decides rather than one bot.

    Over ``conf`` itself rather than over a resolved copy, so an unreadable settings dict raises
    where a rule reads it and is reported as a finding, not out of building the record.
    """
    return BotRecord(alias='', resolved=conf, origins={}, declared=False)


def aliases() -> tuple[str, ...]:
    """Return the name of every configured bot."""
    return tuple(record.alias for record in records())


def record(alias: str) -> BotRecord:
    """Return one bot by alias, or raise naming the ones there are."""
    for found in records():
        if found.alias == alias:
            return found
    known = ', '.join(aliases()) or 'none'
    msg = f'No bot is configured as {alias!r}. Configured: {known}.'
    raise ImproperlyConfigured(msg)


def reset() -> None:
    """Forget the resolved bots, so the next read resolves them again."""
    _registry.reset()


def _reset_on_setting_change(
    sender: object,  # noqa: ARG001 - Django sends this to every receiver, named
    setting: str,
    **kwargs: Any,
) -> None:
    """Drop the cache when either settings dict a bot is resolved from changes."""
    if setting in {SETTINGS_NAME, BOTS_SETTINGS_NAME}:
        reset()


setting_changed.connect(_reset_on_setting_change, dispatch_uid='django_aiogram.config.bots')
