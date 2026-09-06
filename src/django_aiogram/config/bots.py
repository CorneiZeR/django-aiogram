"""Which bots a project configures, and how each one resolves its own settings.

A bot is a section under ``TELEGRAM_BOTS``. Anything it leaves out comes from
``TELEGRAM_BOT_DEFAULTS``, anything left out there from :mod:`django_aiogram.config.defaults`,
and a project that names no sections at all has one bot called ``default`` resolving
entirely from those two.

Its identity is the number in front of the colon in its token. That number is what the
wire, the feed and the logs key on, because an alias is a name a project may change and a
token is a credential a project may rotate -- neither survives what the identity has to.

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

#: an alias is uppercased into an environment variable and will name a queue, so it is held to
#: what both can carry
_ALIAS = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_-]*$')

#: enough of a token to read the identity out of it and to refuse a string that is not one.
#: Deliberately not the exact shape Telegram issues: the secret half has changed length before
_TOKEN = re.compile(r'^(\d+):(\S+)$')


def parse_bot_id(token: object) -> int | None:
    """Return the identity in a token, or ``None`` when the string is not one."""
    found = _TOKEN.match(str(token or '').strip())
    return int(found.group(1)) if found else None


def env_prefix(alias: str) -> str:
    """Return the environment prefix one bot's settings are read under."""
    return f'{ENV_PREFIX}{alias.upper().replace("-", "_")}_'


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
            msg = f'{BOTS_SETTINGS_NAME} has an alias that is not a name: {alias!r}.'
            raise ImproperlyConfigured(msg)
        if not isinstance(section, Mapping):
            msg = f'{BOTS_SETTINGS_NAME} has a section that is not a mapping: {alias!r} holds {type(section).__name__}.'
            raise ImproperlyConfigured(msg)
    return raw


def _resolve(alias: str, section: Mapping[str, Any], *, declared: bool) -> BotRecord:
    """Resolve one bot: its section, then its own environment, then the shared settings.

    A setting in :data:`~django_aiogram.config.defaults.PROCESS_SCOPED` is skipped here
    whatever the section says. Those belong to the process — the event log's writer is one
    per process, and router discovery happens once — so honouring a per-bot value would mean
    the last bot resolved decided for everybody. `E053` reports the attempt.
    """
    resolved = dict(conf.resolved)
    origins: dict[str, str] = {}
    prefix = env_prefix(alias)
    for key, default in DEFAULTS.items():
        if key in PROCESS_SCOPED:
            continue
        if key in section:
            resolved[key] = section[key]
            origins[key] = f"{BOTS_SETTINGS_NAME}['{alias}']['{key}']"
            continue
        value = from_env(key, default, prefix=prefix)
        if value is not MISSING:
            resolved[key] = value
            origins[key] = f'{prefix}{key}'
    # a transport's own options are not in the package-wide table; kept rather than dropped, so
    # `W003` can call an unknown one a typo
    for key, value in section.items():
        if key in PROCESS_SCOPED or key in DEFAULTS:
            continue
        resolved[key] = value
        origins[key] = f"{BOTS_SETTINGS_NAME}['{alias}']['{key}']"
    return BotRecord(alias=alias, resolved=resolved, origins=origins, declared=declared)


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
                    _resolve(alias, section, declared=declared) for alias, section in declared_sections.items()
                )
        return cache

    def reset(self) -> None:
        """Drop the cache, so the next read picks up changed settings."""
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
