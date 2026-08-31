"""Rules about the bot itself: its credentials, its storage, its webhook, its serializer.

The settings a project sets to make the bot work at all, and the checks that read them without
importing aiogram -- which is what keeps `manage.py check` from paying most of a second on every
`migrate`. Where a rule genuinely needs an aiogram type, it imports it inside the rule and after
the cheap refusals have already returned.
"""

import importlib.util
import math
from collections.abc import Collection, Mapping
from dataclasses import fields

from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string

from django_aiogram.config.checks.problems import Problem
from django_aiogram.config.checks.shapes import _setting
from django_aiogram.config.enums import (
    KNOWN_RATE_LIMIT_KEYS,
    PayloadDetail,
    SerializerKind,
    StorageKind,
    UpdateMode,
    choices,
)
from django_aiogram.config.settings import SETTINGS_NAME, coerce_bool, conf

MODE_CHOICES = choices(UpdateMode)


SERIALIZER_CHOICES = choices(SerializerKind)


PAYLOAD_CHOICES = choices(PayloadDetail)


_STORAGE_CHOICES = choices(StorageKind)


def _known_bot_properties(key: str) -> list[Problem]:
    """Reject names ``DefaultBotProperties`` does not have, which it would drop."""
    value = _setting(key)
    if not isinstance(value, Mapping):
        return []
    # before the import, not after: the default is {}, which is a Mapping, so without
    # this every `manage.py check` in every project would pay for aiogram
    if not value:
        return []
    # deferred: aiogram costs most of a second, and checks only run on demand
    from aiogram.client.default import DefaultBotProperties  # noqa: PLC0415 - as above

    known = {field.name for field in fields(DefaultBotProperties)}
    # keys may be anything a project typed into settings, so stringify before joining
    unknown = sorted(str(name) for name in value if name not in known)
    if not unknown:
        return []
    return [Problem(f'has unknown properties: {", ".join(unknown)}. Known: {", ".join(sorted(known))}.')]


def _the_storage_driver() -> list[Problem]:
    """Whether redis-py is there for the store the setting names.

    Its own function so the rule above keeps one return per shape of value, which is what
    `ruff`'s PLR0911 is counting.
    """
    if importlib.util.find_spec('redis') is not None:
        return []
    return [
        Problem(
            'names the redis store, whose driver is not installed.',
            hint=(
                'pip install "django-aiogram[redis]", or set FSM_STORAGE to '
                "'memory' if this deployment keeps no chat state."
            ),
        )
    ]


def _importable_storage(key: str) -> list[Problem]:
    """Resolve a dotted path here, so a typo fails before the first message.

    And judge the driver behind ``'redis'``, which is the default. The storage is aiogram's
    and it imports redis-py, an extra since 4.0 — so a project that names another transport,
    installs that transport's extra and leaves this setting alone is one ``pip install`` short
    of a bot that starts. Measured on a ``[kafka]``-only install with a ``REDIS_URL`` set:
    ``manage.py check`` reported no issues and ``start_tgbot`` died on
    ``ModuleNotFoundError: No module named 'redis'`` while building the dispatcher. A missing
    driver reaching a project as a traceback is the shape ``E047`` prevents for the broker;
    this rule is what prevents it for the store.

    ``find_spec`` rather than an import: this rule runs on every ``manage.py`` invocation, and
    the point of the deferred imports above is that a check pays for nothing it can avoid.
    """
    value = _setting(key)
    if not isinstance(value, str):
        return []
    if value in _STORAGE_CHOICES:
        # 'memory' needs nothing installed; 'redis' needs a driver this install may not carry
        return _the_storage_driver() if value == StorageKind.REDIS else []
    if '.' not in value:
        return [Problem(f"must be 'redis', 'memory', or a dotted path, got {value!r}.")]
    # deferred like the other aiogram imports: a disabled boot must not pay for it
    from aiogram.fsm.storage.base import BaseStorage  # noqa: PLC0415 - as above

    try:
        storage = import_string(value)
    except (ImportError, ValueError) as error:
        return [Problem(f'cannot be imported: {error}')]
    if not (isinstance(storage, type) and issubclass(storage, BaseStorage)):
        return [Problem(f'must point to a BaseStorage subclass, got {value!r}.')]
    return []


def _sane_rate_limits(key: str) -> list[Problem]:
    """Require known budget names holding non-negative numbers."""
    value = _setting(key)
    if value is None:
        return []
    if not isinstance(value, Mapping):
        return [Problem(f'must be a mapping or None, got {type(value).__name__}.')]
    unknown = sorted(str(name) for name in value if name not in KNOWN_RATE_LIMIT_KEYS)
    if unknown:
        known = ', '.join(sorted(KNOWN_RATE_LIMIT_KEYS))
        return [Problem(f'has unknown keys: {", ".join(unknown)}. Known: {known}.')]
    for name, rate in value.items():
        # `isfinite` before the bound, for the reason `_a_number` gives: every comparison against
        # `nan` is false, so `rate < 0` passes it through -- and a budget of `nan` makes each of the
        # limiter's own comparisons false in turn, which admits every message rather than none
        if isinstance(rate, bool) or not isinstance(rate, (int, float)) or not math.isfinite(rate) or rate < 0:
            return [Problem(f'{name} must be a non-negative number, got {rate!r}.')]
    return []


def _readable_serializer(key: str) -> list[Problem]:
    """Refuse to write pickle the reader would throw away: sends would vanish."""
    # coerced like the reader coerces it: from the environment this is a string
    if _setting(key) != SerializerKind.PICKLE:
        return []
    try:
        # coerced like the reader coerces it: from the environment this is a string
        allowed = coerce_bool(conf.get('ALLOW_PICKLE'), f"{SETTINGS_NAME}['ALLOW_PICKLE']")
    except ImproperlyConfigured:
        # unreadable is E017's finding; this check cannot say anything about it
        return []
    if allowed:
        return []
    return [
        Problem(
            "is 'pickle' while ALLOW_PICKLE is False, so queued messages would be "
            'written and then refused on read. Set ALLOW_PICKLE to True, or use '
            "the 'json' serializer.",
        )
    ]


def _serviceable_webhook(key: str) -> list[Problem]:
    """Reject a webhook Telegram cannot reach, or one anybody could post to."""
    url = str(_setting(key) or '').strip()
    # a member first, for the reason `_redis_fsm_storage` gives: `UpdateMode` mixes in `str`, and
    # since 3.11 `str(UpdateMode.WEBHOOK)` is `'UpdateMode.WEBHOOK'`. Normalising that matches
    # nothing, so a project passing the enum this package publishes had the required-URL finding
    # silently dropped -- measured: no finding at all where the string form reports one
    mode = conf.get('MODE')
    webhook_mode = mode is UpdateMode.WEBHOOK or str(mode or '').strip().lower() == UpdateMode.WEBHOOK.value
    if not url:
        if webhook_mode:
            return [
                Problem(
                    "is required when MODE is 'webhook': Telegram has to be told where to "
                    "post updates. Switch MODE back to 'polling' if you cannot serve one.",
                )
            ]
        return []

    problems: list[Problem] = []
    if not str(conf.get('WEBHOOK_SECRET') or '').strip():
        problems.append(
            Problem(
                'is required when WEBHOOK_URL is set: the view compares it with the header '
                'Telegram echoes back, and without it anyone who finds the URL can feed '
                'your bot updates.',
                key='WEBHOOK_SECRET',
            )
        )
    if not url.startswith('https://'):
        problems.append(Problem(f'must be https, got {url!r} — Telegram refuses anything else.'))
    return problems


def _known_update_types(key: str) -> list[Problem]:
    """Require a real collection: a string would reach Telegram as single characters."""
    allowed = _setting(key)
    # a mapping is refused for the reason `_a_collection_of_strings` gives: `webhook_settings`
    # calls `list()` on this, so a dict would register its keys as the allowed updates -- and
    # before the empty check, since `{}` is falsy and would otherwise slip past the same sentence
    if isinstance(allowed, Mapping):
        return [Problem(f'must be a list, tuple or set of update types, got {type(allowed).__name__}.')]
    if not allowed:
        return []
    if isinstance(allowed, (str, bytes)) or not isinstance(allowed, Collection):
        return [Problem(f'must be a list, tuple or set of update types, got {type(allowed).__name__}.')]

    # deferred for the same reason as DefaultBotProperties above
    from aiogram.enums import UpdateType  # noqa: PLC0415 - as above

    known = {member.value for member in UpdateType}
    # anything unhashable would raise out of the membership test below, so the
    # type is settled first and reported by repr rather than by value
    invalid = [repr(name) for name in allowed if not isinstance(name, str)]
    invalid += [repr(name) for name in allowed if isinstance(name, str) and name not in known]
    if invalid:
        return [
            Problem(f'contains update types Telegram does not have: {sorted(invalid)}. Valid ones are {sorted(known)}.')
        ]
    return []
