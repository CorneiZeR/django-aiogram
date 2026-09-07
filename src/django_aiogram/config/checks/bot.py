"""Rules about the bot itself: which bots there are, and each one's credentials and storage.

The settings a project sets to make the bot work at all, and the checks that read them without
importing aiogram -- which is what keeps `manage.py check` from paying most of a second on every
`migrate`. Where a rule genuinely needs an aiogram type, it imports it inside the rule and after
the cheap refusals have already returned.
"""

import importlib.util
import math
from collections.abc import Collection, Mapping
from dataclasses import fields

from django.conf import settings as django_settings
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string

from django_aiogram.config.bots import BOTS_SETTINGS_NAME, BotRecord, parse_bot_id, records, sections
from django_aiogram.config.checks.problems import Problem
from django_aiogram.config.checks.shapes import _setting
from django_aiogram.config.defaults import PROCESS_SCOPED
from django_aiogram.config.enums import (
    KNOWN_RATE_LIMIT_KEYS,
    PayloadDetail,
    SerializerKind,
    StorageKind,
    UpdateMode,
    choices,
)
from django_aiogram.config.settings import REMOVED_SETTINGS_NAME, SETTINGS_NAME, coerce_bool

MODE_CHOICES = choices(UpdateMode)


SERIALIZER_CHOICES = choices(SerializerKind)


PAYLOAD_CHOICES = choices(PayloadDetail)


_STORAGE_CHOICES = choices(StorageKind)


def _known_bot_properties(key: str, record: BotRecord) -> list[Problem]:
    """Reject names ``DefaultBotProperties`` does not have, which it would drop."""
    value = _setting(key, record)
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


def _importable_storage(key: str, record: BotRecord) -> list[Problem]:
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
    value = _setting(key, record)
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


def _sane_rate_limits(key: str, record: BotRecord) -> list[Problem]:
    """Require known budget names holding non-negative numbers."""
    value = _setting(key, record)
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


def _readable_serializer(key: str, record: BotRecord) -> list[Problem]:
    """Refuse to write pickle the reader would throw away: sends would vanish."""
    # coerced like the reader coerces it: from the environment this is a string
    if _setting(key, record) != SerializerKind.PICKLE:
        return []
    try:
        # coerced like the reader coerces it: from the environment this is a string
        allowed = coerce_bool(record.get('ALLOW_PICKLE'), record.label('ALLOW_PICKLE'))
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


def _serviceable_webhook(key: str, record: BotRecord) -> list[Problem]:
    """Reject a webhook Telegram cannot reach, or one anybody could post to."""
    url = str(_setting(key, record) or '').strip()
    # a member first, for the reason `_redis_fsm_storage` gives: `UpdateMode` mixes in `str`, and
    # since 3.11 `str(UpdateMode.WEBHOOK)` is `'UpdateMode.WEBHOOK'`. Normalising that matches
    # nothing, so a project passing the enum this package publishes had the required-URL finding
    # silently dropped -- measured: no finding at all where the string form reports one
    mode = record.get('MODE')
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
    if not str(record.get('WEBHOOK_SECRET') or '').strip():
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


def _known_update_types(key: str, record: BotRecord) -> list[Problem]:
    """Require a real collection: a string would reach Telegram as single characters."""
    allowed = _setting(key, record)
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


def _a_token_with_an_identity(key: str, record: BotRecord) -> list[Problem]:
    """Refuse a token no identity can be read out of.

    The number before the colon is what identifies the bot, and it is read without asking
    Telegram. A token that has none cannot be told from another bot's, in a deployment where
    "the bot" is not an answer.

    An empty token is `W001`'s finding: a project may boot without credentials.
    """
    token = str(_setting(key, record) or '').strip()
    if not token or parse_bot_id(token) is not None:
        return []
    return [
        Problem(
            "is not a bot token: one reads '<bot id>:<secret>'.",
            hint='The number before the colon is the identity a rotated token keeps.',
        )
    ]


def _a_readable_bots_dict(_key: str, _record: BotRecord) -> list[Problem]:
    """Report a ``TELEGRAM_BOTS`` no bot can be resolved out of.

    A finding rather than the traceback resolution raises, because `manage.py check` is where a
    reader goes to be told what is wrong with their settings. Every other rule about a bot stands
    down while this one is reporting: there are no bots to judge.
    """
    try:
        sections()
    except ImproperlyConfigured as unreadable:
        return [Problem(str(unreadable).removeprefix(f'{BOTS_SETTINGS_NAME} '), label=BOTS_SETTINGS_NAME)]
    return []


def _one_bot_per_token(_key: str, _record: BotRecord) -> list[Problem]:
    """Refuse two aliases holding one token, which are one bot under two names.

    Telegram meters the token and delivers each update once, so the pair would race for the same
    updates and pace against two budgets for one bot.

    **Each alias is named with the place its token came from**, which is not one place: two
    sections can hold the same string, and two sections that hold nothing can both inherit it
    from the defaults. A single label cannot say both, so the label is the dict that configures
    several bots -- there is no duplicate without one -- and the origins go in the message.
    """
    seen: dict[int, list[BotRecord]] = {}
    try:
        configured = records()
    except ImproperlyConfigured:
        return []  # E054 owns a dict that cannot be read at all
    for found in configured:
        if found.bot_id is not None:
            seen.setdefault(found.bot_id, []).append(found)
    shared = sorted((bot_id, found) for bot_id, found in seen.items() if len(found) > 1)
    return [
        Problem(
            f'configures bot {bot_id} more than once: '
            + ', '.join(f'{one.alias} from {one.label("TOKEN")}' for one in found)
            + '.',
            label=BOTS_SETTINGS_NAME,
            hint='One token is one bot. Give each alias its own, or drop the duplicates.',
        )
        for bot_id, found in shared
    ]


def _settings_one_process_decides(_key: str, _record: BotRecord) -> list[Problem]:
    """Refuse a bot section holding a setting the process owns rather than the bot.

    The event log has one writer thread, router discovery runs once and the in-flight list is
    keyed on one name, so a per-bot value could only mean "whichever bot resolved last wins" --
    silently, and differently depending on the order the sections were written in.
    """
    problems = []
    try:
        configured = sections()
    except ImproperlyConfigured:
        return []  # E054 owns a dict that cannot be read at all
    for alias, section in configured.items():
        named = sorted(key for key in section if key in PROCESS_SCOPED)
        if named:
            problems.append(
                Problem(
                    f'sets {", ".join(named)}, which belong to the process rather than to one bot.',
                    label=f"{BOTS_SETTINGS_NAME}['{alias}']",
                    hint=f'Move them to {SETTINGS_NAME}, which every bot in this process shares.',
                )
            )
    return problems


def _a_lease_a_pass_can_renew(key: str, record: BotRecord) -> list[Problem]:
    """Warn when the lease is not comfortably longer than the pass that renews it.

    A polling process renews its leases once per :setting:`BOT_REFRESH_INTERVAL`, so a lease
    shorter than that expires between renewals and the bots are traded between containers on
    every pass -- each trade a 409 from Telegram for whoever was polling. Twice the interval
    is the floor here, which survives one missed pass.

    Silent where nothing polls: a webhook update arrives wherever the request landed, so no
    lease is taken and this deployment would be failing ``check --fail-level WARNING`` over a
    number nothing reads. ``MODE`` is a bot's setting rather than the process's, so the
    question is whether *any* configured bot polls -- and one finding covers them all, since
    the two numbers in it are the process's.
    """
    if not _anything_polls():
        return []
    try:
        lease = float(_setting(key, record))
        # the floor the supervisor applies, not the number as written: `interval()` clamps to
        # a second, so a `BOT_REFRESH_INTERVAL` of 0.5 renews every second and a lease of 1.2s
        # would pass a comparison against the raw value while lapsing between renewals
        interval = max(1.0, float(record['BOT_REFRESH_INTERVAL']))
    except (TypeError, ValueError, OverflowError, ImproperlyConfigured):
        return []  # E056 and the interval's own rule own the type complaints
    if lease >= interval * 2:
        return []
    return [
        Problem(
            f'is {lease:g}s, which a process renewing every {interval:g}s cannot keep held.',
            hint=(
                f'Raise it above twice {SETTINGS_NAME}["BOT_REFRESH_INTERVAL"], or lower that: '
                'a lease that lapses between renewals moves the bot to another container, and '
                'both of them poll it until it settles.'
            ),
        )
    ]


def _anything_polls() -> bool:
    """Whether any configured bot takes its updates by polling, which is the default.

    A member check before the string one, for the reason :func:`_serviceable_webhook` gives:
    ``UpdateMode`` mixes in ``str``, and since 3.11 ``str(UpdateMode.POLLING)`` is
    ``'UpdateMode.POLLING'``.
    """
    try:
        configured = records()
    except ImproperlyConfigured:
        return True  # E054 owns an unreadable dict; a rule about polling assumes it happens
    for bot in configured:
        mode = bot.get('MODE')
        if mode is UpdateMode.POLLING or str(mode or '').strip().lower() == UpdateMode.POLLING.value:
            return True
    return not configured


def _the_dict_5_0_replaced(_key: str, _record: BotRecord) -> list[Problem]:
    """Name the setting 5.0 split in two, where a project still holds the old one.

    Kept as a rule rather than as silence: the old dict configured the bot, so every value left
    in it is now ignored and whatever it configured falls back to the environment or to this
    package's defaults. Reported whether or not the new dicts are also present, because a value
    sitting in a dict nothing reads is a value the project believes is in effect.
    """
    if getattr(django_settings, REMOVED_SETTINGS_NAME, None) is None:
        return []
    return [
        Problem(
            f'is no longer read. 5.0 splits it into {SETTINGS_NAME}, which every bot inherits, '
            f'and {BOTS_SETTINGS_NAME}, which holds one section per bot.',
            label=REMOVED_SETTINGS_NAME,
            hint='Rename it to ' + SETTINGS_NAME + ' if this project runs one bot; see Upgrading.',
        )
    ]
