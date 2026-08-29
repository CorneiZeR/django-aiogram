"""Rules about the shape of a value, whatever setting it came from.

A boolean that can be read as one, an integer above a floor, a string from a list, a dotted path
that looks like one. Nothing here knows what the setting *means* -- that is the other modules --
which is why one function serves a dozen rows of the registry through `partial`.

`_setting` is here for the same reason: resolving a value is a question about the settings tables
rather than about any subject, and every rule asks it.
"""

import math
from collections.abc import Callable, Collection, Mapping
from typing import Any

from django.core.exceptions import ImproperlyConfigured

from django_aiogram.config.checks.problems import Problem
from django_aiogram.config.defaults import DEFAULTS
from django_aiogram.config.settings import SETTINGS_NAME, coerce_bool, conf


def _reads_as_a_dotted_path(path: str) -> bool:
    r"""Whether the string could name something importable at all.

    `str.isidentifier` rather than a `\w`-based pattern, which was the first version and was
    wrong in a way worth recording: with a `str` pattern, Python's `\\w` matches any Unicode word
    character, so `pkg.mod\u00b2.Consumer` satisfied it while `'mod\u00b2'.isidentifier()` is
    False. The rule accepted a path no import could ever resolve.

    Keywords are **not** refused, though `'class'.isidentifier()` being True makes them look like
    the same case. Measured: a file called `class.py` imports perfectly well --
    `importlib.import_module('pkg.class')` returns the module and `import_string('pkg.class.C')`
    returns the class -- because only the `import` *statement* goes through Python's grammar, and
    nothing here does. Refusing that would refuse a path the project can use.
    """
    segments = path.split('.')
    return len(segments) > 1 and all(segment.isidentifier() for segment in segments)


#: what a project reading `DELIVERY` from a 3.x settings file has in it, against the path that
#: does the same thing. Imported from the consumer would cost aiogram at check time -- see
#: `_a_usable_delivery` -- so the two copies are pinned against each other by the suite instead


def _a_readable_boolean(key: str) -> list[Problem]:
    """Accept whatever ``coerce_bool`` accepts, and report what it would refuse.

    This rule used to demand a real ``bool``, which had it backwards in both
    directions. ``{'ENABLED': 'true'}`` is documented, boots, sends — and failed
    ``manage.py check``; while the values ``coerce_bool`` genuinely refuses raise
    ``ImproperlyConfigured`` out of ``apps.ready()`` before any check runs, so the
    error could never fire on the case it was written for. The package's own fixtures
    tripped it on working settings.

    Asked by trying the coercion rather than by reimplementing its rules, so the check
    and the runtime cannot disagree — and the message is the one the runtime would
    have raised, which is the sentence a reader needs.
    """
    try:
        coerce_bool(_setting(key), f"{SETTINGS_NAME}['{key}']")
    except ImproperlyConfigured as error:
        # the message already names the setting, and `Check._message` prefixes it
        # again — so hand back only the tail
        return [Problem(str(error).replace(f"{SETTINGS_NAME}['{key}'] ", '', 1))]
    return []


def _an_integer(key: str, *, minimum: int | None = None) -> list[Problem]:
    """Require an integer, at or above ``minimum`` when one is given."""
    value = _setting(key)
    # bool is a subclass of int, so it has to be rejected explicitly
    if isinstance(value, bool) or not isinstance(value, int):
        return [Problem(f'must be an integer, got {type(value).__name__}.')]
    if minimum is not None and value < minimum:
        return [Problem(f'must be >= {minimum}, got {value}.')]
    return []


def _a_number(key: str, *, minimum: float | None = None) -> list[Problem]:
    """Require a finite number, at or above ``minimum`` when one is given.

    Wider than :func:`_an_integer` because seconds are a place a fraction is a
    reasonable thing to write. `nan` is refused with the rest: comparisons against
    it are all false, so it would slip past the bound and then make every deadline
    built from it expire immediately.
    """
    value = _setting(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return [Problem(f'must be a number, got {type(value).__name__}.')]
    if not math.isfinite(value):
        return [Problem(f'must be a finite number, got {value}.')]
    if minimum is not None and value < minimum:
        return [Problem(f'must be >= {minimum}, got {value}.')]
    return []


def _a_string(key: str, *, allowed: Collection[str] | None = None) -> list[Problem]:
    """Require a string, one of ``allowed`` when the setting is an enumeration."""
    value = _setting(key)
    if not isinstance(value, str):
        return [Problem(f'must be a string, got {type(value).__name__}.')]
    if allowed is not None and value not in allowed:
        return [Problem(f'must be one of {sorted(allowed)}, got {value!r}.')]
    return []


def _a_callable(key: str) -> list[Problem]:
    """Refuse a value the package will call, when calling it would be a `TypeError`.

    The failure this prevents is late and confusing: the setting names a hook, nothing touches it
    at startup, and the traceback arrives on the first message with the *hook's* name in it rather
    than the setting's. Reported as the type that is there, since a dotted path somebody forgot to
    resolve is the common way to get here.
    """
    value = _setting(key)
    if callable(value):
        return []
    return [Problem(f'must be callable, got {type(value).__name__}.')]


def _a_mapping(key: str) -> list[Problem]:
    """Refuse a value the package will read keys out of, when it has none.

    A list of pairs and a JSON string both look close enough to a mapping to be written by
    accident, and neither answers `.get`. Reported as the type that is there, before anything asks
    it for a key it cannot have.
    """
    value = _setting(key)
    if isinstance(value, Mapping):
        return []
    return [Problem(f'must be a mapping, got {type(value).__name__}.')]


def _a_collection_of_strings(key: str) -> list[Problem]:
    """Require a real collection: a string would be read one character per item."""
    value = _setting(key)
    if not value:
        return []
    # `Mapping` is refused by name, and it is the one collection that passes every other test
    # here: a dict *is* a collection, of its keys, so `list(...)` downstream turns
    # `{'message': True}` into `['message']` and the project's values vanish without a word. A
    # set or a frozenset is accepted, because iterating one gives back what was written
    if isinstance(value, (str, bytes, Mapping)) or not isinstance(value, Collection):
        return [Problem(f'must be a list or tuple, got {type(value).__name__}.')]
    # anything unhashable would raise out of a membership test later, so the
    # element type is settled here and reported through repr's eyes
    invalid = sorted(repr(name) for name in value if not isinstance(name, str))
    if invalid:
        return [Problem(f'contains names that are not strings: {", ".join(invalid)}.')]
    return []


def _setting(key: str) -> Any:  # noqa: ANN401 - a setting holds whatever the project put there
    """Resolve one setting the way its own table does, package-wide or transport-owned.

    `conf` folds in the package-wide table and answers `None` for anything outside it, so a rule
    about a transport's own setting read `None` where the transport reads its declared default --
    `E007` reported `REDIS_MESSAGES_KEY` as "must be a string, got NoneType" on the very
    configuration that supplies it. A transport setting is resolved the way the transport resolves
    it, through `option`, which is the one place that knows the default.
    """
    from django_aiogram.config.checks.transport import (  # noqa: PLC0415 - a rule about the queue, asked by one about a value
        _broker_options,
        _configured_broker,
    )

    if key in DEFAULTS or key not in _broker_options():
        return conf.get(key)
    return _configured_broker().option(key)


def _filled_in_when_enabled(key: str, *, hint: str, only_if: Callable[[], bool] | None = None) -> list[Problem]:
    """Warn, never error, when an enabled bot has nothing to connect with.

    A project may legitimately boot without credentials — during migrations or
    image builds — so this must not be able to fail ``manage.py check``.

    ``only_if`` narrows the rule to configurations that need the setting at all. `W001` needs
    none: every deployment talks to Telegram. `W002` does, because two of the four transports
    never open a Redis connection — the list and Streams are both Redis, RabbitMQ and Kafka
    are neither.

    **The gate on ``ENABLED`` leaves one hole, measured and kept.** ``Dispatcher`` is built with
    :func:`build_storage` the first time anything touches it, and neither ``start_polling`` nor
    ``feed_update`` reads ``ENABLED`` first — both are public, and `API.md` says 1.x code drives
    them by hand. So a disabled process driving the dispatcher itself, with the Redis store named
    and no URL, raises ``ImproperlyConfigured`` after a clean ``manage.py check``.

    Warning regardless of the flag was tried and is worse: ``FSM_STORAGE`` defaults to ``redis``,
    so — measured — settings of ``{'ENABLED': False}`` alone produce `W002`. That is every image
    build, migration container and CI job, warned about a URL they exist not to need, which is
    how an operator learns to stop reading the list. The hole needs a caller who disabled the bot
    and then drove its dispatcher anyway; the noise needs nothing.
    """
    # a question about this deployment, asked by a rule about a shape
    from django_aiogram.config.checks.conditions import _bot_is_enabled  # noqa: PLC0415

    if only_if is not None and not only_if():
        return []
    if not _bot_is_enabled() or str(_setting(key) or '').strip():
        return []
    return [Problem('is empty while the bot is enabled.', hint=hint)]
