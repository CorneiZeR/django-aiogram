"""Turning call arguments into something safe to keep in a row.

Deliberately lossy, which is why it is not :func:`~django_aiogram.wire.serializers.encode`.
That one is lossless by contract: it base64s a whole ``BufferedInputFile``, so a
photo send would arrive in the table as megabytes of base64.

Order is fixed and load-bearing: summarize, then redact, then cap. Redaction runs
over the summarized structure so it never walks an aiogram model, and before the
cap so a truncated preview cannot end halfway through a credential.

No aiogram import: unknown values are rendered by class name through duck typing,
which keeps this usable from the delivery thread as well.
"""

import datetime
import json
import logging
import re
from decimal import Decimal
from enum import Enum
from typing import Any

from django_aiogram.config.enums import PayloadDetail, as_member
from django_aiogram.config.settings import conf

logger = logging.getLogger('django_aiogram')

#: how deep to walk before giving up
MAX_DEPTH = 6
#: how many characters of a string to keep under 'full'
MAX_STRING = 2000
MAX_KEYS = 50
MAX_ITEMS = 50

_OMITTED = '__omitted__'
#: written by `bounded` into the marker that replaces a payload too big for the column
_TRUNCATED = '__truncated__'
_REDACTED = '***'
#: <bot id>:<35 base64url characters>, the shape Telegram issues
_TOKEN_RE = re.compile(r'\b\d{5,}:[A-Za-z0-9_-]{30,}\b')


def detail_level() -> PayloadDetail:
    """How much of a call's arguments to keep, defaulting to the safe answer.

    Through `as_member`, because `str()` on a member gives its name: a project writing
    ``PayloadDetail.FULL`` -- the spelling `API.md` documents -- got summaries instead, and nothing
    said so. Falling back quietly is right for a value nobody can read; it was wrong for one this
    package published.
    """
    level = as_member(conf['EVENT_LOG_PAYLOAD'], PayloadDetail)
    # E033 reports an unreadable one at boot; at runtime the quiet answer is the safe one
    return level if level is not None else PayloadDetail.SUMMARY


class _Unhandled:
    """Says a value is not a scalar, without colliding with None as a value."""


_UNHANDLED = _Unhandled()


def _text(value: str, *, bodies: bool) -> Any:  # noqa: ANN401 - a string or a marker
    """Render one string: whole, previewed, or replaced by its length.

    A marker for the over-long case rather than a prefix and an ellipsis, which is what this
    was until 4.1. Two readers wanted the difference and neither could see it: a person
    reading the admin could not tell a truncated body from a message that ended in '…', and
    :func:`lossy_reason` -- which decides whether ``tgbot_replay`` may send a row again --
    could not tell it from the whole message. The information is the same either way.
    """
    if not bodies:
        return {_OMITTED: 'text', 'length': len(value)}
    if len(value) <= MAX_STRING:
        return value
    return {_TRUNCATED: True, 'size': len(value), 'preview': value[:MAX_STRING]}


def _scalar(value: object, *, bodies: bool) -> Any:  # noqa: ANN401 - see `summarize`
    """Render the values that need no recursion, or report that this is not one."""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {_OMITTED: 'bytes', 'size': len(value)}
    if isinstance(value, str):
        return _text(value, bodies=bodies)
    if isinstance(value, bool) or value is None or isinstance(value, (int, float)):
        return value
    if isinstance(value, (datetime.datetime, datetime.date, Decimal)):
        return str(value)
    return _UNHANDLED


# `object` going in, because this accepts anything and narrows by `isinstance` -- and `Any` coming
# out, because what comes back has the *shape* of what went in: a mapping stays a mapping, and
# `describe` hands the result straight to `bounded`, which takes a dict. Expressing that needs an
# overload per shape, which is more machinery than the invariant is worth stating twice
def summarize(value: object, *, bodies: bool, depth: int = 0) -> Any:  # noqa: ANN401 - shape follows the input
    """Render a call argument for the log: readable, bounded, never a file."""
    if depth > MAX_DEPTH:
        return {_OMITTED: 'depth'}
    scalar = _scalar(value, bodies=bodies)
    if scalar is not _UNHANDLED:
        return scalar
    if isinstance(value, Enum):
        return summarize(value.value, bodies=bodies, depth=depth)
    if isinstance(value, dict):
        pairs = list(value.items())
        kept = {str(key): summarize(item, bodies=bodies, depth=depth + 1) for key, item in pairs[:MAX_KEYS]}
        # the same reasoning as the string cap above, and the same reason it is a marker: a
        # dict cut to fifty keys is indistinguishable from a dict of fifty keys, and one of
        # them is a call that must not be replayed. Argument names never begin with `__`, so
        # the marker cannot collide with a key it sits beside
        return kept if len(pairs) <= MAX_KEYS else {**kept, _OMITTED: 'keys', 'keys': len(pairs)}
    if isinstance(value, (list, tuple, set, frozenset)):
        items = list(value)
        shown = [summarize(item, bodies=bodies, depth=depth + 1) for item in items[:MAX_ITEMS]]
        # wrapped rather than marked in place, because a list has no key to put a marker
        # under and appending one would change what the last element is
        return shown if len(items) <= MAX_ITEMS else {_TRUNCATED: True, 'size': len(items), 'preview': shown}
    # aiogram models and input files land here; the class name is what a reader wants
    return {_OMITTED: type(value).__name__}


def secrets() -> tuple[str, ...]:
    """Return the configured credentials, once per walk rather than once per string."""
    found = (str(conf.get(name) or '').strip() for name in ('TOKEN', 'WEBHOOK_SECRET'))
    return tuple(secret for secret in found if secret)


def redact_text(text: str, configured: tuple[str, ...] | None = None) -> str:
    """Strip credentials from anything that came out of an exception.

    This is not paranoia: the token is in the API URL, aiogram and aiohttp put
    that URL in their error messages, and those messages are what an ``error``
    column holds.

    ``configured`` is threaded down by :func:`redact_values` so the settings are
    read once for a whole payload instead of once per string at every depth.
    Resolving them here was half the cost of this function.
    """
    for secret in secrets() if configured is None else configured:
        text = text.replace(secret, _REDACTED)
    # every token has a colon in it, so a string without one cannot match and does
    # not need the regex walked over it. Revisit if the pattern ever widens
    if ':' not in text:
        return text
    # then anything token-shaped: a second bot's token is just as bad in a row
    return _TOKEN_RE.sub(_REDACTED, text)


def redact_values(
    value: object,
    keys: frozenset[str],
    configured: tuple[str, ...] | None = None,
) -> Any:  # noqa: ANN401 - as `summarize`: shape follows the input
    """Blank out values under credential-named keys, at any depth."""
    if configured is None:
        configured = secrets()
    if isinstance(value, dict):
        return {
            key: _REDACTED if str(key).lower() in keys else redact_values(item, keys, configured)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_values(item, keys, configured) for item in value]
    if isinstance(value, str):
        return redact_text(value, configured)
    return value


def redact_keys() -> frozenset[str]:
    """Return the configured key names, lowercased for comparison."""
    configured = conf['EVENT_LOG_REDACT_KEYS'] or ()
    if isinstance(configured, (str, bytes)):
        return frozenset()  # E035 reports the shape; reading it per character would be worse
    return frozenset(str(name).lower() for name in configured)


def bounded(payload: dict[str, Any]) -> dict[str, Any]:
    """Keep a payload under the configured byte cap, whatever it holds."""
    try:
        cap = int(conf['EVENT_LOG_MAX_PAYLOAD_BYTES'])
    # `OverflowError` for an infinite cap, which this runs into once per event: `describe` logs
    # whatever escapes, so it cost a traceback and an `undescribable` payload for every message
    except (TypeError, ValueError, OverflowError):
        return {}  # E034 reports it; a row is not the place to argue
    if cap <= 0:
        return {}
    try:
        text = json.dumps(payload, ensure_ascii=False, default=str)
    except (TypeError, ValueError, RecursionError):
        return {_OMITTED: 'unserializable'}
    if len(text.encode('utf-8')) <= cap:
        return payload
    # a preview string, not a truncated object: half a JSON document is not
    # JSON, and Oracle and SQLite both validate the column
    return _overflow(text, cap)


#: below this many characters the preview shrinks one at a time, not by halves
_FINE_TUNE_BELOW = 8


def _overflow(text: str, cap: int) -> dict[str, Any]:
    """Describe what did not fit, in a form that itself fits.

    The marker is not free: its keys, the size and JSON's own quoting all cost
    bytes, and a preview counted in characters can cost four bytes each. The
    cap is a promise about the column, so the preview shrinks until the whole
    object honours it — and the marker is dropped entirely if even that cannot.
    """
    kept = cap // 2
    while kept >= 0:
        marker: dict[str, Any] = {_TRUNCATED: True, 'size': len(text), 'preview': text[:kept]}
        if len(json.dumps(marker, ensure_ascii=False).encode('utf-8')) <= cap:
            return marker
        kept = kept // 2 if kept > _FINE_TUNE_BELOW else kept - 1
    return {}


def lossy_reason(recorded: object) -> str:
    """Say why ``recorded`` is not the arguments that were sent, or ``''`` where it is.

    This module is deliberately lossy -- summarize, redact, cap -- so a row's arguments are a
    *description* of a call rather than the call. ``manage.py tgbot_replay`` needs to know
    which rows are the exception, and this is the only place that knows what the loss looks
    like: the three markers below are written here and nowhere else.

    **Per row, not per setting.** ``EVENT_LOG_PAYLOAD: 'full'`` is necessary and not
    sufficient: ``'full'`` still replaces bytes and unknown objects with an omission marker,
    still caps a payload that does not fit :func:`bounded`, and still redacts. So a photo send
    is unreplayable under ``'full'`` while the text message beside it is fine, and the answer
    has to be read off the row rather than off the configuration.

    A message whose text is literally ``'***'`` is refused as redacted. That is the trade in
    the honest direction: the alternative is sending a credential to a chat because a marker
    happened to look like a message.

    **A row written before 4.1 needs the last check.** Until then the string cap left a prefix
    and an ellipsis rather than a marker, so a body over ``MAX_STRING`` reads as an ordinary
    string -- and a replay would have sent two thousand characters of a longer message. Nothing
    else can produce a stored string that long, so the length is the signal. The key and item
    caps of such a row are not detectable at all, which is the one loss this cannot see and the
    reason the caps are marked at the source now.
    """
    if isinstance(recorded, dict):
        return _lossy_mapping(recorded)
    if isinstance(recorded, (list, tuple)):
        return next((reason for reason in map(lossy_reason, recorded) if reason), '')
    if recorded == _REDACTED:
        return 'a value was redacted, and redaction is one-way'
    if isinstance(recorded, str) and len(recorded) > MAX_STRING:
        return f'the arguments were recorded as truncated rather than in full (a {len(recorded)}-character prefix)'
    return ''


def _lossy_mapping(recorded: dict[Any, Any]) -> str:
    """Answer for a mapping, which is the half that recurses: a marker here, or one below."""
    for key, value in recorded.items():
        if key in {_OMITTED, _TRUNCATED}:
            return f'the arguments were recorded as {key.strip("_")} rather than in full'
        reason = lossy_reason(value)
        if reason:
            return reason if reason.startswith('the arguments') else f'{key}: {reason}'
    return ''


def describe(kwargs: dict[str, Any]) -> dict[str, Any]:
    """Summarize, redact and cap one call's arguments. Never raises."""
    try:
        level = detail_level()
        if level is PayloadDetail.NONE:
            return {}
        summary = summarize(kwargs, bodies=level is PayloadDetail.FULL)
        return bounded(redact_values(summary, redact_keys()))
    except Exception:
        logger.exception('could not describe a payload for the event log')
        return {_OMITTED: 'undescribable'}
