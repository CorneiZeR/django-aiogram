"""What a queued call looks like on the wire.

2.x wrote ``{'function': name, **kwargs}`` and the consumer splatted it straight
back into aiogram, so there was nowhere to put an identifier, a timestamp or a
version without it arriving at Telegram as an unexpected argument. 3.0 nests the
arguments instead.

The tagged-JSON serializer needs no change for this. ``encode`` recurses through
mappings unconditionally, and ``decode`` only reacts to one when a codec tag is a
key in it — ``__envelope__`` is not a tag, so the envelope decodes as a plain
mapping and its ``kwargs`` still become real aiogram objects.

**Deployment order matters because of this.** The reader below accepts the old
flat shape, but a 2.x reader handed a new payload calls the Telegram method with
``__envelope__`` as a keyword, raises ``TypeError``, logs it and swallows it —
the message is lost silently. Deploy the bot container before the web tier.
"""

import math
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from django_aiogram.exceptions import DjangoRedisAiogramError

#: marks a payload as nested, and says which shape to read it as
ENVELOPE_KEY = '__envelope__'
ENVELOPE_VERSION = 1

#: every version this reader understands, which is not the same as the one it writes. A bump
#: that forgot the difference would be a data loss rather than an upgrade: an older payload
#: reaches `MalformedEnvelopeError` below, and that one is *recorded and acknowledged* -- so
#: every message a not-yet-deployed producer left on the queue would be thrown away by the
#: consumer that arrived first. Reading is what has to be generous; writing stays at one
READABLE_VERSIONS = frozenset({1})


class MalformedEnvelopeError(DjangoRedisAiogramError, ValueError):
    """A payload decoded, but is not a shape any version of this reads.

    Distinct from an unknown *version*, and the difference decides the message's
    fate: a newer version is left in flight for an upgraded consumer to deliver,
    while nothing will ever make sense of this one, so it is recorded and
    acknowledged instead of coming back for ever.
    """

    def __init__(self, found: object) -> None:
        """Name what arrived, never its content: this came off an untrusted queue."""
        super().__init__(f'Queued payload is not a readable envelope: {found}.')


class UnknownEnvelopeVersionError(DjangoRedisAiogramError, ValueError):
    """A payload was written by a newer version than this consumer understands."""

    def __init__(self, version: object) -> None:
        """Name the version found and the newest one this consumer can read.

        The version is kept: this is the one refusal whose message says the payload will make
        sense later, so a caller counting how far ahead the writer is -- to decide whether the
        upgrade is one release or three -- needs the number rather than the sentence.
        """
        self.version = version
        super().__init__(
            f'Queued payload declares envelope version {version!r}, but this consumer '
            f'reads up to {ENVELOPE_VERSION}. Upgrade the bot container first.',
        )


@dataclass(frozen=True)
class Envelope:
    """One queued call, as the consumer needs it."""

    function: str
    kwargs: dict[str, Any]
    correlation_id: uuid.UUID | None = None
    #: a float, not a datetime: it survives both serializers without a codec
    queued_at: float = 0.0
    #: which bot is to make this call, by the number in its token. ``None`` is a payload
    #: written before there was more than one -- by 4.x, or by a 5.0 producer for a bot whose
    #: token has no identity to read -- and the consumer delivers it as the process's own
    bot_id: int | None = None


def pack(
    function: str,
    kwargs: dict[str, Any],
    correlation_id: uuid.UUID,
    queued_at: float,
    bot_id: int | None = None,
) -> dict[str, Any]:
    """Build the payload that goes on the list.

    ``bot`` is added rather than the version bumped, and that is deliberate: :func:`unpack`
    reads the keys it knows and ignores the rest, so a 4.x consumer handed this payload
    delivers it through the one bot it has -- which is the right answer for a deployment that
    has one -- and a 5.0 consumer handed a 4.x payload finds no ``bot`` and does the same. A
    bump would have made the rolling upgrade a choice between losing the backlog and stopping
    the world.

    Left out when there is nothing to say: a bot whose token has no identity in it cannot be
    named, and `E052` is what reports that rather than a payload nobody can route.
    """
    packed = {
        ENVELOPE_KEY: ENVELOPE_VERSION,
        'correlation_id': correlation_id.hex,
        'queued_at': queued_at,
        'function': function,
        'kwargs': kwargs,
    }
    if bot_id is not None:
        packed['bot'] = bot_id
    return packed


def _as_uuid(value: object) -> uuid.UUID | None:
    """Read the identifier back, tolerating a payload that carries nonsense."""
    if isinstance(value, uuid.UUID):
        return value
    if isinstance(value, str) and value:
        try:
            return uuid.UUID(value)
        except ValueError:
            return None
    return None


def _as_time(value: object) -> float:
    """Read a timestamp, or settle for none.

    A figure this cannot read costs the queue latency, not the message, which
    may otherwise be perfectly deliverable. `nan` and the infinities are in that
    class too: `float()` accepts them, arithmetic on them produces more of them,
    and `nan` is not even valid JSON to a strict reader.
    """
    # the same answer the `except TypeError` below gave, reached without asking `float` to refuse:
    # these four are what it reads, and anything else was a `type: ignore` standing in for a check
    if not isinstance(value, (int, float, str, bytes)):
        return 0.0
    try:
        seconds = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    return seconds if math.isfinite(seconds) else 0.0


def _as_bot_id(value: object) -> int | None:
    """Read the identity a payload names, or ``None`` where it names none readably.

    Exactly an ``int``, and not through ``int()``: this came off an untrusted queue, and a
    float or a numeric string that happened to parse would route a message to a bot the
    producer did not name. ``True`` is refused with them -- it is an ``int`` to Python and is
    not an identity anyone wrote.
    """
    if type(value) is not int or value <= 0:
        return None
    return value


def unpack(payload: object) -> Envelope:
    """Read either shape, from whatever the queue actually held.

    A rolling upgrade leaves 2.x payloads on the list for as long as the backlog
    lasts, and refusing them would drop real messages. Everything else this
    cannot read raises, because the consumer thread is on the far side of a
    trust boundary and an exception escaping it ends the worker.
    """
    if not isinstance(payload, Mapping):
        found = f'a decoded {type(payload).__name__}, not a mapping'
        raise MalformedEnvelopeError(found)
    version = payload.get(ENVELOPE_KEY)
    if version is None:
        return Envelope(
            function=str(payload.get('function', '')),
            kwargs={key: value for key, value in payload.items() if key != 'function'},
        )
    # exactly an int: `int()` reads True, 1.0 and 1.5 as version 1, and this
    # package writes an integer. The message names the type and never the
    # value, which came off an untrusted queue and ends up in a log line
    if type(version) is not int:
        unreadable = f'an envelope version of type {type(version).__name__}'
        raise MalformedEnvelopeError(unreadable)
    if version > ENVELOPE_VERSION:
        raise UnknownEnvelopeVersionError(version)
    if version not in READABLE_VERSIONS:
        # not a shape any version of this reads, and not one a later release will either --
        # what a *newer* producer wrote is the branch above, and it is kept for an upgrade
        older = f'envelope version {version}'
        raise MalformedEnvelopeError(older)
    arguments = payload.get('kwargs')
    return Envelope(
        function=str(payload.get('function', '')),
        kwargs=dict(arguments) if isinstance(arguments, Mapping) else {},
        correlation_id=_as_uuid(payload.get('correlation_id')),
        queued_at=_as_time(payload.get('queued_at')),
        bot_id=_as_bot_id(payload.get('bot')),
    )
