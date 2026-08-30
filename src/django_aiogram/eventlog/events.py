"""The registry of event kinds, and the identifier that ties a message together.

``kind`` is an unconstrained ``CharField``: the set of legal values lives here,
in Python, so registering one is not a schema change. The cost is that nothing
at the database level rejects a typo, which is why the recorder validates
against this registry before a row is built.

Imported by :mod:`django_aiogram.models`, so it must stay free of aiogram
and of anything that reads Django settings at import time.
"""

import os
import socket
import time
import uuid
from dataclasses import dataclass

from django_aiogram.config.enums import EventKind
from django_aiogram.config.settings import conf

#: the width of the model's ``kind`` column; a longer code would be truncated by
#: MySQL in non-strict mode and rejected in strict mode, so it is refused here
MAX_KIND_LENGTH = 48


@dataclass(frozen=True)
class EventKindSpec:
    """One registered kind: the stored code, its label, and whether it is bad news."""

    code: str
    label: str
    failure: bool = False


_KINDS: dict[str, EventKindSpec] = {}


def register_kind(code: str, label: str, *, failure: bool = False) -> str:
    """Register an event kind, and return its code.

    Adding a kind is not a schema change. Namespace your own as
    ``<app>.<noun>.<verb>`` and keep the total in the tens: ``kind`` is the
    leading column of an index, and it stays worth having only while its
    cardinality is low.
    """
    if len(code) > MAX_KIND_LENGTH:
        msg = f'Event kind {code!r} is longer than {MAX_KIND_LENGTH} characters.'
        raise ValueError(msg)
    spec = EventKindSpec(code, label, failure=failure)
    existing = _KINDS.get(code)
    if existing is not None and existing != spec:
        msg = f'Event kind {code!r} is already registered as {existing.label!r}.'
        raise ValueError(msg)
    _KINDS[code] = spec
    return code


def known_kinds() -> frozenset[str]:
    """Every registered code, for membership checks."""
    return frozenset(_KINDS)


def kind_choices() -> list[tuple[str, str]]:
    """Every registered kind as Django choices, for the admin filter.

    Deliberately not passed to the model field: a callable there would have to
    survive ``deconstruct()`` unexpanded, and if it ever did not, registering a
    kind would become a migration every consumer has to run.
    """
    return [(spec.code, spec.label) for spec in _KINDS.values()]


def failure_kinds() -> tuple[str, ...]:
    """Return the kinds that mean something went wrong."""
    return tuple(spec.code for spec in _KINDS.values() if spec.failure)


#: Crockford's base32, which drops `I`, `L`, `O` and `U`: a short id is read aloud, copied from a
#: screenshot and typed into a ticket, and those four are where that goes wrong
_ALPHABET = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'

#: how many of those a short id has. Twelve is 60 bits, and the arithmetic is the reason: with
#: `n**2 / 2**61` expected collisions, a hundred million rows expect 0.004 of them and a billion
#: expect 0.43. Ten characters would expect 4.4 at a hundred million, which is a number somebody
#: would eventually meet
SHORT_ID_LENGTH = 12

#: what a person may type instead of the character the alphabet uses, all four of them from the
#: pairs Crockford's alphabet exists to separate
_CONFUSABLE = {'I': '1', 'L': '1', 'O': '0', 'U': 'V'}


def short_id(correlation_id: uuid.UUID) -> str:
    """Render a correlation id as something a person can read, say and search by.

    **From the random bits, never a prefix.** The first 48 bits of a UUIDv7 are a millisecond
    clock, so a prefix names the minute rather than the message: measured, 100 ids written in one
    instant share their first *eight* hex characters, and 100 written over a minute produce two
    distinct values. The low 60 bits are `rand_b` in version 7 and random in version 4 alike.

    **Derived, not stored beside the id as a second identity.** A pure function means there is
    nothing to keep in sync, nothing extra on the wire, and anyone holding an id -- a log pipeline,
    a support script, a person with a traceback -- can compute it without the database.

    Not reversible, and not unique: it names 60 of the 122 random bits, so two rows *can* share one.
    The admin shows every row a code matches rather than pretending otherwise.
    """
    number = correlation_id.int & (2**60 - 1)
    digits = []
    for _ in range(SHORT_ID_LENGTH):
        number, remainder = divmod(number, 32)
        digits.append(_ALPHABET[remainder])
    return ''.join(reversed(digits))


def normalise_short_id(text: str) -> str:
    """Read a short id the way a person wrote it down, or return ``''`` when it is not one.

    Upper-cased, hyphens and spaces dropped, and the four confusable characters folded onto the
    ones the alphabet uses -- somebody reading `0` aloud says "oh", and the ticket comes back with
    an `O` in it. That is what Crockford's alphabet is for, and honouring it on input is the half
    that makes it worth having on output.

    An empty answer means "this is not a short id", which is how the admin's search tells it apart
    from a chat id or a full UUID without guessing.
    """
    written = (character for character in text.strip().upper() if character not in ' -')
    cleaned = ''.join(_CONFUSABLE.get(character, character) for character in written)
    if len(cleaned) != SHORT_ID_LENGTH or any(character not in _ALPHABET for character in cleaned):
        return ''
    return cleaned


def new_correlation_id() -> uuid.UUID:
    """Build a time-ordered identifier, so its index appends instead of scattering.

    uuid4 is uniformly random, and inserting random keys into a large B-tree
    touches a different leaf page every time. RFC 9562's version 7 puts a 48-bit
    millisecond prefix first, which sorts the same way on every backend —
    PostgreSQL compares the raw bytes, and the others store lowercase hex.
    """
    generate = getattr(uuid, 'uuid7', None)
    if generate is not None:  # pragma: no cover - 3.14 and newer
        # `getattr` with a default gives back `Any`, and this is 3.14's `uuid.uuid7`
        return generate()  # type: ignore[no-any-return]  # reached through getattr, see above
    raw = bytearray(int(time.time() * 1000).to_bytes(6, 'big') + os.urandom(10))
    raw[6] = (raw[6] & 0x0F) | 0x70
    raw[8] = (raw[8] & 0x3F) | 0x80
    return uuid.UUID(bytes=bytes(raw))


def worker_identity() -> str:
    """Name this process, for the in-flight list and for the rows it records.

    Defaults to the hostname, which a container keeps across restarts — that is
    what lets a restarted worker find its own interrupted messages. Set
    WORKER_NAME when several workers share a host.
    """
    configured = conf.get('WORKER_NAME')
    if configured:
        return str(configured)
    return os.environ.get('HOSTNAME') or socket.gethostname()


register_kind(EventKind.OUTBOUND_QUEUED.value, 'Queued')
register_kind(EventKind.OUTBOUND_CONSUMED.value, 'Taken off the queue')
register_kind(EventKind.OUTBOUND_SENT.value, 'Sent')
register_kind(EventKind.OUTBOUND_RETRIED.value, 'Rate limited, retrying', failure=True)
register_kind(EventKind.OUTBOUND_FAILED.value, 'Send failed', failure=True)
register_kind(EventKind.OUTBOUND_DROPPED.value, 'Dropped', failure=True)
register_kind(EventKind.INBOUND_RECEIVED.value, 'Update received')
register_kind(EventKind.INBOUND_HANDLED.value, 'Update handled')
register_kind(EventKind.INBOUND_FAILED.value, 'Handler raised', failure=True)
register_kind(EventKind.FSM_TRANSITION.value, 'FSM transition')
register_kind(EventKind.QUEUE_UNDECODABLE.value, 'Undecodable payload', failure=True)
register_kind(EventKind.QUEUE_REJECTED.value, 'Rejected payload', failure=True)
register_kind(EventKind.LOG_DROPPED.value, 'Events dropped', failure=True)
