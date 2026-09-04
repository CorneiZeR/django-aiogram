"""Named constants for the strings this package treats as data.

Every value here is frozen: queued payloads carry serialization tags and user
settings carry delivery, serializer, storage and mode names, so changing a value
would break in-flight messages and every deployment's ``TELEGRAM_BOT`` block.
The classes subclass ``str`` so that a member is interchangeable with the string
it names, which is what keeps existing settings and payloads readable as-is.
"""

from enum import Enum, unique
from typing import TypeVar


@unique
class SerializerKind(str, Enum):
    """Which encoding writes and reads the queue."""

    JSON = 'json'
    PICKLE = 'pickle'


@unique
class StorageKind(str, Enum):
    """Built-in aiogram FSM storage backends."""

    REDIS = 'redis'
    MEMORY = 'memory'


@unique
class UpdateMode(str, Enum):
    """Where Telegram updates come from."""

    POLLING = 'polling'
    WEBHOOK = 'webhook'


@unique
class SerializationTag(str, Enum):
    """Keys that mark a decoded JSON object as something richer than a mapping."""

    MODEL = '__model__'
    DEFAULT = '__default__'
    DATETIME = '__datetime__'
    DATE = '__date__'
    DECIMAL = '__decimal__'
    BYTES = '__bytes__'
    INPUT_FILE = '__input_file__'


@unique
class RateLimitKey(str, Enum):
    """Budget names inside the ``RATE_LIMIT`` setting."""

    OVERALL_PER_SECOND = 'overall_per_second'
    PER_CHAT_PER_SECOND = 'per_chat_per_second'
    GROUP_PER_MINUTE = 'group_per_minute'


@unique
class OutcomeState(str, Enum):
    """What the feed can say about one correlation id, and the four are exhaustive.

    ``UNKNOWN`` and ``PENDING`` are deliberately apart. Under ``UNKNOWN`` no *outbound* row
    an outcome is decided from **exists** — not "was never written", which is a different
    claim this cannot make: retention prunes rows, so one recorded weeks ago and pruned since
    reads the same as one nothing ever wrote. Others may well exist under the id, a handler's
    ``inbound.*`` among them, and none of those decides anything. Under ``PENDING`` a
    deciding row exists and it is not an ending.

    A caller polling for a result treats both as *not yet*, and bounds its own polling:
    ``UNKNOWN`` can be permanent, because the writer drops events under pressure and
    retention prunes them. An operator reading ``unknown`` a minute later knows to look at
    whether the log dropped the event rather than at the queue.
    """

    SENT = 'sent'
    FAILED = 'failed'
    PENDING = 'pending'
    UNKNOWN = 'unknown'


class EventKind(str, Enum):
    """What one row of the event log records.

    Namespaced by direction and dotted, so a project registering its own kinds
    has an obvious convention to follow. These land in a database column and in
    saved admin filters, which is why the values are frozen like the rest.
    """

    #: written where a send names an `eta`, and the only outbound row that is not about a
    #: message on its way: it says one is waiting for a time
    OUTBOUND_SCHEDULED = 'outbound.scheduled'
    OUTBOUND_QUEUED = 'outbound.queued'
    OUTBOUND_CONSUMED = 'outbound.consumed'
    OUTBOUND_SENT = 'outbound.sent'
    OUTBOUND_RETRIED = 'outbound.retried'
    OUTBOUND_FAILED = 'outbound.failed'
    OUTBOUND_DROPPED = 'outbound.dropped'
    INBOUND_RECEIVED = 'inbound.received'
    INBOUND_HANDLED = 'inbound.handled'
    INBOUND_FAILED = 'inbound.failed'
    FSM_TRANSITION = 'fsm.transition'
    QUEUE_UNDECODABLE = 'queue.undecodable'
    QUEUE_REJECTED = 'queue.rejected'
    LOG_DROPPED = 'log.dropped'


@unique
class PayloadDetail(str, Enum):
    """How much of a call's arguments the event log keeps."""

    NONE = 'none'
    SUMMARY = 'summary'
    FULL = 'full'


#: any of the enums above, for the reader that takes a member or the string beside it
_MemberT = TypeVar('_MemberT', bound=Enum)


def choices(kind: type[Enum]) -> frozenset[str]:
    """Return the values of ``kind`` as a frozenset, for membership checks."""
    return frozenset(member.value for member in kind)


def as_member(value: object, kind: type[_MemberT]) -> '_MemberT | None':
    """Read a setting that names one of ``kind``'s members, written either way, or ``None``.

    **A member is not readable with ``str()``.** These enums mix in ``str``, so a member compares
    equal to its own value -- which is why `SERIALIZER` and `FSM_STORAGE` happened to work -- but
    since 3.11 ``str(UpdateMode.POLLING)`` is ``'UpdateMode.POLLING'``, not ``'polling'``. A reader
    that normalises the text gets ``'updatemode.polling'`` and matches nothing.

    That is not a hypothetical spelling: **API.md** tells a project to write
    ``'MODE': UpdateMode.POLLING``, and doing so raised `ImproperlyConfigured` at startup naming a
    value nobody typed. Every reader of a choice setting goes through here now, so the two ways of
    writing one are the same thing to all of them.

    Returns ``None`` rather than raising or guessing, because what an unreadable value means
    differs by caller: `current_mode` refuses it, `detail_level` falls back to the safe answer, and
    the checks report it.
    """
    if isinstance(value, kind):
        return value
    text = str(value or '').strip().lower()
    for member in kind:
        if member.value == text:
            return member
    return None


#: here rather than beside the limiter that enforces them: `config.checks` needs the names
#: to judge the setting, and reaching into `producer` for them would make configuration
#: depend on the thing it configures — and would pay for whatever the limiter imports on
#: every boot that registers a check
KNOWN_RATE_LIMIT_KEYS: frozenset[str] = choices(RateLimitKey)
