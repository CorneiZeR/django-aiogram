"""What two bots have to agree on before they may share anything.

A profile is the answer, and it is **computed** rather than configured: two sections that
resolve to the same values get the same profile whether one wrote them out and the other
inherited them. A setting naming the group would be a second source of truth, and the two
would disagree the first time somebody edited one of them.

The settings here are the ones that decide what a :class:`~django_aiogram.runtime.groups.
RuntimeGroup` owns -- the transport and the consumer that drains it. Everything else is
either a bot's own -- its token, its default properties, its rate limits -- or the process's,
which is where the dispatcher, the handlers and the FSM store live. `Bot` is cheap once its
session is shared, which is why the split falls where it does.

**When in doubt the profile splits.** Sharing a group two bots should not share is a defect --
a message on the wrong transport, or in a queue nothing serving that bot reads -- while
splitting one they could have shared costs a connection. So a setting that is *sometimes* read is
included rather than reasoned about: `REDIS_URL` is in here even on a deployment that reaches
no Redis at all.
"""

import hashlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from django_aiogram.broker.registry import broker_class

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = ('Profile', 'profile_of')

#: the package-wide settings a group is built from. A transport's own options are added to
#: these, read off whichever class ``BROKER`` names
SHARED = (
    'BROKER',
    # the queue itself: two bots on different queues must not share a broker, or each would
    # read the other's messages off the queue it is addressed to. Read through
    # `queues.named` below rather than raw, because everything that *uses* it strips the
    # name -- so ' vip ' and 'vip' address one queue and must not build two groups for it
    'QUEUE',
    'DELIVERY',
    'SERIALIZER',
    'ALLOW_PICKLE',
    'REDIS_URL',
    'REDIS_TIMEOUT',
    'BLPOP_TIMEOUT',
    'HEARTBEAT_INTERVAL',
    'MAX_IN_FLIGHT',
    'REQUIRE_CRASH_SAFE',
)


def _hashable(value: object) -> object:
    """Return something that can key a dict, whatever the project wrote.

    A setting is whatever a settings file holds, and a dict or a list cannot key one. Ordered
    where the container is ordered and sorted where it is not, so two mappings written in
    different orders are one profile -- they resolve to the same configuration, and the whole
    point of computing this is that equal values group together.

    **The type travels with the value**, which is not decoration: `True == 1` and
    `hash(True) == hash(1)` in Python, so without it two settings that read differently would
    key the same group -- and the group keeps the settings of whichever bot built it, so the
    other bot's would be the ones nothing read. The same for a list against a tuple of the
    same items. Where the two really are the same configuration this costs a connection, which
    is the side this errs on everywhere.
    """
    if isinstance(value, dict):
        # the key through the same normalisation as the value, so `{1: 'x'}` and `{'1': 'x'}`
        # are two configurations -- `str(key)` made them one, and the group then kept whichever
        # bot built it first
        entries = ((_hashable(key), _hashable(item)) for key, item in value.items())
        return ('dict', tuple(sorted(entries, key=repr)))
    if isinstance(value, (list, tuple)):
        return (type(value).__name__, tuple(_hashable(item) for item in value))
    if isinstance(value, (set, frozenset)):
        # sorted *by* repr and keeping the value: two unequal items can render the same, and a
        # profile built from the rendering would call two configurations one
        return (type(value).__name__, tuple(sorted((_hashable(item) for item in value), key=repr)))
    try:
        hash(value)
    except TypeError:
        # nothing else is expected here, and a profile that raises would take down a bot over a
        # setting it may not even read. Identity is the honest answer: the same object groups
        # with itself and nothing else
        return f'<unhashable {type(value).__name__} {id(value)}>'
    return (type(value).__name__, value)


@dataclass(frozen=True)
class Profile:
    """The settings two bots must agree on, as a value that can key a registry."""

    #: (name, value) pairs, sorted by name, over :data:`SHARED` and the transport's own options
    settings: tuple[tuple[str, Any], ...]

    @property
    def digest(self) -> str:
        """A short stable name for this profile, for a log line or an operator's listing."""
        return hashlib.blake2b(repr(self.settings).encode(), digest_size=4).hexdigest()

    def __repr__(self) -> str:
        """Name the profile without printing the settings, which may hold a URL with a password."""
        return f'<Profile {self.digest}>'


def profile_of(settings: 'Mapping[str, Any]') -> Profile:
    """Return the profile one bot's resolved settings belong to.

    The transport's own options are read off the class ``BROKER`` names, so a Kafka topic
    splits a group the way a Redis list key does and neither is in the other's namespace.
    Resolving that class is what this cannot do without: a bot whose ``BROKER`` is unusable has
    no profile, and the refusal is `E047`'s own -- raised here rather than swallowed, because a
    group built from a transport nobody could name would be a group nothing can deliver on.
    """
    options = broker_class(settings, verify_driver=False).OPTIONS
    named = sorted({*SHARED, *options})
    return Profile(settings=tuple((key, _value_of(key, settings)) for key in named))


def _value_of(key: str, settings: 'Mapping[str, Any]') -> object:
    """Return what this key contributes to a profile, as whoever reads it will read it.

    ``QUEUE`` is the one that needs saying: `Broker.queue` and `queues.named` both strip it, so
    ' vip ' and 'vip' address one queue -- and hashed raw they would build a runtime group and
    a transport each for it, each consuming the other's messages.
    """
    if key == 'QUEUE':
        # deferred: `runtime.queues` reaches the ORM in the functions this does not call
        from django_aiogram.runtime.queues import named  # noqa: PLC0415 - as above

        return _hashable(named(settings))
    return _hashable(settings.get(key))
