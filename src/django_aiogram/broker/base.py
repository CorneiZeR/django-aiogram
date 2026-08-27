"""What the rest of this package needs of a queue, and nothing about which queue.

Seven operations were transport-specific in 3.x, and one of them decided the shape of this
contract. Acknowledgement was ``LREM processing 1 raw`` — it matches **by value**, so the
name of an in-flight message was the payload itself. That is the one thing that cannot
generalise: Redis Streams name an entry by id, RabbitMQ by delivery tag, Kafka by offset.

So a take returns a pair, and the handle inside it is opaque. ``Delivery`` never looks into
one and the producer in ``client.py`` never sees one, which is what lets each transport name
its own messages however it already does.
"""

import importlib.util
from abc import ABC, abstractmethod
from collections.abc import Mapping
from collections.abc import Sequence as Seq
from typing import Any, ClassVar

from django.core.exceptions import ImproperlyConfigured

from django_aiogram.broker.exceptions import BrokerDependencyError
from django_aiogram.broker.models import Liveness, Taken
from django_aiogram.config.defaults import DEFAULTS
from django_aiogram.config.settings import SETTINGS_NAME, conf

__all__ = ('REQUIRED', 'Broker')


class _Required:
    """A default that says there is none: this option has to be configured.

    A sentinel rather than ``None``, because ``None`` is a legitimate default for an
    optional setting and the two must not read the same to a check.
    """

    def __repr__(self) -> str:
        """Show why a value is missing, wherever this leaks into a message."""
        return 'REQUIRED'


#: no default: the broker cannot run until the project sets it
REQUIRED = _Required()


class Broker(ABC):
    """One transport, and the whole of what delivery asks of it.

    Resolved once per process from the ``BROKER`` setting, by dotted path and never by
    looking at what happens to be importable: a name whose driver is missing is a system
    check with an install line in its hint, not an ``ImportError`` on the first send.

    Implementations own their settings through :attr:`OPTIONS`, so the checks can validate
    the *active* broker's keys without knowing what they mean, and ``Settings.md`` carries a
    table per broker rather than one table of everything anyone might need.
    """

    #: this broker's own settings: name to default, or :data:`REQUIRED` where there is no
    #: sensible one. Read by :meth:`option`, by the checks, and by the settings table for
    #: this broker — which is why the keys live here and not in the package-wide defaults:
    #: `REDIS_MESSAGES_KEY` means nothing to Kafka, and a topic means nothing to a list.
    OPTIONS: ClassVar[Mapping[str, Any]] = {}

    #: importable module name, and the extra that installs it. Empty means no driver.
    REQUIRES: ClassVar[tuple[str, str] | None] = None

    def option(self, key: str) -> object:
        """Read one of this broker's own settings, with its own default.

        Returns ``object``, so the caller narrows: a setting can arrive from the environment
        as a string whatever its declared default, and the existing code already writes
        ``int(...)`` and ``str(...)`` at the point of use for exactly that reason.

        Not through the package-wide defaults, because a key that belongs to one transport
        is noise in every other one's namespace. The broker declares what it needs, reads it
        here, and a project that names this broker configures exactly those keys.

        Raises when a :data:`REQUIRED` option is unset, naming the broker and the key —
        a check calls this at startup so the failure is a report rather than a traceback
        from the first send.

        **A key that also lives in the package-wide defaults resolves there**, because
        `conf` has already folded those in and cannot say whether a value came from the
        project or from the table. The four the Redis list declares are in both today, for
        the releases that read them from `conf` directly; they leave that table when the
        extras work makes each driver optional. Declaring a different default here than the
        package-wide one would therefore be a lie, and this refuses to tell it.
        """
        if key not in self.OPTIONS:
            msg = f'{type(self).__name__} declares no option {key!r}'
            raise KeyError(msg)
        default = self.OPTIONS[key]
        if key in DEFAULTS:
            if default is REQUIRED:
                # a contradiction, and a bug in the broker rather than the project: `conf`
                # would answer with the package-wide default and the refusal below would
                # never run, so a required option would silently become optional
                msg = (
                    f'{type(self).__name__} declares {key!r} as REQUIRED, but it is also in the '
                    'package-wide defaults, where it would always resolve. A required option '
                    'has to belong to one broker alone.'
                )
                raise ImproperlyConfigured(msg)
            if default != DEFAULTS[key]:
                msg = (
                    f'{type(self).__name__} declares {key!r} defaulting to {default!r} while the '
                    f'package-wide default is {DEFAULTS[key]!r}, which is what would be used. '
                    'Declare the same value or take the key out of the package-wide table.'
                )
                raise ImproperlyConfigured(msg)
        value = conf.get(key, None if default is REQUIRED else default)
        if default is REQUIRED and (value is None or (isinstance(value, str) and not value.strip())):
            msg = (
                f"{type(self).__name__} needs {SETTINGS_NAME}['{key}'], which is not set. "
                'It has no default: this transport cannot say where to put a message without it.'
            )
            raise ImproperlyConfigured(msg)
        return value

    @classmethod
    def required(cls) -> tuple[str, ...]:
        """Which of this broker's options a project has to set. Used by the checks."""
        return tuple(key for key, default in cls.OPTIONS.items() if default is REQUIRED)

    @classmethod
    def verify(cls) -> None:
        """Refuse now, by name, rather than on the first send.

        Uses ``find_spec`` rather than an import: this runs from a system check, where
        importing a driver to see whether it exists would pay for it on every
        ``manage.py`` invocation of every project — and the answer is the same either way.

        A broker whose module imports its driver at module scope makes this unreachable:
        the ``ImportError`` arrives first and the reader never sees the extra they need.
        ``AGENTS.md`` forbids it for that reason, and the shipped Redis list obeys it —
        measured, importing ``django_aiogram.broker.redis_list`` leaves ``redis`` out of
        ``sys.modules``, and ``tests/test_package_layout.py`` fails if that stops being
        true. The connection module it reaches through imports the driver inside the two
        functions that build a client, so the driver arrives with the connection and not
        with the import.

        Belt and braces: :data:`~django_aiogram.broker.registry.SHIPPED` is consulted
        *before* anything is imported, so a shipped transport names its extra even if its
        module were to break this rule tomorrow.
        """
        if cls.REQUIRES is None:
            return
        module, extra = cls.REQUIRES
        if importlib.util.find_spec(module) is None:
            raise BrokerDependencyError(cls.__name__, module, extra)

    # ------------------------------------------------------------------ producer

    @abstractmethod
    def publish(self, payloads: Seq[bytes]) -> None:
        """Queue every payload, in order, from synchronous code.

        An empty sequence queues nothing and raises nothing. Worth stating because the
        transports disagree by nature: a producer that batches accepts an empty batch
        quietly, while ``RPUSH key`` with no values is a syntax error to Redis. A caller
        holding a list that turned out empty should not have to know which it is talking to.
        """

    @abstractmethod
    async def apublish(self, payloads: Seq[bytes]) -> None:
        """Queue every payload without blocking the loop the caller is on.

        Empty means nothing, as for :meth:`publish`.
        """

    # ------------------------------------------------------------------ consumer

    @abstractmethod
    def take(self, timeout: float) -> Taken | None:
        """Take one message, waiting up to ``timeout`` seconds, or return ``None``.

        Must return rather than block for ever, however the transport spells that: the
        consumer checks for shutdown between takes, and a liveness marker refreshed only
        between them would expire under a consumer that is perfectly well.
        """

    @abstractmethod
    def take_nowait(self) -> Taken | None:
        """Take one message if one is there, and return ``None`` if none is."""

    @abstractmethod
    def ack(self, handle: object) -> None:
        """Settle a message whose send has finished. It must not come back."""

    @abstractmethod
    def release(self, handle: object) -> None:
        """Give up a message without having sent it, so it is delivered again.

        Distinct from never acknowledging, and deliberately so. The refusal paths added in
        3.1.0 already know the difference between *delivered* and *refused*; this is that
        difference reaching the transport instead of being implied by silence. A broker
        where leaving a message alone already means "redeliver it" implements this as a
        documented no-op, and a broker with an explicit nack does not have to pretend
        otherwise.
        """

    # ---------------------------------------------------------------- operations

    @abstractmethod
    def reclaim(self) -> int | None:
        """Put back what this worker left in flight, and say how many.

        ``None`` means the question does not apply — the transport returns an unsettled
        message to the group itself when a consumer disconnects, so there is nothing for a
        restart to reclaim and nothing for an operator to run by hand.
        """

    @abstractmethod
    def depth(self) -> int:
        """How many messages are waiting for a consumer to take them."""

    @abstractmethod
    def inflight_depth(self, worker: str | None = None) -> int:
        """How many this worker has taken and not yet settled.

        "This worker" on three of the four. A transport whose unsettled work belongs to a
        *group* rather than to a member may answer for the group, and Redis Streams does: a
        pending entry after a crash is whoever picks it up, so the group's total is the number
        with a meaning while one consumer's is an accident of who happened to read it. Its own
        implementation says so, and so does its page -- the contract allows it rather than
        having it slip through.

        ``worker`` names somebody else, which is how a monitor reads what a worker that is
        gone was still holding. Only a transport that records unsettled work under a *name*
        can answer that -- the Redis list keeps a key per worker, a stream group records the
        consumer each entry went to -- and the two that do not must raise
        :class:`~django_aiogram.broker.exceptions.WorkerDepthUnavailableError` rather than
        return a number.

        Refusing rather than answering zero, because zero is what stops somebody looking. The
        argument reached a Redis client directly before this existed, so on a RabbitMQ or Kafka
        deployment the call either raised about a missing ``REDIS_URL`` or -- worse -- answered
        from an unrelated Redis the project happened to run for caching.
        """

    @abstractmethod
    async def adepth(self) -> int:
        """How many are waiting, read without blocking the loop the caller is on.

        The async half exists for the same reason `apublish` does: a producer under ASGI
        asks this from a request, and a synchronous read there is a socket write on the
        thread serving it.
        """

    @abstractmethod
    async def ainflight_depth(self, worker: str | None = None) -> int:
        """How many this worker holds, read without blocking the loop.

        ``worker`` means what it means on :meth:`inflight_depth`, including the refusal.
        """

    def alive(self) -> None:
        """Say the consumer is still turning, if this transport needs telling.

        Paced by the caller, because the pace is policy rather than transport. Doing
        nothing is right for every broker that tracks its consumers itself, which is why
        this is a default rather than something each of them has to spell.
        """
        return

    def liveness(self) -> Liveness:
        """Report what a probe outside this process can say about the consumer.

        The default says nothing is written down, which is the honest answer for a broker
        whose group membership *is* the liveness signal.
        """
        return Liveness(reported=False, age=None, detail='this transport tracks its own consumers')

    @property
    @abstractmethod
    def crash_safe(self) -> bool:
        """Whether a message survives this worker being killed mid-send.

        Each broker answers for itself rather than for a command: 3.x read this off whether
        ``LMOVE`` existed, which was true of exactly one transport.
        """

    @property
    @abstractmethod
    def call_ceiling(self) -> float:
        """The longest a single call into this transport may take, in seconds.

        Abstract, and answered from the transport's own setting — ``REDIS_TIMEOUT``,
        ``RABBITMQ_TIMEOUT``, ``KAFKA_TIMEOUT`` — because two things outside this class are
        derived from it and both were wrong while `REDIS_TIMEOUT` stood in for all four:

        * the deadline `start_tgbot` gives its `join`. Shorter than a call the consumer thread
          can still be inside, and a worker that outlives the join goes on to acknowledge a
          message `close()` has already refused, which is 3.1.0's B3 in a new place. Longer,
          and a `docker stop` spends grace the rest of the shutdown budgeted for.
        * the cap the consumer applies to `take`, so a blocking read returns before the
          deadline that would otherwise fire underneath it.

        A number rather than a setting name, because a caller needs the arithmetic and not the
        key: `checks.py` compares it against `BLPOP_TIMEOUT` and `HEARTBEAT_INTERVAL`, and
        naming the key there would put a Redis string in a message a Kafka deployment reads.
        """

    @property
    def needs_identity(self) -> bool:
        """Whether this broker needs a stable name for each worker.

        True only where the transport cannot say which consumer holds a message, so the
        package keeps that bookkeeping under a name of its own — a Redis list. Where it is
        false, ``WORKER_NAME`` buys nothing and the checks should stop asking for it.
        """
        return False

    def close(self) -> None:
        """Release whatever this broker holds.

        Called once, at shutdown, by two paths: `bot.close()`, which the long-running commands
        reach after joining the consumer, and an `atexit` hook the registry arms the first time
        a broker is built — for the processes that only queue and never close anything.

        **Neither promises a thread.** `atexit` runs on the main one during interpreter
        shutdown; `bot.close()` runs wherever its caller is. So an implementation may only touch
        what is safe to touch from a thread that did not open it — its own client, the process's
        shared one — and must ask the owner for anything else.
        """
        return
