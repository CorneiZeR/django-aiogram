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

from django_aiogram.broker.exceptions import BrokerDependencyError
from django_aiogram.broker.models import Liveness, Taken

__all__ = ('Broker',)


class Broker(ABC):
    """One transport, and the whole of what delivery asks of it.

    Resolved once per process from the ``BROKER`` setting, by dotted path and never by
    looking at what happens to be importable: a name whose driver is missing is a system
    check with an install line in its hint, not an ``ImportError`` on the first send.

    Implementations own their settings through :attr:`OPTIONS`, so the checks can validate
    the *active* broker's keys without knowing what they mean, and ``Settings.md`` carries a
    table per broker rather than one table of everything anyone might need.
    """

    #: this broker's settings and their defaults, read by the checks and the docs
    OPTIONS: ClassVar[Mapping[str, Any]] = {}

    #: importable module name, and the extra that installs it. Empty means no driver.
    REQUIRES: ClassVar[tuple[str, str] | None] = None

    @classmethod
    def verify(cls) -> None:
        """Refuse now, by name, rather than on the first send.

        Uses ``find_spec`` rather than an import: this runs from a system check, where
        importing a driver to see whether it exists would pay for it on every
        ``manage.py`` invocation of every project — and the answer is the same either way.

        A broker whose module imports its driver at module scope makes this unreachable,
        which is why ``AGENTS.md`` forbids it: the ``ImportError`` would arrive first and
        the reader would never see the extra they need.
        """
        if cls.REQUIRES is None:
            return
        module, extra = cls.REQUIRES
        if importlib.util.find_spec(module) is None:
            raise BrokerDependencyError(cls.__name__, module, extra)

    # ------------------------------------------------------------------ producer

    @abstractmethod
    def publish(self, payloads: Seq[bytes]) -> None:
        """Queue every payload, in order, from synchronous code."""

    @abstractmethod
    async def apublish(self, payloads: Seq[bytes]) -> None:
        """Queue every payload without blocking the loop the caller is on."""

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
    def inflight_depth(self) -> int:
        """How many this worker has taken and not yet settled."""

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
    def needs_identity(self) -> bool:
        """Whether this broker needs a stable name for each worker.

        True only where the transport cannot say which consumer holds a message, so the
        package keeps that bookkeeping under a name of its own — a Redis list. Where it is
        false, ``WORKER_NAME`` buys nothing and the checks should stop asking for it.
        """
        return False

    def close(self) -> None:
        """Release whatever this broker holds. Called once, at shutdown."""
        return
