"""What the rest of this package needs of a queue, and nothing about which queue.

Seven operations were transport-specific in 3.x, and one of them decided the shape of this
contract. Acknowledgement was ``LREM processing 1 raw`` — it matches **by value**, so the
name of an in-flight message was the payload itself. That is the one thing that cannot
generalise: Redis Streams name an entry by id, RabbitMQ by delivery tag, Kafka by offset.

So a take returns a pair — the payload to decode, and an opaque handle to settle with.
:class:`~django_aiogram.delivery.Delivery` never looks inside a handle and the producer in
``client.py`` never sees one, which is what lets a broker choose whatever names its own
messages.
"""

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from typing import Any, ClassVar, NamedTuple


class Taken(NamedTuple):
    """One message off the queue, and the way back to it.

    ``payload`` is what the envelope is decoded from. ``handle`` belongs to the broker that
    produced it and to nothing else: it is passed back to :meth:`Broker.ack` or
    :meth:`Broker.release` unread. A broker whose messages name themselves — a Redis list,
    where the value *is* the name — may put the payload in both.
    """

    payload: bytes
    handle: object


class Liveness(NamedTuple):
    """What a probe outside this process can say about the consumer.

    ``age`` is seconds since the consumer last said it was turning, or ``None`` where the
    broker tracks membership itself and no one has to write it down — a probe reads
    ``reported`` in that case and asks the broker rather than a key.
    """

    reported: bool
    age: float | None
    detail: str = ''


class Broker(ABC):
    """One transport, and the whole of what delivery asks of it.

    A broker is resolved once per process from the ``BROKER`` setting, by dotted path and
    never by looking at what happens to be importable: a name whose driver is missing is a
    system check with an install line in its hint, not an ``ImportError`` on the first send.

    Implementations own their own settings through :attr:`OPTIONS`, so ``checks.py`` can
    validate the *active* broker's keys without knowing what they mean, and ``Settings.md``
    can carry a table per broker rather than one table of everything anyone might need.
    """

    #: this broker's settings and their defaults, read by the checks and the docs
    OPTIONS: ClassVar[Mapping[str, Any]] = {}

    # ------------------------------------------------------------------ producer

    @abstractmethod
    def publish(self, payloads: Sequence[bytes]) -> None:
        """Queue every payload, in order, from synchronous code."""

    @abstractmethod
    async def apublish(self, payloads: Sequence[bytes]) -> None:
        """Queue every payload without blocking the loop the caller is on."""

    # ------------------------------------------------------------------ consumer

    @abstractmethod
    def take(self, timeout: float) -> Taken | None:
        """Take one message, waiting up to ``timeout`` seconds, or return ``None``.

        Must return rather than block for ever, however the transport spells that: the
        consumer checks for shutdown between takes, and a liveness marker that is only
        refreshed between them would expire under a consumer that is perfectly well.
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
        documented no-op — a Redis list is one — and a broker with an explicit nack does
        not have to pretend otherwise.
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
        this is a default and not something each of them has to spell.
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
        package has to keep that bookkeeping under a name of its own — a Redis list. Where
        it is false, ``WORKER_NAME`` buys nothing and the checks should stop asking for it.
        """
        return False

    def close(self) -> None:
        """Release whatever this broker holds. Called once, at shutdown."""
        return
