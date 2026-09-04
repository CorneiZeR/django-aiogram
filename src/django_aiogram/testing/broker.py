"""A queue that lives in this process, so a test can assert a send without a server.

Every other transport here needs something running: a Redis, a RabbitMQ, a Kafka. That is
right for a deployment and wrong for a test suite, and the gap is why ``Testing.md`` used to
hand projects a fakeredis recipe -- which pinned their tests to one transport *and* to this
package's wire format.

This is a real :class:`~django_aiogram.broker.base.Broker` rather than a stub: it publishes,
takes, acknowledges, releases, reclaims and counts, so the path under test is the whole path.
It is held to the same contract as the four that ship, in ``tests/test_broker_conformance.py``.

**Its state belongs to the instance**, which is what makes it usable at all: the registry
builds one broker per process and drops it whenever ``TELEGRAM_BOT`` changes, so each
``override_settings`` block starts from an empty queue rather than from whatever the last test
left. Nothing here is written down, so nothing survives the process -- which is what
:attr:`crash_safe` says out loud.
"""

import itertools
import threading
from collections import deque
from collections.abc import Sequence
from typing import Any, ClassVar

from django_aiogram.broker.base import Broker
from django_aiogram.broker.exceptions import WorkerDepthUnavailableError
from django_aiogram.broker.models import Taken

__all__ = ('InMemoryBroker',)


class InMemoryBroker(Broker):
    """The four operations, against a ``deque`` and a dict of what is in flight.

    Point ``BROKER`` at ``'django_aiogram.testing.InMemoryBroker'`` in a test settings module
    and every producer in this package reaches it: ``send``, ``enqueue``, ``send_many``, their
    awaiting twins, and the mover behind ``eta``. Nothing is patched, so nothing about the
    path is different from a deployment's except where the bytes end up.
    """

    #: the only knob, and it exists because the contract requires a call deadline. A memory
    #: queue cannot spend time on IO, so the number bounds one thing: how long `take` waits
    #: for a message that has not arrived
    OPTIONS: ClassVar[dict[str, Any]] = {'MEMORY_TIMEOUT': 1.0}
    CALL_TIMEOUT_OPTION: ClassVar[str] = 'MEMORY_TIMEOUT'

    def __init__(self) -> None:
        """Start empty, with one lock that is also the condition a blocking take waits on."""
        self._ready = threading.Condition()
        self._waiting: deque[bytes] = deque()
        self._inflight: dict[int, bytes] = {}
        self._handles = itertools.count(1)

    # ------------------------------------------------------------------ reading it

    @property
    def messages(self) -> tuple[bytes, ...]:
        """Everything still waiting, oldest first, **without taking any of it**.

        The affordance a memory queue has and a socket does not, and what
        :func:`~django_aiogram.testing.capture.capture_sends` reads: a helper that asserted by
        consuming would empty the queue the test is about, so a case that both asserts and
        then runs the consumer could not exist.
        """
        with self._ready:
            return tuple(self._waiting)

    # ------------------------------------------------------------------ producer

    def publish(self, payloads: Sequence[bytes]) -> None:
        """Queue every payload in order. An empty sequence does nothing, as the contract says."""
        if not payloads:
            return
        with self._ready:
            self._waiting.extend(payloads)
            self._ready.notify_all()

    async def apublish(self, payloads: Sequence[bytes]) -> None:
        """Queue the same way the synchronous half does, because here that is the same call.

        No thread and no ``run_in_executor``: the synchronous one holds a lock over a ``deque``
        for the length of an ``extend``, so the loop is not being blocked on IO -- it is being
        blocked on memory, which is what every ``await`` in this package is trying to avoid
        *doing to a socket*. Pretending otherwise would add a hop that hides ordering bugs.
        """
        self.publish(payloads)

    # ------------------------------------------------------------------ consumer

    def take(self, timeout: float) -> Taken | None:
        """Take one, waiting up to ``timeout`` seconds for it to arrive.

        A real wait rather than an immediate ``None``, because the consumer's loop is built
        around one: a broker that returned at once would spin it at the speed of the CPU and
        make every timing assumption in `Delivery` untestable here.
        """
        deadline = min(float(timeout), self.call_ceiling)
        with self._ready:
            if not self._ready.wait_for(lambda: bool(self._waiting), timeout=max(0.0, deadline)):
                return None
            return self._issue()

    def take_nowait(self) -> Taken | None:
        """Take one if one is there, and answer ``None`` if none is."""
        with self._ready:
            return self._issue() if self._waiting else None

    def _issue(self) -> Taken:
        """Move the oldest message into flight under a fresh handle. Called holding the lock."""
        payload = self._waiting.popleft()
        handle = next(self._handles)
        self._inflight[handle] = payload
        return Taken(payload=payload, handle=handle)

    def ack(self, handle: object) -> None:
        """Settle a message, which here is forgetting it."""
        with self._ready:
            self._inflight.pop(self._as_handle(handle), None)

    def release(self, handle: object) -> None:
        """Give a message back unsent, at the front so it is the next one taken.

        The front and not the back: a release means the worker refused *this* message, and
        putting it behind everything queued since would reorder a chat's messages for a
        reason that has nothing to do with the chat.
        """
        with self._ready:
            payload = self._inflight.pop(self._as_handle(handle), None)
            if payload is not None:
                self._waiting.appendleft(payload)
                self._ready.notify_all()

    @staticmethod
    def _as_handle(handle: object) -> int:
        """Refuse a handle this broker cannot have issued, by shape and by name.

        Each transport names its messages its own way -- a payload, an entry id, a
        ``(channel, tag)`` pair -- so a handle from another one is a mistake worth reporting
        as one. Here the name is an ``int`` counter.

        ``bool`` is refused although it *is* an ``int``: ``ack(True)`` is a caller who has
        confused a handle with a result, and settling message 1 for them would be worse than
        the exception.
        """
        if not isinstance(handle, int) or isinstance(handle, bool):
            msg = f'handle must be an int issued by {InMemoryBroker.__name__}, not {type(handle).__name__}'
            raise TypeError(msg)
        return handle

    # ---------------------------------------------------------------- operations

    def reclaim(self) -> int:
        """Put everything this instance holds back on the queue, and say how many.

        A number rather than ``None``: this broker keeps its own books, so the question does
        apply -- there is simply nobody else who could answer it.
        """
        with self._ready:
            count = len(self._inflight)
            for payload in reversed(list(self._inflight.values())):
                self._waiting.appendleft(payload)
            self._inflight.clear()
            if count:
                self._ready.notify_all()
            return count

    def depth(self) -> int:
        """How many are waiting to be taken."""
        with self._ready:
            return len(self._waiting)

    def inflight_depth(self, worker: str | None = None) -> int:
        """How many are taken and unsettled here, for this instance and no other.

        A named worker is refused, as it is on RabbitMQ and Kafka: this queue belongs to one
        process and keeps no bookkeeping under a name, so answering from its own count would
        make the reply depend on whether a string happened to match `worker_identity()`.
        """
        if worker is not None:
            raise WorkerDepthUnavailableError(type(self).__name__, worker)
        with self._ready:
            return len(self._inflight)

    async def adepth(self) -> int:
        """Answer the waiting count, awaited. Nothing here can block a loop."""
        return self.depth()

    async def ainflight_depth(self, worker: str | None = None) -> int:
        """Answer the in-flight count, awaited, with the same refusal for a named worker."""
        return self.inflight_depth(worker)

    @property
    def crash_safe(self) -> bool:
        """``False``: nothing here outlives the process, let alone the worker.

        Stated rather than implied, because a deployment refuses on this answer -- and a
        project that pointed a *running* bot at this broker should be told by the checks
        rather than by a lost message.
        """
        return False

    @property
    def call_ceiling(self) -> float:
        """The longest one call may take, from the option this broker names."""
        return self.call_timeout()

    def close(self) -> None:
        """Keep what is queued, since there is nothing to release.

        The registry closes a broker whenever ``TELEGRAM_BOT`` changes, which in a test suite
        is often and in the middle of things. Dropping the messages there would make a helper
        that reads them after an ``override_settings`` block return an empty list rather than
        what the block queued.
        """
        return
