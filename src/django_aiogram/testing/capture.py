"""What a block of code queued, as records rather than as bytes off a queue.

The recipe this replaces asked a project's test suite to point a connection at fakeredis,
read a list back by key, ``loads`` the payload and ``unpack`` the envelope. Four internal
names and one transport, in every project, pinning all of them to a wire format this package
changes when it needs to -- envelope v1 accepts the 2.x shape precisely because it moved once.

So the decoding lives here, on this side of the line. A test names ``function``, ``kwargs``
and ``correlation_id``; what those travel inside stays this package's business.
"""

import contextlib
import uuid
from typing import TYPE_CHECKING, Any, NamedTuple

from django_aiogram.testing.broker import InMemoryBroker
from django_aiogram.wire.envelope import unpack
from django_aiogram.wire.serializers import loads

if TYPE_CHECKING:
    from collections.abc import Iterator

__all__ = ('Captured', 'Sent', 'capture_sends')

#: the string a project writes into ``BROKER`` to use this transport for a whole test settings
#: module, rather than a block at a time. Written out rather than derived from the class,
#: because a rename that broke the path should break something here too
BROKER_PATH = 'django_aiogram.testing.InMemoryBroker'


class Sent(NamedTuple):
    """One queued call, named the way the caller wrote it.

    ``kwargs`` is what reached the producer: ``chat_id``, ``text`` and the rest, exactly as
    passed. ``function`` is the aiogram method the worker will call. ``correlation_id`` is
    what ``bot.send()`` returned to the caller, so a test can tie the two together.
    """

    function: str
    kwargs: dict[str, Any]
    correlation_id: uuid.UUID | None
    queued_at: float


class Captured:
    """The sends a block queued, read as a sequence and decoded when asked.

    Decoded on access rather than as they arrive, for two reasons that pull the same way: the
    block may still be running, and nothing should be paid by a test that never asserts.

    Reads the queue **without consuming it**, so a case may assert what was queued and then go
    on to run the consumer over the very same messages.
    """

    def __init__(self, broker: InMemoryBroker) -> None:
        """Hold the queue this reads from, which outlives the block that filled it."""
        self._broker = broker

    @property
    def payloads(self) -> tuple[bytes, ...]:
        """The raw bytes, for a test that really does mean to read the wire format."""
        return self._broker.messages

    def __len__(self) -> int:
        """How many messages were queued."""
        return len(self.payloads)

    def __iter__(self) -> 'Iterator[Sent]':
        """Every send, oldest first."""
        return iter(self._decoded())

    def __getitem__(self, index: int) -> Sent:
        """Return the nth send, so a single-message case reads ``sent[0].kwargs``."""
        return self._decoded()[index]

    def __repr__(self) -> str:
        """Show the calls, which is what a failing assertion should print."""
        return f'<Captured {[(one.function, one.kwargs) for one in self._decoded()]}>'

    def of(self, function: str) -> list[Sent]:
        """Only the sends that named this aiogram method.

        A convenience with a reason: a block that both answers a user and notifies an admin
        queues two, and a case about one of them should not have to index past the other.
        """
        return [one for one in self._decoded() if one.function == function]

    @property
    def kwargs(self) -> list[dict[str, Any]]:
        """Just the arguments, which is what most assertions compare."""
        return [one.kwargs for one in self._decoded()]

    def _decoded(self) -> list[Sent]:
        """Turn the payloads into records, envelope and serializer both handled here."""
        calls = []
        for raw in self.payloads:
            envelope = unpack(loads(raw))
            calls.append(
                Sent(
                    function=envelope.function,
                    kwargs=dict(envelope.kwargs),
                    correlation_id=envelope.correlation_id,
                    queued_at=envelope.queued_at,
                )
            )
        return calls


@contextlib.contextmanager
def capture_sends() -> 'Iterator[Captured]':
    """Collect what the block queues, with no server, no settings and no patching.

    An :class:`~django_aiogram.testing.broker.InMemoryBroker` is made this process's broker
    for the duration, through
    :func:`~django_aiogram.broker.registry.use_broker` -- so every producer in the package
    runs for real and lands in memory. A fresh one per block, so one test's messages are never
    visible to the next.

    **Ahead of the settings rather than through them**, which is the one design decision worth
    knowing about. Installing it with ``override_settings(TELEGRAM_BOT=...)`` looked simpler
    and was wrong: every such override replaces the dict whole, so a case carrying its own --
    a decorator on the test method, which pytest applies *after* a fixture has already started
    capturing -- took the capture's broker away again and left the case asserting against a
    queue nothing had written to. Measured, by writing it that way first.

    **Three things it does not catch, each for a reason a test can see:**

    * ``send_raw`` reaches Telegram from the calling process and never queues.
    * ``ENABLED = False`` makes every send a no-op that returns an id and writes nothing.
    * ``TRANSACTIONAL = True`` holds the write until the caller's transaction commits, so a
      send inside an ``atomic()`` block that is still open has not been queued yet -- read
      the capture after the block, not inside it.
    """
    from django_aiogram.broker.registry import use_broker  # noqa: PLC0415 - django.conf, not at import

    broker = InMemoryBroker()
    with use_broker(broker):
        yield Captured(broker)
