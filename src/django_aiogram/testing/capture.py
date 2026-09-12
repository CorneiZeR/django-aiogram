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

    from django_aiogram.config.bots import BotRecord

__all__ = ('Captured', 'NotCapturedError', 'Sent', 'capture_sends')

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
    #: which bot queued it, from the envelope. Last, because this is a `NamedTuple` a project
    #: may already be unpacking positionally -- a field in the middle would rebind the rest
    bot_id: int | None = None


class NotCapturedError(AssertionError):
    """Raised when a case asks a capture about a bot it was never capturing.

    An ``AssertionError`` because that is what it is: a test asking the wrong question, and
    the answer it would otherwise get -- an empty list -- is the one a passing assertion is
    made of. A suite that asserted *nothing was sent* about a bot nobody was watching would
    go green for ever.
    """


class Captured:
    """The sends a block queued, read as a sequence and decoded when asked.

    Decoded on access rather than as they arrive, for two reasons that pull the same way: the
    block may still be running, and nothing should be paid by a test that never asserts.

    Reads the queue **without consuming it**, so a case may assert what was queued and then go
    on to run the consumer over the very same messages.
    """

    def __init__(
        self,
        broker: InMemoryBroker,
        watching: 'frozenset[int] | None' = None,
        queue: str | None = None,
    ) -> None:
        """Hold the queue this reads from, and what it was asked to watch.

        ``watching`` is ``None`` for a capture that was not narrowed to bots -- every bot in
        the process -- and a set of identities otherwise. ``queue`` is the queue it was
        narrowed to, which watches a *set of bots nobody wrote down*: whichever ones name that
        queue. Both are what :meth:`for_bot` refuses on, because a question about a bot outside
        either has no true answer here.
        """
        self._broker = broker
        self._watching = watching
        self._queue = queue

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

    def for_bot(self, bot: 'int | str') -> list[Sent]:
        """Only the sends made through one bot, by identity or by the alias it is configured under.

        Refuses a bot this capture was not watching rather than answering with an empty list:
        the empty list is what a passing assertion is made of, and a case that asserted
        *nothing was sent* about a bot nobody was capturing would pass for ever. A capture
        narrowed to a queue refuses on the same ground -- it is watching the bots that name
        that queue, whichever those are, and a bot on another one is outside it.
        """
        identity = _identity(bot)
        if self._watching is not None and identity not in self._watching:
            watched = ', '.join(str(one) for one in sorted(self._watching)) or 'nothing'
            msg = (
                f'this capture is not watching bot {identity}; it is watching {watched}. '
                'Pass that bot to capture_sends(), or ask the capture that is watching it.'
            )
            raise NotCapturedError(msg)
        if self._queue is not None:
            self._refuse_off_queue(bot, identity)
        return [one for one in self._decoded() if one.bot_id == identity]

    def _refuse_off_queue(self, bot: 'int | str', identity: int) -> None:
        """Refuse a bot whose queue is not the one this capture is on.

        The queue a bot names is what put it inside or outside the capture, so the question is
        answered from the bot's own resolved settings -- and a bot nothing is configured as
        cannot be shown to be on this queue, which is a refusal rather than a guess.
        """
        # deferred: the settings, and this module is imported by a project's conftest
        from django_aiogram.runtime.queues import named  # noqa: PLC0415 - as above

        found = _record_for(bot)
        on = None if found is None else named(found)
        if on != self._queue:
            where = f'queue {on!r}' if found is not None else 'no bot these settings configure'
            msg = (
                f'this capture is on queue {self._queue!r}, and bot {identity} is {where}. '
                'Capture that queue instead, or ask a capture that is watching this bot.'
            )
            raise NotCapturedError(msg)

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
                    bot_id=envelope.bot_id,
                )
            )
        return calls


def _record_for(bot: 'int | str') -> 'BotRecord | None':
    """Return the configured bot a test named, or ``None`` where nothing configures it.

    An alias that resolves to nothing is refused where it is written, by the same reader the
    runtime uses. An identity is looked up among the configured bots instead, and answers
    ``None`` when none of them carries it -- a payload can name a bot this process does not
    serve, and that is a fact about the deployment rather than a mistake in the case.
    """
    # deferred: the settings, and this module is imported by a project's conftest
    from django_aiogram.config.bots import record, records  # noqa: PLC0415 - as above

    if isinstance(bot, str):
        return record(bot)
    return next((found for found in records() if found.bot_id == bot), None)


def _identity(bot: 'int | str') -> int:
    """Return the identity a test named, whether it wrote the number or the alias.

    An alias is what a settings section is called and an identity is what the wire carries,
    and a case reads better naming the one the project wrote. Resolved through the same
    reader the runtime uses, so an alias that does not exist is refused there rather than
    quietly becoming a bot nothing sends as.
    """
    if isinstance(bot, int):
        return bot
    found = _record_for(bot)
    if found is None or found.bot_id is None:
        msg = f'the bot {bot!r} has no identity in its token, so nothing it sends can name it.'
        raise NotCapturedError(msg)
    return found.bot_id


@contextlib.contextmanager
def capture_sends(
    bot: 'int | str | None' = None,
    *,
    queue: str | None = None,
) -> 'Iterator[Captured]':
    """Collect what the block queues, with no server, no settings and no patching.

    An :class:`~django_aiogram.testing.broker.InMemoryBroker` is made this process's broker
    for the duration, through
    :func:`~django_aiogram.broker.registry.use_broker` -- so every producer in the package
    runs for real and lands in memory. A fresh one per block, so one test's messages are never
    visible to the next.

    **Ahead of the settings rather than through them**, which is the one design decision worth
    knowing about. Installing it with ``override_settings(TELEGRAM_BOT_DEFAULTS=...)`` looked simpler
    and was wrong: every such override replaces the dict whole, so a case carrying its own --
    a decorator on the test method, which pytest applies *after* a fixture has already started
    capturing -- took the capture's broker away again and left the case asserting against a
    queue nothing had written to. Measured, by writing it that way first.

    ``bot`` narrows it to one bot, by identity or by the alias it is configured under, and
    ``queue`` to one queue; given both, it is that bot on that queue and nothing else. Every
    other bot then goes on using the transport it was configured with, so a case can capture
    one client's sends while another's keep flowing. Left out, the capture stands for every
    bot, which is what a single-bot project's suite already has.

    Asking a narrowed capture about a bot it is *not* watching raises
    :class:`NotCapturedError` rather than answering with an empty list -- see
    :meth:`Captured.for_bot` for why that matters more than it sounds.

    **Three things it does not catch, each for a reason a test can see:**

    * ``send_raw`` reaches Telegram from the calling process and never queues.
    * ``ENABLED = False`` makes every send a no-op that returns an id and writes nothing.
    * ``TRANSACTIONAL = True`` holds the write until the caller's transaction commits, so a
      send inside an ``atomic()`` block that is still open has not been queued yet -- read
      the capture after the block, not inside it.
    """
    from django_aiogram.broker.registry import use_broker  # noqa: PLC0415 - django.conf, not at import

    watching = None if bot is None else frozenset({_identity(bot)})
    broker = InMemoryBroker()
    with use_broker(broker, queue=queue, bots=watching):
        yield Captured(broker, watching, queue)
