"""What became of one outbound message, read back from the feed.

``bot.send()`` answers with a correlation id and nothing else. The reply Telegram gave is
produced in the bot container, and the caller is in a different process — so a message
queued from a web request could never be edited or deleted by whoever queued it, for want
of the one id Telegram will ever give it.

That id is not thrown away. The ``outbound.sent`` row has carried ``message_id`` and
``chat_id`` since the log existed, which is everything an edit or a delete needs; what was
missing is a way to ask, and any statement that it was there at all.

**The feed is the only source, and that is a decision rather than a shortcut.** The
recorder never lets a send wait for the database — it drops an event rather than block, and
``Event log`` says so — so a store that always holds the answer would be a promise this
package refuses to make everywhere else. The price is that an absent row is not a failed
send, and :class:`Outcome` says which of the two it is rather than leaving a caller to
guess from ``None``.
"""

import datetime
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

from django_aiogram.config.enums import EventKind, OutcomeState
from django_aiogram.config.settings import SETTINGS_NAME
from django_aiogram.eventlog.recorder import recorder
from django_aiogram.eventlog.writer import log_alias
from django_aiogram.exceptions import OutcomesUnavailableError

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from django_aiogram.models import TelegramEvent

__all__ = ('Outcome', 'SentMessage', 'aoutcome', 'outcome')

#: the kinds an outcome is decided from, so the query reads a few rows rather than every
#: row an id ever collected — an update's handlers can share one with their replies
DECIDING_KINDS: tuple[str, ...] = (
    EventKind.OUTBOUND_SENT.value,
    EventKind.OUTBOUND_FAILED.value,
    EventKind.OUTBOUND_DROPPED.value,
    EventKind.OUTBOUND_QUEUED.value,
    EventKind.OUTBOUND_CONSUMED.value,
    EventKind.OUTBOUND_RETRIED.value,
)

#: the kinds a project must keep for an answer to be *right*, which is not all of the above.
#: Each one's absence costs something a caller acts on:
#:
#: * ``sent`` — there would be no result at all.
#: * ``failed`` and ``dropped`` — a message that will never arrive reads ``unknown``, which
#:   means *not yet*, so a caller polling for an end never reaches one.
#: * ``queued`` — worse than a missing answer, a wrong one: the shutdown-drop rule reads
#:   this row to tell a message the next start will reclaim from one nothing will, so
#:   without it a redeliverable send is reported ``failed`` and re-sending duplicates it.
#:
#: ``consumed`` and ``retried`` are deliberately not here. They can only ever produce
#: ``pending``, and their absence moves an in-flight message to ``unknown`` — a different
#: word for the same instruction, *ask again*, so it costs precision and not correctness.
REQUIRED_KINDS: tuple[str, ...] = (
    EventKind.OUTBOUND_SENT.value,
    EventKind.OUTBOUND_FAILED.value,
    EventKind.OUTBOUND_DROPPED.value,
    EventKind.OUTBOUND_QUEUED.value,
)


@dataclass(frozen=True)
class SentMessage:
    """One message Telegram accepted, by the pair that names it to Telegram."""

    chat_id: int | None
    message_id: int | None
    at: datetime.datetime | None


@dataclass(frozen=True)
class Outcome:
    """What the feed knows about one correlation id.

    ``sent`` is every message recorded under it, newest first, because an id is not one per
    message: a handler's replies inherit the id of the update that caused them, so one id
    can name several. :attr:`message_id` and :attr:`chat_id` read the newest of them, which
    is the answer for the ordinary case of one ``send()`` and its one message.
    """

    state: OutcomeState
    correlation_id: uuid.UUID
    sent: tuple[SentMessage, ...] = ()
    #: what Telegram or the transport said, for a state of ``failed``; empty otherwise
    error: str = ''
    #: how many attempts the send had had when the last recorded row was written
    attempt: int = 0

    @property
    def message_id(self) -> int | None:
        """The newest message sent under this id, or ``None`` where none was."""
        return self.sent[0].message_id if self.sent else None

    @property
    def chat_id(self) -> int | None:
        """The chat the newest message went to, or ``None`` where none was."""
        return self.sent[0].chat_id if self.sent else None

    @property
    def at(self) -> datetime.datetime | None:
        """When the newest message was recorded as sent, or ``None`` where none was."""
        return self.sent[0].at if self.sent else None


def _identifier(correlation_id: uuid.UUID | str) -> uuid.UUID:
    """Accept either shape a caller holds, and refuse anything that is not one.

    A ``str`` because that is what a project's own column hands back, and the id `send()`
    returned went into one — asking every caller to wrap it would be a conversion this can
    do once.
    """
    if isinstance(correlation_id, uuid.UUID):
        return correlation_id
    try:
        return uuid.UUID(str(correlation_id))
    except (AttributeError, ValueError):
        msg = f'correlation_id must be a UUID, got {correlation_id!r}.'
        raise ValueError(msg) from None


def _refuse_where_nothing_records() -> None:
    """Raise where the answer would be missing or wrong, rather than giving it anyway.

    At the call rather than as a system check, and that is the whole reason it can be a
    refusal: a project may narrow ``EVENT_LOG_KINDS`` for perfectly good reasons and never
    ask for an outcome, and a check cannot tell whether this one does. Asked, it can — so a
    deployment that reads outcomes gets one exception naming what to add, and one that does
    not gets nothing at all.

    Every missing kind at once, because a caller fixing them one exception at a time is
    four deploys for one mistake.
    """
    if not recorder.enabled:
        msg = (
            f"{SETTINGS_NAME}['EVENT_LOG'] is False, so nothing records what became of a "
            f'message and there is no outcome to read.'
        )
        raise OutcomesUnavailableError(msg)
    missing = [kind for kind in REQUIRED_KINDS if not recorder.wants(kind)]
    if missing:
        msg = (
            f"{SETTINGS_NAME}['EVENT_LOG_KINDS'] leaves out {', '.join(repr(kind) for kind in missing)}, "
            f'which an outcome is decided from — so it would read as if nothing had happened to a '
            f'message that will never arrive. Add them, or leave the setting empty to keep every kind.'
        )
        raise OutcomesUnavailableError(msg)


def _rows(identifier: uuid.UUID) -> 'Iterator[TelegramEvent]':
    """Read the deciding rows for one id, newest first, on the alias the log is written to."""
    from django_aiogram.models import TelegramEvent  # noqa: PLC0415 - django.db, not at import

    return iter(
        TelegramEvent.objects.using(log_alias())
        .filter(correlation_id=identifier, kind__in=DECIDING_KINDS)
        .order_by('-id')
    )


def _cannot_come_back(row: 'TelegramEvent', *, queued: bool) -> bool:
    """Whether this drop row means the message will never arrive.

    ``outbound.dropped`` is written from three places and only some of them are the end of
    the message, which is why this reads the row rather than the kind. Calling the others
    ``failed`` is not a cosmetic mistake: a caller told a message will not arrive re-sends
    it, and two of these three are cases where the first one still might.

    * **Retries exhausted** — ``detail`` carries ``max_retries``. Telegram kept refusing and
      the consumer has acknowledged the message. Nothing will try again.
    * **``stage`` is ``serialising``** — the payload never left the process, so there is
      nothing on any queue. Nothing will try again, and re-sending is safe.
    * **``stage`` is ``queueing``** — the publish raised, and a publish that raised may
      still have been applied. The message may yet be delivered by a worker, so this is
      *pending*: the one thing a caller must not do is re-send it.
    * **``NotScheduled``** — refused or cancelled by a shutdown. Whether it comes back
      depends on where the send came from, and the feed says which: a message that came off
      the queue was refused *without* being acknowledged, so it is still in the in-flight
      list for the next start to reclaim, and one row proves it was queued. A send that took
      the *direct* route — ``send_raw`` anywhere, or ``send`` inside the bot container — was
      never on a queue, so nothing will reclaim it and this row is the only one it will ever
      have.

    That last rule is why ``outbound.queued`` is in :data:`REQUIRED_KINDS`: without the row,
    ``queued`` reads False for a message that was queued, and a send the next start will
    deliver is reported as one that never will.

    **``queued`` is about the id, not about one message under it, and the feed cannot narrow
    it.** Where an id names several messages — a handler's replies inherit the update's —
    one of them being queued makes every ``NotScheduled`` drop under that id read as
    reclaimable, including a direct send's, which nothing will reclaim. There is no row that
    would settle it: an id is deliberately not one per message, so the rows carry no finer
    identity to match a drop to its own queueing.

    So the ambiguity is resolved in the direction that cannot duplicate a message. Reading
    ``pending`` for a send that is really finished costs a caller a wait and then a decision
    of its own; reading ``failed`` for one the next start will deliver costs the chat two
    copies, because ``failed`` is what a caller re-sends on.
    """
    detail = row.detail or {}
    if 'max_retries' in detail:
        return True
    stage = detail.get('stage')
    if stage is not None:
        # a JSONField holds whatever was written, so the comparison is against the string
        # this package writes and not against the column's type
        return str(stage) != 'queueing'
    return not queued


def _decide(identifier: uuid.UUID, rows: 'Iterable[TelegramEvent]') -> Outcome:
    """Reduce the rows to one answer, from the newest backwards.

    Ordered by ``-id`` and read once, so this walks a handful of rows rather than sorting
    them again. A ``sent`` row anywhere in the set settles the state whatever came after
    it — a later ``retried`` belongs to a different message under the same id, and a send
    Telegram accepted is not made pending by one that did not.

    A drop is weighed by :func:`_cannot_come_back` rather than counted as a failure, and the
    ``queued`` row it needs is in the same set — which is why the query asks for that kind
    even though a queued row alone only ever means *pending*.
    """
    sent: list[SentMessage] = []
    newest_sent: TelegramEvent | None = None
    failure: TelegramEvent | None = None
    dropped: TelegramEvent | None = None
    progress: TelegramEvent | None = None
    queued = False
    for row in rows:
        if row.kind == EventKind.OUTBOUND_SENT.value:
            sent.append(SentMessage(chat_id=row.chat_id, message_id=row.message_id, at=row.created_at))
            newest_sent = newest_sent or row
        elif row.kind == EventKind.OUTBOUND_FAILED.value:
            failure = failure or row
        elif row.kind == EventKind.OUTBOUND_DROPPED.value:
            dropped = dropped or row
        else:
            queued = queued or row.kind == EventKind.OUTBOUND_QUEUED.value
            progress = progress or row
    if failure is None and dropped is not None:
        # weighed after the loop, because the rule needs to know whether the message was
        # ever queued and that row may come later in the walk than the drop
        if _cannot_come_back(dropped, queued=queued):
            failure = dropped
        else:
            progress = progress or dropped
    if newest_sent is not None:
        return Outcome(
            state=OutcomeState.SENT,
            correlation_id=identifier,
            sent=tuple(sent),
            attempt=newest_sent.attempt,
        )
    if failure is not None:
        return Outcome(
            state=OutcomeState.FAILED,
            correlation_id=identifier,
            error=failure.error or failure.error_code,
            attempt=failure.attempt,
        )
    if progress is not None:
        return Outcome(state=OutcomeState.PENDING, correlation_id=identifier, attempt=progress.attempt)
    return Outcome(state=OutcomeState.UNKNOWN, correlation_id=identifier)


def outcome(correlation_id: uuid.UUID | str) -> Outcome:
    """Return what the feed knows about one message, by the id ``send()`` gave back."""
    identifier = _identifier(correlation_id)
    _refuse_where_nothing_records()
    return _decide(identifier, _rows(identifier))


async def aoutcome(correlation_id: uuid.UUID | str) -> Outcome:
    """Return the same answer without blocking the loop this coroutine runs on.

    The synchronous twin queries on the calling thread, which under ASGI is the thread
    serving requests. Everything else about it is identical, refusals included.
    """
    identifier = _identifier(correlation_id)
    _refuse_where_nothing_records()
    from django_aiogram.models import TelegramEvent  # noqa: PLC0415 - as in `_rows`

    query = (
        TelegramEvent.objects.using(log_alias())
        .filter(correlation_id=identifier, kind__in=DECIDING_KINDS)
        .order_by('-id')
    )
    return _decide(identifier, [row async for row in query])
