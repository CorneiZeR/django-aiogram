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
    """Raise where an outcome cannot exist, rather than reporting one that never will.

    Two configurations, and both would otherwise answer ``unknown`` for ever — the one
    answer a caller is meant to read as *not yet*. With the table off nothing is written at
    all; with ``EVENT_LOG_KINDS`` naming a set that leaves ``outbound.sent`` out, the send
    is recorded and its result is not.
    """
    if not recorder.enabled:
        msg = (
            f"{SETTINGS_NAME}['EVENT_LOG'] is False, so nothing records what became of a "
            f'message and there is no outcome to read.'
        )
        raise OutcomesUnavailableError(msg)
    if not recorder.wants(EventKind.OUTBOUND_SENT.value):
        msg = (
            f"{SETTINGS_NAME}['EVENT_LOG_KINDS'] does not include "
            f"'{EventKind.OUTBOUND_SENT.value}', so a message's result is never recorded."
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


def _decide(identifier: uuid.UUID, rows: 'Iterable[TelegramEvent]') -> Outcome:
    """Reduce the rows to one answer, from the newest backwards.

    Ordered by ``-id`` and read once, so this walks a handful of rows rather than sorting
    them again. A ``sent`` row anywhere in the set settles the state whatever came after
    it — a later ``retried`` belongs to a different message under the same id, and a send
    Telegram accepted is not made pending by one that did not.
    """
    sent: list[SentMessage] = []
    newest_sent: TelegramEvent | None = None
    failure: TelegramEvent | None = None
    progress: TelegramEvent | None = None
    for row in rows:
        if row.kind == EventKind.OUTBOUND_SENT.value:
            sent.append(SentMessage(chat_id=row.chat_id, message_id=row.message_id, at=row.created_at))
            newest_sent = newest_sent or row
        elif row.kind in (EventKind.OUTBOUND_FAILED.value, EventKind.OUTBOUND_DROPPED.value):
            failure = failure or row
        else:
            progress = progress or row
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
