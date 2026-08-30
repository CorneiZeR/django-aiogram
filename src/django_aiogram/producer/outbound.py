"""One send in flight: what names it, and what says it is finished.

Every stage of a send -- the row the event log writes, the task the loop runs, the
acknowledgement the consumer is waiting for -- needs the same three things, and they are
here rather than in the client so that the consumer, the log and the tests can name a send
without importing the bot.

The settling half is the at-least-once guarantee in two functions: a send that was
cancelled is *not* finished, and saying so is what leaves the message in the in-flight list
for the next start to pick up.
"""

import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from django_aiogram.context import current_correlation_id
from django_aiogram.eventlog.events import new_correlation_id

if TYPE_CHECKING:
    import asyncio

logger = logging.getLogger('django_aiogram')

#: how a scheduled send carries its correlation id, so shutdown can name what
#: it canceled without threading an argument through asyncio
TASK_PREFIX = 'tgbot:'


def resolve_correlation_id(supplied: uuid.UUID | str | None) -> uuid.UUID:
    """Pick the identifier this send belongs to.

    An explicit argument wins, then whatever update is being handled here, then
    a fresh one. Reading the context variable happens synchronously, before any
    scheduling: _hand_off creates its task from a call_soon_threadsafe callback
    whose context belongs to the loop, so a read from in there is empty.
    """
    if isinstance(supplied, uuid.UUID):
        return supplied
    if isinstance(supplied, str) and supplied:
        try:
            return uuid.UUID(supplied)
        except ValueError:
            msg = f'correlation_id must be a UUID, got {supplied!r}.'
            raise ValueError(msg) from None
    return current_correlation_id() or new_correlation_id()


@dataclass(frozen=True)
class Outbound:
    """What every stage of one outbound send needs to name itself."""

    correlation_id: uuid.UUID
    function: str
    call_kwargs: dict[str, Any]


def task_correlation_id(task: 'asyncio.Task[Any]') -> uuid.UUID:
    """Recover the id a scheduled send was named with, or mint one to say so."""
    name = task.get_name()
    if name.startswith(TASK_PREFIX):
        try:
            return uuid.UUID(name.removeprefix(TASK_PREFIX))
        except ValueError:
            pass
    return new_correlation_id()


def completion(on_complete: Callable[[], None]) -> 'Callable[[asyncio.Task[None]], None]':
    """Turn a completion callback into a task done-callback.

    Cancellation is not completion. A send drained away at shutdown never reached
    Telegram, so the consumer must *not* acknowledge it — leaving it in the
    in-flight list is exactly what lets the next start pick it up again, and is
    what makes the at-least-once guarantee true rather than documented.

    Everything else counts as finished, including a send that was refused or that
    gave up: redelivering those would only fail again, which is the contract
    `Delivery.dispatch` has always had.
    """

    def done(task: 'asyncio.Task[None]') -> None:
        """Settle unless the task was canceled, which is the one case that must not.

        Cancellation says the task did not finish, and nothing about what Telegram saw:
        the request may already have been sent, or even acted on, when the cancel landed
        on the await. So the message stays unacknowledged and will be redelivered — which
        can duplicate it, and is the trade this release makes deliberately, because the
        alternative is acknowledging a send whose outcome nobody ever learned.

        This is not a rare path: it is what ``_drain`` does to whatever outlasts
        ``DRAIN_TIMEOUT`` at shutdown.
        """
        if task.cancelled():
            return
        settle(on_complete)

    return done


def settle(on_complete: Callable[[], None] | None) -> None:
    """Say the send is finished, without letting that break anything.

    The callback runs on the loop's callback path, where an exception would be
    reported against the task rather than against whatever the callback does —
    and the send itself is over either way.
    """
    if on_complete is None:
        return
    try:
        on_complete()
    except Exception:
        logger.exception('could not acknowledge a completed send')
