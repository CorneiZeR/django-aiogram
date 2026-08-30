"""What crosses the queue: one thing that happened, and the marker that ends a wait.

Apart from the recorder because these travel further than it does. An :class:`Event` is
built by every seam that records -- the producer, the consumer, the update middleware --
read by the writer on its way into the database, and handed to every ``events_recorded``
receiver, whose field names are public API. The recorder is the thing in the middle, and
none of those need to import it to name what they are passing.

Nothing here reads a setting, takes a lock or touches the ORM.
"""

import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from django_aiogram.eventlog.events import new_correlation_id

#: what a signed BIGINT holds, which is the width of every id column here
ID_RANGE = range(-(2**63), 2**63)


def as_identifier(value: object) -> int | None:
    """Keep what a BIGINT column can hold, and nothing else.

    A Telegram chat_id may be a `@username`, which is a valid destination and
    not a number; `True` is an int to Python and not an id to anyone; and a
    Python integer has no width, so one off an untrusted queue can be wider
    than the column and cost the row it was meant to describe.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if value in ID_RANGE else None


@dataclass(frozen=True)
class Wake:
    """A marker that ends the writer's current wait.

    ``done`` is what makes :meth:`~django_aiogram.eventlog.recorder.EventRecorder.flush`
    honest: the queue going empty means the writer has *taken* the batch, not that it has
    written it.
    """

    done: threading.Event | None = None


@dataclass(frozen=True)
class Event:
    """One thing that happened. Indexed columns first, the rest in ``detail``."""

    kind: str
    correlation_id: uuid.UUID = field(default_factory=new_correlation_id)
    created_at: float = field(default_factory=time.time)
    function: str = ''
    chat_id: int | None = None
    user_id: int | None = None
    message_id: int | None = None
    update_id: int | None = None
    worker: str = ''
    attempt: int = 0
    duration_ms: int | None = None
    error_code: str = ''
    error: str = ''
    #: already JSON-safe by the time it arrives: encoding aiogram objects is the
    #: caller's job, because this module must stay free of aiogram
    detail: dict[str, Any] | None = None
