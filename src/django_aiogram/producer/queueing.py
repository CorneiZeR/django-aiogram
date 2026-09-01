"""Everything a queue write does, except the write.

One producer is synchronous and the other awaits, and the only line they cannot share is
the one that hits the transport. Everything around it lives here: the serialization, the
batching, and both event rows -- including the rule that a message lost on the way to the
broker records a drop rather than letting silence imply it was queued.

So each transport is the two lines that write. The consumer knows one payload shape and the
event log has one definition of ``queued``, because neither path owns them.

In two halves as well as in one call. :func:`serialise` always runs where the caller stands;
:func:`publishing` may be held back to the caller's commit, which is what ``TRANSACTIONAL``
does with it. :func:`queueing` is the two together, for every producer that does not wait.
"""

import contextlib
import logging
import time
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from django_aiogram.config.enums import EventKind
from django_aiogram.eventlog.events import new_correlation_id
from django_aiogram.eventlog.recorder import recorder
from django_aiogram.eventlog.records import Event, as_identifier
from django_aiogram.wire.envelope import pack
from django_aiogram.wire.payloads import describe
from django_aiogram.wire.serializers import get_serializer

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

logger = logging.getLogger('django_aiogram')


@dataclass
class Queueing:
    """One write a producer is about to make, and what it stands for.

    Carries the ids so a failure can be recorded against the messages that were
    actually lost: a variadic ``RPUSH`` fails for its whole chunk, not one entry.
    """

    payloads: list[bytes]
    messages: list[tuple[uuid.UUID, dict[str, Any]]]
    queued_at: float


def _dropped(
    function: str,
    messages: list[tuple[uuid.UUID, dict[str, Any]]],
    stage: str,
    error: Exception,
) -> None:
    """Record every message a failure lost, and where it lost them.

    The two ways a message is lost here are **not** the same, and ``stage`` is what tells
    them apart. ``serialising`` — spelled as it is written here, because the value reaches a
    row rather than a reader, and whoever queries the log matches this literal — means the
    payload never left this process, so re-sending it is safe.
    ``queueing`` means the publish raised, and a publish that raised may still have been
    applied: the reply is what went missing, and a variadic ``RPUSH`` or a confirmed AMQP
    publish can both fail that way, so re-sending may duplicate. A broadcast makes that
    distinction the only one available, because the ids go with the exception.
    """
    for identifier, kwargs in messages:
        recorder.record(
            Event(
                kind=EventKind.OUTBOUND_DROPPED.value,
                correlation_id=identifier,
                function=function,
                chat_id=as_identifier(kwargs.get('chat_id')),
                error_code=type(error).__name__,
                error=str(error),
                detail={'stage': stage},
            )
        )


def serialise(function: str, messages: list[tuple[uuid.UUID, dict[str, Any]]]) -> Queueing:
    """Turn the calls into payloads, and stamp the moment they were made.

    Guarded, not left to the caller: a payload that cannot be serialized loses its message
    exactly as a refused write does, and for a chunk the ids go with the exception — so
    those drop rows are the only record of which messages were lost. Resolving the
    serializer sits outside that guard on purpose: a misconfigured ``SERIALIZER`` fails
    identically for every send ever made, and the exception is where that belongs, not a
    drop row per message for as long as it stays misconfigured.

    Always on the calling thread, including when ``TRANSACTIONAL`` holds the publish back
    to the commit. Two reasons, and the second is the one that matters: a payload the
    project cannot serialize raises where the project wrote the call rather than out of a
    commit hook, and the bytes are what a deferred publish then carries.

    ``**kwargs`` already copies the mapping a caller passed, so the shallow layer was never
    at risk — the values under it are, and they are the ones a project holds a handle to: a
    keyboard, a list of entities, a dict it builds and reuses. Encoding here freezes those
    where the call was written, rather than reading them again from a commit hook that runs
    after the caller has moved on.
    """
    queued_at = time.time()
    serializer = get_serializer()
    try:
        return Queueing(
            payloads=[
                serializer.dumps(pack(function, kwargs, identifier, queued_at)) for identifier, kwargs in messages
            ],
            messages=messages,
            queued_at=queued_at,
        )
    except Exception as error:
        _dropped(function, messages, 'serialising', error)
        raise


@contextlib.contextmanager
def publishing(function: str, write: Queueing) -> 'Iterator[Queueing]':
    """Wrap the one line that hits the transport, and record what it did.

    The step that cannot be shared between a synchronous producer and an asynchronous one
    is the ``await`` — the language will not allow it. Everything around it is here: the
    drop row for a write that raised, and the ``queued`` row for one that did not.

    So each transport is the two lines that write, and nothing else. The consumer knows one
    payload shape and the event log has one definition of ``queued``; neither can drift
    between the two paths, because neither path owns them.
    """
    try:
        yield write
    except Exception as error:
        _dropped(function, write.messages, 'queueing', error)
        raise
    if not recorder.active:
        # nothing keeps the table and nothing listens, so there is no event to make
        return
    # two gates, not one: whether to record at all is a different question from
    # whether to summarize the arguments, and describing them is the expensive
    # half. A metrics receiver counts sends; it does not read message bodies
    described = recorder.wants_payload
    for identifier, kwargs in write.messages:
        recorder.record(
            Event(
                kind=EventKind.OUTBOUND_QUEUED.value,
                correlation_id=identifier,
                created_at=write.queued_at,
                function=function,
                chat_id=as_identifier(kwargs.get('chat_id')),
                detail=describe(kwargs) if described else None,
            )
        )


@contextlib.contextmanager
def queueing(function: str, messages: list[tuple[uuid.UUID, dict[str, Any]]]) -> 'Iterator[Queueing]':
    """Everything a queue write does, except the write.

    The two halves in one call, for the producer that publishes where it stands. A producer
    that hands the publish to a commit hook holds them apart instead — see
    :func:`~django_aiogram.producer.committing.defer` — because only the second half may
    wait.
    """
    with publishing(function, serialise(function, messages)) as write:
        yield write


def chunks(
    chat_ids: 'Iterable[int | str]',
    chunk_size: int,
    kwargs: dict[str, Any],
) -> 'Iterator[list[tuple[uuid.UUID, dict[str, Any]]]]':
    """Group the chats into the batches one write covers.

    :func:`serialise` runs on one chunk at a time, which is what keeps peak memory bounded:
    a ``BufferedInputFile`` payload times fifty thousand chats would otherwise all exist at
    once. ``TRANSACTIONAL`` is the exception and cannot not be: a write held for the commit
    is a payload held in memory, so a broadcast inside a transaction does carry the whole
    batch until the block ends.

    The method is validated by ``TelegramBot._accept_bulk`` before either caller gets here,
    so a refused one never reaches this generator.
    """
    size = max(1, int(chunk_size))
    chunk: list[tuple[uuid.UUID, dict[str, Any]]] = []
    for chat_id in chat_ids:
        chunk.append((new_correlation_id(), {**kwargs, 'chat_id': chat_id}))
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk
