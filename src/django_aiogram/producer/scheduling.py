"""Sends that are not due yet: writing them down, claiming them, calling them off.

``eta`` cannot be a transport feature. Of the four transports only RabbitMQ delays a message
at all, and only through a plugin or a dead-letter detour -- a Redis list, a stream and a
Kafka topic have nothing to offer -- so building on it would make ``eta`` work on a quarter
of the deployments, against everything ``BROKER`` promises. The wait therefore happens above
the broker contract, in a table, and a row that comes due becomes an ordinary queued message
on whichever transport is configured.

What is here is the three operations that table needs and nothing about the loop that runs
them: :mod:`django_aiogram.management.commands.tgbot_dispatch_scheduled` is the loop.
"""

import datetime
import logging
import uuid
from typing import TYPE_CHECKING

from django.conf import settings as django_settings
from django.core.exceptions import ImproperlyConfigured
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from django_aiogram.config.enums import EventKind
from django_aiogram.eventlog.events import worker_identity
from django_aiogram.eventlog.recorder import recorder
from django_aiogram.eventlog.records import Event, as_identifier
from django_aiogram.producer.committing import after_commit

if TYPE_CHECKING:
    from django_aiogram.models import TelegramScheduledSend
    from django_aiogram.producer.queueing import Queueing

logger = logging.getLogger('django_aiogram')

__all__ = ('DEFAULT_LEASE', 'aschedule', 'cancel', 'claim', 'due_moment', 'schedule')

#: how long a claim is believed before another mover may take the row back. A mover that dies
#: between claiming and deleting would otherwise strand its rows for ever -- the recovery on
#: offer was "an operator clears the claim by hand", which is not one. Long enough that a slow
#: publish is not overtaken, short enough that a crash is not a message nobody sends
DEFAULT_LEASE = 300


def due_moment(eta: datetime.datetime) -> datetime.datetime:
    """Read a caller's ``eta``, refusing the one shape that silently means another time.

    Both mismatches are refused, and each for its own reason.

    A naive datetime under ``USE_TZ`` is not a moment: Django would attach a timezone to it
    on the way to the database, and *which* it attaches is the project's ``TIME_ZONE`` rather
    than anything the caller said. A message an hour early is not a value worth guessing at.

    An aware datetime with ``USE_TZ`` **off** is refused here because the database refuses it
    too -- measured, SQLite answers *does not support timezone-aware datetimes when USE_TZ is
    False*. Left to the backend it surfaces from inside ``bulk_create`` as a ``ValueError``
    about SQLite, which is a long way from the ``eta`` that caused it.

    An ``eta`` already past is accepted and comes due at once. Refusing it would make clock
    skew between a web tier and a database a caller's problem, and "as soon as possible" is a
    reasonable thing to schedule.
    """
    if not isinstance(eta, datetime.datetime):
        msg = f'eta must be a datetime, got {type(eta).__name__}.'
        raise ImproperlyConfigured(msg)
    if timezone.is_naive(eta) and django_settings.USE_TZ:
        msg = (
            'eta must be an aware datetime under USE_TZ; a naive one is read in the '
            "project's TIME_ZONE rather than in whatever the caller meant."
        )
        raise ImproperlyConfigured(msg)
    if timezone.is_aware(eta) and not django_settings.USE_TZ:
        msg = (
            'eta must be a naive datetime while USE_TZ is False, which is what this '
            "project's own datetime columns hold; the database refuses an aware one."
        )
        raise ImproperlyConfigured(msg)
    return eta


def schedule(function: str, write: 'Queueing', due_at: datetime.datetime) -> None:
    """Write one row per message, and record that each was scheduled.

    The payload goes in as :func:`~django_aiogram.producer.queueing.serialise` produced it,
    which is what makes the mover's publish byte-identical to an immediate one -- no second
    serialization, and no chance of the bytes drifting with the settings between now and
    then.

    On the caller's connection, deliberately: a scheduled send made inside ``atomic()`` is
    rolled back by the transaction that made it, needing nothing from ``TRANSACTIONAL``.
    """
    from django_aiogram.models import TelegramScheduledSend  # noqa: PLC0415 - django.db, not at import

    TelegramScheduledSend.objects.bulk_create(_rows(function, write, due_at))
    _record(function, write, due_at)


async def aschedule(function: str, write: 'Queueing', due_at: datetime.datetime) -> None:
    """Write the same rows, awaited, because the synchronous ones cannot come from a loop.

    Measured: the synchronous ``bulk_create`` from a coroutine raises
    ``SynchronousOnlyOperation``, so the awaiting producers did not "block like their
    synchronous twins" -- a comment that said so was simply wrong, and an ``eta`` on
    ``aenqueue`` raised. ``abulk_create`` has been there since Django 4.1 and this package
    floors at 5.2, so there was never a reason to reach for the blocking one.

    The event is still recorded through :func:`~django_aiogram.producer.committing.after_commit`,
    which is synchronous and touches no ORM: it hands a callback to Django or calls it, and the
    recorder's queue is a queue.
    """
    from django_aiogram.models import TelegramScheduledSend  # noqa: PLC0415 - as above

    await TelegramScheduledSend.objects.abulk_create(_rows(function, write, due_at))
    _record(function, write, due_at)


def _rows(function: str, write: 'Queueing', due_at: datetime.datetime) -> list['TelegramScheduledSend']:
    """One row per message, ready for either bulk create."""
    from django_aiogram.models import TelegramScheduledSend  # noqa: PLC0415 - as above

    return [
        TelegramScheduledSend(
            correlation_id=identifier,
            due_at=due_at,
            function=function,
            chat_id=as_identifier(kwargs.get('chat_id')),
            payload=payload,
        )
        for (identifier, kwargs), payload in zip(write.messages, write.payloads, strict=True)
    ]


def _record(function: str, write: 'Queueing', due_at: datetime.datetime) -> None:
    """Record the scheduling, once the transaction that wrote the rows has committed.

    After the commit and not before, because the rows are the caller's own write: a block
    that rolls back removes them, and an event queued already would outlive them. The
    recorder's writer commits on its own connection and on its own schedule, so nothing else
    would ever take that event back -- a durable row about a send that never existed.
    """
    if not recorder.active:
        return
    scheduled = [
        Event(
            kind=EventKind.OUTBOUND_SCHEDULED.value,
            correlation_id=identifier,
            function=function,
            chat_id=as_identifier(kwargs.get('chat_id')),
            detail={**(detail or {}), 'due_at': due_at.isoformat()},
        )
        for (identifier, kwargs), detail in zip(write.messages, write.details, strict=True)
    ]

    def record_them() -> None:
        """Hand the batch over, which never blocks and never raises."""
        for event in scheduled:
            recorder.record(event)

    after_commit(record_them)


def claim(
    limit: int,
    *,
    lease: int = DEFAULT_LEASE,
    now: datetime.datetime | None = None,
) -> list['TelegramScheduledSend']:
    """Take ownership of up to ``limit`` due rows, and return the ones this call won.

    A compare-and-set update rather than ``SELECT ... FOR UPDATE SKIP LOCKED``, which is not
    available on SQLite -- and this suite runs there. Each row is claimed by an update
    filtered on the same condition the select used, so two movers racing for one row produce
    one winner and one rowcount of zero, on every database this package supports. The filter
    alone is not enough for that: both can *select* the row before either writes.

    **A claim is a lease, not a deed.** A mover that dies between claiming a row and deleting
    it would otherwise strand the message for ever, since every later pass filters claimed
    rows out -- so a claim older than ``lease`` is available again. What that costs is a
    message going out twice where the mover died *after* publishing, which is the trade this
    package makes everywhere: at-least-once, never silent loss.

    One transaction per row rather than one for the batch: a mover that dies holding a batch
    releases them one lease later either way, and one row is one message.
    """
    from django_aiogram.models import TelegramScheduledSend  # noqa: PLC0415 - as above

    moment = timezone.now() if now is None else now
    worker = worker_identity()
    # never claimed, or claimed so long ago that the mover holding it is not coming back
    unheld = Q(claimed_at__isnull=True)
    if lease > 0:
        unheld |= Q(claimed_at__lt=moment - datetime.timedelta(seconds=lease))
    available = TelegramScheduledSend.objects.filter(unheld, due_at__lte=moment).order_by('due_at', 'id')
    won = []
    # the ids first, so the loop below is not walking a queryset it is also mutating
    for row in list(available[:limit]):
        with transaction.atomic():
            # the *same* condition, or a lease taken back would always lose the race with
            # itself: filtering on `claimed_at__isnull` alone can never match a stale claim
            taken = TelegramScheduledSend.objects.filter(unheld, pk=row.pk).update(claimed_at=moment, claimed_by=worker)
        if taken:
            won.append(row)
    return won


def cancel(correlation_id: uuid.UUID) -> int:
    """Delete the rows this id still has waiting, and say how many there were.

    Unclaimed rows only. A claimed one is already on its way to the broker -- or has reached
    it and is waiting to be deleted -- and deleting it here would neither stop the message
    nor be visible to the mover that owns it. So the number this returns is what was called
    off, not what was scheduled, and the difference is what a caller has to read: zero means
    nothing was waiting, which is the same answer for an id that was never scheduled and for
    one whose message is already going out.
    """
    from django_aiogram.models import TelegramScheduledSend  # noqa: PLC0415 - as above

    deleted, _ = TelegramScheduledSend.objects.filter(correlation_id=correlation_id, claimed_at__isnull=True).delete()
    return deleted
