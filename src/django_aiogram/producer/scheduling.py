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
from django.utils import timezone

from django_aiogram.config.enums import EventKind
from django_aiogram.eventlog.events import worker_identity
from django_aiogram.eventlog.recorder import recorder
from django_aiogram.eventlog.records import Event, as_identifier

if TYPE_CHECKING:
    from django_aiogram.models import TelegramScheduledSend
    from django_aiogram.producer.queueing import Queueing

logger = logging.getLogger('django_aiogram')

__all__ = ('cancel', 'claim', 'due_moment', 'schedule')


def due_moment(eta: datetime.datetime) -> datetime.datetime:
    """Read a caller's ``eta``, refusing the one shape that silently means another time.

    A naive datetime under ``USE_TZ`` is not a moment: Django would attach a timezone to it
    on the way to the database, and *which* it attaches is the project's ``TIME_ZONE`` rather
    than anything the caller said. A message an hour early is not a value worth guessing at,
    so it is refused by name.

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
    # a project with USE_TZ off keeps naive datetimes throughout, and one here is the same
    # kind of value its own columns hold
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

    rows = [
        TelegramScheduledSend(
            correlation_id=identifier,
            due_at=due_at,
            function=function,
            chat_id=as_identifier(kwargs.get('chat_id')),
            payload=payload,
        )
        for (identifier, kwargs), payload in zip(write.messages, write.payloads, strict=True)
    ]
    TelegramScheduledSend.objects.bulk_create(rows)
    if not recorder.active:
        return
    for (identifier, kwargs), detail in zip(write.messages, write.details, strict=True):
        recorder.record(
            Event(
                kind=EventKind.OUTBOUND_SCHEDULED.value,
                correlation_id=identifier,
                function=function,
                chat_id=as_identifier(kwargs.get('chat_id')),
                detail={**(detail or {}), 'due_at': due_at.isoformat()},
            )
        )


def claim(limit: int, *, now: datetime.datetime | None = None) -> list['TelegramScheduledSend']:
    """Take ownership of up to ``limit`` due rows, and return the ones this call won.

    A compare-and-set update rather than ``SELECT ... FOR UPDATE SKIP LOCKED``, which is not
    available on SQLite -- and this suite runs there. Each row is claimed by an update
    filtered on ``claimed_at`` still being null, so two movers racing for one row produce one
    winner and one rowcount of zero, on every database this package supports.

    One transaction per row rather than one for the batch: a mover that dies with a batch
    claimed would strand every row in it until an operator noticed, where one row is one
    message and the rest stay available to whoever is still running.
    """
    from django_aiogram.models import TelegramScheduledSend  # noqa: PLC0415 - as above

    moment = timezone.now() if now is None else now
    worker = worker_identity()
    available = TelegramScheduledSend.objects.filter(claimed_at__isnull=True, due_at__lte=moment).order_by(
        'due_at', 'id'
    )
    won = []
    # the ids first, so the loop below is not walking a queryset it is also mutating
    for row in list(available[:limit]):
        with transaction.atomic():
            taken = TelegramScheduledSend.objects.filter(pk=row.pk, claimed_at__isnull=True).update(
                claimed_at=moment, claimed_by=worker
            )
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
