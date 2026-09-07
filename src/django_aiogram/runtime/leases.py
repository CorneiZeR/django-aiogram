"""Which process polls which bot, when several of them could.

``getUpdates`` is exclusive: two processes calling it for one token get a 409 each and half
the updates. With bots arriving at run time there is no deploy-time list to answer that from,
so the processes agree through a row -- one per bot, claimed by compare-and-set.

**A lease rather than a lock.** A process that dies holding a lock strands its bots for ever,
so a claim expires and the next process to ask takes it. That is what makes failover and
balancing the same mechanism, and what it costs is two pollers for as long as the two overlap
-- which is a 409, which is why the renewal happens well inside the lease.

Compare-and-set, and never ``SELECT ... FOR UPDATE SKIP LOCKED``: SQLite does not have it, and
this claim has to be atomic on all four databases this package supports. The pattern is the
one :func:`django_aiogram.producer.scheduling.claim` and ``TelegramReplayClaim`` already use.

**Polling only.** A webhook update arrives wherever the request landed, so nothing has to be
exclusive there, and competing consumers on a queue are something every transport supports.
"""

import datetime
import logging
from typing import TYPE_CHECKING

from django.core.exceptions import ImproperlyConfigured
from django.db import IntegrityError, transaction
from django.db.models import Q
from django.utils import timezone

from django_aiogram.config.defaults import DEFAULTS
from django_aiogram.config.settings import conf
from django_aiogram.eventlog.events import worker_identity

if TYPE_CHECKING:
    from collections.abc import Iterable

    from django_aiogram.models import TelegramBotLease

__all__ = ('claim', 'holder', 'release')

logger = logging.getLogger('django_aiogram')


def holder() -> str:
    """Name this process the way the in-flight list and the feed already name it."""
    return worker_identity()


def _seconds() -> float:
    """How long a lease is believed, never less than a second."""
    try:
        return max(1.0, float(conf['BOT_LEASE_SECONDS']))
    except (TypeError, ValueError, ImproperlyConfigured):
        # `E056` reports this at boot; at run time a deployment that serves nothing because a
        # number is unreadable is the worse answer, so the default decides -- the same trade
        # `Supervisor.interval` makes
        logger.warning('BOT_LEASE_SECONDS is unreadable; falling back to the default')
        return float(DEFAULTS['BOT_LEASE_SECONDS'])


def _ceiling() -> int:
    """How many bots this process may hold at once, or ``0`` for as many as it is given."""
    try:
        return max(0, int(conf['MAX_BOTS_PER_WORKER']))
    except (TypeError, ValueError, ImproperlyConfigured):
        logger.warning('MAX_BOTS_PER_WORKER is unreadable; falling back to the default')
        return int(DEFAULTS['MAX_BOTS_PER_WORKER'])


def claim(bot_ids: 'Iterable[int]') -> tuple[int, ...]:
    """Return the bots this process may poll: the ones it still holds, plus what it could take.

    One call per pass rather than a renew and a claim, because the answer the supervisor needs
    is one set: what it holds *now*. A lease it lost -- it stopped renewing long enough for
    another process to take the bot -- is not in the answer, and the bot is then stopped, which
    is the half a renewal on its own would miss.

    The order asked for is the order taken, so two processes over one set of bots reach the
    same conclusion about who takes what rather than trading them back and forth.
    """
    from django_aiogram.models import TelegramBotLease  # noqa: PLC0415 - the ORM, deferred

    moment = timezone.now()
    me = holder()
    until = moment + datetime.timedelta(seconds=_seconds())
    ceiling = _ceiling()
    held: list[int] = []
    for bot_id in bot_ids:
        if ceiling and len(held) >= ceiling:
            break
        if _take(TelegramBotLease, bot_id, me=me, moment=moment, until=until):
            held.append(bot_id)
    return tuple(held)


def _take(
    model: 'type[TelegramBotLease]',
    bot_id: int,
    *,
    me: str,
    moment: 'datetime.datetime',
    until: 'datetime.datetime',
) -> bool:
    """Hold one bot's lease, whether this process had it or not.

    Three cases and one answer: no row, a row this process holds, a row whose lease has
    lapsed. A row somebody else holds and is renewing matches nothing and the answer is no.
    """
    try:
        with transaction.atomic():
            model.objects.create(bot_id=bot_id, holder=me, claimed_at=moment, expires_at=until)
    except IntegrityError:
        pass
    else:
        return True
    # the *same* condition the update is filtered on, or a lease taken back would lose the
    # race with itself -- the note `producer.scheduling.claim` carries, for the same reason
    mine_or_lapsed = Q(holder=me) | Q(expires_at__lte=moment)
    with transaction.atomic():
        taken = model.objects.filter(mine_or_lapsed, bot_id=bot_id)
        return bool(taken.update(holder=me, claimed_at=moment, expires_at=until))


def release(bot_ids: 'Iterable[int]') -> int:
    """Give up the leases this process holds, so another container takes the bots at once.

    A shutdown that skipped this leaves them stranded for one lease -- correct, and slower
    than it needs to be. Only this process's own rows: a lease somebody else has taken over
    is theirs, and deleting it would put two pollers on one token.
    """
    from django_aiogram.models import TelegramBotLease  # noqa: PLC0415 - as above

    wanted = list(bot_ids)
    if not wanted:
        return 0
    try:
        return TelegramBotLease.objects.filter(bot_id__in=wanted, holder=holder()).delete()[0]
    except Exception:
        # a shutdown is the caller, and a database that refuses this must not be what keeps a
        # container from exiting: the leases lapse on their own one lease later
        logger.exception(
            'could not release bot leases; another container will take them one lease later',
            extra={'tg_bots': len(wanted)},
        )
        return 0
