"""Which queues this deployment has, and why naming one means declaring it.

A queue is the isolation boundary: a client that must not wait behind everybody else gets one
of their own. So a bot's ``QUEUE`` is a name, and a name that is not declared anywhere is a
typo -- which without this would be a queue nothing consumes, holding messages nobody is
waiting for, reported as a healthy send.

Two places declare one, and both are read: ``QUEUES`` in the settings, for a deployment that
knows its queues when it deploys, and the ``TelegramQueue`` table, for one whose clients
arrive at run time. A deployment that declares neither has one queue -- whatever its transport
addresses -- and no bot may name anything.

**A table that cannot be read declares nothing and refuses nothing.** The refusal exists to
catch a typo, and a database blinking is not evidence of one: a bot serving a queue it has
always served must not stop because the row confirming it is momentarily unreachable.

That is *could not read*, which is narrower than *read nothing*. A database that is down, or
one this app has not been migrated on, is a table whose answer is unknown -- so nothing is
refused. A deployment with no database at all is a different thing: there is no table to know
better, the settings are the whole declaration, and a name that is not in them is a typo. Told
apart by the failure the ORM raises, because those two states are not each other.
"""

import logging
from typing import TYPE_CHECKING

from django.core.exceptions import ImproperlyConfigured
from django.db import DatabaseError

from django_aiogram.config.defaults import DEFAULTS
from django_aiogram.config.settings import SETTINGS_NAME, conf

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping
    from typing import Any

__all__ = ('declaration', 'declared', 'named', 'refuse_undeclared', 'served_by', 'settings_for')

logger = logging.getLogger('django_aiogram')


def named(settings: 'Mapping[str, object] | None' = None) -> str:
    """Return the queue one bot names, or the empty string for the transport's own."""
    resolved = conf if settings is None else settings
    return str(resolved.get('QUEUE', '') or '').strip()


def in_settings() -> tuple[str, ...]:
    """Return the queues ``QUEUES`` declares, in the order it names them."""
    declared_names = conf['QUEUES'] or ()
    if isinstance(declared_names, str | bytes):
        # a bare string is a collection of its characters, so it would declare 'v', 'i', 'p'
        # -- and `E058` reports it. Read as nothing rather than as that
        return ()
    return tuple(str(name).strip() for name in declared_names if str(name).strip())


def in_table() -> 'tuple[str, ...] | None':
    """Return the queues the table declares, or ``None`` where it could not be read at all.

    ``None`` rather than an empty tuple, and the difference is what the refusal turns on: a
    table with no rows declares nothing, and a table nobody could reach says nothing. Read as
    the same thing, an unreachable database would refuse every queue a project ever created
    through its own interface.
    """
    try:
        # deferred: the ORM, and this module is imported wherever a bot's settings are
        from django_aiogram.models import TelegramQueue  # noqa: PLC0415 - as above

        return tuple(TelegramQueue.objects.values_list('name', flat=True))
    except DatabaseError:
        # down, or not migrated here: the table's answer is unknown, so nothing is declared
        # and nothing is denied
        logger.warning('could not read the declared queues; going by the settings alone')
        return None
    except Exception:  # noqa: BLE001 - no database at all, or none this process may touch
        # a different state, and not an outage: there is no table to know better than the
        # settings, so the settings are the whole declaration and a name outside them is a typo
        logger.debug('no queue table to read; the settings are the whole declaration')
        return ()


def declaration() -> 'tuple[frozenset[str], bool]':
    """Return every declared queue and whether the table was readable, from **one** read.

    Both answers together, because asking for them separately is two reads of the table and
    two chances to disagree: the first failing and the second succeeding leaves a caller with
    the settings alone and a table it believes it read, which reports a queue that exists as a
    typo. One read cannot say two things.
    """
    rows = in_table()
    return frozenset(in_settings()) | frozenset(rows or ()), rows is not None


def declared() -> frozenset[str]:
    """Return every queue name this deployment has declared, from both places."""
    return declaration()[0]


def refuse_undeclared(settings: 'Mapping[str, object] | None' = None) -> None:
    """Raise where a bot names a queue nothing declares, naming both.

    Raised rather than logged, and at the moment the transport for that queue is built: a
    message published to a name nobody consumes is a send that reports success and arrives
    nowhere, which is the one outcome this package is built to not have.
    """
    wanted = named(settings)
    if not wanted:
        return
    known, readable = declaration()
    if not readable:
        # nothing was declared *or* denied: see `in_table`. A queue this deployment has always
        # served must not stop being served because the row naming it is unreachable
        return
    if wanted in known:
        return
    msg = (
        f'{SETTINGS_NAME}["QUEUE"] is {wanted!r}, which this deployment has not declared. '
        f'Declared: {", ".join(sorted(known)) or "none"}. '
        f'Add it to {SETTINGS_NAME}["QUEUES"] or to the TelegramQueue table -- a queue nothing '
        'declares is one nothing consumes, and a message published to it is lost quietly.'
    )
    raise ImproperlyConfigured(msg)


def in_pools(pools: 'Iterable[str]') -> tuple[str, ...]:
    """Return the declared queues whose pool is one of these, in a stable order.

    A pool is the label a queue is *served* by, and selecting on it is what a deployment with
    clients arriving at run time needs: a container started with a pool serves a queue created
    after it started, with no redeploy and no glob -- a glob would include a queue by the
    accident of its name.
    """
    wanted = {str(pool).strip() for pool in pools if str(pool).strip()}
    if not wanted:
        return ()
    try:
        from django_aiogram.models import TelegramQueue  # noqa: PLC0415 - the ORM, deferred

        rows = TelegramQueue.objects.filter(pool__in=sorted(wanted)).order_by('name')
        return tuple(rows.values_list('name', flat=True))
    except DatabaseError:
        # a pool is a table's answer and nothing else's, so an unreadable table means this
        # container was asked to serve a set nobody can tell it. Raised rather than read as
        # empty: a consumer serving no queues looks healthy and delivers nothing
        logger.exception('could not read the queues in the pools this container was asked to serve')
        raise


def served_by(queues: 'Iterable[str] | None' = None, pools: 'Iterable[str] | None' = None) -> tuple[str, ...]:
    """Return the queues one container serves, from what it was told and what is declared.

    Three answers, in this order: the queues named, the queues in the pools named, and -- when
    neither was given -- the process's own single queue, which is what every deployment before
    this had. Named and pooled together are a union, because a container serving a pool plus
    one queue by name is the shape a migration between pools takes.

    A name nothing declares is refused here rather than consumed as an empty queue: a
    container consuming a queue nobody publishes to is a container that looks healthy and
    delivers nothing, which is the same failure the publish side refuses.
    """
    asked = [str(name).strip() for name in (queues or ()) if str(name).strip()]
    wanted_pools = [str(pool).strip() for pool in (pools or ()) if str(pool).strip()]
    if not asked and not wanted_pools:
        # told nothing, which is every deployment before there were several queues
        return (named(),)
    # *told* nothing is not the same as *resolving* to nothing: a pool that holds no queues
    # comes back empty here, and the caller refuses the run rather than consuming the
    # process's own queue instead -- which would be a container serving somebody else's work
    from_pools = in_pools(wanted_pools)
    known, readable = declaration()
    if readable:
        unknown = sorted({name for name in asked if name not in known})
        if unknown:
            msg = (
                f'These queues are not declared: {", ".join(unknown)}. '
                f'Declared: {", ".join(sorted(known)) or "none"}. '
                f'Add them to {SETTINGS_NAME}["QUEUES"] or to the TelegramQueue table -- a '
                'container consuming a queue nothing publishes to looks healthy and delivers nothing.'
            )
            raise ImproperlyConfigured(msg)
    # the order asked for first, then the pools', and each queue once: a container told
    # `--queues vip --pools vip` serves `vip` once, not twice
    ordered = list(dict.fromkeys([*asked, *from_pools]))
    return tuple(ordered)


def settings_for(queue: str) -> 'dict[str, Any]':
    """Return the process's settings with one queue named, for a consumer of that queue.

    A plain dict rather than the settings object: it is read as a mapping by the transport and
    by the profile, and both of those are given whatever a bot's resolution produced anyway.

    **Everything the process resolved, not only the package's own keys.** A transport's
    options -- ``KAFKA_BOOTSTRAP``, ``RABBITMQ_URL``, ``REDIS_STREAM_KEY`` -- are not in
    `DEFAULTS`, and a broker built from a mapping without them falls back to its own defaults:
    for a required one that is a refusal, so a container serving a named queue on Kafka,
    RabbitMQ or Redis Streams could not build its transport at all. Measured -- the Kafka case
    in `tests/db/test_prune_queues.py` is what found it.
    """
    resolved = dict(conf.resolved)
    for key, default in DEFAULTS.items():
        resolved.setdefault(key, default)
    resolved['QUEUE'] = queue
    return resolved
