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

from django_aiogram.config.settings import SETTINGS_NAME, conf

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = ('declared', 'named', 'readable', 'refuse_undeclared')

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


def readable() -> bool:
    """Whether the queue table could be read, which decides whether silence means anything."""
    return in_table() is not None


def declared() -> frozenset[str]:
    """Return every queue name this deployment has declared, from both places."""
    return frozenset(in_settings()) | frozenset(in_table() or ())


def refuse_undeclared(settings: 'Mapping[str, object] | None' = None) -> None:
    """Raise where a bot names a queue nothing declares, naming both.

    Raised rather than logged, and at the moment the transport for that queue is built: a
    message published to a name nobody consumes is a send that reports success and arrives
    nowhere, which is the one outcome this package is built to not have.
    """
    wanted = named(settings)
    if not wanted:
        return
    rows = in_table()
    if rows is None:
        # nothing was declared *or* denied: see `in_table`. A queue this deployment has always
        # served must not stop being served because the row naming it is unreachable
        return
    known = frozenset(in_settings()) | frozenset(rows)
    if wanted in known:
        return
    msg = (
        f'{SETTINGS_NAME}["QUEUE"] is {wanted!r}, which this deployment has not declared. '
        f'Declared: {", ".join(sorted(known)) or "none"}. '
        f'Add it to {SETTINGS_NAME}["QUEUES"] or to the TelegramQueue table -- a queue nothing '
        'declares is one nothing consumes, and a message published to it is lost quietly.'
    )
    raise ImproperlyConfigured(msg)
