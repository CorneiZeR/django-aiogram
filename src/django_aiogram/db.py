"""The connection reset the bot worker has no request cycle to give it.

Django closes a broken or obsolete connection in ``close_old_connections()``, which is wired to
the ``request_started`` and ``request_finished`` signals **only**. A bot worker has no request
cycle, so nothing in a long-running consumer or webhook process ever calls it: a connection that
dies once -- a server restart, an idle timeout, a killed backend -- stays dead in that process for
ever, and every handler after it raises ``InterfaceError: connection already closed``.

Measured in production on 2026-08-27: Postgres restarted at 23:43, the first update after it
arrived at 03:22, and every handler raised for the next ten hours until the container was
restarted. **Outbound delivery was unaffected** -- the consumer kept sending and the probe kept
answering, because neither touches the database -- so `docker ps` and
``python -m django_aiogram.healthcheck`` both said the bot was fine while every deeplink and every
inline button silently did nothing. aiogram logs the exception and leaves the update unhandled, so
the person pressing the button gets no answer at all.

``CONN_MAX_AGE`` is not the fix and never was: at ``0``, the default, the connection is still only
closed inside that same request hook.

So this brackets every update with the reset, which is what
:class:`django.core.handlers.asgi.ASGIHandler` does around a request -- and for the same reason.
"""

import logging
from typing import Any

from aiogram import BaseMiddleware
from asgiref.sync import sync_to_async
from django.db import close_old_connections

logger = logging.getLogger('django_aiogram')


async def reset_connections() -> None:
    """Close what Django would close between requests, and never fail an update for it.

    ``thread_sensitive=True`` is load-bearing rather than a default copied over. Handlers reach the
    ORM through Django's async API (``afirst``, ``aget``, ...) and through ``sync_to_async``, both
    of which run on asgiref's thread-sensitive executor -- a different thread from the event loop.
    A Django connection is thread-local, so closing on the loop thread would close a connection
    nobody used and leave the broken one exactly where it was: the fix would look applied and
    change nothing. Django's own ASGI handler sends ``request_started`` through the same flag.

    The wrapper is built per call rather than once at import, which costs 2.3 microseconds --
    measured, against an update that has a network round trip in it -- and buys the thing a
    module-level constant cannot: the function it closes through is looked up when it is called,
    so a test can stand in for Django's and still be testing the flag above rather than one of its own.

    Logged rather than raised, the same shape and the same reason as the event log's own
    :func:`~django_aiogram.eventlog.writer.close_connections`: a connection this cannot release
    is worth a line in the log, and worth nothing at all if it costs the update.
    """
    try:
        await sync_to_async(close_old_connections, thread_sensitive=True)()
    except Exception:
        logger.exception('could not reset the database connections around an update')


class DatabaseConnectionMiddleware(BaseMiddleware):
    """Reset the connections before and after every update.

    Both sides, and each does a different job. The **leading** close is what recovers from a
    connection that died while the process was idle -- the outage above, where the first update
    after a database restart was the one that broke. The **trailing** close is what returns the
    connection at ``CONN_MAX_AGE=0`` instead of parking one open socket per worker between
    updates.

    Unconditional, unlike the recording middleware, which returns before it is built when nothing
    reads events: the event log is optional, a live database connection is not.
    """

    async def __call__(
        self,
        handler: Any,  # noqa: ANN401 - aiogram types this as a bare callable
        event: Any,  # noqa: ANN401 - any TelegramObject, and this looks at none of it
        data: dict[str, Any],
    ) -> Any:  # noqa: ANN401 - whatever the handler chain returns
        """Bracket the handler chain with the reset."""
        await reset_connections()
        try:
            return await handler(event, data)
        finally:
            await reset_connections()
