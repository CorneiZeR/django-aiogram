"""What every bot in this process shares, whatever its settings say.

Three objects, and one reason for all three. A ``Router`` cannot be attached to two
dispatchers -- aiogram refuses the second -- so a dispatcher per bot would be a *handler tree*
per bot, and whether a project's handlers served a bot would then depend on whether its
transport settings happened to match another's. Handlers belong to the project, so the
dispatcher they hang from is the process's.

The FSM store comes with the dispatcher, which is why ``FSM_STORAGE`` is process-scoped; the
shipped Redis store keys on the bot as well as the chat, and that is what keeps one person's
state with each bot rather than shared between them.

The HTTP session is here for a plainer reason: nothing this package configures varies it, and
one connector shared by twenty bots is the difference between a `Bot` that costs a few
kilobytes and one that costs a pool.

Everything is built on first use and rebuilt after a close, so nothing here runs at import.
"""

import asyncio
import logging
import threading
from typing import TYPE_CHECKING, Any

from aiogram import Dispatcher, Router
from django.core.signals import setting_changed
from django.dispatch import receiver

from django_aiogram.config.bots import BOTS_SETTINGS_NAME
from django_aiogram.config.settings import SETTINGS_NAME

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from aiogram.client.session.aiohttp import AiohttpSession

    #: what the two closers hand back: the work, for a caller that owns a loop to run it on
    Closing = Coroutine[Any, Any, None] | None

logger = logging.getLogger('django_aiogram')

__all__ = ('close_session', 'close_storage', 'dispatcher', 'router', 'session')

_lock = threading.RLock()
_router: Router | None = None
_dispatcher: Dispatcher | None = None
_session: 'AiohttpSession | None' = None
#: dispatchers a settings change retired, kept until something with a loop can close their
#: stores. Dropping one without closing it leaks whatever its store holds -- a `RedisStorage`
#: owns an async client nothing else releases
_retired: list[Dispatcher] = []
#: bumped whenever what a bot was built from moves: the settings changed, or the session it
#: holds was closed. A `TelegramBot` compares it against the one it built its aiogram `Bot` at
#: and rebuilds when they differ, which is what keeps a cached `Bot` from outliving its token
#: or its session
_generation = 0


def generation() -> int:
    """Return the number that changes whenever a cached aiogram ``Bot`` has gone stale."""
    with _lock:
        return _generation


def _moved_on() -> None:
    """Say that anything built from the process's objects or its settings is now stale."""
    global _generation  # noqa: PLW0603 - one counter per process, like what it describes
    with _lock:
        _generation += 1


def router() -> Router:
    """Return the one router every handler in this process is registered on."""
    global _router  # noqa: PLW0603 - one per process, which is the whole subject of this module
    with _lock:
        if _router is None:
            _router = Router()
        return _router


def dispatcher() -> Dispatcher:
    """Return the one dispatcher, with its FSM store and the middleware every update goes through.

    The order of the two registrations is the contract, not a detail.
    :class:`~django_aiogram.db.DatabaseConnectionMiddleware` goes on first and so runs
    outermost, which is what makes the connection reset the first thing that happens to an
    update and the last: a recording middleware that wrote its row through a dead connection
    would be the same outage one frame further in.

    And it is unconditional, where `install_instrumentation` returns before building anything
    if nothing reads events. The event log is optional; a live database connection is not.
    """
    global _dispatcher  # noqa: PLW0603 - as above
    with _lock:
        if _dispatcher is None:
            # deferred: each reaches aiogram or the ORM, and a process that only queues must
            # be able to import this module without paying for either
            from django_aiogram.db import DatabaseConnectionMiddleware  # noqa: PLC0415 - as above
            from django_aiogram.eventlog.instrumentation import install_instrumentation  # noqa: PLC0415 - as above
            from django_aiogram.producer.from_settings import build_storage  # noqa: PLC0415 - as above

            built = Dispatcher(storage=build_storage())
            built.update.outer_middleware.register(DatabaseConnectionMiddleware())
            install_instrumentation(built)
            built.include_router(_detached())
            _dispatcher = built
        return _dispatcher


def _detached() -> Router:
    """Return the shared router, ready to be attached to a dispatcher that is being built.

    aiogram offers no way to detach one: the `parent_router` setter refuses `None` by type and
    refuses a second attachment by state, because a router is meant to be included once and
    stay there. This router outlives dispatchers on purpose -- it holds the handlers a project
    registered at startup, and rebuilding it would leave a closed bot answering nothing -- so
    the reference it keeps is to a dispatcher that has already been closed and dropped.

    Written directly for that reason, in the one place a dispatcher is built. Without it the
    second dispatcher in a process silently has no handlers at all: `include_router` is never
    reached, because the router still names the first.
    """
    shared = router()
    if shared.parent_router is not None:
        shared._parent_router = None  # noqa: SLF001 - the reason is the paragraph above
    return shared


def session() -> 'AiohttpSession':
    """Return the HTTP session every bot in this process talks to Telegram through.

    Shared because nothing here configures it and a connector each is what makes a bot
    expensive. Its limit is the one aiogram sets, which bounds how many bots may be waiting on
    Telegram at once -- polling holds one connection per bot for as long as it waits, so a
    deployment past a few dozen wants the webhook mode rather than a bigger pool.
    """
    global _session  # noqa: PLW0603 - as above
    with _lock:
        if _session is None:
            from aiogram.client.session.aiohttp import AiohttpSession  # noqa: PLC0415 - aiogram, deferred

            _session = AiohttpSession()
        return _session


def holding() -> bool:
    """Whether this process has built anything a close would have to release.

    Asked by `TelegramBot.close`, which skips its teardown when there is nothing to tear down.
    The dispatcher and the session are the process's, so a bot that built neither a loop nor an
    aiogram `Bot` may still be the one that has to close them.
    """
    with _lock:
        return _dispatcher is not None or _session is not None


async def _close_each(stores: list[Any]) -> None:
    """Close every store handed over, and let none of them stop the rest.

    A shutdown reaches this once, so a store that raises on the way out must not take the
    others with it -- the one that raised is the one nobody hears about otherwise.
    """
    # gathered rather than closed in turn, and `return_exceptions` is the point of it: a
    # shutdown reaches this once, so a store that raises must not take the rest with it --
    # the one that raised is then the one nobody hears about
    outcomes = await asyncio.gather(*(store.close() for store in stores), return_exceptions=True)
    for outcome in outcomes:
        if isinstance(outcome, BaseException):
            logger.error('a storage refused to close', exc_info=outcome)


def close_storage() -> 'Closing':
    """Return the coroutine that closes every store this process built, or ``None``.

    A coroutine rather than the closing itself: a store is async and the caller owns the loop
    it has to be closed on. Handing the work back is what keeps this module free of one.

    Every store, not only the current one: a settings change retires a dispatcher without
    closing it -- closing needs a loop, and the change may land on a thread that has none --
    so the retired ones wait here until something that has one comes through.
    """
    global _dispatcher  # noqa: PLW0603 - one dispatcher per process, which is the subject here
    with _lock:
        held = [*_retired, _dispatcher] if _dispatcher is not None else list(_retired)
        stores = [dispatcher.storage for dispatcher in held]
        _retired.clear()
        _dispatcher = None
    _moved_on()
    return _close_each(stores) if stores else None


def close_session() -> 'Closing':
    """Return the coroutine that closes the session, and forget it, or ``None``.

    Every bot in the process holds this session, so closing it retires each of their aiogram
    ``Bot`` objects too -- which is what the generation says, and what makes a sibling bot
    build a working one rather than reach for a closed socket.
    """
    global _session
    with _lock:
        current, _session = _session, None
    _moved_on()
    return None if current is None else current.close()


@receiver(setting_changed, dispatch_uid='django_aiogram.runtime.process')
def _forget_the_dispatcher(**kwargs: Any) -> None:
    """Drop the dispatcher when the settings its store was built from change.

    The router is kept: it holds the handlers a project registered at startup, and dropping it
    would leave a suite that changes one setting with a bot that answers nothing.

    Not closed here, only retired. Closing needs a loop, and this runs wherever
    ``override_settings`` was entered -- which may be a request thread with no loop of its own.
    So it is kept for :func:`close_storage`, which is reached by something that has one: a
    dispatcher dropped and forgotten takes its store's connections with it.
    """
    global _dispatcher  # noqa: PLW0603 - as above
    if kwargs.get('setting') in {SETTINGS_NAME, BOTS_SETTINGS_NAME}:
        with _lock:
            if _dispatcher is not None:
                _retired.append(_dispatcher)
            _dispatcher = None
        _moved_on()
