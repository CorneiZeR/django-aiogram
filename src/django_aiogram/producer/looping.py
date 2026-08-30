"""The event loop this package drives, and what it costs the thread that calls in.

Three things that are about the loop rather than about any one send: the lock that keeps
two callers out of one loop, the budget a shutdown drain may spend, and the notice a
synchronous send prints when it finds itself inside a running loop.

Together they are the answer to "who is allowed to drive this loop, and for how long".
"""

import asyncio
import logging
import math
import threading
import weakref
from asyncio import AbstractEventLoop

from django.core.exceptions import ImproperlyConfigured

from django_aiogram.config.defaults import DEFAULTS
from django_aiogram.config.settings import conf

logger = logging.getLogger('django_aiogram')

#: the loop thread a web process starts, so a log line or a test can name it
LOOP_THREAD = 'tgbot-loop'
#: how long starting or stopping that thread may take before it is worth saying so
RUNNER_TIMEOUT = 5.0


# run_until_complete is not reentrant, and the loop — not the bot — is what
# cannot be entered twice. Two bots handed the same loop must share one lock.
_loop_locks: 'weakref.WeakKeyDictionary[AbstractEventLoop, threading.Lock]' = weakref.WeakKeyDictionary()
_loop_locks_guard = threading.Lock()


def loop_lock(loop: AbstractEventLoop) -> threading.Lock:
    """Return the one lock that everything driving ``loop`` has to hold."""
    with _loop_locks_guard:
        lock = _loop_locks.get(loop)
        if lock is None:
            lock = _loop_locks[loop] = threading.Lock()
        return lock


def drain_budget() -> float:
    """How long :meth:`TelegramBot.close` may spend draining.

    Falls back rather than raising: check E044 reports an unreadable value at boot,
    and shutdown is the worst moment to refuse — the drain sits between stopping
    the consumer and flushing the event log, so an exception here costs the rows
    that describe what the drain just did.
    """
    try:
        budget = float(conf['DRAIN_TIMEOUT'])
    except (ImproperlyConfigured, TypeError, ValueError):
        # `ImproperlyConfigured` too: `conf[...]` resolves the whole settings dict on a
        # cold cache, so a non-mapping `TELEGRAM_BOT` or a non-finite value from the
        # environment raises here — and this function exists not to raise
        budget = float(DEFAULTS['DRAIN_TIMEOUT'])
    return budget if math.isfinite(budget) and budget >= 0 else float(DEFAULTS['DRAIN_TIMEOUT'])


#: latched once per process: a line per send would be noise nobody can act on
_asend_mentioned = threading.Event()


def mention_asend(alternative: str) -> None:
    """Say once that there is a version of this that does not block the loop.

    Deliberately not a ``DeprecationWarning``: calling the synchronous method from
    async code is *correct* and nothing about it will stop working. It writes to a
    socket on the thread the loop is running on, which is worth knowing once and is
    not worth an exception.

    From every synchronous route that publishes, which is three of them —
    :meth:`send`, :meth:`enqueue` and :meth:`send_many`. Naming only the first
    left the two a web tier is most likely to reach for silent, and the fan-out is
    the one that holds the loop longest.

    Not from :meth:`send_raw`: there the caller is the worker's own consumer, which
    has no async alternative to move to, and a warning on a healthy path is how
    people learn to stop reading them.
    """
    if _asend_mentioned.is_set():
        return
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    _asend_mentioned.set()
    logger.warning(
        'a synchronous send was called from a running event loop',
        extra={'tg_alternative': alternative},
    )
