"""What a bot's failure means, how long it is believed, and who is told about it.

A quarantine is not one thing. A token Telegram has refused will not start working on a
timer, and retrying it every five minutes for a week is a log nobody reads and a request
nobody needed; a conflict on the updates clears itself the moment the old process exits; a
429 or a socket that closed is the ordinary weather of talking to an API. Same shape --
the bot is not being served -- and three different right answers, so the difference is
named here rather than guessed at the call site.

**Nothing here decides to stop serving a bot.** :mod:`django_aiogram.runtime.supervisor`
does that; this says what the failure was, writes it where a person can read it, and tells
the project. Kept apart because the supervisor must run in a process with no database at
all, and because a project acting on a revoked token -- emailing the client whose bot it
was, most likely -- is not something a reconciliation pass should be waiting on.
"""

import logging
import threading
from enum import Enum
from typing import TYPE_CHECKING

from django.dispatch import Signal

if TYPE_CHECKING:
    from datetime import datetime

__all__ = ('Fate', 'bot_quarantined', 'bot_recovered', 'classify', 'flush', 'forget', 'remember', 'waits')

logger = logging.getLogger('django_aiogram')


class Fate(str, Enum):
    """What kind of failure kept a bot from being served.

    A string enum for the reason the rest of this package uses them: the value is written
    into a row and read back out of one, so it has to survive a round trip through a column
    and a log line unchanged.
    """

    #: 401. The token is gone -- revoked in BotFather, or the bot deleted -- and no wait
    #: makes it come back. Only a new token does, which is a changed record and not a timer
    REVOKED = 'revoked'
    #: 409. Something else is polling this token: the usual cause is a deploy where the old
    #: process has not exited yet, so it clears itself, and the wait is what carries it
    CONFLICT = 'conflict'
    #: 429, a closed socket, a 5xx from Telegram. The ordinary weather, and the case the
    #: backoff was written for
    TRANSIENT = 'transient'
    #: anything else -- a broker that refused a connection, a profile naming a transport
    #: that cannot be imported, a bug. Retried, because a guess is not evidence: a failure
    #: this does not recognise must not be the one that takes a bot off the air for good
    UNKNOWN = 'unknown'


#: sent when a bot stops being served, with ``bot_id``, ``fate``, ``reason`` and ``until``
#: (``None`` where nothing will retry it). A project connects this to reach the person whose
#: bot it was: a revoked token is their problem to fix and nothing in a container can fix it
bot_quarantined = Signal()
#: sent when a bot that was quarantined is being served again, with ``bot_id``
bot_recovered = Signal()

_lock = threading.Lock()
#: the state writes a database refused, by identity, newest only. Retried by :func:`flush`
_unwritten: 'dict[int, dict[str, object]]' = {}


def classify(failure: BaseException) -> Fate:
    """Say what a failure to serve a bot was, by what Telegram answered.

    By exception class rather than by status code: aiogram has already read the code and
    chosen the class, and re-reading it would be this package's own second opinion about
    somebody else's API. aiogram is imported here rather than at module scope -- the
    supervisor is reached from ``AppConfig.ready`` in every enabled process, including the
    ones that only queue, and ``tests/test_lazy_init.py`` is what says so.
    """
    try:
        from aiogram.exceptions import (  # noqa: PLC0415 - aiogram, deferred; see above
            TelegramConflictError,
            TelegramNetworkError,
            TelegramRetryAfter,
            TelegramServerError,
            TelegramUnauthorizedError,
        )
    except Exception:  # noqa: BLE001 - an unimportable aiogram is not this function's finding
        return Fate.UNKNOWN
    if isinstance(failure, TelegramUnauthorizedError):
        return Fate.REVOKED
    if isinstance(failure, TelegramConflictError):
        return Fate.CONFLICT
    if isinstance(failure, TelegramRetryAfter | TelegramNetworkError | TelegramServerError):
        return Fate.TRANSIENT
    return Fate.UNKNOWN


def waits(fate: Fate) -> bool:
    """Whether a timer will ever try this bot again.

    Only :attr:`Fate.REVOKED` says no, and it is the whole reason this module exists: a
    revoked token retried on a backoff is a request per bot per five minutes for as long as
    the container runs, and every one of them fails for the same reason. What clears it is a
    new token, which the supervisor sees as a changed record rather than as an elapsed wait.
    """
    return fate is not Fate.REVOKED


def remember(bot_id: int, fate: Fate, reason: str, until: 'datetime | None') -> None:
    """Write a quarantine where a person can read it, and tell the project about it.

    Best effort, and deliberately: this is called from a reconciliation pass, and a database
    that cannot take the row must not be the reason the other nineteen bots stop being
    reconciled. What a failed write costs is the admin page's copy of the reason, and it is
    kept for :func:`flush` rather than dropped -- see there for why the next pass cannot be
    relied on to write it again.

    **A quarantine is a process's, not a deployment's.** The row is a copy for a person to
    read; a container that starts fresh has an empty quarantine and tries every configured
    bot once, revoked tokens included. That is deliberate: the alternative is hydrating the
    quarantine from the row, and a row cannot say whether the token in it is still the one
    that was refused -- so a token corrected while the container was down would stay
    quarantined until somebody edited the row a second time. One refused request per bot per
    process start is the cheaper end of that trade.

    A bot configured in ``settings.py`` has no row to write, and that is not a failure
    either: nothing is created, because a row created here would be a bot the table claims
    to configure and does not.
    """
    _write({'quarantine_reason': f'{fate.value}: {reason}'[:64], 'quarantined_until': until}, bot_id)
    bot_quarantined.send_robust(
        sender=None,
        bot_id=bot_id,
        fate=fate,
        reason=reason,
        until=until,
    )


def forget(bot_id: int) -> None:
    """Clear a bot's quarantine, and say it is being served again."""
    _write({'quarantine_reason': '', 'quarantined_until': None}, bot_id)
    bot_recovered.send_robust(sender=None, bot_id=bot_id)


def flush() -> None:
    """Retry the state writes a database refused, and let nothing about them reach the caller.

    Called at the top of every reconciliation pass, and free when there is nothing held.

    It exists because *the next pass does not write the state again*. A pass writes when
    something changes, and both ends of this are steady states: a bot that started working is
    then running and unchanged, so no later pass touches it, and a revoked token is held with
    no clock, so no later pass tries it. A write lost in either place would leave the admin
    saying the opposite of the truth for as long as the container runs -- a client's bot
    reading as quarantined while it is answering, or as healthy while nothing is serving it.

    The signals are not resent from here. A signal is this process telling the project what
    happened, and it happened once; the row is a copy of it, and this is what makes the copy
    catch up.
    """
    with _lock:
        held = dict(_unwritten)
    for bot_id, fields in held.items():
        _write(fields, bot_id)


def _write(fields: 'dict[str, object]', bot_id: int) -> None:
    """Update one bot's row, if it has one, and keep what a database refused for `flush`."""
    try:
        # deferred: importing models reaches the app registry, and a supervisor runs in
        # processes that have no database configured at all
        from django_aiogram.models import TelegramBot  # noqa: PLC0415 - as above

        TelegramBot.objects.filter(bot_id=bot_id).update(**fields)
    except Exception:
        with _lock:
            # the latest state, not a queue of them: a row holds one, and replaying an
            # earlier quarantine over a bot that has since recovered is the failure this
            # would otherwise introduce
            _unwritten[bot_id] = fields
        logger.exception(
            "could not record a bot's state; it will be retried on the next pass",
            extra={'tg_bot_id': bot_id},
        )
        return
    with _lock:
        _unwritten.pop(bot_id, None)
