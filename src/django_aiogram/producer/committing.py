"""When a queue write happens, relative to the transaction the caller is inside.

``bot.send()`` writes to the broker as it is called, and that write is not part of the
caller's transaction. A block that creates a row, announces it in Telegram and then raises
leaves the message sent and the row gone — the bot has told a chat about something the
database does not have.

``TRANSACTIONAL`` moves the write behind the commit, so nothing is announced until the
thing being announced exists. Off by default, because it changes *when* a message reaches
the queue and a deployment measuring queue latency will see that.

Only the queue write. Inside the bot container a send reaches Telegram directly and there
is no publish to hold back, so a handler's reply is unaffected by this setting.

And only where a commit will actually happen: a connection under manual transaction
management publishes immediately and says so once, because its hooks wait for
``set_autocommit(True)`` rather than for the block.

That last answer belongs to the publish alone. :func:`after_commit`, which the event log uses
to keep a durable row from outliving the write it describes, takes the deferral there anyway --
arriving late costs an event nothing and existing when the row does not costs it everything. It
has no unsupported configuration of its own as a result: where a caller leaves no block for a
hook to live in, ``scheduling.schedule`` opens one. The two conditions and why they differ are
in the functions.

And only a synchronous one. ``connections`` is context-aware, so a coroutine holds its own
connection rather than the one a surrounding ``atomic()`` opened -- measured: inside
``asyncio.run`` the object differs and ``in_atomic_block`` is False. That is not a gap this
module could close by looking harder, it is what Django is: there are no asynchronous
transactions, so ``await bot.asend()`` is never inside one and has nothing to wait for.
"""

import logging
import threading
from collections.abc import Callable
from typing import TYPE_CHECKING

from django.db import DEFAULT_DB_ALIAS, connections, transaction

from django_aiogram.config.settings import SETTINGS_NAME, coerce_bool, conf

if TYPE_CHECKING:
    from django.db.backends.base.base import BaseDatabaseWrapper

logger = logging.getLogger('django_aiogram')

__all__ = ('after_commit', 'defer')

#: said once per process, like `mention_asend`: the condition is a deployment's database
#: configuration, so a line per send is noise nobody can act on twice
_manual_mentioned = threading.Event()
_manual_guard = threading.Lock()


def defer(publish: Callable[[], None]) -> bool:
    """Hold the queue write for the caller's transaction, where ``TRANSACTIONAL`` asks.

    ``False`` means nothing was arranged and **the caller publishes now** -- this never runs
    the write itself, which is the difference from :func:`after_commit` and the reason the two
    are not one function. A version that did both duplicated every immediate send.
    """
    if not coerce_bool(conf['TRANSACTIONAL'], f"{SETTINGS_NAME}['TRANSACTIONAL']"):
        return False
    if not _will_commit():
        return False
    transaction.on_commit(publish, using=DEFAULT_DB_ALIAS)
    return True


def after_commit(work: Callable[[], None]) -> bool:
    """Run ``work`` when the caller's transaction commits, or **now** where none can.

    ``True`` means it was arranged for later, ``False`` that it has already run. Gated by no
    setting, because two callers want different things of the same mechanism: :func:`defer`
    waits only where ``TRANSACTIONAL`` says to, while a scheduled send's *event* has to wait
    unconditionally -- the row it describes is rolled back by a failing block, and a durable
    event about a send that never existed is worse than no event at all.

    **A weaker condition than :func:`defer` uses, and the difference is the point.** Under
    manual transaction management an ``atomic()`` block does not commit on exit, so its hooks
    wait for ``set_autocommit(True)`` -- measured: they run after the caller's ``commit()``
    and *not* after its ``rollback()``. For a publish that is unacceptable, because the
    message would leave at a moment nobody chose, which is why ``defer`` refuses there. For an
    event it is exactly right: late is harmless, and existing when the row does not is not.

    The one case with no hook at all is autocommit off with no block anywhere, where
    ``on_commit`` raises rather than deferring. There the work is done now and said once --
    a fallback no caller in this package reaches any more, because
    :func:`~django_aiogram.producer.scheduling.schedule` opens a block of its own precisely so
    that a hook exists to take. It stays because this is a helper about a connection's state
    and not about that one caller.
    """
    connection = connections[DEFAULT_DB_ALIAS]
    if connection.in_atomic_block:
        transaction.on_commit(work, using=DEFAULT_DB_ALIAS)
        return True
    if connection.connection is not None and not connection.get_autocommit():
        _mention_manual_transactions()
    work()
    return False


def _will_commit() -> bool:
    """Whether a commit is coming on the default connection that will run its hooks.

    The default alias, because a send names no database. It is the connection a view's
    ``atomic()`` opens and the one whose rollback the work would outlive; a project writing to
    a second alias inside a block of its own is not one this can see.
    """
    connection = connections[DEFAULT_DB_ALIAS]
    if connection.in_atomic_block:
        if _commits_on_exit(connection):
            return True
        _mention_manual_transactions()
        return False
    # only a connection that is already open, because asking an unopened one opens it:
    # a send would connect to a database the process never otherwise touches, and from a
    # coroutine that is `SynchronousOnlyOperation`. Nothing has run on it, so there is no
    # server-side transaction for it to be holding either
    if connection.connection is not None and not connection.get_autocommit():
        _mention_manual_transactions()
    return False


def _commits_on_exit(connection: 'BaseDatabaseWrapper') -> bool:
    """Whether leaving the outermost block will commit, and so run the hooks queued in it.

    An ``atomic()`` entered while autocommit is off does not. Django sets ``commit_on_exit``
    False there — its own comment calls it "a note to deal with this case in ``__exit__``" —
    and the hooks then wait for ``set_autocommit(True)``. Measured: leaving the block runs
    nothing, ``transaction.commit()`` runs nothing, and restoring autocommit runs them, which
    is a moment no caller of ``send()`` chose and a process that never restores it never
    reaches.

    ``get_autocommit()`` cannot answer this, which is the trap. It is False inside an
    *ordinary* ``atomic()`` too, because the block turns autocommit off to open its
    transaction — measured on both shapes — so reading it here would refuse every deferral
    the setting exists for.

    True where the attribute is gone, so a Django that renames it keeps deferring rather than
    silently stopping for everyone; the manual-management test then fails on that leg, which
    is where a rename should be found.
    """
    return bool(getattr(connection, 'commit_on_exit', True))


def _mention_manual_transactions() -> None:
    """Say once that the setting cannot be honoured on this connection.

    Two shapes, one answer. With autocommit off and no block, the server holds a transaction
    open from the first statement while ``in_atomic_block`` is still False — the case
    ``eventlog.writer._recycle`` documents from the other side — and ``on_commit`` raises
    ``TransactionManagementError`` rather than deferring, so honouring the setting would turn
    every send on such an alias into a failure. With a block inside that, the hook is
    accepted and then runs at a moment nobody chose; see :func:`_commits_on_exit`.

    Both shapes are about the *publish*. :func:`after_commit` reaches this only in the first
    of them, because the second is a moment it is happy to wait for.

    Publishing now is the same behaviour the deployment had before the setting, said out loud
    rather than assumed.
    """
    if _manual_mentioned.is_set():
        return
    with _manual_guard:
        if _manual_mentioned.is_set():
            return
        _manual_mentioned.set()
    logger.warning(
        'publishing without waiting for a commit: this connection is in a manually managed '
        'transaction, which does not run commit hooks when it ends',
        extra={'tg_database': DEFAULT_DB_ALIAS},
    )
