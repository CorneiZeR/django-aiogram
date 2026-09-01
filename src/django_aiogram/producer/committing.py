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

And only a synchronous one. ``connections`` is context-aware, so a coroutine holds its own
connection rather than the one a surrounding ``atomic()`` opened -- measured: inside
``asyncio.run`` the object differs and ``in_atomic_block`` is False. That is not a gap this
module could close by looking harder, it is what Django is: there are no asynchronous
transactions, so ``await bot.asend()`` is never inside one and has nothing to wait for.
"""

import logging
import threading
from collections.abc import Callable

from django.db import DEFAULT_DB_ALIAS, connections, transaction

from django_aiogram.config.settings import SETTINGS_NAME, coerce_bool, conf

logger = logging.getLogger('django_aiogram')

__all__ = ('defer',)

#: said once per process, like `mention_asend`: the condition is a deployment's database
#: configuration, so a line per send is noise nobody can act on twice
_manual_mentioned = threading.Event()
_manual_guard = threading.Lock()


def defer(publish: Callable[[], None]) -> bool:
    """Hand the write to the caller's transaction, and say whether that happened.

    ``False`` means nothing was arranged and the caller has to publish now.

    The default alias, because a send names no database. It is the connection a view's
    ``atomic()`` opens and the one whose rollback the message would outlive; a project
    writing to a second alias inside a block of its own gets the immediate write, which is
    what happened before this setting existed.
    """
    if not coerce_bool(conf['TRANSACTIONAL'], f"{SETTINGS_NAME}['TRANSACTIONAL']"):
        return False
    connection = connections[DEFAULT_DB_ALIAS]
    if connection.in_atomic_block:
        transaction.on_commit(publish, using=DEFAULT_DB_ALIAS)
        return True
    # only a connection that is already open, because asking an unopened one opens it:
    # a send would connect to a database the process never otherwise touches, and from a
    # coroutine that is `SynchronousOnlyOperation`. Nothing has run on it, so there is no
    # server-side transaction for it to be holding either
    if connection.connection is not None and not connection.get_autocommit():
        _mention_manual_transactions()
    return False


def _mention_manual_transactions() -> None:
    """Say once that the setting cannot be honoured on this connection.

    With autocommit off the server holds a transaction open from the first statement and
    ``in_atomic_block`` is still False — the case ``eventlog.writer._recycle`` documents
    from the other side. ``on_commit`` does not defer there, it raises
    ``TransactionManagementError``, so honouring the setting would turn every send on an
    alias configured ``AUTOCOMMIT: False`` into a failure. Publishing now is the same
    behaviour the deployment had before the setting, said out loud rather than assumed.
    """
    if _manual_mentioned.is_set():
        return
    with _manual_guard:
        if _manual_mentioned.is_set():
            return
        _manual_mentioned.set()
    logger.warning(
        'publishing without waiting for a commit: autocommit is off on this connection, '
        'so there is no commit hook to wait on',
        extra={'tg_database': DEFAULT_DB_ALIAS},
    )
