"""The connection reset around an update, and the thread it has to happen on.

Django closes a broken connection between requests, and a bot worker has no requests. Measured in
production: Postgres restarted, and every handler raised `InterfaceError: connection already
closed` for the next ten hours while outbound delivery and the healthcheck both stayed green,
because neither touches the database. Only a container restart recovered it.

Two cases, because the fix has two halves and one of them is invisible to the other:

* the update recovers -- what the middleware is for;
* the reset happens on the **thread-sensitive executor thread**, which is where a handler's ORM
  access happens. Django connections are thread-local, so a reset on the loop thread closes a
  connection nobody used and leaves the broken one in place. The fix would look applied and change
  nothing, and the first case cannot tell the difference.

`is_usable` is patched rather than trusted, and that is about sqlite rather than about this
package: `close_if_unusable_or_obsolete` closes a poisoned connection only if the backend agrees
it is unusable, and `django.db.backends.sqlite3.base.DatabaseWrapper.is_usable` returns `True`
unconditionally. Postgres reports the dead socket, which is what the patch stands in for. The patch
is on Django's backend, never on the code under test.
"""

import asyncio
import threading

import pytest
from aiogram import Bot, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Chat, Message, Update, User
from asgiref.sync import sync_to_async
from django.db import connection
from django.test import override_settings
from django.utils import timezone

from django_aiogram import TelegramBot
from django_aiogram.models import TelegramEvent

TOKEN = '123456:AAFakeTokenThatLooksLikeARealOneXXXXXXX'
#: memory storage, so the dispatcher builds without a Redis: what is under test is the database
#: connection around an update, and the FSM store is not part of it
SETTINGS = {'TOKEN': TOKEN, 'FSM_STORAGE': 'memory'}


def an_update(text='/probe'):
    return Update(
        update_id=1,
        message=Message(
            message_id=1,
            date=timezone.now(),
            chat=Chat(id=42, type='private'),
            from_user=User(id=42, is_bot=False, first_name='Tester'),
            text=text,
        ),
    )


def a_bot_with(handler):
    """A `TelegramBot` whose dispatcher carries the real middleware chain."""
    instance = TelegramBot()
    router = Router()
    router.message.register(handler)
    instance.dispatcher.include_router(router)
    return instance


def poison():
    """Leave the connection open as far as Django is concerned, and dead underneath.

    Both halves are needed and neither is enough: `errors_occurred` is what makes Django ask the
    backend whether the connection still works, and closing the DBAPI object is what makes the
    answer matter -- a query through it raises rather than returning rows.
    """
    connection.ensure_connection()
    connection.connection.close()
    connection.errors_occurred = True


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_handler_recovers_from_a_connection_that_died_while_the_process_was_idle(
    a_backend_that_can_lose_a_connection,
):
    """The outage, reproduced: a dead connection and the first update after it.

    Without the middleware the query raises `InterfaceError`; with it, the reset closes the
    connection Django thought it had and the next query opens a new one.

    On PostgreSQL nothing is patched and this is the outage itself: `poison` closes the DBAPI
    connection, `is_usable()` answers false because it is, and the close is a real one. The fixture
    supplies both answers on SQLite, where an in-memory database can give neither -- and that
    difference is the reason #64 pointed this suite at a second backend.
    """
    counted = []

    async def handler(message):
        counted.append(await TelegramEvent.objects.acount())

    instance = a_bot_with(handler)

    async def drive():
        # on the executor thread, through the same mechanism the middleware and the handler's ORM
        # access use -- poisoning the loop thread's connection would be a different connection
        await sync_to_async(poison, thread_sensitive=True)()
        await instance.dispatcher.feed_update(Bot(token=TOKEN, default=DefaultBotProperties()), an_update())

    asyncio.run(drive())

    assert counted == [0], 'the handler never completed its query through the reset connection'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_reset_runs_on_the_thread_the_handler_queries_from(monkeypatch):
    """`thread_sensitive=True`, asserted rather than assumed.

    A Django connection is thread-local. If the reset runs anywhere but the thread a handler's ORM
    access runs on, it closes a connection nobody used: the recovery case above would still pass
    on a *fresh* connection while a real deployment's broken one stayed broken. So the two threads
    are recorded and compared.

    Dropping the flag makes asgiref take a pool thread of its own for the reset, and these two
    idents stop matching.
    """
    reset_on = []
    queried_on = []

    def recording_close(*args, **kwargs):
        reset_on.append(threading.get_ident())

    monkeypatch.setattr('django_aiogram.db.close_old_connections', recording_close)

    async def handler(message):
        queried_on.append(await sync_to_async(threading.get_ident, thread_sensitive=True)())

    instance = a_bot_with(handler)
    asyncio.run(instance.dispatcher.feed_update(Bot(token=TOKEN, default=DefaultBotProperties()), an_update()))

    assert len(reset_on) == 2, f'the middleware reset {len(reset_on)} times, expected before and after'
    assert queried_on, 'the handler never ran'
    assert set(reset_on) == set(queried_on), (
        f'the reset ran on {sorted(set(reset_on))} and the ORM on {sorted(set(queried_on))}: '
        'a thread-local connection was closed on a thread that never used one'
    )
