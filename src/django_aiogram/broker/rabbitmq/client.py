"""One connection per thread, because ``BlockingConnection`` is not thread-safe.

``pika`` was chosen by measurement rather than by which face it matched — see the changelog.
The short version: reaching across the thread boundary costs about 100 microseconds whichever
driver is used (measured, 119.2 for aio-pika from synchronous code and 120.1 for pika from a
coroutine, which are the same number), so the driver that needs no crossing on the *common*
path wins. That path is synchronous: a view, a task, a management command.

The cost pika brings instead is this module. A ``BlockingConnection`` belongs to one thread,
so a threaded WSGI server needs one per worker thread — the same shape as the per-loop
registry ``redis.asyncio`` needs, and for the same reason.
"""

import threading
from contextlib import suppress
from typing import TYPE_CHECKING, Any

from django.core.exceptions import ImproperlyConfigured
from django.core.signals import setting_changed

from django_aiogram.config.settings import SETTINGS_NAME

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel
else:  # the annotations above are evaluated at runtime in a `|` union
    BlockingChannel = Any

__all__ = ('channel_for_thread', 'close_connections')

#: one connection and channel per thread, discarded when settings change
_local = threading.local()
#: every connection this process opened, so `close_connections` can reach the ones it did not
#: open itself — a test switching settings runs on the thread it is testing from, and a
#: consumer thread's connection would otherwise outlive the settings that built it
_opened: list[Any] = []
_lock = threading.Lock()


def channel_for_thread(url: str, queue: str, prefetch: int, blocked_timeout: float) -> 'BlockingChannel':
    """Open or reuse the calling thread's channel, confirmed for publishing.

    ``confirm_delivery`` here rather than per publish: it is a channel mode, and turning it on
    once is what makes every ``basic_publish`` on this channel answerable. The queue is
    declared durable on the way, so a broker restart does not lose the queue itself — the
    messages in it are marked persistent by the publisher.

    Reused only while every part of its identity still matches. A settings change that moves
    the queue or the prefetch has to reach a thread that is not the one running the receiver,
    and comparing here is how it does: that thread rebuilds on its next ask, without anybody
    reaching across a connection they do not own.
    """
    from pika import BlockingConnection, URLParameters  # noqa: PLC0415 - the driver is an extra

    identity = (url, queue, prefetch, blocked_timeout)
    existing: BlockingChannel | None = getattr(_local, 'channel', None)
    if existing is not None and getattr(_local, 'identity', None) == identity and existing.is_open:
        return existing
    if not url:
        msg = f"{SETTINGS_NAME}['RABBITMQ_URL'] is required to talk to RabbitMQ."
        raise ImproperlyConfigured(msg)
    # the one being replaced is closed here, which is the only place that can: closing a
    # channel does not close its connection, so a thread whose settings moved a few times
    # would otherwise hold a socket per move — and this is that thread, so no cross-thread
    # close is involved
    _drop_this_threads_connection()
    parameters = URLParameters(url)
    if parameters.blocked_connection_timeout is None:
        # pika leaves this unset, measured, and a broker that blocks the connection under
        # resource pressure then blocks every synchronous call on it for ever. `publish` runs
        # on request threads, so that is a web worker that never comes back. An explicit URL
        # value wins: somebody who wrote one meant it
        parameters.blocked_connection_timeout = blocked_timeout
    connection = BlockingConnection(parameters)
    channel = connection.channel()
    channel.confirm_delivery()
    channel.queue_declare(queue=queue, durable=True)
    # always, including zero. Skipping the call is not the same as asking for no limit: a
    # server with `default_consumer_prefetch` configured applies it to a consumer that never
    # sent QoS, so a package documenting 0 as unlimited has to say 0 out loud. Measured,
    # `basic_qos(prefetch_count=0)` is accepted
    channel.basic_qos(prefetch_count=prefetch)
    _local.identity, _local.connection, _local.channel = identity, connection, channel
    with _lock:
        _opened.append(connection)
    return channel


def _drop_this_threads_connection() -> None:
    """Close and forget the calling thread's connection, if it has one."""
    connection = getattr(_local, 'connection', None)
    if connection is None:
        return
    with _lock:
        _opened[:] = [held for held in _opened if held is not connection]
    with suppress(Exception):
        connection.close()
    for attribute in ('identity', 'connection', 'channel'):
        if hasattr(_local, attribute):
            delattr(_local, attribute)


def close_connections(**_kwargs: object) -> None:
    """Close this thread's connection, and ask the other threads to close theirs.

    The asking is the point. A ``BlockingConnection`` belongs to the thread that opened it —
    pika documents ``add_callback_threadsafe`` as the only operation another thread may
    perform on one — so closing a consumer thread's connection from here would be a race with
    whatever frame it is in the middle of. It appears to work on an idle connection, which is
    the worst kind of evidence: the failure needs the owner to be busy.

    So the owner closes it, on its own loop, the next time it processes events. A thread
    parked somewhere else may never get there, and that is accepted: its connection dies with
    the process, and it will not be *used* again either way, because
    :func:`channel_for_thread` rebuilds when the settings behind it have moved.

    Failures are swallowed on purpose: this runs while things are being torn down, and a
    connection that is already gone is the outcome being asked for.
    """
    mine = getattr(_local, 'connection', None)
    with _lock:
        others = [connection for connection in _opened if connection is not mine]
        _opened[:] = [connection for connection in _opened if connection is mine and mine is not None]
    for connection in others:
        with suppress(Exception):
            connection.add_callback_threadsafe(connection.close)
    _drop_this_threads_connection()


def _forget(**kwargs: object) -> None:
    """Drop the connections when the settings that built them change.

    Only for this app's own setting, as the broker registry's receiver is: every
    ``override_settings`` in a project's suite fires this, and closing a connection because an
    unrelated setting moved costs a reconnect for nothing.
    """
    if kwargs.get('setting') == SETTINGS_NAME:
        close_connections()


setting_changed.connect(_forget)
