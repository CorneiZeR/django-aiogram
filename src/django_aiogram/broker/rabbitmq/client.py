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

__all__ = ('channel_for_thread', 'close_connections')

#: one connection and channel per thread, discarded when settings change
_local = threading.local()
#: every connection this process opened, so `close_connections` can reach the ones it did not
#: open itself — a test switching settings runs on the thread it is testing from, and a
#: consumer thread's connection would otherwise outlive the settings that built it
_opened: list[Any] = []
_lock = threading.Lock()


def channel_for_thread(url: str, queue: str, prefetch: int) -> 'BlockingChannel':
    """Open or reuse the calling thread's channel, confirmed for publishing.

    ``confirm_delivery`` here rather than per publish: it is a channel mode, and turning it on
    once is what makes every ``basic_publish`` on this channel answerable. The queue is
    declared durable on the way, so a broker restart does not lose the queue itself — the
    messages in it are marked persistent by the publisher.
    """
    from pika import BlockingConnection, URLParameters  # noqa: PLC0415 - the driver is an extra

    existing = getattr(_local, 'channel', None)
    if existing is not None and getattr(_local, 'url', None) == url and existing.is_open:
        return existing
    if not url:
        msg = f"{SETTINGS_NAME}['RABBITMQ_URL'] is required to talk to RabbitMQ."
        raise ImproperlyConfigured(msg)
    connection = BlockingConnection(URLParameters(url))
    channel = connection.channel()
    channel.confirm_delivery()
    channel.queue_declare(queue=queue, durable=True)
    if prefetch:
        channel.basic_qos(prefetch_count=prefetch)
    _local.url, _local.connection, _local.channel = url, connection, channel
    with _lock:
        _opened.append(connection)
    return channel


def close_connections(**_kwargs: object) -> None:
    """Close every connection this process opened, from whichever thread asks.

    Called at shutdown and from the settings receiver below. Failures are swallowed on
    purpose: this runs while things are being torn down, and a connection that is already
    gone is the outcome being asked for.
    """
    with _lock:
        connections, _opened[:] = list(_opened), []
    for connection in connections:
        # a connection that is already gone is the outcome being asked for, and this runs
        # while things are being torn down
        with suppress(Exception):
            connection.close()
    for attribute in ('url', 'connection', 'channel'):
        if hasattr(_local, attribute):
            delattr(_local, attribute)


def _forget(**kwargs: object) -> None:
    """Drop the connections when the settings that built them change.

    Only for this app's own setting, as the broker registry's receiver is: every
    ``override_settings`` in a project's suite fires this, and closing a connection because an
    unrelated setting moved costs a reconnect for nothing.
    """
    if kwargs.get('setting') == SETTINGS_NAME:
        close_connections()


setting_changed.connect(_forget)
