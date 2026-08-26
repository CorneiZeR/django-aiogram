"""Which transport this process uses, decided once and never guessed.

The dotted path is read from ``BROKER``. Nothing is inferred from what happens to be
installed: two drivers present would make the choice ambiguous, and one present would make
a typo in the setting look like a working configuration.
"""

import atexit
import threading
from typing import TYPE_CHECKING

from django.core.signals import setting_changed
from django.dispatch import receiver
from django.utils.module_loading import import_string

from django_aiogram.broker.base import Broker
from django_aiogram.broker.exceptions import BrokerDependencyError, BrokerNotConfiguredError
from django_aiogram.config.settings import SETTINGS_NAME, conf

if TYPE_CHECKING:
    from typing import Any

__all__ = ('SHIPPED', 'broker_class', 'close_broker', 'get_broker')

#: what each shipped broker needs, keyed by dotted path — readable *without* importing the
#: module, so a check can name the missing extra even where the import would fail
SHIPPED: dict[str, tuple[str, str]] = {
    'django_aiogram.broker.redis_list.RedisListBroker': ('redis', 'redis'),
    # the same driver and the same extra: Streams is a different data structure on the
    # server, not a different dependency
    'django_aiogram.broker.redis_streams.RedisStreamsBroker': ('redis', 'redis'),
    'django_aiogram.broker.rabbitmq.RabbitMQBroker': ('pika', 'rabbitmq'),
    'django_aiogram.broker.kafka.KafkaBroker': ('confluent_kafka', 'kafka'),
}

_lock = threading.Lock()
_broker: Broker | None = None
#: registered once per process rather than per build, so a settings change that replaces the
#: broker does not stack another callback. `close_broker` is idempotent either way
_exit_hook_armed = False


def broker_class() -> type[Broker]:
    """Resolve ``BROKER`` to a class, and refuse anything that is not one.

    Separate from :func:`get_broker` because the checks want the class and its declared
    requirement without building a connection, and a check must never be the thing that
    opens a socket.
    """
    path = str(conf['BROKER'] or '').strip()
    if not path:
        msg = f"{SETTINGS_NAME}['BROKER'] is empty, so no transport is chosen."
        raise BrokerNotConfiguredError(msg)
    if path in SHIPPED:
        # verified before the import, which is belt to `verify`'s braces. Every shipped
        # transport imports its driver lazily, so importing the class would *not* raise —
        # `verify` would name the extra by itself. This table is what keeps that true of a
        # transport added later whose module reaches its driver at import: the extra is
        # named without importing anything, so the reader never meets the `ImportError`
        module, extra = SHIPPED[path]
        _require(path, module, extra)
    try:
        resolved = import_string(path)
    except ImportError as error:
        msg = f"{SETTINGS_NAME}['BROKER'] is {path!r}, which cannot be imported: {error}"
        raise BrokerNotConfiguredError(msg) from error
    if not (isinstance(resolved, type) and issubclass(resolved, Broker)):
        msg = f"{SETTINGS_NAME}['BROKER'] is {path!r}, which is not a Broker subclass."
        raise BrokerNotConfiguredError(msg)
    return resolved


def _require(path: str, module: str, extra: str) -> None:
    """Raise the install line for a shipped broker whose driver is absent."""
    import importlib.util  # noqa: PLC0415 - only when a broker is being resolved

    if importlib.util.find_spec(module) is None:
        raise BrokerDependencyError(path.rsplit('.', 1)[-1], module, extra)


def get_broker() -> Broker:
    """Return the one broker this process uses, building it on the first ask.

    Cached like the Redis client was, and for the same reason: a transport holds a
    connection, and building one per send is what the 3.x accessor existed to avoid.
    """
    global _broker, _exit_hook_armed  # noqa: PLW0603 - one per process, like the connection it holds
    if _broker is not None:
        return _broker
    with _lock:
        if _broker is None:
            cls = broker_class()
            cls.verify()
            _broker = cls()
            if not _exit_hook_armed:
                # `Broker.close()` says it is "called once, at shutdown", and until this line
                # nothing called it there: the only path to `close_broker` was the
                # `setting_changed` receiver below, which fires in a test suite and never in a
                # deployment. So the Kafka producer was never flushed and its consumer never
                # left its group -- a member that disappears without saying so holds its
                # partitions until the session times out, which is a bot restart that delivers
                # nothing for that long. `EventRecorder` has armed an `atexit` for its writer
                # all along; this is the same trade in the same shape.
                #
                # This hook runs during interpreter shutdown, on the main thread, which is not
                # the thread that opened a consumer's connection -- and `bot.close()`, the other
                # path here, runs wherever a caller happens to be. Neither can promise the
                # owning thread, which is why each transport's `close()` already restricts what
                # it touches from a foreign one: pika's asks the owner through
                # `add_callback_threadsafe`, and librdkafka's flushes the process producer and
                # closes only the calling thread's consumer.
                atexit.register(close_broker)
                _exit_hook_armed = True
    return _broker


def close_broker() -> None:
    """Drop the cached broker, closing it first. Safe to call when there is none."""
    global _broker
    with _lock:
        current, _broker = _broker, None
    if current is not None:
        current.close()


@receiver(setting_changed)
def _forget_the_broker(**kwargs: 'Any') -> None:
    """Rebuild on the next ask when the settings change, as the client does.

    Only for this app's own setting: every ``override_settings`` in a project's test suite
    fires this, and closing a connection because an unrelated setting moved is a cost
    nobody asked for.
    """
    if kwargs.get('setting') == SETTINGS_NAME:
        close_broker()
