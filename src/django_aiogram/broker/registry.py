"""Which transport this process uses, decided once and never guessed.

The dotted path is read from ``BROKER``. Nothing is inferred from what happens to be
installed: two drivers present would make the choice ambiguous, and one present would make
a typo in the setting look like a working configuration.
"""

import contextlib
import threading
from typing import TYPE_CHECKING

from django.core.signals import setting_changed
from django.dispatch import receiver
from django.utils.module_loading import import_string

from django_aiogram.broker.base import Broker
from django_aiogram.broker.exceptions import BrokerDependencyError, BrokerNotConfiguredError
from django_aiogram.config.settings import SETTINGS_NAME, conf, setting_label

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping
    from typing import Any

__all__ = ('SHIPPED', 'broker_class', 'close_broker', 'get_broker', 'overriding', 'use_broker')

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
#: a broker handed in rather than resolved, for the length of a test. Consulted *before*
#: `BROKER` and never built from it, which is the whole reason it is not simply an
#: `override_settings` in `django_aiogram.testing`: a case that overrides the setting itself --
#: and every `@override_settings(TELEGRAM_BOT_DEFAULTS=...)` replaces the dict whole -- would otherwise
#: undo the helper it is running inside, silently, and at a moment it did not choose
_overrides: list[tuple[object, Broker]] = []


def broker_class(settings: 'Mapping[str, Any] | None' = None, *, verify_driver: bool = True) -> type[Broker]:
    """Resolve ``BROKER`` to a class, and refuse anything that is not one.

    Separate from :func:`get_broker` because the checks want the class and its declared
    requirement without building a connection, and a check must never be the thing that
    opens a socket.

    ``verify_driver=False`` skips the missing-driver refusal, for a caller that needs what the
    class *declares* rather than what it can do. `W004` is the one: the cap it reports is
    arithmetic over settings, and refusing to compute it because an extra is not installed would
    silence a settings warning on every machine that has not installed the driver -- including the
    unit legs in CI. `E047` owns the missing driver, and says so with the install line.

    Every shipped broker imports its driver lazily, so the import below succeeds without it.
    """
    resolved = conf if settings is None else settings
    named = setting_label(settings, 'BROKER')
    path = str(resolved['BROKER'] or '').strip()
    if not path:
        msg = f'{named} is empty, so no transport is chosen.'
        raise BrokerNotConfiguredError(msg)
    if path in SHIPPED and verify_driver:
        # verified before the import, which is belt to `verify`'s braces. Every shipped
        # transport imports its driver lazily, so importing the class would *not* raise —
        # `verify` would name the extra by itself. This table is what keeps that true of a
        # transport added later whose module reaches its driver at import: the extra is
        # named without importing anything, so the reader never meets the `ImportError`
        module, extra = SHIPPED[path]
        _require(path, module, extra)
    try:
        resolved = import_string(path)
    # `ValueError` for a path with an empty module part, which `import_module('')` raises rather
    # than `ImportError` -- see `producer.from_settings.build_storage` for the same catch and the
    # same reason
    except (ImportError, ValueError) as error:
        msg = f'{named} is {path!r}, which cannot be imported: {error}'
        raise BrokerNotConfiguredError(msg) from error
    if not (isinstance(resolved, type) and issubclass(resolved, Broker)):
        msg = f'{named} is {path!r}, which is not a Broker subclass.'
        raise BrokerNotConfiguredError(msg)
    return resolved


def _require(path: str, module: str, extra: str) -> None:
    """Raise the install line for a shipped broker whose driver is absent."""
    import importlib.util  # noqa: PLC0415 - only when a broker is being resolved

    if importlib.util.find_spec(module) is None:
        raise BrokerDependencyError(path.rsplit('.', 1)[-1], module, extra)


def get_broker(settings: 'Mapping[str, Any] | None' = None) -> Broker:
    """Return the transport one bot uses, building its group's on the first ask.

    Cached by *profile* since 5.0 rather than per process: twenty bots configured alike share
    one connection, and one configured differently gets its own. `settings` names the bot;
    without them the shared defaults answer, which is what a process with one bot has.

    An override from :func:`use_broker` wins over both the group and the setting, and is the
    only way anything but `BROKER` decides this.
    """
    # deferred: `runtime.groups` reaches back here for `broker_class`, and a module-scope import
    # either way round would be a cycle
    from django_aiogram.runtime.groups import group_for  # noqa: PLC0415 - as above

    held = overriding()
    if held is not None:
        return held
    return group_for(conf if settings is None else settings).broker


def overriding() -> Broker | None:
    """Return the broker a :func:`use_broker` block installed, if one is standing.

    Read by the groups as well as here, so a capture reaches a bot whichever way its transport
    is asked for.
    """
    with _lock:
        return _overrides[-1][1] if _overrides else None


@contextlib.contextmanager
def use_broker(broker: Broker) -> 'Iterator[Broker]':
    """Make ``broker`` this process's broker for the length of the block.

    The seam ``django_aiogram.testing`` is built on, and public because a project's own
    fixtures reach for the same thing. Not for a deployment: ``BROKER`` decides there, and a
    process that could be talked out of its transport by a caller is one whose configuration
    means less than it says.

    Ahead of the setting rather than through it, which is a deliberate difference from
    ``override_settings(TELEGRAM_BOT_DEFAULTS=...)``. Every such override replaces the dict whole, so a
    case that carries one of its own -- a decorator on the method, applied *after* a fixture
    has already started capturing -- would silently take the helper's broker away again. The
    override is a fact about this process, and nothing in the settings can undo it.

    **A stack, and each block removes its own entry rather than restoring what it replaced.**
    The obvious version keeps the broker it displaced and puts it back on the way out, which is
    correct only while blocks end in the order they began. They need not: a fixture holding one
    open across cases, an ``ExitStack`` closed in the order it was built, or two threads each
    capturing -- and then the block that exits first reinstates a broker whose own block has
    ended, and the one that exits last leaves it installed for good. Removing an entry by
    identity cannot get that wrong, and the innermost block still standing is the one that wins.
    """
    entry = (object(), broker)
    with _lock:
        _overrides.append(entry)
    try:
        yield broker
    finally:
        with _lock:
            # by identity, because two blocks may hold the same broker instance and `remove`
            # would take the wrong one -- the tuple's first field is a token for exactly this
            for index in range(len(_overrides) - 1, -1, -1):
                if _overrides[index][0] is entry[0]:
                    del _overrides[index]
                    break


def close_broker() -> None:
    """Close every transport this process built. Safe to call when there is none."""
    # deferred for the reason `get_broker` gives
    from django_aiogram.runtime.groups import close_groups  # noqa: PLC0415 - as above

    close_groups()


@receiver(setting_changed)
def _forget_the_broker(**kwargs: 'Any') -> None:
    """Rebuild on the next ask when the settings change, as the client does.

    Only for this app's own setting: every ``override_settings`` in a project's test suite
    fires this, and closing a connection because an unrelated setting moved is a cost
    nobody asked for.
    """
    if kwargs.get('setting') == SETTINGS_NAME:
        close_broker()
