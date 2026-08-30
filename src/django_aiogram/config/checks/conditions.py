"""The questions a rule asks before it answers.

Not rules themselves and not registered anywhere: each is a fact about this deployment that
several rules gate on -- whether the bot is enabled, whether the log is on, whether anything here
still reaches Redis, whether the transport keys anything on a worker's name.

Together in one module because a rule in any of the three subject modules asks them, and putting
them beside one subject would make the other two import that subject to ask a question that is
not about it.
"""

from django.core.exceptions import ImproperlyConfigured

from django_aiogram.config.enums import StorageKind, as_member
from django_aiogram.config.settings import SETTINGS_NAME, coerce_bool, conf


def _bot_is_enabled() -> bool:
    """Whether the bot is on, coerced the way startup and sending coerce it."""
    try:
        return coerce_bool(conf['ENABLED'], f"{SETTINGS_NAME}['ENABLED']")
    except ImproperlyConfigured:
        # unreadable is E001's finding; assume on, so the credential warnings show
        return True


def _the_log_is_on() -> bool:
    """Whether events are recorded, coerced the way the recorder coerces it."""
    try:
        return coerce_bool(conf['EVENT_LOG'], f"{SETTINGS_NAME}['EVENT_LOG']")
    except ImproperlyConfigured:
        # unreadable is E031's finding; assume off, so the rest stays quiet
        return False


def _redis_is_in_use() -> bool:
    """Whether anything this configuration selects actually connects to Redis.

    ``REDIS_URL`` was a hard requirement while Redis was the only transport, and `W002` asked
    every enabled bot for it. It is one setting among several now, so on a RabbitMQ or Kafka
    deployment with memory storage that warning is about a key nothing reads — and a warning
    about an unused setting is how an operator learns to stop reading warnings.

    Two consumers, and either is enough: a Redis broker, or the Redis FSM storage. ``SHIPPED``
    answers for the broker rather than the class, so this stays true where the driver is not
    installed and imports nothing to find out.

    A ``BROKER`` outside that table is somebody's own, and nothing here can know what it
    connects with. The answer is then no: a missing warning costs a moment's confusion at the
    first send, and a wrong one costs the credibility of every other warning in the list.

    ``FSM_STORAGE`` is compared as a member first, because ``str()`` on one does not give its
    value: ``StorageKind`` mixes in ``str``, and since 3.11 ``str(StorageKind.REDIS)`` is
    ``'StorageKind.REDIS'``. Normalising that reads ``'storagekind.redis'`` and matches nothing,
    so a project passing the enum this package publishes — which is what `API.md` documents it
    for — had its warning suppressed and then needed ``REDIS_URL`` at runtime anyway.
    """
    from django_aiogram.broker.registry import SHIPPED  # noqa: PLC0415 - only when the checks run

    broker = str(conf.get('BROKER') or '').strip()
    driver, _extra = SHIPPED.get(broker, ('', ''))
    return driver == 'redis' or _redis_fsm_storage()


def _redis_fsm_storage() -> bool:
    """Whether ``FSM_STORAGE`` names the Redis store.

    Compared as a member first, because ``str()`` on one does not give its value:
    ``StorageKind`` mixes in ``str``, and since 3.11 ``str(StorageKind.REDIS)`` is
    ``'StorageKind.REDIS'``. Normalising that reads ``'storagekind.redis'`` and matches
    nothing, so a project passing the enum this package publishes — which `API.md` documents
    it for — had its warning suppressed and then needed ``REDIS_URL`` at runtime anyway.
    """
    return as_member(conf.get('FSM_STORAGE'), StorageKind) is StorageKind.REDIS


def _identity_matters() -> bool:
    """Whether the configured transport keys anything on the worker's name.

    True when it cannot be answered, which is the safe direction: an unresolvable ``BROKER``
    is `E047`'s finding, and suppressing an unrelated rule on the back of it would hide advice
    that is right for the default transport.

    A throwaway instance rather than :func:`~django_aiogram.broker.registry.get_broker`,
    which builds the one this process will *use* and caches it. A system check runs in
    `migrate`, `shell` and every other management command, and leaving a live broker behind
    in each of them is a side effect nobody asked this rule for. Constructing one costs
    nothing and connects to nothing — both shipped transports only set flags in `__init__`.
    """
    from django_aiogram.config.checks.transport import (  # noqa: PLC0415 - a rule's helpers, asked by a question
        _broker_error,
        _configured_broker,
    )

    try:
        return bool(_configured_broker(verify_driver=True)().needs_identity)
    except (_broker_error(), ImproperlyConfigured):
        return True
