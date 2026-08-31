"""The aiogram objects a project's settings describe, and the refusals they earn.

Settings in, aiogram objects out. Everything here is built once, lazily, from a value a
project wrote -- so every failure is a configuration error rather than a runtime one, and
each is raised as ``ImproperlyConfigured`` naming the key that produced it.

Apart from the client because the client is a facade over objects, not a builder of them,
and because a check or a test asking "would this setting build" should not have to make a
bot to find out.
"""

import logging
from typing import TYPE_CHECKING, Any

from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.base import BaseStorage
from aiogram.fsm.storage.memory import MemoryStorage
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string

from django_aiogram.config.enums import StorageKind
from django_aiogram.config.settings import SETTINGS_NAME, conf
from django_aiogram.eventlog.instrumentation import instrumented
from django_aiogram.redis import connection_kwargs

if TYPE_CHECKING:
    from collections.abc import Mapping

logger = logging.getLogger('django_aiogram')


def build_default_properties() -> DefaultBotProperties:
    """Build the bot-wide defaults such as parse_mode.

    aiogram applies these to every call, which is why unset fields carry a
    ``Default`` sentinel rather than None.
    """
    properties: Mapping[str, Any] = conf['DEFAULT_BOT_PROPERTIES']
    try:
        return DefaultBotProperties(**properties)
    except TypeError as error:
        msg = f"{SETTINGS_NAME}['DEFAULT_BOT_PROPERTIES']: {error}"
        raise ImproperlyConfigured(msg) from None


def build_storage() -> BaseStorage:
    """Build the FSM storage: 'redis', 'memory', or a dotted path to a BaseStorage.

    The type is checked before anything is done with the value. ``E011`` already refuses a
    non-string at ``manage.py check``, but a project that boots without running the checks
    reached ``import_string(None)`` and got ``AttributeError: 'NoneType' object has no
    attribute 'rsplit'`` out of Django's internals -- a traceback that names neither this
    package nor the setting that caused it, from a module whose whole remit is that every
    refusal names its key.
    """
    name = conf['FSM_STORAGE']
    if not isinstance(name, str):
        msg = f"{SETTINGS_NAME}['FSM_STORAGE'] must be 'redis', 'memory', or a dotted path, got {type(name).__name__}."
        raise ImproperlyConfigured(msg)
    if name == StorageKind.MEMORY:
        return instrumented(MemoryStorage())
    if name == StorageKind.REDIS:
        url = str(conf['REDIS_URL'] or '').strip()
        if not url:
            msg = f"{SETTINGS_NAME}['REDIS_URL'] is required for the redis FSM storage."
            raise ImproperlyConfigured(msg)
        # imported here, not at module scope: aiogram's Redis storage imports the driver,
        # which is an extra since 4.0 — and a project on `memory` or another transport must
        # be able to import this module at all. `django_aiogram.redis` does the same
        from aiogram.fsm.storage.redis import RedisStorage  # noqa: PLC0415 - as above

        # the same deadlines the shared client gets: every update reads FSM state,
        # so a half-open Redis here wedges the whole bot rather than one send
        return instrumented(RedisStorage.from_url(url, connection_kwargs=connection_kwargs()))

    try:
        storage_class = import_string(name)
    # `ValueError` alongside it: a path whose module part is empty -- `'.Storage'`, which is what a
    # copied path or a half-written relative import looks like -- reaches `import_module('')` and
    # raises that instead. Measured on Django 6.1
    except (ImportError, ValueError) as error:
        msg = f"{SETTINGS_NAME}['FSM_STORAGE'] cannot be imported: {error}"
        raise ImproperlyConfigured(msg) from error
    if not (isinstance(storage_class, type) and issubclass(storage_class, BaseStorage)):
        msg = f"{SETTINGS_NAME}['FSM_STORAGE'] must point to a BaseStorage subclass, got {name!r}."
        raise ImproperlyConfigured(msg)
    # wrapped last, so the project's own class is validated before it is hidden
    return instrumented(storage_class())
