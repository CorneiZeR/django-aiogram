"""How long the writer waits, when it gives up, and how it reads the numbers it is given.

Four fixed constants and three settings readers, together because they answer one
question -- how fast the writer runs -- and because the readers all obey a rule the
constants make possible: **nothing here raises.**

That rule is not caution. Every one of these is read on the writer thread, inside its
loop, past the net :meth:`~django_aiogram.eventlog.recorder.EventRecorder._flush` puts
around a write: an exception here ends the writer and takes the whole buffer with it,
which is a steep price for a typo in a batch size. The checks (``E036`` to ``E038``)
report an unreadable value at boot, once, where somebody can act on it; here the answer
is the default and the writer keeps going.
"""

from collections.abc import Callable
from typing import Any

from django.core.exceptions import ImproperlyConfigured

from django_aiogram.config.defaults import DEFAULTS
from django_aiogram.config.settings import conf

#: the writer's thread name, so a log line or a test can name it
WRITER_THREAD = 'tgbot-event-writer'

#: how long stop() waits for the writer before giving up on what it holds
STOP_TIMEOUT = 5.0
#: consecutive failed flushes after which the writer stops trying for a while
FAILURE_LIMIT = 5
#: how long it drains and discards before probing the database again
FAILURE_BACKOFF = 60.0


def number(key: str, cast: Callable[[Any], float]) -> float:
    """Read one of the writer's own dials, falling back to its default.

    See this module's own docstring for why a raise is not an option. ``OverflowError``
    is the member of that list that is not a typo: ``int(float('inf'))`` raises it, and a
    settings dict can hold ``inf`` directly -- the environment cannot, it is refused
    there -- so without it the writer thread ends on a value ``E044`` only reports.
    """
    try:
        return cast(conf[key])
    except (ImproperlyConfigured, KeyError, TypeError, OverflowError, ValueError):
        # ImproperlyConfigured from resolving the settings, the rest from the cast
        return cast(DEFAULTS[key])


def flush_interval() -> int:
    """Seconds before a partial batch is written anyway, as ``E038`` defines it.

    An integer, matching the check and the settings page. Read as a float this honoured a
    fractional interval the check refuses, so a value could pass ``manage.py check`` and
    then behave in a way the check called impossible -- one setting with two rules. Named
    rather than inline so the rule has one reader and a test can ask it directly.
    """
    return int(max(1, number('EVENT_LOG_FLUSH_INTERVAL', int)))


def batch_size() -> int:
    """How many events one flush may take, at least one."""
    return max(1, int(number('EVENT_LOG_BATCH_SIZE', int)))


def buffer_size() -> int:
    """How many events may wait before producers start losing them, at least one."""
    return max(1, int(number('EVENT_LOG_BUFFER_SIZE', int)))
