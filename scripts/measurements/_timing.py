"""What both driver measurements need: a timer that reports rather than prints.

Kept apart from the two scripts so they differ only in what they measure, and so the thing that
makes a number comparable — same rounds, same statistic, same reporting — is written once.
"""

import logging
import statistics
import time
import uuid
from collections.abc import Callable

__all__ = ('configure_reporting', 'measure')

#: the package's logger, exactly. `tests/test_logging_discipline.py` looks only at `src/`, so a
#: child name here would pass — but the convention it enforces there is exactness, and a second
#: spelling of the same idea is the kind of thing a reader has to stop and check
logger = logging.getLogger('django_aiogram')

#: what a run reports on. The median rather than the mean, because a single scheduling hiccup
#: in a few hundred rounds moves a mean and says nothing about the driver
ROUNDS = 300


def configure_reporting() -> None:
    """Send this log to stdout, since a measurement's output *is* its result.

    Called by a script a person ran on purpose, which is the only place changing the package
    logger's level is anybody's business.
    """
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def run_name(kind: str) -> str:
    """Name this run's queue, topic or key so it cannot collide with anything already there.

    Every script here creates something on the broker and tears it down afterwards, and the
    broker is whatever the environment points at. A fixed name would make that teardown destroy
    somebody else's queue of the same name, and two runs at once would measure each other's
    traffic. Unique per process, prefixed so a leftover is recognisable as this package's.
    """
    return f'django-aiogram-{kind}-{uuid.uuid4().hex[:8]}'


def measure(label: str, call: Callable[[], None], rounds: int = ROUNDS) -> float:
    """Time ``call`` ``rounds`` times and report the median in microseconds.

    One warm-up call first, and it is not counted: the first publish of a run pays for a
    connection, a topic lookup or a channel, none of which is what is being compared.
    """
    call()
    samples = [_one(call) for _ in range(rounds)]
    median = statistics.median(samples)
    ninetieth = sorted(samples)[int(rounds * 0.9)]
    logger.info('  %-46s %9.1f us   (p90 %9.1f)', label, median, ninetieth)
    return median


def _one(call: Callable[[], None]) -> float:
    """Time a single call, in microseconds."""
    started = time.perf_counter()
    call()
    return (time.perf_counter() - started) * 1e6
