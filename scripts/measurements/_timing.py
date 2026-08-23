"""What both driver measurements need: a timer that reports rather than prints.

Kept apart from the two scripts so they differ only in what they measure, and so the thing that
makes a number comparable — same rounds, same statistic, same reporting — is written once.
"""

import logging
import statistics
import time
from collections.abc import Callable

__all__ = ('configure_reporting', 'measure')

#: a child of the package's logger, so it propagates to whatever a reader has configured for
#: `django_aiogram` without mixing benchmark output into it under the same name. Never the root,
#: which is the rule `AGENTS.md` states
logger = logging.getLogger('django_aiogram.measurements')

#: what a run reports on. The median rather than the mean, because a single scheduling hiccup
#: in a few hundred rounds moves a mean and says nothing about the driver
ROUNDS = 300


def configure_reporting() -> None:
    """Send this module's log to stdout, since a measurement's output *is* its result."""
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


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
