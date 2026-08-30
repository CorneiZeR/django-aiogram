"""Handing a batch to whoever connected to ``events_recorded``, and never failing at it.

Its own module, and not part of :mod:`django_aiogram.eventlog.signals`: that module
imports ``django.dispatch`` and nothing else, on purpose, so a project's metrics module
can import the signal at settings time without dragging this package in behind it.

The promise here is one sentence -- :func:`publish` cannot raise -- and the rest of the
writer is built on it: anything escaping this would land in the flush's ``except`` and be
counted as a database refusing a batch it never saw.
"""

import contextlib
import logging
from typing import TYPE_CHECKING

from django_aiogram.eventlog.signals import events_recorded

if TYPE_CHECKING:
    from django_aiogram.eventlog.records import Event

logger = logging.getLogger('django_aiogram')


def receiver_name(receiver: object) -> str:
    """Name a signal receiver for a log line, without calling anything that can raise.

    ``repr()`` is deliberately not the fallback. Python evaluates every argument before
    the call, so ``getattr(receiver, '__qualname__', repr(receiver))`` evaluates ``repr``
    *even when the attribute is there* -- and a receiver whose ``__repr__`` raises would
    then take this line, and with it the rest of the batch's receivers, out through the
    flush's ``except``, where it would be counted as a failed write. That is the failure
    this module exists to contain, arriving through the code that reports it.

    ``type(receiver).__name__`` is the last resort because reading it runs nothing.
    """
    for attribute in ('__qualname__', '__name__'):
        name = getattr(receiver, attribute, None)
        if isinstance(name, str) and name:
            return name
    return type(receiver).__name__


def publish(sender: object, batch: list['Event']) -> None:
    """Hand a batch to whoever connected to :data:`events_recorded`.

    ``send_robust``, so one broken receiver neither loses the batch for the others nor
    stops the writer, and it is logged here because a receiver that fails silently is a
    metric that reads as zero traffic. Django logs it too, on its own ``django.dispatch``
    logger; the line here is on the logger a project configures for this package, which
    is where it will actually be seen.

    **Wrapped anyway, because ``send_robust`` does not contain everything.** Django's own
    failure logging reads ``receiver.__qualname__`` unguarded, and a callable *instance*
    -- an ordinary shape for a metrics collector -- has no such attribute. So a receiver
    like that raising makes ``send_robust`` itself raise ``AttributeError``, measured on
    Django 6.1, and without this ``try`` it would land in the flush's ``except`` and be
    counted as a failed *write*: the other receivers lose the batch, a ``log.dropped``
    row appears, and the log blames the database for something a receiver did.

    The upshot is a function that **cannot raise**, which is the property the rest of the
    writer needs from it rather than a defensive habit.

    One limit worth stating, because it is Django's and not ours: when ``send_robust``
    raises on that unnamed receiver it abandons **its own loop**, so receivers connected
    after the offending one do not run for that batch at all. Containing it here keeps
    the write and every earlier receiver whole; it cannot reach past Django into a
    dispatch that already stopped. Calling receivers ourselves would need
    ``Signal._live_receivers``, a private API, which is a worse trade than one documented
    sentence. A collector written as a callable instance can close the gap on its side by
    defining ``__qualname__``; one written as a function or a bound method has it already,
    and is the shape every recipe uses.

    A tuple rather than the list itself: receivers run one after another with the same
    argument, so one of them sorting or clearing a list would decide what the next sees.
    """
    if not events_recorded.receivers:
        return
    # the reporting loop is inside the guard as well as the dispatch, because
    # `getattr(..., None)` absorbs only `AttributeError` -- a receiver whose
    # `__getattr__` raises anything else makes naming it raise, and the whole point is
    # that nothing about a receiver reaches the flush's failure counter
    try:
        for receiver, outcome in events_recorded.send_robust(sender=sender, events=tuple(batch)):
            if isinstance(outcome, BaseException):
                logger.error(
                    'an events_recorded receiver raised',
                    exc_info=outcome,
                    extra={'tg_receiver': receiver_name(receiver), 'tg_count': len(batch)},
                )
    except Exception:
        # even this is suppressed: `logger.exception` is `logger.error` with `exc_info`,
        # so a project whose handler or formatter raises would take the fallback out too
        # -- and the whole purpose here is that **nothing** about publishing reaches the
        # flush's failure counter, where it would be reported as a database refusing a
        # batch it never saw
        with contextlib.suppress(Exception):
            logger.exception('publishing recorded events failed', extra={'tg_count': len(batch)})
