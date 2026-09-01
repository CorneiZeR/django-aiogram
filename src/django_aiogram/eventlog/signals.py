"""The seam a project gets metrics out of.

Deliberately a ``django.dispatch.Signal`` rather than a setting naming a dotted
path. A setting would need an entry in ``Settings.md``, a check id, an
``import_string``, a lazy cache to keep the import off the hot path, and a
decision about what to do when the path is wrong. A signal needs none of that,
``send_robust`` contains most of what a receiver can do wrong, and connecting one is
the thing every Django developer already knows how to do. *Most*: see
:func:`~django_aiogram.eventlog.publishing.publish` for the receiver shape
Django's own containment misses, which is why this package does not rely on it
alone.

This module imports ``django.dispatch`` and the standard library's ``logging`` — not
aiogram, not the ORM, not the rest of this package. A metrics module can import it at
settings time without dragging anything in.
"""

import logging
from collections.abc import Callable

from django.dispatch import Signal

logger = logging.getLogger('django_aiogram')


def _nameable(receiver: object) -> None:
    """Give a receiver a ``__qualname__`` if it has none, before Django needs one.

    ``Signal.send_robust`` contains what a receiver raises — but it then logs the failure,
    and the log line names the receiver: ``receiver.__qualname__``, evaluated *inside* the
    ``except``. A callable instance has none unless its class was written to provide one, and
    the attribute lookup then raises out of ``send_robust`` itself, abandoning its own loop.
    Every receiver connected after the offending one silently stops seeing batches.

    Measured on Django 6.1, with a collector whose ``__getattr__`` raises: unnamed, the
    dispatch ends in ``AttributeError`` and a later receiver does not run; named, both
    receivers run and the broken one's error comes back as its result, which is what
    ``send_robust`` promises.

    So the name is set here, at connect time, where it is one attribute on an object the
    project just handed us — rather than at dispatch time, where fixing it would mean
    calling receivers ourselves through a private API. Nothing is wrapped: the object Django
    stores is the object the caller passed, so a weak connection still dies with its
    referent and ``disconnect`` still finds it by identity.
    """
    try:
        named = getattr(receiver, '__qualname__', None) is not None
    except Exception:  # noqa: BLE001 - a lookup that raises is exactly the case being fixed
        named = False
    if named:
        return

    kind = type(receiver)
    try:
        receiver.__qualname__ = f'{kind.__module__}.{kind.__qualname__}'  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001 - __slots__, a read-only property, a C extension
        logger.warning(
            'a receiver of events_recorded cannot be named, so a failure in it would stop '
            'the dispatch before later receivers ran; give the class a __qualname__',
            extra={'tg_receiver': f'{kind.__module__}.{kind.__qualname__}'},
        )


class _RecordedEvents(Signal):
    """The signal, with one thing added: every receiver can be named in a log line.

    ``connect`` is public API and this override changes nothing else about it — the
    signature, the weak reference, the ``dispatch_uid`` and the return value are Django's.
    """

    def connect(
        self,
        receiver: Callable[..., object],
        sender: object = None,
        # Django's signature, positional and boolean; an override that changed it would be a
        # different method. `typing.override` would say so to the linter and needs 3.12
        weak: bool = True,  # noqa: FBT001, FBT002
        dispatch_uid: object = None,
    ) -> None:
        """Name the receiver, then connect it exactly as Django would."""
        _nameable(receiver)
        super().connect(receiver, sender=sender, weak=weak, dispatch_uid=dispatch_uid)


#: Fired once per batch of recorded events, on whichever thread flushed that batch —
#: normally the event writer's own, and three other threads can be it.
#:
#: Receivers get ``events``: a tuple of :class:`~django_aiogram.eventlog.records.Event`,
#: whose field names are pinned by ``tests/test_public_surface.py`` and are
#: therefore public API. ``sender`` is the recorder instance.
#:
#: Four things about it are load-bearing, and three of them are surprising:
#:
#: * **It fires whether or not the event log is on.** The table and the metrics
#:   are separate decisions: connect a receiver and the events flow, with
#:   ``EVENT_LOG`` left off and no migration in sight.
#: * **Payload summaries are the only part of ``detail`` the log gates.** With the
#:   log off, ``detail`` still carries what the recording seam measured itself —
#:   a send's ``duration_ms``, a retry's ``retry_after``, a queueing failure's
#:   ``stage``, a gap's ``dropped`` count. What is missing is the summarized
#:   arguments, because redacting and bounding a payload is the expensive part of
#:   recording and no part of counting. A receiver that needs message bodies needs
#:   the log on as well, and then ``EVENT_LOG_PAYLOAD`` decides what is in there.
#: * **``EVENT_LOG_KINDS`` filters this too, with one exemption.** It is one answer
#:   to "which events does this deployment care about", not two — so a receiver
#:   sees exactly the kinds the table would have kept. ``log.dropped`` is the
#:   exception, in both directions: it is the record that recording itself fell
#:   behind, and a deployment that filtered it out would read the hole as quiet
#:   traffic. The table has always been exempt from the filter for that row, and
#:   receivers are exempt with it.
#: * **The write is attempted before receivers see the batch.** So nothing a
#:   receiver does can change a row that was written, which is why they get real
#:   ``Event`` objects rather than copies — the ``detail`` dict inside one is an
#:   ordinary mutable dict shared with the other receivers, so treat it as
#:   read-only. It is *attempted*, not guaranteed: a write that failed still
#:   publishes, because a database being down is exactly when someone is watching a
#:   dashboard. Receiving a batch is therefore not evidence that a row exists for
#:   it, and with ``EVENT_LOG`` off there is no row by design.
#:
#: The rule is **whichever thread flushed the batch publishes it**, and normally that
#: is the event writer's, once the batch's own write has been attempted — so a slow
#: receiver delays neither a send nor that write, only later batches and the writer's
#: shutdown. Never delaying a send is the point of the design; the rest follows from
#: the rule rather than being promised separately.
#:
#: Three other threads can flush a batch, so three other threads can publish one:
#:
#: * whatever calls ``EventRecorder.drain_once()``, which exists so a test can drive
#:   the real flush path on its own thread
#: * at shutdown, whichever thread called ``stop()``, for whatever the writer had not
#:   drained. Those events are published rather than dropped because they are the last
#:   ones before the process goes, and there is by then no writer left to hand them to
#: * under ``EVENT_LOG_SYNC``, the thread that recorded the event, which is the one
#:   case with no batch and no writer involved at all
#:
#: ``EVENT_LOG_SYNC`` only takes effect with the log on — there is nothing to insert
#: synchronously otherwise — so the write is always attempted there, and may still
#: fail. It is a testing setting, and receivers running inside the send path is one
#: more reason to keep it one.
#:
#: **A receiver that raises no longer costs the ones after it.** ``send_robust`` logs each
#: failure by naming the receiver, and that name is looked up inside its own ``except`` — so
#: a callable instance without a ``__qualname__`` used to end the dispatch there, and every
#: receiver connected later silently stopped seeing batches. ``connect`` names such a
#: receiver now, from its class, which is the whole fix. Where the name cannot be set — a
#: class with ``__slots__``, a read-only property — connecting logs a warning saying so,
#: because that receiver can still take the dispatch down with it.
#:
#: The write happens before either, and only when ``EVENT_LOG`` is on: with the log
#: off nothing is written at all, and a receiver is the only thing the batch reaches.
events_recorded: Signal = _RecordedEvents()
