"""Record what happened, without making anyone wait for the database.

Every seam that records runs somewhere the ORM cannot be used directly: the
send coroutine runs on the bot's event loop, the delivery consumer runs on a
thread nothing manages a connection for, and a Django view must not pay a
second round trip to log the one it just made. All of them hand an :class:`Event`
to a bounded queue that one writer thread drains in batches.

``record()`` reaches only ``Queue.put_nowait`` — a lock, a deque append and a
notify. Nothing in that chain is decorated ``@async_unsafe``, which is what
makes it legal from a coroutine with no ``sync_to_async`` and no
``SynchronousOnlyOperation``. One setting suspends that, and only one:
``EVENT_LOG_SYNC`` inserts on the calling thread on purpose, which is why it is
documented as a testing setting and why it declines to act inside a running loop.

Going through the queue also avoids what a synchronous insert would do
inside a caller's ``atomic()`` block: on PostgreSQL a failed statement aborts
the whole transaction, so logging would corrupt the caller's data.

This module must not import ``django.db``. :mod:`django_aiogram.eventlog.writer`
does, and the writer thread imports it on its first flush — on its *first write*,
more precisely, because since 3.1.0 the writer also runs with the log off, for
``events_recorded`` receivers alone, and such a process never reaches it at all.

What is left here is the queue, the one thread that drains it, and the lifecycle around
both: when the writer starts, what it does with a batch, what a fork or a ``stop()``
leaves behind, and the gap row that says how much was lost. Four neighbours carry the
parts that are not about that thread, and each states its own rule:

* :mod:`~django_aiogram.eventlog.records` — the shapes that cross the queue, which
  travel further than the recorder does.
* :mod:`~django_aiogram.eventlog.pacing` — the numbers, and the promise that reading one
  never raises on this thread.
* :mod:`~django_aiogram.eventlog.bookkeeping` — the two counts more than one thread
  touches, each behind its own lock.
* :mod:`~django_aiogram.eventlog.publishing` — the fan-out to ``events_recorded``, and
  the promise that it cannot raise.
"""

import asyncio
import atexit
import contextlib
import logging
import os
import queue
import threading
import time
from dataclasses import replace

from django.core.signals import setting_changed

from django_aiogram.config.enums import EventKind
from django_aiogram.config.settings import SETTINGS_NAME, coerce_bool, conf
from django_aiogram.eventlog.bookkeeping import DropLedger, ThreadMarks
from django_aiogram.eventlog.events import known_kinds, worker_identity
from django_aiogram.eventlog.pacing import (
    FAILURE_BACKOFF,
    FAILURE_LIMIT,
    STOP_TIMEOUT,
    WRITER_THREAD,
    batch_size,
    buffer_size,
    flush_interval,
)
from django_aiogram.eventlog.publishing import publish
from django_aiogram.eventlog.records import Event, Wake
from django_aiogram.eventlog.signals import events_recorded

logger = logging.getLogger('django_aiogram')


def _acknowledge(wakes: list[Wake]) -> None:
    """Release everything waiting on this batch."""
    for wake in wakes:
        if wake.done is not None:
            wake.done.set()


class EventRecorder:
    """A bounded queue, and the one thread that drains it into the database."""

    def __init__(self) -> None:
        """Hold nothing: no setting is read and no thread starts until the first event."""
        self._queue: queue.Queue[Event | Wake] | None = None
        self._thread: threading.Thread | None = None
        self._guard = threading.Lock()
        self._stopping = threading.Event()
        self._enabled: bool | None = None
        self._kinds: frozenset[str] | None = None
        self._worker: str | None = None
        self._owner_pid = os.getpid()
        self._fork_hook = False
        # each with its own lock, and neither is `_guard`: that one is held across
        # starting a thread, and both of these are reached from paths that must not wait
        # on it. What they hold and who reaches them is in `eventlog.bookkeeping`
        self._drops = DropLedger()
        self._marks = ThreadMarks()

    @property
    def enabled(self) -> bool:
        """Whether this process writes the event feed at all."""
        # one read, kept local: a reset() between two reads would return None
        enabled = self._enabled
        if enabled is None:
            enabled = self._enabled = self._read_flag()
        return enabled

    @property
    def active(self) -> bool:
        """Whether anything at all is reading events: the table, a receiver, or both.

        This is the gate every seam that *produces* an event belongs behind, and
        :attr:`enabled` is not — a project that connects a receiver and leaves the
        table off must still get its events, and gating on the table alone is how
        an advertised metric comes out silently empty.

        ``bool(receivers)`` rather than ``has_listeners()``: 7ns against 172ns
        measured, and this is read once per event. The difference is a receiver
        whose weak reference has died but whose entry has not been cleaned up yet,
        which makes this answer yes while nothing listens — so an event is recorded
        that nobody reads, and ``send_robust`` short-circuits on it. Wasted work,
        never a wrong row. ``tests/test_metrics_seam.py`` pins the attribute, since
        it is Django's to rename.
        """
        return self.enabled or bool(events_recorded.receivers)

    @property
    def wants_payload(self) -> bool:
        """Whether anything will read a payload summary, which is the costly part.

        Only the table does. ``describe()`` redacts credentials, walks the
        structure and bounds the result — measured in tens of microseconds, against
        nothing for a counter keyed on ``kind`` and ``function``. So unless the log
        is on too, a receiver gets ``Event`` objects whose ``detail`` carries what
        the seam measured itself and not the summarized arguments. Rows are what the
        table gets; with the log off there are none.
        """
        return self.enabled

    @property
    def worker(self) -> str:
        """Name the process recording, cached: gethostname() is a system call."""
        # one read, kept local, for the same reason `enabled` does it
        worker = self._worker
        if worker is None:
            worker = self._worker = worker_identity()
        return worker

    def _read_flag(self) -> bool:
        """Read the flag once, treating an unreadable one as off."""
        try:
            return coerce_bool(conf['EVENT_LOG'], f"{SETTINGS_NAME}['EVENT_LOG']")
        except Exception:
            # a misconfigured flag is E031's finding at boot; at runtime it must
            # not become the reason a message was not sent
            logger.exception('could not read the event log flag; recording is off')
            return False

    def wants(self, kind: str) -> bool:
        """Whether this kind is one the project asked to keep."""
        kinds = self._kinds
        if kinds is None:
            configured = conf['EVENT_LOG_KINDS'] or ()
            kinds = self._kinds = frozenset(str(name) for name in configured) or known_kinds()
        return kind in kinds

    def record(self, event: Event) -> None:
        """Hand one event over. Never blocks, never raises, never touches the ORM."""
        if not self.active:
            return
        try:
            if not self.wants(event.kind):
                return
            if not event.worker:
                # every row says which process recorded it, and only the
                # consumer knew its own name before
                event = replace(event, worker=self.worker)
            if self._write_here():
                # counted, which it was not: under EVENT_LOG_SYNC the row is written on
                # this thread, and a database that refuses it raises `EventLogRefusedError`
                # — which the broad `except` below logged and did not count, so the row
                # vanished with no gap marker. A one-row batch either lands or raises, so
                # there is no partial case to weigh here, only this one
                try:
                    self._deliver([event])
                except Exception:
                    self._drops.lost(1)
                    logger.exception('could not record an event on the calling thread', extra={'tg_kind': event.kind})
                finally:
                    # this thread wrote, and this thread is not the one that closes: the
                    # mark exists for the writer's exit, and a caller's mark left behind
                    # outlives the caller. Thread idents are reused, so a receiver-only
                    # writer could inherit one and close a connection it never opened —
                    # importing `eventlog`, and `django.db` with it
                    self._marks.forget()
                return
            buffer = self._buffer()
            buffer.put_nowait(event)
            if self._queue is not buffer:
                # stop() detached this queue between the lookup and the put, and
                # nothing will ever drain a detached one again
                self._rehome(buffer)
        except queue.Full:
            self._drops.lost(1)
        except Exception:
            # the recorder failing is not the caller's problem to handle
            logger.exception('could not record an event', extra={'tg_kind': event.kind})

    def _rehome(self, orphan: 'queue.Queue[Event | Wake]') -> None:
        """Move what is in a detached queue onto the live one.

        Not "move my own event": ``get_nowait`` may hand back somebody else's, and
        it does not matter which — what matters is that the detached queue ends up
        empty and everything in it lands somewhere that will be drained. Two
        producers doing this at once take disjoint items, and stop()'s own drain
        competing with them is equally harmless.

        Both halves are non-blocking, so :meth:`record`'s promise holds: an event
        that cannot be rehomed because the live queue is full is counted, which is
        what would have happened to it there anyway.
        """
        while True:
            try:
                item = orphan.get_nowait()
            except queue.Empty:
                return
            try:
                self._buffer().put_nowait(item)
            except queue.Full:
                if isinstance(item, Wake):
                    _acknowledge([item])
                    continue
                self._drops.lost(1)

    def _write_here(self) -> bool:
        """Whether to write on this thread instead of handing it to the writer.

        Only when asked to, and never from inside a running loop: the ORM is
        @async_unsafe there, so the seam that records an update would raise
        SynchronousOnlyOperation instead of recording anything.

        Requires the log as well as the flag. ``EVENT_LOG_SYNC`` is about *where the
        insert happens*, so with nothing being inserted it has nothing to say — and
        answering yes would have a process that only has receivers run them on the
        thread that recorded the event, which is the one thing this design exists
        to avoid.
        """
        if not self.enabled:
            return False
        if not coerce_bool(conf['EVENT_LOG_SYNC'], f"{SETTINGS_NAME}['EVENT_LOG_SYNC']"):
            return False
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return True
        return False

    def _buffer(self) -> queue.Queue[Event | Wake]:
        """Return the queue, starting the writer the first time anything is recorded."""
        if self._owner_pid != os.getpid():
            # a thread does not survive fork(), but the queue object does, so a
            # child would fill one nobody drains
            self._forget()
        buffer = self._queue
        if buffer is not None:
            return buffer
        with self._guard:
            if self._queue is None:
                self._install_fork_hook()
                self._stopping.clear()
                self._owner_pid = os.getpid()
                buffer = queue.Queue(maxsize=buffer_size())
                thread = threading.Thread(target=self._run, args=(buffer,), name=WRITER_THREAD, daemon=True)
                self._queue, self._thread = buffer, thread
                try:
                    thread.start()
                except RuntimeError:
                    # out of threads: leave nothing half-built for the next call,
                    # and count the event this loses so the gap row still says so
                    self._queue = self._thread = None
                    self._drops.lost(1)
                    raise
                # CPython runs atexit callbacks while daemon threads are still
                # alive, so the writer is still joinable from one
                atexit.register(self.stop)
            return self._queue

    def _install_fork_hook(self) -> None:
        """Reset in the child as well as on the pid check, where the platform allows."""
        if self._fork_hook or not hasattr(os, 'register_at_fork'):
            return
        self._fork_hook = True
        os.register_at_fork(after_in_child=self._forget)

    def _forget(self) -> None:
        """Drop everything a fork invalidated, so the next event starts fresh."""
        # new locks throughout: the parent may have held any of them at the moment of the
        # fork, and a lock inherited held is a lock nothing in this process can release
        self._guard = threading.Lock()
        self._drops = DropLedger()
        self._marks = ThreadMarks()
        self._queue = None
        self._thread = None
        self._owner_pid = os.getpid()

    def _run(self, buffer: queue.Queue[Event | Wake]) -> None:
        """Drain the queue into the database until stopped.

        A thread target: anything escaping it would end recording for the life
        of the process, so the slot is cleared on the way out and the next
        record() starts a replacement.
        """
        failures = 0
        blocked_until = 0.0
        try:
            while True:
                batch, wakes = self._collect(buffer)
                try:
                    if batch:
                        if time.monotonic() < blocked_until:
                            # the database has been refusing us; keep draining so
                            # producers never fill up, but do not hammer it
                            self._drops.lost(len(batch))
                        else:
                            failures, blocked_until = self._flush(batch, failures=failures)
                finally:
                    # after the write, never before: a waiter released early was
                    # told the batch was durable while it was still in flight
                    _acknowledge(wakes)
                # not `wakes and ...`: the wake `stop()` queues is the usual way this
                # thread learns, but it is not the only one — `stop()` called from a
                # receiver runs on *this* thread and drains this very buffer through
                # `_abandon`, taking that wake with it. The loop then never saw one and
                # spun for the life of the process, holding a connection.
                #
                # `_queue is not buffer` is the other half, and it is per writer where the
                # flag is not: `stop()` detaches this queue and sets `_stopping`, and a
                # `record()` that lands next calls `_buffer()`, which *clears* the flag and
                # starts a replacement. This writer then saw an empty detached queue with
                # the flag down and waited on it for the life of the process. Its own
                # buffer no longer being the recorder's queue says the same thing and
                # cannot be undone by anybody else
                if buffer.empty() and (self._stopping.is_set() or self._queue is not buffer):
                    return
        except Exception:
            logger.exception('the event writer stopped; it restarts on the next event')
        finally:
            with self._guard:
                if self._queue is buffer:
                    self._queue = self._thread = None
            # the slot is cleared above, so nothing will ever drain this queue
            # again: without this, everything still in it disappears with no row
            # and no counter, and the gap reads as quiet traffic
            self._abandon(buffer)
            if self._marks.take():
                # a process that only has receivers never opened one, and importing
                # `eventlog` to close it would pull in `django.db` — the one import
                # this module exists to keep out of a process that does not need it
                self._close_connections()

    @staticmethod
    def _empty(buffer: 'queue.Queue[Event | Wake]') -> tuple[list[Event], list[Wake]]:
        """Take everything left in a queue, without waiting for more."""
        events: list[Event] = []
        wakes: list[Wake] = []
        while True:
            try:
                item = buffer.get_nowait()
            except queue.Empty:
                return events, wakes
            if isinstance(item, Wake):
                wakes.append(item)
            else:
                events.append(item)

    def _abandon(self, buffer: 'queue.Queue[Event | Wake]') -> None:
        """Write what is left in a queue nobody will drain again, or count it lost.

        **Receivers run on whatever thread calls this**, which is the writer's own
        when it is exiting and the caller's when :meth:`stop` reached a queue the
        writer had already left behind. That is not a lapse in the writer-thread
        contract so much as the end of it: this queue exists precisely because no
        writer will ever drain it, so there is no writer thread to route through.

        Publishing anyway rather than dropping, because these are the last events
        before the process goes — the same reasoning that makes this method write
        them instead of discarding them. The contract says so on all three surfaces
        that state it.
        """
        leftover, wakes = self._empty(buffer)
        _acknowledge(wakes)
        if not leftover:
            return
        try:
            # the refused count, not only the raise: a database that takes some of these
            # rows and refuses others leaves a hole exactly as large as what it refused,
            # and ignoring the return counted that hole as zero
            refused = self._deliver(leftover)
        except Exception:
            logger.exception('could not write the events a stopping writer left behind')
        else:
            self._drops.lost(refused)
            return
        # counted, not silent: the next flush that succeeds turns this into a
        # log.dropped row, which is the only place the gap becomes visible
        self._drops.lost(len(leftover))

    def _collect(self, buffer: queue.Queue[Event | Wake]) -> tuple[list[Event], list[Wake]]:
        """Gather up to one batch, with any wake-ups that ended the wait."""
        interval = flush_interval()
        limit = batch_size()
        deadline = time.monotonic() + interval
        batch: list[Event] = []
        wakes: list[Wake] = []
        while len(batch) < limit:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                item = buffer.get(timeout=remaining)
            except queue.Empty:
                break
            if isinstance(item, Wake):
                wakes.append(item)
                break
            batch.append(item)
        return batch, wakes

    def _flush(self, batch: list[Event], *, failures: int) -> tuple[int, float]:
        """Write one batch and publish it, containing whatever the write raises.

        Both happen inside :meth:`_deliver`, which writes first and publishes in a
        ``finally`` — so a failing database costs rows and not metrics, and nothing
        reaching the ``except`` here came from a receiver:
        :func:`~django_aiogram.eventlog.publishing.publish` cannot raise.

        Which means **receivers run on whatever thread calls this**, and that is not
        only the writer's: :meth:`drain_once` calls it on the caller's, which is what
        lets a test drive the real flush path. The signal's own documentation states
        the rule that way round rather than listing the threads, so a fourth one does
        not make it wrong.
        """
        dropped_before = self._drops.total()
        try:
            refused = self._deliver(batch)
        except Exception:
            failures += 1
            self._drops.lost_quietly(len(batch))
            # one line per failure, not two: the suspension is a different
            # sentence about the same exception, not a second thing that broke
            if failures >= FAILURE_LIMIT:
                logger.exception(
                    'the event log is suspended after repeated failures; run migrate or check the database',
                    extra={'tg_count': len(batch), 'tg_failures': failures},
                )
                return 0, time.monotonic() + FAILURE_BACKOFF
            logger.exception(
                'could not write an event batch',
                extra={'tg_count': len(batch), 'tg_failures': failures},
            )
            return failures, 0.0
        if refused:
            # rows this very batch lost, one at a time, on the ladder below `write_batch`.
            # Counted rather than reported now: the gap row belongs to the *next*
            # successful flush, the same way a producer's drop does
            self._drops.lost_quietly(refused)
            logger.warning(
                'the database refused part of an event batch',
                extra={'tg_count': refused, 'tg_batch': len(batch)},
            )
        if dropped_before:
            self._record_gap(dropped_before)
        return 0, 0.0

    def _record_gap(self, dropped: int) -> None:
        """Put the gap in the feed, not only in the log: a silent hole reads as coverage.

        **Claimed, then written, and given back if the write fails.** Two flushes can be
        in progress at once — the writer thread's and a ``drain_once()`` on somebody
        else's — and both snapshot the drop count before their batch. Subtracting after
        the write let each of them report the same hole and take it off twice, which
        drives the count negative; subtracting before the write, which is where this
        started, lost the hole whenever the gap row itself was refused. Taking the count
        out of the counter first makes the claim exclusive, and putting it back on failure
        keeps it for the next flush. Both properties, one lock.

        Claims no more than is there: a count that another flush has already taken leaves
        nothing to report, and this returns rather than writing a row about zero events.
        Anything a producer drops while the write is in flight stays for the next one.

        A refusal counts as a failure here, not only an exception. ``_deliver`` returns how
        many rows the database refused one at a time, and this batch is one row — so a
        return of 1 means the gap row did *not* land, which is the same loss as a raise and
        was the one path this method used to ignore. Both give the claim back.

        The failure stays suppressed either way: the batch this follows did land, and a
        gap row that cannot be written must not turn a successful flush into a failed one.
        """
        claimed = self._drops.claim(dropped)
        if not claimed:
            return
        try:
            refused = self._deliver([Event(kind=EventKind.LOG_DROPPED.value, detail={'dropped': claimed})])
        except Exception:
            self._drops.give_back(claimed)
            logger.exception('could not record the gap; keeping the count for the next flush')
            return
        if refused:
            self._drops.give_back(claimed)
            logger.error(
                'the database refused the gap row; keeping the count for the next flush',
                extra={'tg_dropped': claimed},
            )

    @staticmethod
    def _write(batch: list[Event]) -> int:
        """Hand a batch to the ORM, importing it here so a disabled process never does.

        Returns how many rows did not land, which only a partial refusal produces.
        """
        from django_aiogram.eventlog.writer import write_batch  # noqa: PLC0415 - the point: no django.db above

        return write_batch(batch)

    def _deliver(self, batch: list[Event]) -> int:
        """Write a batch if this process keeps the table, then publish it either way.

        Returns how many rows the database refused individually — zero unless a partial
        refusal happened, and zero in a process that does not write at all.


        The order is the contract, and it is two claims rather than one. The write
        is **attempted first**, so nothing a receiver does can change a row that was
        written — which is why receivers get the real ``Event`` objects and not
        copies. And the publish is in a ``finally``, so a write that *failed* still
        reaches them: a database that is down or unmigrated is exactly when someone
        is watching a dashboard, and the metrics have no reason to go with it. A
        receiver seeing a batch is therefore not evidence that a row exists for it.

        Publishing first was the original order, and it handed receivers the same
        list and the same ``Event`` objects the ORM was about to read. A frozen
        dataclass does not freeze the ``detail`` dict inside it, so a receiver
        clearing the list or editing a ``detail`` changed what got persisted. This
        way round makes that impossible instead of asking receivers to be careful.
        """
        refused = 0
        try:
            if self.enabled:
                self._marks.mark()
                refused = self._write(batch)
        finally:
            publish(self, batch)
        return refused

    @staticmethod
    def _close_connections() -> None:
        """Release the writer thread's own connection on the way out."""
        try:
            from django_aiogram.eventlog.writer import close_connections  # noqa: PLC0415 - as above
        except Exception:
            logger.exception('could not import the event log to close its connection')
            return
        close_connections()

    def drain_once(self, timeout: float = 0.0) -> int:
        """Write whatever is buffered, on the calling thread. Returns events processed.

        Events taken off the queue, not rows that landed: `_flush` swallows a failed write
        and the refused count `_deliver` returns is dropped here, so a batch the database
        rejected still counts. A caller reading this as a durability signal is reading the
        wrong number — the gap rows and `log.dropped` are what say what was lost.

        Goes through the same flush the writer uses, gap recording included, so
        a test driving this exercises the path production takes.
        """
        buffer = self._queue
        if buffer is None:
            return 0
        batch: list[Event] = []
        wakes: list[Wake] = []
        deadline = time.monotonic() + timeout
        while True:
            try:
                item = buffer.get(timeout=max(0.0, deadline - time.monotonic())) if timeout else buffer.get_nowait()
            except queue.Empty:
                break
            if isinstance(item, Wake):
                wakes.append(item)
                continue
            batch.append(item)
        try:
            if batch:
                self._flush(batch, failures=0)
        finally:
            # same reason as the synchronous `record()` path: this thread wrote, and it is
            # not the thread whose exit closes connections
            self._marks.forget()
            _acknowledge(wakes)
        return len(batch)

    def flush(self, timeout: float = STOP_TIMEOUT) -> None:
        """Wait until what has been recorded so far has reached the database.

        Waits on an acknowledgement from the writer rather than on the queue
        going empty: the queue empties when a batch is *taken*, which is before
        it is written, so polling it would return mid-insert.
        """
        buffer = self._queue
        if buffer is None:
            return
        done = threading.Event()
        try:
            buffer.put_nowait(Wake(done))
        except queue.Full:
            # no room even for the marker, so there is nothing to wait behind
            return
        if not done.wait(timeout):
            logger.warning('the event writer did not flush in time', extra={'tg_timeout': timeout})

    def stop(self, timeout: float = STOP_TIMEOUT) -> None:
        """Flush and end the writer. Idempotent: atexit and start_tgbot both call it."""
        with self._guard:
            buffer, thread = self._queue, self._thread
            self._queue = self._thread = None
        if buffer is None:
            return
        with contextlib.suppress(Exception):
            atexit.unregister(self.stop)
        self._stopping.set()
        with contextlib.suppress(queue.Full):
            buffer.put_nowait(Wake())
        if thread is not None and thread is not threading.current_thread():
            # a thread that never started cannot be joined, and this runs from
            # atexit where raising is noise nobody can act on
            with contextlib.suppress(RuntimeError):
                thread.join(timeout)
            if thread.is_alive():
                logger.warning('the event writer did not finish in time', extra={'tg_timeout': timeout})
        elif thread is not None:
            # `stop()` from a receiver, which runs on the writer's own thread: joining
            # would be waiting for itself, and the old code reported that as a writer
            # that missed its deadline. It has not missed anything — it unwinds through
            # the loop below as soon as this returns
            logger.debug('stop() was called on the writer thread; it will unwind on its own')
        # a record() that read self._queue before the swap above puts into a queue
        # this method has already detached, and nothing else will ever look at it.
        # Draining after the join is what keeps those events; the few instructions
        # between this drain and the producer's put stay a gap, because closing it
        # would mean a lock on the one path that may never wait
        try:
            self._abandon(buffer)
        finally:
            # the same rule as the synchronous `record()` and `drain_once()`: this ran on
            # whoever called `stop()`, and that thread is not the one whose exit closes
            # connections. Left behind, its mark can be inherited by a later
            # receiver-only writer through a reused ident.
            #
            # Unless `stop()` was called *from* the writer, where `_run` is still on the
            # stack below and has that mark to consume on its way out. Reached by a
            # receiver calling `stop()`, since receivers run on that thread —
            # `test_a_receiver_that_stops_the_log_does_not_strand_the_writer` covers both
            # halves and fails without either
            if threading.current_thread() is not thread:
                self._marks.forget()

    def reset(self) -> None:
        """Re-read the settings next time; used by override_settings.

        It does not flush. Every ``override_settings(TELEGRAM_BOT=...)`` in a
        consumer's own test suite fires this twice, and waiting for the writer
        there would put a second on each one. A test that needs its rows calls
        :meth:`flush`; queued events survive the reset either way.
        """
        self._enabled = None
        self._kinds = None
        self._worker = None


recorder = EventRecorder()


def _reset_on_setting_change(setting: str, **_kwargs: object) -> None:
    """Drop the cached flags after writing whatever was recorded under the old ones."""
    if setting == SETTINGS_NAME:
        recorder.reset()


# dispatch_uid keeps autoreload from stacking duplicate receivers
setting_changed.connect(_reset_on_setting_change, dispatch_uid='django_aiogram.eventlog.recorder')
