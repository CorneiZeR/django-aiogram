"""The two counts the writer keeps, each behind its own lock.

Neither is about one event. They are about what more than one thread did to a shared
recorder, which is why they are objects with a lock rather than attributes on it: a
``+=`` on a plain integer is a read and a write, and the drop that lands between them is
the drop nobody hears about.

They are separate because they answer to different threads and different questions.
:class:`DropLedger` counts what was lost and hands the count to whoever writes the gap
row; :class:`ThreadMarks` remembers which threads handed a batch to the ORM, and so which
of them has a connection to close on the way out. One lock each: they are never taken
together, and sharing one only made it look as though they were related.
"""

import logging
import threading
import time

logger = logging.getLogger('django_aiogram')

#: how often the drop counter is allowed to reach the log
DROP_REPORT_INTERVAL = 60.0


class DropLedger:
    """How many events were lost, and who is allowed to say so.

    Reached by producers on a full queue, by the writer on a failed flush, and by
    whichever thread called ``stop()`` draining what the writer left. Every one of them
    goes through a method here, which is what makes the lock discipline structural rather
    than remembered.
    """

    def __init__(self) -> None:
        """Start empty, and far enough back that the first drop always reports."""
        self._lock = threading.Lock()
        self._dropped = 0
        # monotonic() is time since boot on Linux, so a fresh container starts it near
        # zero: a report time of 0.0 would swallow the first drop of its first minute
        self._reported_at = -DROP_REPORT_INTERVAL

    def total(self) -> int:
        """How much is waiting to be written as a gap row."""
        with self._lock:
            return self._dropped

    def lost(self, count: int) -> None:
        """Count lost events, and say so at most once a minute."""
        if not count:
            # callers pass the refused count straight through, and that is zero on every
            # successful write -- reporting "the event log is falling behind" for a batch
            # that landed in full is a false alarm on the line people watch for real ones
            return
        with self._lock:
            self._dropped += count
            dropped = self._dropped
            # inside the lock, because "at most once a minute" is a claim about threads:
            # read outside it, two of them see the same report time before either writes
            # one, and both log the backlog line inside a single interval -- on the one
            # line an operator watches for a real backlog. The logging itself stays out
            # here: a handler is a project's code and may do anything, including block
            now = time.monotonic()
            if now - self._reported_at < DROP_REPORT_INTERVAL:
                return
            self._reported_at = now
        logger.error(
            'the event log is falling behind; events are being dropped',
            extra={'tg_dropped': dropped},
        )

    def lost_quietly(self, count: int) -> None:
        """Count lost events where the caller is already logging a sentence about them.

        A failed flush and a partial refusal each report themselves, with the batch size
        and the failure count that make them worth reading. Adding the once-a-minute
        backlog line to that would be the same news twice, in a less specific form.
        """
        if not count:
            return
        with self._lock:
            self._dropped += count

    def claim(self, upto: int) -> int:
        """Take out what this flush is going to report, so no other flush reports it too.

        Two flushes can be in progress at once -- the writer thread's and a
        ``drain_once()`` on somebody else's -- and both snapshot the count before their
        batch. Subtracting after the write let each report the same hole and take it off
        twice, which drives the count negative; subtracting before it lost the hole
        whenever the gap row itself was refused. Taking the count out first makes the
        claim exclusive, and :meth:`give_back` keeps it when the row does not land.

        Claims no more than is there: a count another flush already took leaves nothing,
        and its caller then writes no row about zero events.
        """
        with self._lock:
            claimed = min(upto, self._dropped)
            self._dropped -= claimed
            return claimed

    def give_back(self, count: int) -> None:
        """Put a claim back, so a gap nobody could record survives to be recorded."""
        with self._lock:
            self._dropped += count

    def reset(self) -> None:
        """Forget the count, for a fork -- where nothing here describes this process."""
        with self._lock:
            self._dropped = 0


class ThreadMarks:
    """Which threads handed a batch to the ORM, and so have a connection to close.

    Per thread rather than one flag, because ``close_old_connections()`` acts on the
    calling thread and because two writers can overlap: a ``stop()`` whose join times out
    leaves the old one running while a replacement starts, and one flag let the old one's
    exit clear the new one's.
    """

    def __init__(self) -> None:
        """Nobody has written yet."""
        self._lock = threading.Lock()
        self._idents: set[int] = set()

    def mark(self) -> None:
        """Record that this thread is handing a batch to the ORM."""
        with self._lock:
            self._idents.add(threading.get_ident())

    def forget(self) -> None:
        """Drop this thread's mark without acting on it, for a thread that does not close.

        Only the writer's exit closes connections. ``record()`` under ``EVENT_LOG_SYNC``
        and ``drain_once()`` write on their caller's thread, and Django owns that thread's
        connection -- so their marks are bookkeeping nobody reads, and thread idents get
        reused, which would leave a later writer closing a connection it never opened.
        """
        with self._lock:
            self._idents.discard(threading.get_ident())

    def take(self) -> bool:
        """Whether *this* thread wrote, clearing the mark as it answers.

        Read and cleared together, because the mark describes one writer: left set it
        outlives the thread that earned it, and a later writer with only receivers closes
        a connection it never opened -- importing the writer module, and ``django.db``
        with it, into the one process the recorder exists to keep it out of.
        """
        ident = threading.get_ident()
        with self._lock:
            took = ident in self._idents
            self._idents.discard(ident)
        return took

    def clear(self) -> None:
        """Forget every mark, for a fork -- no thread here survived it."""
        with self._lock:
            self._idents.clear()
