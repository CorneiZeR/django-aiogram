"""The consumers a container is running, and one pass that makes that the right set.

The bots have this already -- :mod:`django_aiogram.runtime.supervisor` -- and the queues need
it for the same reason: a container told to serve a *pool* is told a label, and the queues
carrying that label are a table's answer, which changes while the container runs. Read once at
startup, a queue created for a client an hour later would wait for a redeploy, which is
exactly what selecting by pool exists to avoid.

**A read that failed is not an empty pool.** The set stays as it is and the pass says so, the
same rule the bot providers follow: a database blinking must not stop a container consuming.

**Level-triggered, like the supervisor.** Every pass reads the whole set and compares; nothing
acts on "what changed". So a missed pass costs one interval and a duplicate costs nothing.
"""

import logging
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from django_aiogram.consumer.delivery import Delivery

__all__ = ('Consumers',)

logger = logging.getLogger('django_aiogram')


class Consumers:
    """One consumer per queue this container serves, started and stopped by name.

    ``build`` is handed in rather than inherited: the command knows how to build a consumer
    for a queue -- which class, which handler, which route -- and this knows when there should
    be one.
    """

    def __init__(
        self,
        build: 'Callable[[str], Delivery]',
        join_timeout: float,
        ready: 'dict[str, Delivery] | None' = None,
    ) -> None:
        """Hold the consumers already built; the first pass starts them and builds the rest.

        ``ready`` is the startup set, built before anything was started so that a refusal --
        `REQUIRE_CRASH_SAFE` on a transport that cannot promise it -- stops the container
        instead of being logged per queue. A queue that *arrives* later is built here, where
        the same refusal is one queue's problem and the container keeps serving the others.
        """
        self.build = build
        self.join_timeout = join_timeout
        self._ready = dict(ready or {})
        #: consumers a shutdown has stopped, kept until `collect` has settled what they
        #: finished: the sends drained on the way out report themselves into a queue only
        #: their own consumer reads, and a consumer dropped at `stop` takes those with it --
        #: every message the drain delivered would be sent again by the next container
        self._stopped: dict[str, Delivery] = {}
        #: the consumer and the thread serving each queue, by queue name
        self.running: dict[str, tuple[Delivery, threading.Thread]] = {}
        self._lock = threading.Lock()

    def reconcile(self, wanted: 'Iterable[str]') -> None:
        """Serve exactly these queues: start what is new, stop what is gone.

        One queue's failure to start is its own -- the others keep being served, and the next
        pass tries it again -- because a container that stopped consuming everything because
        one queue was misconfigured is the outage the whole set was meant to survive.
        """
        asked = list(dict.fromkeys(wanted))
        with self._lock:
            for queue in [held for held in self.running if held not in asked]:
                # settled here rather than kept: this is a queue that went away while the
                # container runs, so nothing later in this process will collect for it
                self._stop(queue).collect()
            for queue in asked:
                if queue in self.running:
                    continue
                consumer = None
                try:
                    consumer = self._ready.pop(queue, None) or self.build(queue)
                    self.running[queue] = (consumer, consumer.start_thread())
                except Exception:
                    # a consumer that was built has already reclaimed, so dropping it here
                    # would strand whatever it took: only it can settle its own in-flight
                    # list. Stopped and settled before the queue is left for the next pass
                    if consumer is not None:
                        _settled(consumer, queue)
                    logger.exception(
                        'could not start consuming a queue; the next pass will try again',
                        extra={'tg_queue': queue},
                    )

    def stop(self) -> None:
        """Stop every consumer and wait for its thread, which is a shutdown's half of this.

        The ones built and never started are stopped too. A shutdown can arrive before the
        loop ran the callback that starts them, and a consumer that was built holds a
        transport and has already reclaimed -- so leaving it unstopped strands whatever it
        took, and it is exactly the case a container killed during startup is.
        """
        with self._lock:
            for queue in list(self.running):
                self._stopped[queue] = self._stop(queue)
            for consumer in self._ready.values():
                consumer.stop()

    def collect(self) -> None:
        """Let every consumer settle what its sends finished, after the bot has been closed.

        Including the ones never started, for the reason :meth:`stop` gives: what a built
        consumer reclaimed is in its in-flight list, and only it can settle it.
        """
        with self._lock:
            for consumer, _ in self.running.values():
                consumer.collect()
            for consumer in (*self._stopped.values(), *self._ready.values()):
                consumer.collect()

    def _stop(self, queue: str) -> 'Delivery':
        """Stop one consumer, wait out its thread, and hand it back for settling.

        Dropped even where the thread outlives the join: a consumer this container believes it
        is running and is not is the state nothing recovers from without a restart, and the
        warning is what an operator has instead.
        """
        consumer, thread = self.running.pop(queue)
        consumer.stop()
        thread.join(timeout=self.join_timeout)
        if thread.is_alive():
            # the wording a wiki page quotes, kept: `Troubleshooting.md` tells an operator to
            # grep for it, and a message that moved would be one nobody finds
            logger.warning(
                'the delivery consumer did not stop in time',
                extra={'tg_queue': queue, 'tg_timeout': self.join_timeout},
            )
        return consumer

    def consumers(self) -> 'tuple[Delivery, ...]':
        """Every consumer running now, for a caller that has to reach all of them."""
        with self._lock:
            return tuple(consumer for consumer, _ in self.running.values())


def _settled(consumer: 'Delivery', queue: str) -> None:
    """Stop a consumer that never ran and settle what it holds, saying nothing further.

    Called where starting failed, so the failure being reported is the caller's; a stop that
    raises on top of it would replace the reason with a second one.
    """
    try:
        consumer.stop()
    except Exception:
        logger.exception('could not stop a consumer that failed to start', extra={'tg_queue': queue})
    try:
        consumer.collect()
    except Exception:
        logger.exception('could not settle a consumer that failed to start', extra={'tg_queue': queue})
