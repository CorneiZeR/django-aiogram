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

__all__ = ('Consumers', 'Lane', 'lane_name', 'lanes', 'settle')

#: what a consumer is held under: one queue by name, or the profile every queue in the lane
#: shares a connection through. Tagged, so a queue called like a profile cannot be one
Lane = tuple[str, object]

logger = logging.getLogger('django_aiogram')


def lane_name(consumer: 'Delivery | None', lane: 'Lane') -> str:
    """Name a lane for a log line: the queues it serves, or its key where nothing knows them.

    A lane's key is a profile where several queues share a connection, and an operator reading
    `tg_queue` wants the names they configured rather than a profile's repr.
    """
    serving = tuple(getattr(consumer, 'queues', ()) or ())
    if serving:
        return ', '.join(serving)
    kind, held = lane
    return str(held) if kind == 'queue' else f'the queues on {held!r}'


def lanes(queues: 'Iterable[str]') -> 'dict[Lane, tuple[str, ...]]':
    """Group the queues a container serves into the consumers that will read them.

    One consumer per *connection* where the transport can read several queues over one, and one
    per queue where it cannot -- which is what a Redis list deployment has always had. Twenty
    client queues on RabbitMQ are then one connection and one thread instead of twenty of each.

    The key is what the container holds a consumer under, and it has to hold still while the
    membership moves: a lane keyed by the queues in it would be a different lane the moment a
    client arrived, so every queue already in it would be stopped and started again. So a
    multiplexed lane is keyed by the *profile* its queues share and a lane of one by the queue
    itself, each tagged with which of the two it is -- a `Profile` rather than its digest, and
    tagged rather than bare, because either shortcut is a way for two different lanes to land
    on one key and be served by one consumer built from the wrong settings.

    A queue whose settings cannot be resolved is a lane of its own, because the answer to
    "which connection is this?" is the refusal `Consumers.reconcile` reports per queue: one
    queue's misconfiguration must not decide the shape of everybody else's.
    """
    # deferred: all three reach the settings, and this module is imported from the command
    from django_aiogram.broker.registry import broker_class  # noqa: PLC0415 - as above
    from django_aiogram.runtime.profiles import connection_of  # noqa: PLC0415 - as above
    from django_aiogram.runtime.queues import settings_for  # noqa: PLC0415 - as above

    grouped: dict[Lane, tuple[str, ...]] = {}
    for queue in dict.fromkeys(queues):
        key: Lane = ('queue', queue)
        try:
            settings = settings_for(queue)
            transport = broker_class(settings, verify_driver=False)
            if transport.MULTIPLEXES:
                key = ('connection', connection_of(settings))
        except Exception:  # noqa: BLE001 - whatever it was, `reconcile` meets it again with a queue name on it
            # not swallowed: `reconcile` builds this lane and reports the same failure with the
            # queue's name on it, which is where an operator can act on it
            logger.debug('could not group a queue by its connection; serving it on its own', extra={'tg_queue': queue})
        grouped[key] = (*grouped.get(key, ()), queue)
    return grouped


class Consumers:
    """The consumers this container is running, started and stopped by lane.

    A lane is the set of queues one consumer reads: all of them that share a connection where
    the transport can multiplex, and one queue otherwise -- :func:`lanes` decides, and the key
    it hands back is what a consumer is held under here.

    ``build`` is handed in rather than inherited: the command knows how to build a consumer
    for a set of queues -- which class, which handler, which route -- and this knows when there
    should be one.
    """

    def __init__(
        self,
        build: 'Callable[[tuple[str, ...]], Delivery]',
        join_timeout: float,
        ready: 'dict[Lane, Delivery] | None' = None,
    ) -> None:
        """Hold the consumers already built; the first pass starts them and builds the rest.

        ``ready`` is the startup set, keyed by lane and built before anything was started so
        that a refusal -- `REQUIRE_CRASH_SAFE` on a transport that cannot promise it -- stops
        the container instead of being logged per queue. A queue that *arrives* later is built
        here, where the same refusal is one lane's problem and the container keeps serving the
        others.
        """
        self.build = build
        self.join_timeout = join_timeout
        self._ready = dict(ready or {})
        #: consumers a shutdown has stopped, kept until `collect` has settled what they
        #: finished: the sends drained on the way out report themselves into a queue only
        #: their own consumer reads, and a consumer dropped at `stop` takes those with it --
        #: every message the drain delivered would be sent again by the next container
        self._stopped: list[tuple[Lane, Delivery, threading.Thread]] = []
        #: set by `stop`, and never cleared: a pass can be inside a database read when the
        #: shutdown begins, and one that came back afterwards would start a daemon consumer
        #: behind the joins -- doing transport work while `bot.close()` runs, with nothing
        #: left to stop it
        self._done = False
        #: the consumer and the thread serving each lane, by the key :func:`lanes` gave it
        self.running: dict[Lane, tuple[Delivery, threading.Thread]] = {}
        self._lock = threading.Lock()

    def reconcile(self, wanted: 'Iterable[str]') -> None:
        """Serve exactly these queues: start what is new, stop what is gone.

        One queue's failure to start is its own -- the others keep being served, and the next
        pass tries it again -- because a container that stopped consuming everything because
        one queue was misconfigured is the outage the whole set was meant to survive.
        """
        asked = lanes(wanted)
        with self._lock:
            if self._done:
                logger.info('not reconciling the queues: the shutdown had already begun')
                return
            for lane in [held for held in self.running if held not in asked]:
                # settled here where the thread has actually gone, and kept where it has not:
                # this is a queue that went away while the container runs, so nothing later
                # would collect for it -- but `collect` on a consumer whose thread is still
                # inside `run` would be two threads settling one in-flight list and calling
                # one transport
                self._stopped.append((lane, *self._stop(lane)))
            self._settle_what_has_stopped()
            for lane, serving in asked.items():
                if lane in self.running:
                    self._follow(lane, serving)
                    continue
                if self._still_turning(lane):
                    # left the set, its stop timed out, and now it is back: starting a
                    # replacement while the old thread is inside `run` puts two consumers on
                    # one queue, both taking and both settling. It starts on the pass after
                    # the old thread has gone, which is one interval later at worst
                    logger.warning(
                        'not starting a queue whose previous consumer is still running',
                        extra={'tg_queue': ', '.join(serving)},
                    )
                    continue
                consumer = None
                try:
                    consumer = self._ready.pop(lane, None) or self.build(serving)
                    self.running[lane] = (consumer, consumer.start_thread())
                except Exception:
                    # a consumer that was built has already reclaimed, so dropping it here
                    # would strand whatever it took: only it can settle its own in-flight
                    # list. Stopped and settled before the queue is left for the next pass
                    if consumer is not None:
                        settle(consumer, ', '.join(serving))
                    logger.exception(
                        'could not start consuming a queue; the next pass will try again',
                        extra={'tg_queue': ', '.join(serving)},
                    )

    def _follow(self, lane: 'Lane', serving: tuple[str, ...]) -> None:
        """Tell a running consumer which queues its lane holds now, if the set has moved.

        Told rather than restarted, which is the whole reason a lane has a key of its own: a
        client's queue arriving would otherwise stop the consumer reading the other nineteen,
        and every one of those would pause for the length of a join over somebody else's
        arrival.

        Held under the caller's lock, like everything else that reads `running`.
        """
        consumer, _thread = self.running[lane]
        if tuple(getattr(consumer, 'queues', ())) == serving:
            return
        try:
            consumer.serve(serving)
        except Exception:
            # stopped rather than left reading the old set, and that is the whole difference
            # between a lane that is behind and a lane that is wrong: a consumer that cannot
            # take the new queues would leave their backlogs with nobody on them while the
            # container believed it was serving them. The next pass builds this lane again,
            # where a `DELIVERY` that cannot serve a set is refused by name
            logger.exception(
                'could not change the queues a consumer serves; stopping it so the next pass rebuilds it',
                extra={'tg_queue': ', '.join(serving)},
            )
            self._stopped.append((lane, *self._stop(lane)))

    def stop(self) -> None:
        """Stop every consumer and wait for its thread, which is a shutdown's half of this.

        The ones built and never started are stopped too. A shutdown can arrive before the
        loop ran the callback that starts them, and a consumer that was built holds a
        transport and has already reclaimed -- so leaving it unstopped strands whatever it
        took, and it is exactly the case a container killed during startup is.
        """
        with self._lock:
            self._done = True
            for lane in list(self.running):
                self._stopped.append((lane, *self._stop(lane)))
            for consumer in self._ready.values():
                consumer.stop()

    def collect(self) -> None:
        """Let every consumer settle what its sends finished, after the bot has been closed.

        Including the ones never started, for the reason :meth:`stop` gives: what a built
        consumer reclaimed is in its in-flight list, and only it can settle it. A consumer
        whose thread is *still running* is not settled from here -- see
        :meth:`_settle_what_has_stopped`.
        """
        with self._lock:
            for consumer, _ in self.running.values():
                consumer.collect()
            for consumer in self._ready.values():
                consumer.collect()
            self._settle_what_has_stopped()

    def _still_turning(self, lane: 'Lane') -> bool:
        """Whether a consumer this container stopped for that lane is still inside ``run``.

        Held under the caller's lock.
        """
        return any(name == lane and thread.is_alive() for name, _, thread in self._stopped)

    def _settle_what_has_stopped(self) -> None:
        """Settle every stopped consumer whose thread has actually exited, and forget it.

        The aliveness check is the whole of this. `Delivery.collect` drains the queue its own
        consumer thread writes to and touches the transport, so calling it while that thread
        is still inside ``run`` is two threads settling one in-flight list -- a count that
        drifts, and two callers on one connection. A thread that outlived its join is left
        with its consumer and tried again by the next pass; on the way out it is what the
        warning in :meth:`_stop` is about.

        Held under the caller's lock.
        """
        for entry in list(self._stopped):
            lane, consumer, thread = entry
            if thread.is_alive():
                continue
            self._stopped.remove(entry)
            try:
                consumer.collect()
            except Exception:
                logger.exception('could not settle a stopped consumer', extra={'tg_queue': lane_name(consumer, lane)})

    def _stop(self, lane: 'Lane') -> 'tuple[Delivery, threading.Thread]':
        """Stop one consumer, wait out its thread, and hand both back for settling.

        Dropped from the running set even where the thread outlives the join: a consumer this
        container believes it is running and is not is the state nothing recovers from without
        a restart, and the warning is what an operator has instead. The thread comes back with
        it because whether it is still turning decides whether anything may settle it.
        """
        consumer, thread = self.running.pop(lane)
        consumer.stop()
        thread.join(timeout=self.join_timeout)
        if thread.is_alive():
            # the wording a wiki page quotes, kept: `Troubleshooting.md` tells an operator to
            # grep for it, and a message that moved would be one nobody finds
            logger.warning(
                'the delivery consumer did not stop in time',
                extra={'tg_queue': lane_name(consumer, lane), 'tg_timeout': self.join_timeout},
            )
        return consumer, thread

    def serving(self) -> tuple[str, ...]:
        """Every queue this container is consuming now, whichever lane it is in.

        The question a reader of `running` usually has, answered without their having to know
        what a lane is keyed by -- and answered from the consumers themselves, so a lane whose
        set moved says what it is reading rather than what it was built for.
        """
        with self._lock:
            return tuple(
                queue for consumer, _thread in self.running.values() for queue in getattr(consumer, 'queues', ())
            )

    def consumers(self) -> 'tuple[Delivery, ...]':
        """Every consumer running now, for a caller that has to reach all of them."""
        with self._lock:
            return tuple(consumer for consumer, _ in self.running.values())


def settle(consumer: 'Delivery', queue: str) -> None:
    """Stop a consumer that never ran and settle what it holds, saying nothing further.

    Called where starting -- or a startup that had already built others -- failed, so the
    failure being reported is the caller's: a stop that raises on top of it would replace the
    reason with a second one. A built consumer has already reclaimed, and only it can
    acknowledge what it took.
    """
    try:
        consumer.stop()
    except Exception:
        logger.exception('could not stop a consumer that failed to start', extra={'tg_queue': queue})
    try:
        consumer.collect()
    except Exception:
        logger.exception('could not settle a consumer that failed to start', extra={'tg_queue': queue})
