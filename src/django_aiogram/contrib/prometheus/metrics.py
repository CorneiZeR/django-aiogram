"""Counters and histograms over the event feed, so a dashboard needs no receiver of its own.

``eventlog/signals.py`` is a metrics seam with no consumer: every project that wanted a
dashboard wrote the same receiver over the same kinds and invented its own metric names.
The kinds are already a registry, so turning them into metrics is mechanical -- which is the
argument for doing it once here rather than fifteen times badly.

**What this is not.** It does not scrape, serve, or push: exposing the registry is the
project's business, through ``django_prometheus``, ``prometheus_client.start_http_server``,
or a view of its own. This only fills a registry in.
"""

import contextlib
import logging
from typing import TYPE_CHECKING

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Histogram

from django_aiogram.eventlog.signals import events_recorded

if TYPE_CHECKING:
    from collections.abc import Sequence

    from django_aiogram.eventlog.records import Event

__all__ = ('EventMetrics', 'connect', 'disconnect')

logger = logging.getLogger('django_aiogram')

#: seconds, and chosen for what this measures rather than taken from the default: a Telegram
#: API call is a network round trip with a rate limiter in front of it, so the interesting
#: range is tens of milliseconds to a few seconds, and the client's own retry-after waits put
#: a real tail above that. The default buckets stop at 10s and start at 5ms
DURATION_BUCKETS = (0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0)


class EventMetrics:
    """One counter and one histogram, filled from every batch the feed publishes.

    **A callable instance**, which is the shape `eventlog.signals` went to trouble for: an
    instance has no ``__qualname__`` of its own, and Django's ``send_robust`` reads that name
    inside its own ``except`` when a receiver fails. :func:`connect` goes through this
    package's ``connect``, which names it -- so a failure here cannot take the dispatch down
    with it. `test_metrics.py` holds that.

    Metrics, and why there are two rather than fourteen:

    * ``django_aiogram_events_total{kind}`` -- one counter for every kind, because a counter
      per kind is fourteen names to learn and a query per panel, while a label is one of each.
      The kinds are a registry with a documented convention, so the label is bounded by code
      rather than by traffic.
    * ``django_aiogram_event_duration_seconds{kind}`` -- observed for every event that carries
      a ``duration_ms``, which today is a send and a handled update. Seconds, because that is
      what Prometheus expects and what every dashboard's arithmetic assumes; the feed's
      milliseconds are the ones a person reads in a row.

    **What is deliberately not a label.** ``chat_id`` and ``user_id`` are one series per
    person, which is a cardinality bomb rather than a dimension. ``error_code`` is unbounded
    from this side -- Telegram's strings are whatever Telegram sends -- and ``correlation_id``
    is one series per message. ``function`` is bounded in practice and still left out: it
    multiplies every kind by the number of methods a project uses, for a breakdown the event
    log answers exactly. A failure's detail belongs in the feed, and the feed is queryable.

    ``worker`` is not a label either, for a different reason: Prometheus already knows which
    process it scraped, and putting the name in the series as well is the same fact twice --
    with a redeploy's worth of new series behind it in any deployment that names workers by
    pod.
    """

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        """Build the metrics in ``registry``, or in ``prometheus_client``'s process-wide one.

        Taken rather than assumed, because a project running ``django_prometheus`` has a
        registry of its own and metrics in the wrong one are scraped by nobody. ``None`` means
        the default, which is what a project with no opinion has.
        """
        self.registry = REGISTRY if registry is None else registry
        self.events = Counter(
            'django_aiogram_events',
            'Events recorded by django-aiogram, by kind.',
            ('kind',),
            registry=self.registry,
        )
        self.duration = Histogram(
            'django_aiogram_event_duration_seconds',
            'How long the work an event describes took, where it was measured.',
            ('kind',),
            buckets=DURATION_BUCKETS,
            registry=self.registry,
        )

    def __call__(
        self,
        sender: object = None,  # noqa: ARG002 - Django's receiver signature; the recorder is not needed
        events: 'Sequence[Event]' = (),
        **kwargs: object,
    ) -> None:
        """Count a batch. Django's signature, and every argument but ``events`` is ignored.

        ``**kwargs`` because ``Signal.send`` passes ``signal=`` and may pass more later: a
        receiver that named exactly today's arguments would raise on a new one, and the seam
        would then log this package's own exporter as a broken receiver.
        """
        for event in events:
            self.observe(event)

    def observe(self, event: 'Event') -> None:
        """Record one event, and refuse to be the reason a batch fails.

        The seam promises that publishing cannot raise, and the wrapper in
        ``eventlog.publishing`` is what keeps that promise for *any* receiver. This one does
        not lean on it: a shipped exporter that quietly relied on somebody else's ``try``
        would be the first thing to break the promise if that wrapper ever narrowed. Held by
        a test with a registry that refuses everything.
        """
        try:
            kind = str(event.kind)
            self.events.labels(kind=kind).inc()
            if event.duration_ms is not None:
                self.duration.labels(kind=kind).observe(event.duration_ms / 1000)
        except Exception:
            logger.exception('the prometheus exporter could not record an event', extra={'tg_kind': event.kind})


#: what :func:`connect` installed, so :func:`disconnect` can find it and -- more importantly --
#: so it is not collected the moment it is connected. See :func:`connect`
_connected: EventMetrics | None = None


def connect(registry: CollectorRegistry | None = None) -> EventMetrics:
    """Build the metrics, connect them to the feed, and answer with them.

    One line in a project's ``AppConfig.ready()``, which is where a receiver connected to a
    signal belongs.

    **Connected strongly, deliberately.** ``Signal.connect`` keeps a weak reference by
    default, and this receiver is an *instance* rather than a module-level function -- so a
    project writing ``events_recorded.connect(EventMetrics())`` gets a receiver that is
    collected before the first batch and metrics that read as no traffic at all. That is the
    trap this function exists to close; the module keeps the reference either way.

    Calling it twice returns the one already connected rather than registering the same metric
    names into the registry again, which ``prometheus_client`` refuses with a
    ``DuplicateTimeseries`` naming three collectors -- a poor way to learn that two apps both
    called this.

    **A second call naming a different registry raises**, and the message says which two.
    Answering with the first exporter would leave the second registry with no metrics in it
    at all: whoever passed it would scrape zeros for ever, from a call that looked like it
    worked. ``connect()`` with no registry is not that case -- it is a caller with no opinion,
    and it gets what is already connected.
    """
    global _connected  # noqa: PLW0603 - one exporter per process, like the registry it fills
    if _connected is not None:
        if registry is not None and registry is not _connected.registry:
            msg = (
                f'django_aiogram metrics are already connected to {_connected.registry!r}, so '
                f'{registry!r} would be left empty. Call disconnect() first, or pass the registry '
                f'that is already in use.'
            )
            raise ValueError(msg)
        return _connected
    metrics = EventMetrics(registry)
    events_recorded.connect(metrics, weak=False)
    _connected = metrics
    return metrics


def disconnect() -> None:
    """Stop counting, unregister the metrics, and forget what was connected.

    Safe to call when nothing is. For a test that wants a clean registry between cases,
    mostly: a deployment connects once and never comes back here.

    **The collectors are unregistered, not merely disconnected.** Left in the registry, the
    next ``connect()`` on that same registry raises ``DuplicateTimeseries`` about names
    nobody typed twice -- so a suite that disconnects between cases would fail on its second
    one, which is exactly the suite most likely to call this.
    """
    global _connected  # noqa: PLW0603 - as above
    if _connected is None:
        return
    events_recorded.disconnect(_connected)
    for collector in (_connected.events, _connected.duration):
        # tolerated rather than assumed: a caller may have unregistered them itself, and
        # `unregister` raises `KeyError` on a collector the registry does not hold
        with contextlib.suppress(KeyError):
            _connected.registry.unregister(collector)
    _connected = None
