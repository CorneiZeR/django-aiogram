"""The shipped exporter: what it counts, what it refuses to label, and what it cannot break.

`eventlog/signals.py` is a metrics seam that had no consumer, so every project wrote the same
receiver over the same kinds. These cases are about the three things a shipped one has to get
right that a hand-written one usually does not: it must land in the registry the project
scrapes, it must survive being connected the way Django connects things, and it must not be
able to take the event feed down with it.
"""

import gc
import logging

import pytest
from django.test import override_settings
from prometheus_client import CollectorRegistry

from django_aiogram.contrib.prometheus import EventMetrics, connect, disconnect
from django_aiogram.eventlog.publishing import publish
from django_aiogram.eventlog.records import Event
from django_aiogram.eventlog.signals import events_recorded

SETTINGS = {'TOKEN': '42:x', 'FSM_STORAGE': 'memory', 'RATE_LIMIT': None}


@pytest.fixture
def registry():
    """A registry of this test's own, so nothing lands in the process-wide default."""
    yield CollectorRegistry()
    disconnect()


def value(registry, name, **labels):
    """One sample, or None where nothing has been observed for those labels."""
    return registry.get_sample_value(name, labels)


def test_a_batch_is_counted_by_kind(registry):
    """One counter with a label, rather than a counter per kind."""
    metrics = EventMetrics(registry)

    metrics(events=[Event(kind='outbound.sent'), Event(kind='outbound.sent'), Event(kind='outbound.failed')])

    assert value(registry, 'django_aiogram_events_total', kind='outbound.sent') == 2
    assert value(registry, 'django_aiogram_events_total', kind='outbound.failed') == 1


def test_a_kind_this_package_never_registered_is_counted_too(registry):
    """Kinds are a registry a project may add to, so the exporter cannot hold a list of its own.

    A whitelist here would silently drop every custom kind -- and the failure mode is a panel
    that reads zero, which nobody debugs because nothing is broken.
    """
    metrics = EventMetrics(registry)

    metrics(events=[Event(kind='billing.invoice.sent')])

    assert value(registry, 'django_aiogram_events_total', kind='billing.invoice.sent') == 1


def test_a_measured_duration_becomes_seconds(registry):
    """The feed records milliseconds because a person reads them; Prometheus wants seconds."""
    metrics = EventMetrics(registry)

    metrics(events=[Event(kind='outbound.sent', duration_ms=250)])

    assert value(registry, 'django_aiogram_event_duration_seconds_sum', kind='outbound.sent') == 0.25
    assert value(registry, 'django_aiogram_event_duration_seconds_count', kind='outbound.sent') == 1


def test_an_event_with_nothing_measured_observes_nothing(registry):
    """`duration_ms` is None on most kinds, and a zero there is a lie about a fast send."""
    metrics = EventMetrics(registry)

    metrics(events=[Event(kind='outbound.queued')])

    assert value(registry, 'django_aiogram_events_total', kind='outbound.queued') == 1
    assert value(registry, 'django_aiogram_event_duration_seconds_count', kind='outbound.queued') is None


def test_nothing_that_identifies_a_person_is_a_label(registry):
    """A label per chat is a series per chat, which is a cardinality bomb rather than a dimension.

    Asserted on the metric's own label names rather than by sending events with ids in them: a
    case that only checked the samples would pass while the label existed and happened to be
    empty.
    """
    metrics = EventMetrics(registry)

    assert metrics.events._labelnames == ('kind',)
    assert metrics.duration._labelnames == ('kind',)


def test_the_exporter_cannot_break_the_batch_it_is_counting(registry, caplog):
    """The seam promises publishing cannot raise, and a shipped receiver does not lean on that.

    `eventlog.publishing.publish` would contain this anyway. That is exactly why the exporter
    has to contain it itself: a receiver that relied on somebody else's `try` would be the
    first thing to break when that wrapper narrowed, and it ships with this package rather
    than being a project's own risk.
    """
    metrics = EventMetrics(registry)
    metrics.events = None  # anything at all going wrong inside the observation

    with caplog.at_level(logging.ERROR):
        metrics(events=[Event(kind='outbound.sent')])

    assert 'could not record an event' in caplog.text


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_failing_exporter_does_not_cost_the_receivers_after_it(registry):
    """The `__qualname__` fix in `eventlog/signals.py`, exercised by the shape it was written for.

    An instance has no `__qualname__` of its own, and `send_robust` reads that name inside its
    own `except` when a receiver fails -- so an unnamed callable instance used to end the
    dispatch, and every receiver connected after it stopped seeing batches. This package's
    `connect` names it. Asserted through the real publish path with a receiver behind it.
    """
    metrics = connect(registry)
    seen = []

    def after(sender=None, events=(), **kwargs):
        """A receiver connected behind the exporter, taking the signal's own arguments."""
        seen.append(events)

    events_recorded.connect(after, weak=False)
    try:
        assert getattr(metrics, '__qualname__', None), 'the exporter reached the signal unnamed'

        metrics.events = None
        publish(sender=None, batch=[Event(kind='outbound.sent')])
    finally:
        events_recorded.disconnect(after)

    assert len(seen) == 1, 'a receiver connected after the exporter stopped seeing batches'


def test_the_exporter_is_not_collected_the_moment_it_is_connected(registry):
    """`Signal.connect` keeps a weak reference by default, and this receiver is an instance.

    So `events_recorded.connect(EventMetrics())` -- the obvious line a project writes -- is
    collected before the first batch, and the metrics read as no traffic at all. `connect()`
    exists to close that trap.

    Both halves, because either alone is green while the other holds: the signal stores the
    receiver itself rather than a `weakref` (Django's own structure, asserted directly, since
    the module reference below would keep a weak connection alive and hide a `weak=True`), and
    the exporter still counts after a collection with nothing local referring to it.
    """
    import weakref

    connect(registry)
    gc.collect()

    stored = [receiver for _key, receiver, *_ in events_recorded.receivers]
    assert not any(isinstance(one, weakref.ReferenceType) for one in stored), (
        'the exporter is connected weakly, so it lives only as long as something else holds it'
    )

    publish(sender=None, batch=[Event(kind='outbound.sent')])

    assert value(registry, 'django_aiogram_events_total', kind='outbound.sent') == 1


def test_connecting_twice_answers_with_the_one_already_connected(registry):
    """Two apps calling `ready()` must not be a `ValueError` about duplicate registration.

    `prometheus_client` refuses a second collector under the same name, so a second
    `EventMetrics` in the same registry raises -- and an error about duplicate registration is
    a poor way to learn that two apps both wanted metrics.
    """
    first = connect(registry)
    second = connect(registry)

    assert first is second

    publish(sender=None, batch=[Event(kind='outbound.sent')])

    assert value(registry, 'django_aiogram_events_total', kind='outbound.sent') == 1, 'the batch was counted twice'


def test_connecting_again_after_disconnecting_does_not_collide_with_itself(registry):
    """`disconnect` has to give the names back, or the next `connect` raises about them.

    Measured before the fix: `DuplicateTimeseries: {'django_aiogram_events_total',
    'django_aiogram_events', 'django_aiogram_events_created'}` -- names nobody typed twice.
    A suite that disconnects between cases is the one most likely to call this, and it would
    have failed on its second case.
    """
    connect(registry)
    disconnect()

    connect(registry)
    publish(sender=None, batch=[Event(kind='outbound.sent')])

    assert value(registry, 'django_aiogram_events_total', kind='outbound.sent') == 1


def test_a_second_registry_is_refused_rather_than_quietly_ignored(registry):
    """Answering with the first exporter would leave the second registry empty for ever.

    And the caller would have no way to know: the call returns an exporter, the metrics are
    real, and the registry they were asked for is the one nothing scrapes.
    """
    connect(registry)
    other = CollectorRegistry()

    with pytest.raises(ValueError, match='already connected'):
        connect(other)

    assert not list(other._collector_to_names), 'the refused registry was written to anyway'


def test_connecting_with_no_registry_is_a_caller_with_no_opinion(registry):
    """`connect()` after `connect(registry)` is not the mistake above, so it is not refused."""
    first = connect(registry)

    assert connect() is first


def test_disconnecting_stops_the_counting(registry):
    """A test wants a clean registry between cases, and a deployment never comes back here.

    **The receiver is asserted gone, not only the samples.** A `disconnect` that unregistered
    the collectors and left the exporter attached passes the sample check on its own: the
    batch still reaches the old exporter and still increments its counters, which are simply
    no longer in the registry being read. And every reconnect would then add another receiver
    to the pile.
    """
    metrics = connect(registry)
    disconnect()

    publish(sender=None, batch=[Event(kind='outbound.sent')])

    assert metrics not in [receiver for _key, receiver, *_ in events_recorded.receivers], (
        'the exporter is still attached to the feed, so it goes on counting into a registry nobody reads'
    )
    assert value(registry, 'django_aiogram_events_total', kind='outbound.sent') is None


def test_the_package_does_not_import_the_metrics_client():
    """An extra is only an extra while nothing in the package reaches for it.

    A subprocess, because `sys.modules` in this one already has everything -- including
    `prometheus_client`, which the dev group installs so this file can run at all. That is
    precisely why the check cannot be made in-process.
    """
    from tests.support import run_python

    code = (
        'import sys\n'
        'import django_aiogram\n'
        'import django_aiogram.eventlog.signals\n'
        'import django_aiogram.eventlog.publishing\n'
        "print('prometheus_client' in sys.modules)\n"
    )
    finished = run_python(code, check=True)

    assert finished.stdout.strip() == 'False', 'importing the package pulled prometheus_client'


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_the_default_series_count_does_not_grow_with_the_bots(registry):
    """#123's acceptance, and the reason `METRICS_PER_BOT` is off.

    A label per bot is a series per bot *per kind*, and the histogram multiplies that again
    by its buckets: a thousand clients counted that way are hundreds of thousands of series,
    which is a Prometheus problem rather than a dashboard.
    """
    metrics = EventMetrics(registry)

    metrics(events=[Event(kind='outbound.sent', bot_id=123456), Event(kind='outbound.sent', bot_id=654321)])

    assert value(registry, 'django_aiogram_events_total', kind='outbound.sent') == 2
    assert 'bot' not in metrics.events._labelnames, metrics.events._labelnames


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'METRICS_PER_BOT': True})
def test_a_project_that_asks_for_it_gets_a_label_per_bot(registry):
    """Knowingly: the exporter is imported by a project, so the cardinality is their call."""
    metrics = EventMetrics(registry)

    metrics(events=[Event(kind='outbound.sent', bot_id=123456), Event(kind='outbound.sent', bot_id=654321)])

    assert value(registry, 'django_aiogram_events_total', kind='outbound.sent', bot='123456') == 1
    assert value(registry, 'django_aiogram_events_total', kind='outbound.sent', bot='654321') == 1


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'METRICS_PER_BOT': True})
def test_a_row_that_names_no_bot_is_labelled_unknown(registry):
    """An undecodable payload names no bot, and an empty label is one a query cannot exclude."""
    metrics = EventMetrics(registry)

    metrics(events=[Event(kind='queue.undecodable')])

    assert value(registry, 'django_aiogram_events_total', kind='queue.undecodable', bot='unknown') == 1


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'METRICS_PER_BOT': 'nonsense'})
def test_a_flag_nobody_can_read_labels_by_kind_alone(registry, caplog):
    """`E064` is the finding; the exporter's own answer is the safe half of the choice.

    A traceback out of the receiver that counts sends would take the batch with it, which is
    the one thing this exporter promises not to do.
    """
    with caplog.at_level(logging.WARNING, logger='django_aiogram'):
        metrics = EventMetrics(registry)

    metrics(events=[Event(kind='outbound.sent', bot_id=123456)])

    assert value(registry, 'django_aiogram_events_total', kind='outbound.sent') == 1
    assert any('METRICS_PER_BOT' in record.getMessage() for record in caplog.records)


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'METRICS_PER_BOT': True})
def test_the_bot_label_is_bounded_however_many_identities_arrive(registry):
    """A `queue.rejected` row is recorded before a message is routed.

    Its identity is whatever the envelope claimed, so anything able to publish to the queue
    could otherwise mint a Prometheus series per message. Past the cap the rest are counted
    under `other`: the totals stay right and the series stop growing.
    """
    from django_aiogram.contrib.prometheus.metrics import MAX_BOT_LABELS

    metrics = EventMetrics(registry)

    metrics(events=[Event(kind='queue.rejected', bot_id=identity) for identity in range(1, MAX_BOT_LABELS + 51)])

    assert value(registry, 'django_aiogram_events_total', kind='queue.rejected', bot='1') == 1
    assert value(registry, 'django_aiogram_events_total', kind='queue.rejected', bot='other') == 50
    assert len(metrics._labelled) == MAX_BOT_LABELS
