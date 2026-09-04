"""The Prometheus exporter, behind ``pip install 'django-aiogram[prometheus]'``.

One line in an ``AppConfig.ready()``:

.. code-block:: python

    from django_aiogram.contrib.prometheus import connect


    class MyConfig(AppConfig):
        def ready(self):
            connect()

Two metrics -- ``django_aiogram_events_total{kind}`` and
``django_aiogram_event_duration_seconds{kind}`` -- filled from the batches
``events_recorded`` publishes. It fills a registry in and nothing else: serving the numbers is
the project's business, and it already has a way.

Importing this module imports ``prometheus_client``, which is why nothing in the package
imports this module.
"""

from django_aiogram.contrib.prometheus.metrics import EventMetrics, connect, disconnect

__all__ = ('EventMetrics', 'connect', 'disconnect')
