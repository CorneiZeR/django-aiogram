"""Helpers a project's own tests import, supported the way ``bot.send`` is.

The point is subtraction. Before this, asserting that a message was queued meant pointing a
connection at fakeredis, reading a list back by key, and decoding the payload through
``wire.serializers.loads`` and ``wire.envelope.unpack`` -- so every project wrote the same
fixture, and every project's suite depended on a wire format that this package is free to
change. And the recipe only worked on one of the four transports.

Three pieces, none of which needs a token, a server or a running loop:

* :func:`~django_aiogram.testing.capture.capture_sends` -- what a block queued, as records.
* :class:`~django_aiogram.testing.broker.InMemoryBroker` -- a real broker with no server,
  which is also what gives Kafka and RabbitMQ projects a testing story at all.
* a pytest fixture in :mod:`django_aiogram.testing.plugin` and
  :class:`~django_aiogram.testing.case.SendCaptureMixin` for ``TestCase``, so neither half of
  the Django world is the assumed one.

Imported eagerly here, unlike the package root: these names are only ever reached from a test,
and nothing in them touches ``django.db`` or aiogram.
"""

from django_aiogram.testing.broker import InMemoryBroker
from django_aiogram.testing.capture import Captured, Sent, capture_sends
from django_aiogram.testing.case import SendCaptureMixin

__all__ = ('Captured', 'InMemoryBroker', 'SendCaptureMixin', 'Sent', 'capture_sends')
