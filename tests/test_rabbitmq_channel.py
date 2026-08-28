"""What deadline the channel is built with, and where that number comes from.

One case, and it is here rather than in the integration suite because it is about a number the
broker passes and not about anything RabbitMQ does with it: a double records the argument, so this
runs on a machine with no pika and no server.

The defect it pins is the shape a second reader always has. ``RABBITMQ_TIMEOUT`` was read twice —
once by :meth:`~django_aiogram.broker.base.Broker.call_timeout`, which `W004` and the consumer's
cap go through, and once here with an ``or 10`` on it — and the two agreed on every value except
the ones a project writes and ``or`` treats as unset. A configured ``0`` reached pika as ten while
the cap was computed from zero.
"""

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.test import override_settings

from django_aiogram.broker.rabbitmq import broker as rabbitmq

SETTINGS = {
    'TOKEN': '42:x',
    'BROKER': 'django_aiogram.broker.rabbitmq.RabbitMQBroker',
    'RABBITMQ_URL': 'amqp://guest:guest@localhost:5672/',
    'RABBITMQ_QUEUE': 'tg',
}


@pytest.fixture
def recorded(monkeypatch):
    """Everything `channel_for_thread` is called with, without opening a connection."""
    calls = []

    def record(url, queue, prefetch, blocked_timeout):
        calls.append({'url': url, 'queue': queue, 'prefetch': prefetch, 'timeout': blocked_timeout})
        return object()

    monkeypatch.setattr(rabbitmq, 'channel_for_thread', record)
    return calls


@pytest.mark.parametrize('timeout', [0.5, 3, 20])
def test_the_channel_is_built_with_the_deadline_the_ceiling_reports(recorded, timeout):
    """One number, asked for twice: what pika is handed and what the cap is computed from."""
    with override_settings(TELEGRAM_BOT={**SETTINGS, 'RABBITMQ_TIMEOUT': timeout}):
        instance = rabbitmq.RabbitMQBroker()
        instance._channel()

        assert recorded[0]['timeout'] == instance.call_ceiling, (
            f'the channel got {recorded[0]["timeout"]!r} while the ceiling reports {instance.call_ceiling!r}'
        )
        assert recorded[0]['timeout'] == float(timeout), f'and neither is RABBITMQ_TIMEOUT: {recorded[0]["timeout"]!r}'


@pytest.mark.parametrize('timeout', [0, '', None], ids=repr)
def test_a_deadline_or_would_read_as_unset_reaches_nobody(recorded, timeout):
    """The values the two readers disagreed on, and the only ones they could disagree on.

    `or 10` fires exactly here, so a case with a truthy timeout is green against the defect and
    proves nothing. What it did was substitute the declared default for a number the project
    wrote — so this asks for the refusal instead: one reader means the channel cannot be built
    with a deadline `call_ceiling` would not report, and zero is not a deadline.
    """
    with override_settings(TELEGRAM_BOT={**SETTINGS, 'RABBITMQ_TIMEOUT': timeout}):
        instance = rabbitmq.RabbitMQBroker()
        with pytest.raises(ImproperlyConfigured, match='RABBITMQ_TIMEOUT'):
            instance._channel()

    assert recorded == [], f'a channel was built with {[call["timeout"] for call in recorded]} instead'
