"""Kafka: a topic to produce to, an offset to commit, and no message-level acknowledgement."""

from django_aiogram.broker.kafka.broker import KafkaBroker

#: the class the `BROKER` setting names, and nothing else a caller needs from in here
__all__ = ('KafkaBroker',)
