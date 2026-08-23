"""RabbitMQ: an exchange to publish to, a delivery tag to settle, and no worker names."""

from django_aiogram.broker.rabbitmq.broker import RabbitMQBroker

#: the class the `BROKER` setting names, and nothing else a caller needs from in here
__all__ = ('RabbitMQBroker',)
