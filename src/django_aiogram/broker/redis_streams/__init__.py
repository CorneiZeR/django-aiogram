"""Redis Streams: the same server as the list, with the ack model the rest of them use."""

from django_aiogram.broker.redis_streams.broker import RedisStreamsBroker

#: the class the `BROKER` setting names, and nothing else a caller needs from in here
__all__ = ('RedisStreamsBroker',)
