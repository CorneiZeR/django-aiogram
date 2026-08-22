"""The transport 3.x was: two Redis lists, one of them named after the worker."""

from django_aiogram.broker.redis_list.broker import RedisListBroker

#: the class the `BROKER` setting names, and nothing else a caller needs from in here
__all__ = ('RedisListBroker',)
