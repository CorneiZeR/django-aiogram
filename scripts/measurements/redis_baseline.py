"""The divisor the other two scripts' multiples are quoted against.

`CHANGELOG.md`, `Settings.md` and this directory's README all describe the AMQP and Kafka
publishes as a multiple of a Redis list publish -- roughly 18x and roughly 10x. Those multiples
were being quoted against a number nobody had written down, which makes five claims unreadable
in the direction that matters: a reader cannot check a ratio whose divisor is invisible.

So this measures the baseline the same way and on the same protocol as the other two: median of
300, run it more than once. What it times is the one call `RedisListBroker.publish` makes -- a
variadic ``RPUSH``, which answers with the new list length, and that answer is the whole reason
this package can promise a queued message is queued.

Two rows rather than one, because the list transport and the Streams transport are both "Redis"
and a reader comparing against "a Redis publish" deserves to know they are not the same number.

Run it against a throwaway server:

    docker run -d --rm --name redis -p 6399:6379 redis:8
    python -m scripts.measurements.redis_baseline
"""

import os

import redis
from scripts.measurements._timing import configure_reporting, logger, measure, run_name

#: a throwaway server, overridable because a reader's port is their own business
URL = os.environ.get('DJANGO_AIOGRAM_TEST_REDIS_URL', 'redis://127.0.0.1:6399/0')
KEY = run_name('baseline')
#: the shape of a queued call this package actually sends
BODY = b'{"function": "send_message", "chat_id": 1}'


def main() -> None:
    """Measure both Redis transports' publish and report them as the baseline they are."""
    configure_reporting()
    client = redis.Redis.from_url(URL, decode_responses=False)
    try:
        client.delete(KEY)
        listed = measure('RPUSH (RedisListBroker)', lambda: client.rpush(KEY, BODY))
        client.delete(KEY)
        streamed = measure('XADD (RedisStreamsBroker)', lambda: client.xadd(KEY, {b'p': BODY}))
        logger.info('')
        logger.info(
            'the baseline the AMQP and Kafka multiples are quoted against: %.1f us for a list '
            'publish, %.1f us for a stream one',
            listed,
            streamed,
        )
    finally:
        client.delete(KEY)
        client.close()


if __name__ == '__main__':
    main()
