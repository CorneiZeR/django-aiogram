# Logging

Everything goes to the `django_aiogram` logger. Values are attached as
structured fields rather than interpolated into the message, so a JSON or
structlog backend can index and filter them.

```python
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'console': {'class': 'logging.StreamHandler'},
    },
    'loggers': {
        'django_aiogram': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': False,
        },
    },
}
```

Drop to `DEBUG` to also see the sends a disabled process skips.

## Fields

All prefixed with `tg_`, to avoid colliding with `LogRecord` attributes.

| Field | Where |
| ----- | ----- |
| `tg_function` | the aiogram method being called |
| `tg_retry_after` | seconds Telegram asked to wait |
| `tg_retries` | attempts made so far |
| `tg_max_retries` | the limit that was reached |
| `tg_delivery` | the consumer that started, always `blpop` |
| `tg_key` | the queue being consumed: a Redis list, a stream, an AMQP queue, or a Kafka topic |
| `tg_timeout` | blocking-pop timeout, or how long a shutdown waited |
| `tg_error` | the class name of a non-fatal error, not its text — a webhook secret or a chat id can end up in the message, and this field is what a log aggregator groups on |
| `tg_crash_safe` | whether the consumer holds messages in flight. The transport answers for itself: false on a Redis without `LMOVE`, and always true on a stream, where the pending list is how delivery works |
| `tg_mode` | `polling` or `webhook` |
| `tg_update` | the update id being handled |
| `tg_correlation_id` | the id every event about one message carries |
| `tg_short_id` | that id as the admin shows it: twelve characters to paste into the log's search box |
| `tg_alternative` | the awaitable method a synchronous send from a loop should move to |
| `tg_pending` | work still in flight at shutdown: sends, or the updates a webhook process is answering |
| `tg_low` | the first id of the range a copy is retrying, when a row landed under an id it was about to move |
| `tg_drain_timeout` | how long shutdown gave them |
| `tg_kind` | the event log kind of a row |
| `tg_receiver` | the `events_recorded` receiver that raised |
| `tg_batch` | how many rows the batch held, when part of it was refused |
| `tg_worker` | the worker name an in-flight list is keyed on |
| `tg_entry` | the id of a stream entry this package did not write, left pending rather than acknowledged |
| `tg_count` | events in the batch being written, or kafka messages left unsent when a producer was replaced or the process shut down |
| `tg_lost` | stream entries that were pending and no longer exist, so that work is gone — the fingerprint of a `MAXLEN` trim or an `XDEL` reaching unacknowledged work |
| `tg_setting` | the setting a message is about |
| `tg_variable` | the environment variable a message is about |
| `tg_dropped` | events lost because the buffer was full, or sends dropped at shutdown |
| `tg_failures` | consecutive failures of the event writer |
| `tg_reason` | why one of the healthcheck's two Redis-only extras could not answer — every such path carries it: the client could not be built, or the call it made failed. Not a verdict: the verdict is the broker's, and it has already been decided by the time this is written. The same text reaches the operator, as `unknown` in the probe's line or as the warning about an unfinished sweep |

## Events worth alerting on

| Message | Level | Meaning |
| ------- | ----- | ------- |
| `giving up on message` | ERROR | retries exhausted, the message was dropped |
| `handler failed for queued message` | ERROR | the send itself raised |
| `dropping undecodable queued message` | ERROR | a payload could not be deserialized |
| `blocking pop failed, retrying` | ERROR | lost the Redis connection; it retries |
| `a message finished after its channel was replaced, so it will be redelivered` | WARNING | RabbitMQ: a send completed across a reconnect, and the delivery tag it held is meaningless on the new channel. Nothing is acknowledged, because the broker has already put the message back — so it arrives again, and a handler that is not idempotent sends it twice |
| `a message finished after its partition was rewound, so it will be redelivered` | WARNING | Kafka: a send completed after a `release` rewound its partition, so the delivery it named no longer exists. Nothing is committed — accepting it could commit past the messages the rewind put back — and it arrives again with them |
| `kafka messages were accepted locally and never reached the broker` | WARNING | Kafka: librdkafka accepted these and could not hand them over before its producer was replaced or the process went away. `tg_count` says how many; they cannot be recovered, and a settings change during a burst is the usual way there |
| `entries were pending but no longer exist in the stream, so that work is lost` | WARNING | Redis Streams: work that was taken and never settled has been deleted from the stream, so those messages are gone. Nothing in this package can cause it — a `MAXLEN` trim or an `XDEL` reached unacknowledged entries. `tg_lost` carries how many |
| `a stream entry carries no payload field and was left pending` | WARNING | Redis Streams: something else is writing to this stream. The entry is left pending rather than acknowledged, because settling it would be a guess about another producer's data. `tg_entry` names it |
| `the delivery consumer did not stop in time` | WARNING | the consumer outlived its join at shutdown; a message it holds may be redelivered |
| `cancelling updates still in flight` | WARNING | a webhook update outlasted the drain at shutdown; its request is answered 503, so Telegram redelivers it rather than a worker hanging on a stopped loop |
| `webhook is not configured to serve updates` | ERROR | `MODE` or `WEBHOOK_SECRET` could not be read, so the view answered 503 rather than raising an unauthenticated 500 |
| `webhook refused an update` | WARNING | an update arrived while the process was shutting down; answered 503 so Telegram redelivers it |
| `the event loop thread did not start in time` | WARNING | a webhook process cannot hand updates to its loop; every request is refused with 503 until a thread starts |
| `the event loop thread is gone; starting another` | WARNING | that thread died and was replaced; the update that lost it was refused |
| `the event loop thread did not stop in time` | WARNING | it outlived its join at shutdown, so the teardown was skipped and `close()` can be retried |
| `skipping close` | WARNING | the loop was still running, so nothing was torn down; stop polling or the loop thread and call it again |
| `skipping drain` | WARNING | the same, for the drain alone: in-flight sends were left rather than waited for |
| `scheduling a send on a loop nothing in this process runs` | WARNING | nothing polls this process and no loop thread exists, so the send is created and never stepped |
| `rate limited by telegram` | WARNING | refused and backing off |
| `a synchronous send was called from a running event loop` | WARNING | `send`, `enqueue` or `send_many` from async code: correct, but it writes on the loop's own thread. `tg_alternative` names the awaitable form. Said once per process |
| `an events_recorded receiver raised` | ERROR | one of your metrics receivers raised; the batch reached the database if the event log is on and the write succeeded, and usually the other receivers too — `send_robust` isolates them, but the row below is the case where it cannot. `tg_receiver` names it |
| `publishing recorded events failed` | ERROR | the signal dispatch itself raised, not a receiver — Django's own failure logging cannot name a callable instance. The batch reached the database if the event log is on; some receivers may have missed it |
| `delivery started` | INFO | the consumer is up |
| `message sent` | INFO | one call succeeded |
| `the event log is falling behind; events are being dropped` | ERROR | the writer cannot keep up; rows are being lost, messages are not |
| `the event log is suspended after repeated failures` | ERROR | five failed batches in a row, usually a missing `migrate` |
| `leaving a refused pickle message in flight` | ERROR | `ALLOW_PICKLE` is off and a pickled payload is waiting for it |
| `leaving a message from a newer version in flight` | ERROR | the web tier was deployed ahead of the bot container |
| `could not close the client a settings change replaced` | ERROR | a settings change replaced the async Redis client and closing the old one raised; its replacement is already in use, so nothing was refused |

## The database event log

This page is about the structured log: a stream, shipped somewhere, rotated.
**[Event log](Event-log.md)** is the other tool — an optional table you can query
and join against your own models, off by default. Use the log for volume and
alerting, and the table for the questions that outlive a retention window.

## With structlog

`ProcessorFormatter` drops stdlib `extra` unless `ExtraAdder` is in its
`foreign_pre_chain`, so wire that up:

```python
import logging

import structlog

handler = logging.StreamHandler()
handler.setFormatter(
    structlog.stdlib.ProcessorFormatter(
        processor=structlog.processors.JSONRenderer(),
        foreign_pre_chain=[structlog.stdlib.ExtraAdder()],
    )
)
logging.getLogger('django_aiogram').addHandler(handler)
```

With it in place the `tg_` fields arrive as event keys:

```python
logger = logging.getLogger('django_aiogram')
logger.warning('rate limited by telegram', extra={'tg_function': 'send_message'})
# -> {"event": "rate limited by telegram", "tg_function": "send_message", ...}
```

The message text is a constant, so the same event groups together regardless
of its values.
