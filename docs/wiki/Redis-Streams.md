# Redis Streams

The same server as the **[[Redis-list|Redis list]]** and the same driver, with a different data
structure behind it: a stream and a consumer group instead of a list. The group is what changes
the operational story, not the dependency.

```python
TELEGRAM_BOT = {
    'BROKER': 'django_aiogram.broker.redis_streams.RedisStreamsBroker',
    'REDIS_URL': 'redis://localhost:6379/0',
    'REDIS_STREAM_KEY': 'telegram-bot',
}
```

`pip install django-aiogram[redis]` — the same extra, because it is the same driver.

| Setting | Default | What it is |
| --- | --- | --- |
| `REDIS_URL` | — | where the server is |
| `REDIS_STREAM_KEY` | **required** | the stream. No default on purpose: a stream is created on first use, so a default name would silently make one |
| `REDIS_STREAM_GROUP` | `django-aiogram` | the consumer group every worker joins |
| `REDIS_TIMEOUT` | `10` | the deadline on any single call |
| `BLPOP_TIMEOUT` | `5` | how long a read blocks before it is interrupted to check for shutdown |

## What it guarantees

**At-least-once, and answered by the mechanism rather than probed for.** `XREADGROUP` records
the delivery on the server *before* the consumer sees the entry, so there is no version check
and no fallback: unlike the list, this transport cannot be configured into a degraded mode.

Ordering is the stream's, and the group hands each entry to one member.

## No worker identity, and what that buys

Unsettled entries sit in the **group's** pending list rather than under a worker's name. So a
name buys nothing here, and that is the practical difference from the list:

- a replacement container with a fresh hostname strands nothing;
- any consumer reclaims what a dead one held;
- `I001` stays quiet, and `manage.py tgbot_reclaim` refuses, because there is nothing for
  `--worker` to select.

Recovery is on a clock rather than on a command: a worker that comes back — or one already
running — claims every entry idle longer than the liveness TTL. Nobody has to declare a worker
dead, which is exactly the judgement the list's manual reclaim exists to avoid making wrongly.

A refusal is a real one, and it does not wait for that clock: releasing an entry makes it
reclaimable **now** instead of after the idle threshold.

## Liveness

The group knows when any of its consumers last spoke to the server, so that is what a probe
reads — not a key this package writes. It answers for the group rather than for one worker,
which is the honest shape: with no per-worker state there is no per-worker answer to give.

## What it costs

An `XADD` took 116 to 124 microseconds on the same laptop and container as the list's 120 to
147 — that is, at or just inside it. Both are one round trip to the same server, and the claim
that survives repetition is the ordering rather than the ratio: Streams ≤ list < Kafka <
RabbitMQ. `scripts/measurements` re-takes all three.

## Where it shows through

- The stream grows. Nothing here trims it: `XADD MAXLEN` would cut exactly the entries a
  consumer deliberately leaves unsettled, so trimming is yours to schedule, with a length you
  choose against a backlog you are willing to lose.
- The group is created on first use with `MKSTREAM`, at id 0, so a group joining a stream that
  already has entries starts at the beginning rather than skipping what nobody has read.
- `decode_responses` on a shared `REDIS_URL` is the same trap it is on the list, and `E043`
  refuses the same combination.
