# RabbitMQ

A broker that tracks its own consumers, which removes more operational work than any other
choice here. An unacknowledged message returns to the queue when the channel holding it drops,
and a dying worker drops its channel by dying.

```python
TELEGRAM_BOT = {
    'BROKER': 'django_aiogram.broker.rabbitmq.RabbitMQBroker',
    'RABBITMQ_URL': 'amqp://user:pass@localhost:5672/',
    'RABBITMQ_QUEUE': 'telegram-bot',
}
```

`pip install django-aiogram[rabbitmq]`, which brings `pika`.

| Setting | Default | What it is |
| --- | --- | --- |
| `RABBITMQ_URL` | **required** | the AMQP URL. No default: `guest@localhost` is a credential, not a default |
| `RABBITMQ_QUEUE` | **required** | the queue, declared durable on first use |
| `RABBITMQ_PREFETCH` | `0` | how many unacknowledged messages one consumer may hold; `0` is unlimited |
| `RABBITMQ_TIMEOUT` | `10` | the deadline on a call to the broker |

## What it guarantees

**At-least-once, and the broker is what makes it true** rather than anything in this package.
There is no in-flight list, `reclaim()` has nothing to do and says so by answering nothing at
all, `manage.py tgbot_reclaim` refuses, and `I001` stays quiet. A worker's name buys nothing.

A publish is **persistent, mandatory and confirmed**: it is marked for disk, a message no queue
will take raises rather than vanishing into an exchange, and the broker has answered before the
call returns. What the confirm promises is that the broker has taken responsibility — for a
persistent message on a durable queue that normally means it is on disk, but the protocol allows
the confirm once the message has been handled, so it is not a strict fsync barrier. Most of the
cost below is that disk work all the same: the same publish is 135 to 173 microseconds with
persistence off.

A refusal is a real nack — `basic_nack` with requeue — rather than a documented no-op, so a
message this worker will not take goes back for another to try.

## Thread affinity, which is the one rule to respect

A `pika` connection belongs to **one thread**. That is not a style preference: the driver
documents it, and reaching a connection from another thread is unsupported. So this transport
keeps a connection per thread, and the awaiting half of the API borrows a thread rather than
opening an async client — there is no `aio-pika` here, and `bot.aclose()` has nothing to close.

The choice was measured rather than assumed. Held to the same guarantee, `pika` on its own
synchronous face costs 15 to 20 microseconds unconfirmed and 323 to 393 confirmed; reaching it
from a coroutine costs 67 to 85 unconfirmed. The alternative — an async driver reached from a
synchronous caller, which is what a Django view is — costs 121 to 131. Reaching a *thread* from
a coroutine is about half the price of reaching a *loop* from a synchronous caller, so `pika`
wins on the face this package's traffic actually uses and on the rare one too.

## What it costs

The dearest of the four, and the persistence is most of it: the same publish is 135 to 173
microseconds without it, against 323 to 393 with. What that buys is the broker taking
responsibility for a message marked for disk before the caller is told it was accepted — see
the guarantee above for why that is not the same as an fsync barrier. Against a Redis list
publish on the same laptop — 120 to 147 microseconds — it is a few multiples, and both numbers
come from a container rather than a native server, which is the reason to quote the divisor
rather than the multiple.

`scripts/measurements/amqp_driver_choice.py` re-takes all of it.

## Where it shows through

- `RABBITMQ_PREFETCH` and `MAX_IN_FLIGHT` bound the same thing from two ends: the broker's
  window on unacknowledged deliveries, and this consumer's on outstanding sends. Setting only
  the second leaves the broker willing to hand over more than the worker will hold.
- `bot.inflight_depth()` answers from **this process's memory**, because AMQP has no
  unacknowledged count to ask for — the management HTTP API has one, and reaching for it would
  mean a second way of talking to the broker for a number the contract defines as this worker's.
  So the reading is only meaningful inside the bot container; from a web process it is zero, and
  correctly so. `queue_depth()` has no such limitation.
- The connection registry outlives the threads that owned its entries, which is a known
  rough edge rather than a designed one — a thread that exits without closing leaves its entry
  behind.
- A queue declared with different arguments elsewhere makes the declaration fail rather than
  adopting it, which is deliberate: silently using a queue shaped differently from the one this
  package expects is worse than refusing to start.
