# Kafka

The transport whose settling model is least like a queue's, and the one place where reading this
page before deploying will save you a surprise. An offset is not an acknowledgement of one
message; it is a claim about everything below it.

```python
TELEGRAM_BOT = {
    'BROKER': 'django_aiogram.broker.kafka.KafkaBroker',
    'KAFKA_BOOTSTRAP': 'localhost:9092',
    'KAFKA_TOPIC': 'telegram-bot',
}
```

`pip install django-aiogram[kafka]`, which brings `confluent-kafka` and `librdkafka` under it.

| Setting | Default | What it is |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP` | **required** | the bootstrap servers |
| `KAFKA_TOPIC` | **required** | the topic |
| `KAFKA_GROUP` | `django-aiogram` | the consumer group; every worker joins it |
| `KAFKA_TIMEOUT` | `10` | the socket deadline, and how long a poll waits |

## What it guarantees

**At-least-once, and the group is what makes it true.** An uncommitted offset is redelivered by
the group rather than by anything here, so `reclaim()` answers nothing, `manage.py tgbot_reclaim`
refuses, `I001` stays quiet, and a worker's name buys nothing.

`enable.auto.commit` is **off**, and that is the whole reason this transport can promise
anything: a committed offset means "the consumer settled this message", and letting the driver
commit on a timer would mean it says so about messages it is still holding.

Settled is not the same as *delivered to Telegram*, and the difference belongs to the handler
rather than to Kafka. `bot.send_raw` — what `manage.py start_tgbot` uses — takes `on_complete`
and signals it when the send finishes, so there the commit does follow the send. A handler of
your own that takes only `**kwargs` is settled the moment it returns, which may be before
anything reached Telegram. See **[[Delivery]]**.

Ordering is **per partition**, not per topic. Nothing here sets a key, so records are spread
across partitions and two messages queued in order can be delivered out of it. A single
partition gives total order and one consumer's worth of throughput.

## Offsets settle a prefix, not a message

A committed offset is the *next* record to read, so committing it claims every offset below it
in that partition as done. With `MAX_IN_FLIGHT` above one the consumer holds several sends at
once, so it commits only the highest **contiguous** prefix per partition:

- settle the second while the first is still in flight and **nothing** is committed for that
  partition until the first finishes — committing the second would claim the first as done;
- a worker killed at that moment loses nothing. Replay starts *at* the committed offset, so what
  comes back is that record and everything above it in the partition — with `MAX_IN_FLIGHT` above
  two, more than the pair that caused the gap;
- partitions are independent. A gap in one does not hold up commits in another.

## A refusal rewinds, and takes its neighbours with it

There is no per-message nack. Giving one up rewinds that partition to its offset, and **the
released record together with every later one in that partition** is delivered again. The other
partitions carry on untouched.

A handle the rewind *reached* — one naming its offset or a higher one — stops being settleable,
because the delivery it named no longer exists. A send that finishes afterwards is reported and
settles nothing, and its message comes back with the rest.

**Build idempotency on your own business key.** On this transport that is not general advice: a
single refusal can redeliver a run of messages that had nothing wrong with them.

## The driver, and why this one

`confluent-kafka`, chosen for two reasons in that order. The consumer here is a thread, and a
synchronous driver belongs in one; the alternative would need an event loop inside it. And held
to the same guarantee — both waiting for the broker — it was faster in **every** run of eleven:
166 to 295 microseconds against 351 to 492.

The plan's other argument turned out to be false and is recorded so it is not reopened:
`aiokafka` ships no pure-Python wheel either, so both drivers are compiled and there is no
portability difference. `scripts/measurements/kafka_driver_choice.py` re-takes the timings.

`linger.ms` is set to **0**. The driver's default of 5 milliseconds holds a batch open for
records that are not coming, and `publish` waits for the broker either way: measured, one
confirmed publish costs 6.4 ms on the default against 241 µs at 0. Batching still happens — a
hundred payloads cost 0.44 ms against 7.01 ms on the default — so what is switched off is the
waiting, and the bulk path is faster for it too.

## Where it shows through

- **What a payload may weigh is smallest here, by a wide margin, and it is the one to check
  before sending files.** Two settings have to agree: `message.max.bytes` on the broker and the
  producer's own, and the driver's default is around a megabyte where the Redis and AMQP limits
  are orders larger. This package sets neither, so an oversized publish is refused by the driver
  rather than truncated — but a `BufferedInputFile` carries the file's bytes in the payload, so
  a photo is where a deployment meets this. Raise both settings together, or queue
  `FSInputFile`/`URLInputFile`, which send a reference.
- One producer per process, because librdkafka's is thread-safe and keeps its own I/O thread. One
  consumer per **thread**, because its consumer is not — and because a second consumer would be a
  second group member taking a share of the partitions.
- A queue-depth read uses a consumer that never subscribes. Subscribing is what makes a member,
  and a process that only publishes must not become one: the coordinator would give it partitions
  it never polls, and on a single-partition topic the real worker then receives nothing until
  that member's session times out. A healthcheck could starve the consumer it was checking on.
- `bot.aclose()` has nothing to close here. The driver is synchronous, so the awaiting half of
  the API borrows a thread and uses the same connection the synchronous half does.
- The depth a probe reports is the group's lag rather than a queue length — and `queue_depth()`
  takes this process's unsettled messages off it, which raw lag from `kafka-consumer-groups`
  does not. The two differ by what the worker is holding while sends are in flight, and because
  offsets settle a contiguous prefix the gap can outlast the sends that opened it. Both numbers
  are exact; they answer different questions.
- `bot.inflight_depth()` answers from **this process's memory**. Kafka has nothing to ask: an
  offset is either committed or not, and "taken but not settled" exists only here. So it is a
  reading for the bot container; anywhere else it is zero, correctly and uselessly.
