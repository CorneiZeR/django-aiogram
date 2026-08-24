# The measurements two decisions and every quoted ratio rest on

`BROKER`'s driver for RabbitMQ and for Kafka was chosen by running these, not by reading about
the libraries, and the ratios the pages quote are divided by a number measured here rather than
remembered. They live here so those numbers can be re-taken rather than trusted: a driver
release, a broker version or a different machine can move them, and a decision nobody can
re-check is one that quietly rots.

They are not tests and nothing runs them in CI. Each needs a broker, and what they produce is a
number to read rather than a pass or a fail — so they hold to the same rules as the package
(annotated, documented, reporting through a logger) and are excused nothing. What *can* be
checked without a broker is that each one measures what its transport does, and
`tests/test_measurements.py` does exactly that.

## The rules that make a number mean anything

Each of these was learnt by getting it wrong, and each cost a number that had already reached
the changelog.

**Hold the guarantee constant.** The first attempt at the AMQP one compared `pika`'s unconfirmed
publish against `aio-pika`'s confirmed one — `aio_pika`'s channel confirms by default and pika's
does not — and read as a 15× difference in the *driver* when most of it was the promise. Every
row here is measured with the wait off and on, and each decision rests on the column where both
drivers wait.

**Call a driver the way its own rules allow.** The AMQP script reached pika from an executor
thread over a connection the main thread had opened, which pika documents as unsupported: a
`BlockingConnection` belongs to one thread and `add_callback_threadsafe` is the only thing
another may do to it. That row was timing a path no correct implementation would take.

**Publish what the transport publishes, not what the driver defaults to.** `RabbitMQBroker`
sends persistent and `mandatory`; the script sent neither, so the broker was answering without
writing to disk. Persistence is most of the cost — the same publish is 135–173 µs without it.
The Kafka script had the mirror of this from the other side: it set `linger.ms` to 0 while the
transport took librdkafka's default of 5 ms, so the figure it reported was one no `bot.send()`
could reach — 6.4 ms against 241 µs, measured. The transport sets it to 0 now, and the test
holds the two to the same value.

**Quote the divisor or do not quote the multiple.** These ratios were carried for a while as
"~10×" and "~18×" against a Redis publish nobody had measured here — a figure of 14–19 µs taken
in 3.1.0 on a *native* server, while every broker number below comes from a container. On one
footing the confirmed AMQP publish is two and a half times a list publish and on the other it is
twenty, and both readings are honest about their own machine. `redis_baseline.py` exists so the
divisor is a row in the table rather than folklore; what survives a change of footing is only
the ordering, Streams ≤ list < Kafka < RabbitMQ.

**Run each one more than once.** The first single run of the Kafka one showed 479µs against 502,
and "latency does not decide it" went into the changelog on the strength of it. Repeating it on a
warm broker said 1.3 to 2.2 times instead — the parity was a cold cluster. Every figure below is
a span across runs for that reason, and each ratio is taken run by run: dividing the spans
against each other instead would read 1.5 to 3.0 and describe a run that never happened.

**Leave the broker as you found it.** The queue, the topic and the key are named per run and
removed on the way out. A fixed name would make a run delete somebody else's object of the same
name on a shared server, and two runs at once would measure each other's traffic. The removals
are waited for, because a driver's delete futures *are* the request: a process that exits without
reading them leaves the topic standing — measured, it did. Nothing consumes what these publish
either, so each row empties its queue before it starts: a deep enough one puts RabbitMQ into
publisher flow control, which is a different thing to be timing.

## Running them

The drivers they need but the package does not ship — the two that were *not* chosen — live in
their own dependency group, so nobody installs a driver they cannot configure. One environment
for all three scripts, so that "same way" includes the interpreter:

```shell
python -m venv .measure
.measure/bin/pip install -e '.[redis,rabbitmq,kafka]' --group measure
```

`--group measure` needs pip 25.1+ or uv, the same floor `--group dev` has. `redis` is in there
for the baseline, which needs no group of its own.

```shell
# pinned, like the Kafka image below: `rabbitmq:4` and `redis:8` move, and a run that follows
# this file should be able to reproduce the table rather than a later broker's timings
docker run -d --rm --name amqp -p 5673:5672 rabbitmq:4.3.5
.measure/bin/python -m scripts.measurements.amqp_driver_choice

docker run -d --rm --name redis -p 6399:6379 redis:8.10.0
.measure/bin/python -m scripts.measurements.redis_baseline
```

```shell
# the advertised listener matters: with the image's default the broker answers `localhost:9092`
# from inside the container, and a client on the host retries into a refusal loop rather than
# failing — which is how the first attempt at this spent ten minutes
# 4.3.1, which is what the numbers below were taken on; CI pins 4.0.0 instead, deliberately —
# it tests the oldest 4.x this package supports, and a measurement wants the one it measured
docker run -d --rm --name kafka -p 9093:9093 \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9094,HOST://0.0.0.0:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092,HOST://127.0.0.1:9093 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,HOST:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9094 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  apache/kafka:4.3.1
.measure/bin/python -m scripts.measurements.kafka_driver_choice
```

## What they answered

One machine, loopback, CPython 3.13.14, with RabbitMQ 4.3.5, Apache Kafka 4.3.1 and Redis
8.10.0, and pika 1.4.4, aio-pika 10.0.1, confluent-kafka on librdkafka 2.15.0 and aiokafka
0.14.0. Versions rather than "4", because a broker or a driver release is exactly the sort of
thing that moves these — which is why the recipes above name them too.

| AMQP | unconfirmed | confirmed |
| --- | --- | --- |
| `pika`, on its own synchronous face | 15 – 20 µs | 323 – 393 µs |
| `aio-pika`, reached from a synchronous caller | 121 – 131 µs | 456 – 511 µs |
| `pika`, reached from a coroutine | 67 – 85 µs | 404 – 406 µs |

| Kafka | queued locally | waited for the ack |
| --- | --- | --- |
| `confluent-kafka`, synchronous | 0.2 – 0.3 µs | 166 – 295 µs |
| `aiokafka`, via a loop thread | 66 – 75 µs | 351 – 492 µs |

| Baseline | | acknowledged |
| --- | --- | --- |
| `RPUSH`, the list transport's publish | — | 120 – 147 µs |
| `XADD`, the Streams transport's publish | — | 116 – 124 µs |

The last table is the divisor, and it is a table rather than a sentence because several claims
elsewhere are quoted as a multiple of it. Redis is asked for no disk here and neither transport
waits for one, which is why those rows have no unconfirmed column: a list publish *is* the
acknowledged one.

**RabbitMQ: `pika`.** Each driver has one face it has to bridge, and the two bridges do not cost
the same: reaching a *thread* from a coroutine is 67–85 µs, reaching a *loop* from a synchronous
caller is 121–131 µs — about half. The ratio has read 0.53 to 0.64 across six runs, its floor
moving down on each of the last three, so "about half" is the part to read.

An earlier version of this measured the pika bridge from the caller rather than from the loop,
which added the very hand-off the other row *is*, and the two then looked like the same number.

So pika wins twice rather than once: it is free on the face this package's traffic actually
uses — `bot.send()` from views, tasks and management commands — and it is the cheaper of the two
on the rare one, where `asend()` reaches it from a coroutine. The confirmed column says the same
thing more quietly: 404–406 against 456–511.

**`confluent-kafka`.** The consumer is a thread, where a synchronous driver belongs; `aiokafka`
would need an event loop inside it. It is also 1.3 to 2.2 times faster on the face that waits,
run for run. Both spreads are about 40 per cent of their own floor, which is what a laptop's
broker does to a half-millisecond round trip — the gap between the drivers is what survives it.

The plan's other argument turned out to be false and is recorded so it is not reopened:
`aiokafka` ships no `py3-none-any` wheel either, so both drivers are compiled and there is no
portability difference.
