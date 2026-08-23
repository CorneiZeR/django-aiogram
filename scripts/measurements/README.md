# The measurements two decisions rest on

`BROKER`'s driver for RabbitMQ and for Kafka was chosen by running these, not by reading about
the libraries. They live here so the numbers in `CHANGELOG.md` can be re-taken rather than
trusted: a driver release, a broker version or a different machine can move them, and a decision
nobody can re-check is one that quietly rots.

They are not tests and nothing runs them in CI. Each needs a broker, and what they produce is a
number to read rather than a pass or a fail — so they hold to the same rules as the package
(annotated, documented, reporting through a logger) and are excused nothing.

## The rule that makes a number mean anything

**Hold the guarantee constant.** The first attempt at the AMQP one compared `pika`'s
unconfirmed publish against `aio-pika`'s confirmed one — `aio_pika`'s channel confirms by default
and pika's does not — and read as a 15× difference in the *driver* when most of it was the
promise. Both scripts measure both drivers with the wait off and on.

**Leave the broker as you found it, between rows and not only at the end.** Nothing consumes
what these publish, so the second attempt at the AMQP one timed every row against the backlog
the rows before it had left — and a deep enough queue puts RabbitMQ into publisher flow control,
which is a different thing to be timing. Each row now empties the queue before it starts, and
the run deletes it on the way out.

**Call a driver the way its own rules allow.** The same attempt reached pika from an executor
thread over a connection the main thread had opened, which pika documents as unsupported: a
`BlockingConnection` belongs to one thread and `add_callback_threadsafe` is the only thing
another may do to it. That row was measuring a path no correct implementation would take.

**Publish what the transport publishes, not what the driver defaults to.** The third attempt
held the confirm constant and still measured a cheaper promise than the package makes:
`RabbitMQBroker.publish` sends persistent and `mandatory`, and the script sent neither, so the
broker was answering without writing to disk. Persistence is most of the cost — the same
publish is 135–173µs without it and 323–393µs with it. Together, these three mistakes moved the
confirmed pika figure from a single 170.7µs to a 323–393µs spread over four runs. None of them
changed which driver wins, which is luck rather than a reason to skip any of the rules: the same
question asked of the Kafka script found that the transport's own producer used the driver's
default `linger.ms`, and paid 6.4ms per send for it.

## Running them

The drivers they need but the package does not ship live in their own dependency group, so
nobody installs an `aio-pika` they cannot configure:

```shell
python -m venv .measure
.measure/bin/pip install -e '.[rabbitmq,kafka]' --group measure
docker run -d --rm --name amqp -p 5673:5672 rabbitmq:4
.measure/bin/python -m scripts.measurements.amqp_driver_choice
```

`.[rabbitmq,kafka]` brings the two drivers that were chosen; `--group measure` brings the two
that were not. Needs pip 25.1+ or uv, the same floor `--group dev` has.

The baseline needs neither group — `redis` is a shipped extra, so an ordinary development
checkout can take it:

```shell
docker run -d --rm --name redis -p 6399:6379 redis:8
python -m scripts.measurements.redis_baseline
```

```shell
# the advertised listener matters: with the image's default the broker answers `localhost:9092`
# from inside the container, and a client on the host retries into a refusal loop rather than
# failing — which is how the first attempt at this spent ten minutes
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
  apache/kafka:4.0.0
.measure/bin/python -m scripts.measurements.kafka_driver_choice
```

Both take the broker's address from the same environment variables the integration suite uses,
so a reader with a broker already running needs no edits.

## What they answered

One machine, loopback, CPython 3.13.14, RabbitMQ 4.3.5 and Apache Kafka 4.3.1, with pika 1.4.4,
aio-pika 10.0.1, confluent-kafka on librdkafka 2.15.0 and aiokafka 0.14.0. Versions rather than
"4", because a broker or a driver release is exactly the sort of thing that moves these:

| | unconfirmed | confirmed |
| --- | --- | --- |
| `pika`, synchronous | 18 – 20 µs | 323 – 393 µs |
| `aio-pika`, via a loop thread | 121 – 125 µs | 456 – 495 µs |
| `pika`, awaited via `to_thread` | 119 – 122 µs | 412 – 423 µs |
| `confluent-kafka`, synchronous | 0.2 µs | 166 – 232 µs |
| `aiokafka`, via a loop thread | 66 – 72 µs | 354 – 359 µs |
| `RPUSH`, the list transport's publish | — | 120 – 143 µs |
| `XADD`, the Streams transport's publish | — | 116 – 121 µs |

The last two rows are the baseline, and they are in this table rather than in a sentence
somewhere because five claims elsewhere are quoted as a multiple of them. Redis is asked for no
disk here and neither transport waits for one, which is why they have no unconfirmed column: a
list publish *is* the acknowledged one.

**RabbitMQ: `pika`.** The two bridged rows are the same number, so crossing the thread boundary
is the price rather than the library — about 100 µs either way, 0.98 to 1.00 times each other. That makes the question which
face pays it, and the faces are not equal traffic: `bot.send()` is called from views, tasks and
management commands, `asend()` is for ASGI. pika charges the rare one.

**Kafka: `confluent-kafka`.** The consumer is a thread, where a synchronous driver belongs;
`aiokafka` would need an event loop inside it. It is also 1.6 to 2.1 times faster on the
confirmed face, which is the correction worth reading: the *first* run of that script showed 479
against 502 µs and "latency does not decide it" went into the changelog on the strength of it.
Three runs on a warm broker say otherwise. **Run these three times before believing them** — the
Kafka ranges above are why the tables here give ranges at all.

The plan's other argument turned out to be false and is recorded so it is not reopened:
`aiokafka` ships no `py3-none-any` wheel either, so both drivers are compiled.

**And the guarantee is not optional.** `RPUSH` answers with the new list length, so a Redis
publish is acknowledged before `send()` returns. The confirmed columns are what matching that
costs: on this footing a Redis list publish is 120–143 µs, Kafka 1.2–1.9× that and RabbitMQ
2.3–3.3×, and the difference between the two brokers is mostly the disk, since RabbitMQ is asked
for persistence and a Kafka broker's flushing is its own setting rather than the publisher's.

**Quote the divisor or do not quote the multiple.** These ratios were carried for a while as
"~10×" and "~18×" against a Redis publish nobody had measured here — a figure of 14–19 µs taken
in 3.1.0 on a *native* server, while every broker number above comes from a container. On one
footing the AMQP publish is two and a half times a list publish and on the other it is twenty,
and both readings are honest about their own machine. `redis_baseline.py` exists so the divisor
is a row in this table rather than folklore; what survives a change of footing is only the
ordering, Streams ≤ list < Kafka < RabbitMQ.
