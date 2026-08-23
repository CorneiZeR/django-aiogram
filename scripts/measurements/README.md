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
  apache/kafka:latest
.measure/bin/python -m scripts.measurements.kafka_driver_choice
```

Both take the broker's address from the same environment variables the integration suite uses,
so a reader with a broker already running needs no edits.

## What they answered

One machine, loopback, RabbitMQ 4 and Apache Kafka 4, CPython 3.13:

| | unconfirmed | confirmed |
| --- | --- | --- |
| `pika`, synchronous | 18.9 µs | 170.7 µs |
| `aio-pika`, via a loop thread | 119.2 µs | 290.6 µs |
| `pika`, awaited via `to_thread` | 120.1 µs | — |
| `confluent-kafka`, synchronous | 0.2 µs | 166 – 232 µs |
| `aiokafka`, via a loop thread | 66 – 72 µs | 354 – 359 µs |

**RabbitMQ: `pika`.** The two bridged rows are the same number, so crossing the thread boundary
is the price rather than the library — about 100 µs either way. That makes the question which
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
costs: RabbitMQ ~9× a Redis list, Kafka ~10×.
