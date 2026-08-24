# The measurement this transport's driver rests on

`BROKER`'s Kafka driver was chosen by running this, not by reading about the libraries. It lives
here so the numbers in `CHANGELOG.md` can be re-taken rather than trusted: a driver release, a
broker version or a different machine can move them, and a decision nobody can re-check is one
that quietly rots.

It is not a test and nothing runs it in CI. It needs a broker, and what it produces is a number
to read rather than a pass or a fail — so it holds to the same rules as the package (annotated,
documented, reporting through a logger) and is excused nothing.

A package rather than a loose file because `_timing` is shared, and because a number is only
comparable to another number taken the same way.

## The rules that make a number mean anything

**Hold the guarantee constant.** `produce` answers locally in a fraction of a microsecond and the
broker's acknowledgement arrives later, so timing the first and calling it a publish would be
measuring the queue rather than the transport. Both drivers are measured with the wait off and
on, and the decision rests on the column where they both wait.

**Run it more than once.** The first single run of this showed 479µs against 502 and "latency
does not decide it" went into the changelog on the strength of it. Repeating it on a warm broker
said 1.6 to 2.2 times instead — the parity was a cold cluster. Every figure below is a span
across runs for that reason, and the ratio is each run paired against itself: dividing the spans
against each other would read 1.5 to 3.0 and describe a run that never happened.

**Measure what the transport does, not what the driver defaults to.** This script set
`linger.ms` to 0 while the transport took librdkafka's default of 5ms, so the number it reported
was one no `bot.send()` could reach: measured, 6.4ms against 241µs for a single confirmed
publish, because `publish` waits for the broker while the driver holds a batch open for records
that are not coming. The transport sets it to 0 now, and `tests/test_measurements.py` holds the
two to the same value.

**Leave the broker as you found it.** The topic is named per run and deleted on the way out.
A fixed name would make this delete somebody else's topic of the same name on a shared cluster,
and two runs at once would measure each other's traffic. `delete_topics` is waited for, because
its futures are the request: a process that exits without reading them leaves the topic standing.

## Running it

The driver it needs but the package does not ship — `aiokafka`, the one that was *not* chosen —
lives in its own dependency group, so nobody installs a driver they cannot configure:

```shell
python -m venv .measure
.measure/bin/pip install -e '.[kafka]' --group measure
```

`--group measure` needs pip 25.1+ or uv, the same floor `--group dev` has.

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

## What it answered

One machine, loopback, CPython 3.13.14 and Apache Kafka 4.3.1, with confluent-kafka on
librdkafka 2.15.0 and aiokafka 0.14.0. Versions rather than "4", because a broker or a driver
release is exactly the sort of thing that moves these:

| | queued locally | waited for the ack |
| --- | --- | --- |
| `confluent-kafka`, synchronous | 0.2 – 0.3 µs | 166 – 237 µs |
| `aiokafka`, via a loop thread | 66 – 75 µs | 354 – 492 µs |

**`confluent-kafka`.** The consumer is a thread, where a synchronous driver belongs; `aiokafka`
would need an event loop inside it. It is also 1.6 to 2.2 times faster on the face that waits,
run for run. Both spreads are about 40 per cent of their own floor, which is what a laptop's
broker does to a half-millisecond round trip — the gap between the drivers is what survives it.

The plan's other argument turned out to be false and is recorded so it is not reopened:
`aiokafka` ships no `py3-none-any` wheel either, so both drivers are compiled and there is no
portability difference.
