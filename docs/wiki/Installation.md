# Installation

=== "Redis list"

    ```shell
    pip install 'django-aiogram[redis]'
    ```

    The default: no `BROKER` needed.

=== "Redis Streams"

    ```shell
    pip install 'django-aiogram[redis]'
    ```

    Same driver as the list — a different data structure on the server, not a different
    dependency.

=== "RabbitMQ"

    ```shell
    pip install 'django-aiogram[rabbitmq,redis]'
    ```

    `redis` for the FSM store, which is aiogram's and defaults on; see below.

=== "Kafka"

    ```shell
    pip install 'django-aiogram[kafka,redis]'
    ```

    `redis` for the FSM store, which is aiogram's and defaults on; see below.

Requires Python 3.10–3.14, Django 5.2+ and aiogram 3.30+. Every pair in that range runs in
CI, and one leg pins the floors themselves — `django==5.2.0 aiogram==3.30.0 redis==6.2.0` on
Python 3.10 — because a floor nothing installs is a floor nobody has tried.

Then the transport you name adds two of its own, and they are different questions: the
**driver** is a Python package this project pins, and the **server** is what you run.

| transport | driver | server |
| --- | --- | --- |
| Redis list | `redis>=6.2` | any, but `BLMOVE` — Redis 6.2 — is what makes delivery at-least-once. Below it the consumer says so in the log and drops to at-most-once rather than refusing |
| Redis Streams | `redis>=6.2` | **7.0 or newer, enforced.** `depth()` reads `lag` from `XINFO GROUPS`, a field that arrived in 7.0, and the transport refuses rather than reporting a number a healthcheck would act on |
| RabbitMQ | `pika>=1.3` | RabbitMQ 4 in CI; 1.3.0 of the driver runs the whole integration suite against it |
| Kafka | `confluent-kafka>=2.6` below Python 3.14, `confluent-kafka>=2.12.1` on Python 3.14 and newer | Apache Kafka 4 in CI |

**One extra is not always enough, and the default is why.** `FSM_STORAGE` defaults to
`'redis'`, and that store is aiogram's — it imports redis-py. So a project on RabbitMQ or
Kafka that installs only its own transport's extra has a bot that starts and a dispatcher
that cannot be built:

```shell
pip install 'django-aiogram[kafka,redis]'   # Kafka for the queue, redis for the FSM store
```

or keep one extra and say the state lives in the process:

```python
TELEGRAM_BOT = {'FSM_STORAGE': 'memory', ...}   # per process, lost on restart
```

`manage.py check` reports **`E019`** for exactly one configuration — `FSM_STORAGE` naming
the Redis store on an install that has no redis-py — and says nothing once either line
above is in place. It used to say nothing at all: `start_tgbot` died on
`ModuleNotFoundError: No module named 'redis'` while building the dispatcher. A missing driver
reaching a project as a traceback is the shape `E047` prevents for the *broker*; `E019` is the
rule for this one.

The Kafka floor is two numbers because that driver compiles librdkafka: the first wheel for
Python 3.13 is 2.6.0 and the first for 3.14 is 2.12.1, so an older pin does not fail to *work*
there — it fails to install, asking for a C header. The API needs nothing newer than the wheels
do, measured: 2.6.0 passes every Kafka case against a real broker.

**The transport driver is an extra**, so a deployment downloads the one it uses and
not the other three. `BROKER` names the transport and nothing is inferred from what
happens to be installed, so the two have to agree; when they do not, `manage.py check`
says so with the install line for the one you named:

```text
?: (django_aiogram.E047) TELEGRAM_BOT['BROKER'] names
   'django_aiogram.broker.redis_list.RedisListBroker', whose driver is not installed.
	HINT: pip install "django-aiogram[redis]"
```

**Two extras are not transports.** `hiredis` swaps redis-py's parser for the C one, and
`prometheus` (`prometheus-client>=0.20`) installs the client the shipped exporter fills:

```shell
pip install 'django-aiogram[redis,prometheus]'
```

`django_aiogram.contrib.prometheus` is the only module that imports `prometheus_client`, and
nothing in this package imports *that* module — a project does, from its own
`AppConfig.ready()`. So without the extra the failure is an `ImportError` on the line that
asked for it, rather than a broken install for everyone who never wanted metrics. See
**[Event log](Event-log.md#metrics-without-the-table)**.

A base `pip install django-aiogram` is a valid install — it imports, and every
`manage.py` command runs — it just cannot carry a message anywhere yet. A process
with `ENABLED` off is not asked for a driver, so a web container that only records the
event log needs no extra. A process that reads the queue depth does need one even when
disabled: those reads are not gated on `ENABLED`, and `manage.py check` cannot tell
which processes make them.

The redis *driver* floor is 6.2 because aiogram's `RedisStorage` asks for exactly that —
`redis[hiredis]>=6.2.0` in its own metadata — and `FSM_STORAGE: 'redis'` is the default, so
every transport can reach the storage even when the queue itself is somewhere else. On redis-py below 5.0.1 the storage
raises `AttributeError: 'Redis' object has no attribute 'aclose'`. redis-py 8 is
tested here and works, though aiogram's own optional extra stops at 7.

## Add the app

```python
# settings.py
import os

INSTALLED_APPS = [
    ...,
    'django_aiogram',
]

TELEGRAM_BOT = {
    'TOKEN': os.environ.get('TELEGRAM_BOT_TOKEN', ''),
    'REDIS_URL': os.environ.get('REDIS_URL', ''),
}
```

That is the whole minimum, and both values may be empty at startup. The
package needs them only when something actually reaches Telegram or Redis, so
tests, migrations and a build all run without them.

The package ships two tables — the event log, and the schedule table that an `eta` writes
to — so run migrations after adding it:

```shell
python manage.py migrate
```

The table is created whether or not you turn the event log on — `EVENT_LOG` is
off by default, and nothing is written until you set it. See
**[Event log](Event-log.md)**.

## Configure from the environment

Scalar settings can come from `DJANGO_AIOGRAM_<NAME>`:

```ini
# .env
DJANGO_AIOGRAM_TOKEN=123:abc
DJANGO_AIOGRAM_REDIS_URL=redis://redis:6379/0
DJANGO_AIOGRAM_ENABLED=0
```

Django settings win over the environment. Callables and mappings —
`DEFAULT_KWARGS`, `DEFAULT_BOT_PROPERTIES`, `RATE_LIMIT` — have no sensible
textual form and stay in `settings.py`.

## Run the bot

```yaml
# docker-compose.yml
services:
  telegram_bot:
    image: ${IMAGE}
    command: python manage.py start_tgbot
    restart: always
    env_file: .env
    depends_on: [redis]

  redis:
    image: redis:7-alpine
    restart: always
```

See **[Deployment](Deployment.md)** for the whole file, and for turning the bot off in every
other process.

## Check the configuration

```shell
python manage.py check
```

Settings are validated: wrong types, unknown keys, misspelled bot properties
and impossible rate limits all fail here rather than at the first message.
Missing credentials are reported as warnings, not errors, so a build or a
migration container is not blocked by them.

Run it somewhere the bot is enabled: a process with `ENABLED` off registers no
checks, so it reports nothing either way — unless the event log is on there, which
registers all of them on its own, bot settings included.

## Next

* **[Handlers](Handlers.md)** to answer messages
* **[Sending messages](Sending-messages.md)** to send them
