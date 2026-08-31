# django-aiogram

[![PyPI](https://img.shields.io/pypi/v/django-aiogram.svg)](https://pypi.org/project/django-aiogram/)
[![Python](https://img.shields.io/pypi/pyversions/django-aiogram.svg)](https://pypi.org/project/django-aiogram/)
[![CI](https://github.com/CorneiZeR/django-aiogram/actions/workflows/ci.yml/badge.svg)](https://github.com/CorneiZeR/django-aiogram/actions/workflows/ci.yml)
[![License](https://img.shields.io/pypi/l/django-aiogram.svg)](https://github.com/CorneiZeR/django-aiogram/blob/master/LICENSE)

Run [aiogram](https://docs.aiogram.dev/) next to Django: write handlers as
ordinary Django app code, and send Telegram messages from anywhere in the
project.

One container runs the bot. Every other process — web, Celery, a management
command — hands the call to a broker and returns, so a request never waits on
Telegram. **Four transports can carry it**, and `BROKER` says which:

```text
                              ┌─ Redis list ────┐
  web, celery ──bot.send()──▶ ├─ Redis Streams ─┤ ──▶ start_tgbot ──▶ Telegram
                              ├─ RabbitMQ ──────┤
                              └─ Kafka ─────────┘
```

| transport | `BROKER` | extra | its own required settings |
| --- | --- | --- | --- |
| Redis list *(default)* | `django_aiogram.broker.redis_list.RedisListBroker` | `[redis]` | — |
| Redis Streams | `django_aiogram.broker.redis_streams.RedisStreamsBroker` | `[redis]` | `REDIS_STREAM_KEY` |
| RabbitMQ | `django_aiogram.broker.rabbitmq.RabbitMQBroker` | `[rabbitmq]` | `RABBITMQ_URL`, `RABBITMQ_QUEUE` |
| Kafka | `django_aiogram.broker.kafka.KafkaBroker` | `[kafka]` | `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC` |

Your code does not change with the row: the same `bot.send()`, handlers, event log and
`manage.py start_tgbot`. What differs is what becomes of a message whose worker was
killed mid-send, and what recovery is — a command, a clock, or the broker's own doing.
[Delivery](https://github.com/CorneiZeR/django-aiogram/wiki/Delivery) compares them;
each transport has a page of its own below.

## Install

```shell
pip install 'django-aiogram[redis]'        # Redis list, the default, or Redis Streams
pip install 'django-aiogram[rabbitmq]'     # RabbitMQ
pip install 'django-aiogram[kafka]'        # Kafka
```

One extra per transport, so a deployment downloads only the driver it uses — a project
on Kafka has no reason to carry `redis`. Nothing is inferred from what happens to be
installed: `BROKER` names the transport, and a base `pip install django-aiogram`
imports and runs `manage.py` but carries no message. `manage.py check` says which
extra is missing, with the `pip install` line, in the processes that send;
[Installation](https://github.com/CorneiZeR/django-aiogram/wiki/Installation) has the
exceptions.

```python
# settings.py
import os

INSTALLED_APPS = [..., 'django_aiogram']

TELEGRAM_BOT = {
    'TOKEN': os.environ.get('TELEGRAM_BOT_TOKEN', ''),
    # unset, BROKER resolves to RedisListBroker; the table above has the other three,
    # and each transport reads its own settings on top of these two
    'REDIS_URL': os.environ.get('REDIS_URL', ''),
}
```

Both may be empty: nothing connects or validates credentials at import time, so tests and
migrations run without them. Requires Python 3.10–3.14, Django 5.2+ and aiogram 3.30+, plus
the transport's own floor — Redis 6.2 for the list, 7.0 for Streams.

## Use it

```python
# myapp/tg_router.py — imported automatically from every installed app
from aiogram import F, types

from django_aiogram import bot


@bot.message(F.text == '/start')
async def start(message: types.Message) -> None:
    await message.answer('hi')
```

```python
# anywhere else in the project
from django_aiogram import bot

bot.send(chat_id=CHAT_ID, text='Order approved')
```

```shell
python manage.py start_tgbot
```

A router module, a call, and one process running the bot. That process gets Django's
between-requests connection handling without having any requests — every update is bracketed
with `close_old_connections()`, so a database that restarts under a long-running bot does not
leave every handler raising `InterfaceError` until somebody notices. Nothing to configure;
[Deployment](https://github.com/CorneiZeR/django-aiogram/wiki/Deployment) says what the
healthcheck can and cannot see about it.

Everything else — rate limits, per-process opt-out, healthchecks — is configuration, documented
rather than required. Webhook mode is the one alternative that also asks for a URL route:
[Webhook](https://github.com/CorneiZeR/django-aiogram/wiki/Webhook) has the four steps.

## Documentation

The [wiki](https://github.com/CorneiZeR/django-aiogram/wiki) is the
documentation. Pages live in [`docs/wiki/`](https://github.com/CorneiZeR/django-aiogram/tree/master/docs/wiki), so they are reviewed in
the same pull request as the code they describe and published from `master`.

| | |
| --- | --- |
| [Installation](https://github.com/CorneiZeR/django-aiogram/wiki/Installation) | install, configure, run |
| [Settings](https://github.com/CorneiZeR/django-aiogram/wiki/Settings) | every setting, with defaults and check ids |
| [Handlers](https://github.com/CorneiZeR/django-aiogram/wiki/Handlers) | routers, filters, FSM, the async ORM |
| [Sending messages](https://github.com/CorneiZeR/django-aiogram/wiki/Sending-messages) | routes, keyboards, files, errors |
| [Testing](https://github.com/CorneiZeR/django-aiogram/wiki/Testing) | your suite without a broker, asserting what was queued |
| [API](https://github.com/CorneiZeR/django-aiogram/wiki/API) | the instance, its internals, and what stays public |
| [Delivery](https://github.com/CorneiZeR/django-aiogram/wiki/Delivery) | how queued messages reach Telegram |
| [Redis list](https://github.com/CorneiZeR/django-aiogram/wiki/Redis-list) | the default transport: what it guarantees, and why the worker's name matters |
| [Redis Streams](https://github.com/CorneiZeR/django-aiogram/wiki/Redis-Streams) | the same server, a consumer group, and no worker identity to keep |
| [RabbitMQ](https://github.com/CorneiZeR/django-aiogram/wiki/RabbitMQ) | a broker that tracks its own consumers, and one thread per connection |
| [Kafka](https://github.com/CorneiZeR/django-aiogram/wiki/Kafka) | offsets settle a prefix, ordering is per partition, a refusal rewinds |
| [Webhook](https://github.com/CorneiZeR/django-aiogram/wiki/Webhook) | receiving updates over HTTP instead of polling |
| [Rate limits](https://github.com/CorneiZeR/django-aiogram/wiki/Rate-limits) | staying inside Telegram's published limits |
| [Deployment](https://github.com/CorneiZeR/django-aiogram/wiki/Deployment) | compose recipes, healthchecks, per-process opt-out |
| [Logging](https://github.com/CorneiZeR/django-aiogram/wiki/Logging) | the logger and its structured fields |
| [Event log](https://github.com/CorneiZeR/django-aiogram/wiki/Event-log) | recording what the bot did to a table, and a signal to count it without one |
| [Serialization](https://github.com/CorneiZeR/django-aiogram/wiki/Serialization) | what can be queued |
| [Troubleshooting](https://github.com/CorneiZeR/django-aiogram/wiki/Troubleshooting) | symptoms and their usual causes |
| [Upgrading](https://github.com/CorneiZeR/django-aiogram/wiki/Upgrading) | what each major release changed, and what you must do |
| [AI assistants](https://github.com/CorneiZeR/django-aiogram/wiki/AI-assistants) | the brief to hand a coding agent |

Upgrading from 3.x: the distribution and import path are `django-aiogram`, the driver is
an extra you now name, and the event log has a table of its own. The upgrading page walks
it in order, `migrate` included.

## Contributing

[CONTRIBUTING.md](https://github.com/CorneiZeR/django-aiogram/blob/master/CONTRIBUTING.md) for the workflow, [AGENTS.md](https://github.com/CorneiZeR/django-aiogram/blob/master/AGENTS.md) for
the same ground in the form coding agents read. Changes are in
[CHANGELOG.md](https://github.com/CorneiZeR/django-aiogram/blob/master/CHANGELOG.md); security reports go through
[SECURITY.md](https://github.com/CorneiZeR/django-aiogram/blob/master/SECURITY.md).
