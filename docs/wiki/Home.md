# django-aiogram

Run [aiogram](https://docs.aiogram.dev/) in a container next to Django, write
handlers as ordinary Django app code, and send Telegram messages from anywhere
in the project.

Only the bot container runs the polling loop. Elsewhere `send()` queues the message and
returns — where the process is enabled; `ENABLED=0` makes it a no-op that still names the
message. Which queue it is — a Redis list or stream, an AMQP queue, a Kafka topic — is what
`BROKER` names. `send_raw()` skips the queue and talks to Telegram from the calling process.

```python
from django_aiogram import bot

bot.send(chat_id=CHAT_ID, text='hello')
```

## Start here

* **[[Installation]]** — install, configure, run
* **[[Settings]]** — every setting, with defaults
* **[[Handlers]]** — writing routers and using FSM
* **[[Sending-messages|Sending messages]]** — `send`, `asend`, `send_many`, `enqueue`, `send_raw`, keyboards, files
* **[[Testing]]** — running your suite without a broker, asserting what was queued
* **[[API]]** — the instance, its internals, and what stays public

## Going further

* **[[Delivery]]** — how a message gets from the queue to Telegram, what each transport
  guarantees, and which mode to pick
* **[[Webhook]]** — receiving updates over HTTP instead of polling for them
* **[[Rate-limits|Rate limits]]** — staying inside Telegram's published limits
* **[[Deployment]]** — docker-compose recipes, disabling the bot per process
* **[[Logging]]** — the logger and its structured fields
* **[[Event-log|Event log]]** — recording what the bot did to a table
* **[[Serialization]]** — what can be queued, and the pickle-to-JSON move
* **[[Troubleshooting]]** — symptoms and their usual causes

## Upgrading

* **[[Upgrading]]** — what each major release changed, and what you have to do
* **[[AI-assistants|AI assistants]]** — the brief to hand a coding agent, and what they get wrong

---

Requires Python 3.10–3.14, Django 5.2+ and aiogram 3.30+. No transport driver is a
dependency of the package since 4.0: the one you name brings its own, and asks for its own
server — Redis Streams wants 7.0, the list runs on anything and gives at-least-once from 6.2.
**[[Installation]]** has the pair for each of the four.
[Source](https://github.com/CorneiZeR/django-aiogram) ·
[PyPI](https://pypi.org/project/django-aiogram/) ·
[Changelog](https://github.com/CorneiZeR/django-aiogram/blob/master/CHANGELOG.md)
