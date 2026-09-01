# django-aiogram

Run [aiogram](https://docs.aiogram.dev/) in a container next to Django, write handlers as
ordinary Django app code, and send Telegram messages from anywhere in the project.

```python
from django_aiogram import bot

bot.send(chat_id=CHAT_ID, text='hello')
```

Only the bot container runs the polling loop. Elsewhere `send()` queues the message and
returns — where the process is enabled; `ENABLED=0` makes it a no-op that still names the
message. `send_raw()` skips the queue and talks to Telegram from the calling process.

## Four transports, one setting

`BROKER` names the queue, and your code does not change with it: the same `bot.send()`, the
same handlers, the same event log, the same `manage.py start_tgbot`. What differs is what
becomes of a message whose worker was killed mid-send, and what recovery is — a command, a
clock, or the broker's own doing.

<div class="grid cards" markdown>

-   **[Redis list](Redis-list.md)**

    ---

    The default. `RPUSH` and a blocking pop, with an in-flight list per worker. At-least-once
    wherever `BLMOVE` is available, and a worker name that has to survive its container.

    `pip install 'django-aiogram[redis]'`

-   **[Redis Streams](Redis-Streams.md)**

    ---

    The same server, a consumer group, and no identity to keep: any consumer recovers a dead
    one's work. Wants Redis 7.0 and refuses below it.

    `pip install 'django-aiogram[redis]'`

-   **[RabbitMQ](RabbitMQ.md)**

    ---

    The broker tracks its own consumers, so an unacknowledged message comes back without
    being asked. One thread per connection, which shapes the rest.

    `pip install 'django-aiogram[rabbitmq,redis]'`

-   **[Kafka](Kafka.md)**

    ---

    Offsets settle a contiguous prefix, ordering is per partition, and a refusal rewinds
    everything after it. The model that differs most from the other three.

    `pip install 'django-aiogram[kafka,redis]'`

</div>

`redis` is in the last two lines for the FSM store rather than the queue: `FSM_STORAGE`
defaults to aiogram's Redis one, and `FSM_STORAGE: 'memory'` is what drops it.
**[Delivery](Delivery.md)** compares the four on what each asks of an operator.

## Start here

<div class="grid cards" markdown>

-   **[Installation](Installation.md)**

    ---

    Install, configure, run — and the two floors each transport adds, driver and server.

-   **[Settings](Settings.md)**

    ---

    Every key under `TELEGRAM_BOT`, with its default and the check id that guards it.

-   **[Handlers](Handlers.md)**

    ---

    `tg_router.py` in any installed app: routers, filters, FSM, and the ORM from async code.

-   **[Sending messages](Sending-messages.md)**

    ---

    `send`, `asend`, `send_many`, `enqueue`, `send_raw` — keyboards, files, errors, Celery.

-   **[Testing](Testing.md)**

    ---

    Your suite without a broker: `fakeredis`, asserting what was queued, draining by hand.

-   **[API](API.md)**

    ---

    The shared instance, its internals, and what stays public across releases.

</div>

## Running it

<div class="grid cards" markdown>

-   **[Delivery](Delivery.md)**

    ---

    How a queued message reaches Telegram, what each transport guarantees, and which
    consumer mode to pick.

-   **[Deployment](Deployment.md)**

    ---

    A compose recipe per transport, healthchecks, the shutdown budget, per-process opt-out.

-   **[Webhook](Webhook.md)**

    ---

    Updates over HTTP instead of polling for them, in four steps.

-   **[Rate limits](Rate-limits.md)**

    ---

    Staying inside Telegram's published limits, and what the pacer does when you do not.

-   **[Logging](Logging.md)**

    ---

    The `django_aiogram` logger, its structured fields, and which lines deserve an alert.

-   **[Event log](Event-log.md)**

    ---

    An optional table recording what the bot did, and a signal to count it without one.

</div>

## Reference

<div class="grid cards" markdown>

-   **[Serialization](Serialization.md)**

    ---

    What can be queued, the pickle threat model, and the way off it.

-   **[Troubleshooting](Troubleshooting.md)**

    ---

    Symptom to cause, with every line the healthcheck and the webhook can print.

-   **[Upgrading](Upgrading.md)**

    ---

    What each major release changed and what you have to do, newest first.

-   **[AI assistants](AI-assistants.md)**

    ---

    The brief to hand a coding agent, and what they get wrong without it.

</div>

---

Requires Python 3.10–3.14, Django 5.2+ and aiogram 3.30+. No transport driver is a
dependency of the package since 4.0: the one you name brings its own, and asks for its own
server — Redis Streams wants 7.0 and refuses below it, and the list runs on anything, giving
at-least-once wherever `BLMOVE` is available: a capability, not a version number.
**[Installation](Installation.md)** has the pair for each of the four.
[Source](https://github.com/CorneiZeR/django-aiogram) ·
[PyPI](https://pypi.org/project/django-aiogram/) ·
[Changelog](https://github.com/CorneiZeR/django-aiogram/blob/master/CHANGELOG.md)
