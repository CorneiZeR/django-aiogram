# Upgrading

What each major release changed, newest first — so an upgrade is read from the bottom. Find
the lowest section that applies to the version you are on and work **up** the page, one release
at a time: each covers a single hop and assumes the ones below it are done.

# From 3.1 to 4.0

## Install the new distribution

`django-redis-aiogram` is not updated any more. `pip install django-aiogram`, and drop the
old one — nothing imports it after the steps below.

Redis is no longer a hard dependency. Pick the transport you run and install it with the
package: `pip install django-aiogram[redis]` for the queue 3.x used. A project that names a
transport whose driver is not installed is refused by a system check at startup rather than
by an `ImportError` on the first send.

## Rename the app and the imports

```python
INSTALLED_APPS = ['django_aiogram']
```

```python
from django_aiogram import bot, conf
from django_aiogram.producer.client import TelegramBot
```

`TELEGRAM_BOT` stays as the settings key — it names what it configures, not the package
that reads it. Environment variables move from `DJANGO_REDIS_AIOGRAM_*` to
`DJANGO_AIOGRAM_*`, the logger from `django_redis_aiogram` to `django_aiogram`, and check
ids from `django_redis_aiogram.EXXX` to `django_aiogram.EXXX` — so re-silence anything you
had silenced by id.

## Move the event log's rows, or leave them

The table is `django_aiogram_event` now, and `migrate` creates it empty. The old
`django_redis_aiogram_event` is left exactly where it is: nothing reads it and nothing
drops it.

If the history matters, copy it across before the first write, with the app's own
`migrate` already run:

```sql
INSERT INTO django_aiogram_event
SELECT * FROM django_redis_aiogram_event;
```

The columns are unchanged from 3.1, which is what makes the plain `SELECT *` safe. Check
that first on a copy, then drop the old table when you are satisfied — `DROP TABLE
django_redis_aiogram_event` is yours to run, and this package will never run it for you.

## Choose the transport explicitly

Nothing is detected from what happens to be installed. Name the broker you want, and the
absence of its driver is a startup complaint rather than a runtime surprise.

## Rename what used to say Redis

Four public names said where a message went. Where it goes is a setting now, so they are gone —
removed rather than aliased, because an alias that outlives the release it was written for is a
second name for the same thing for ever.

| 3.x | 4.0 | why |
| --- | --- | --- |
| `bot.send_redis(...)` | `bot.enqueue(...)` | the queue is a list, a stream, an AMQP queue or a Kafka topic, depending on `BROKER` |
| `await bot.asend_redis(...)` | `await bot.aenqueue(...)` | the same, on the awaiting half |
| `bot.redis_conn` | `from django_aiogram.redis import redis_conn` | a client that can carry any of four transports should not answer for one |
| `from django_aiogram import get_redis, redis_conn` | `from django_aiogram.redis import get_redis, redis_conn` | the package stopped exporting one transport's client from its front door |

The last two are moves rather than removals: the objects are the same, they are as lazy as they
were, and there is still one connection behind them. Only the shortcut through the package is
gone. `bot.send()` is untouched — it still delivers directly inside the worker and queues
everywhere else, and `enqueue` is what it calls when it queues.

Nothing is renamed in the settings: `REDIS_URL` and the rest still say Redis because they *are*
Redis's, and a project that runs the list transport writes exactly what it wrote before.

## Verifying the upgrade

```shell
pip show django-redis-aiogram   # nothing: the old distribution is gone, not shadowed
python manage.py check
```

`check` is where this hop fails loudest, and deliberately so: a `BROKER` whose driver is not
installed is refused here rather than left to raise `ModuleNotFoundError` inside the first
send, in a producer, in production.

Then, in a shell on a non-bot process:

```python
from django_aiogram import bot

bot.queue_depth()  # crosses the broker boundary, sends nothing
bot.enqueue(chat_id=YOUR_ID, text='upgrade check')  # `send_redis` under its 4.0 name
```

Read the depth first. It is the only step that proves the transport is configured *and*
reachable without putting a message on the queue, and it answers whether or not the process is
`ENABLED` — so it works on the web tier you keep from sending, which is where you are most
likely to be standing.

Then confirm the bot container logs `message sent`. If the depth answered and this does not,
the queue is fine and the worker is not: see **[[Troubleshooting]]**.

# From 3.0 to 3.1

## Make your handlers idempotent, on your own key

3.0 acknowledged a message when the send was *scheduled*; 3.1 acknowledges it when the
send has actually finished, for a handler that takes an `on_complete` keyword and calls
it. `bot.send_raw` does, and `manage.py start_tgbot` uses it, so a normal worker has the
guarantee the documentation always claimed. It changes what a crash does: before, a
`kill -9` mid-send lost the message silently — now it is redelivered, so a handler can
run twice.

A handler of your own that takes only `**kwargs` is still acknowledged the moment it
returns, which is the pre-3.1.0 behavior and is deliberate — see **[[Delivery]]**. If
you want the message held until your send finishes, take `on_complete`.

**Do not use `correlation_id` as the key.** A handler's own replies inherit the id of the
update that caused them, so it is one per *conversation turn*, not one per message: a
handler that sends three messages produces three rows under one id, and `SET :seen:<id>
NX` would drop two of them. Use something your own domain owns — an order number, a
notification row's primary key.

Nothing to configure. If you would rather have the old behavior for a while, there is no
flag for it: the old behavior lost messages.

## Run migrate

`0002_kind_id_index` swaps the index the event log's admin page and its pruning read:
`AddIndex` for `drai_event_kind_id` on `(kind, -id)`, then `RemoveIndex` for the old
`drai_event_kind_recent`. In that order, so the table is never without an index on
`kind`. It ships whether or not you turn the log on.

`AddIndex` issues a plain `CREATE INDEX` — no `IF NOT EXISTS`, and no adoption of one
that is already there — so on PostgreSQL, where a table big enough for the lock to matter
wants `CONCURRENTLY`, creating it by hand ahead of `migrate` makes the migration **fail**
rather than saving it work. Do the whole swap by hand and then tell Django it is done:

```sql
CREATE INDEX CONCURRENTLY drai_event_kind_id ON django_redis_aiogram_event (kind, id DESC);
DROP INDEX CONCURRENTLY drai_event_kind_recent;
```

```shell
python manage.py migrate django_redis_aiogram 0002_kind_id_index --fake
```

Neither statement can run inside a transaction, so run them outside one — `psql` does
that by default. Everywhere else, plain `migrate` is the whole procedure. The index name
is new rather than reused so that the hand-made one and the migration's own cannot be
confused for each other.

## Change the compose healthcheck

If you copied the healthcheck from **[[Deployment]]** in 3.0, replace it:

```yaml
    environment:
      DJANGO_SETTINGS_MODULE: core.settings   # the probe is its own process
    healthcheck:
      test: ['CMD', 'python', '-m', 'django_redis_aiogram.healthcheck']
```

`manage.py tgbot_healthcheck` still exists and still works, but it runs `django.setup()`
first — every `AppConfig.ready()` in your project, before it reads a single Redis key. One
measured project spent 17.9 seconds there against 0.01 seconds of probing, so Docker
killed the probe at every timeout and the container read `unhealthy` while the bot was
fine. The `python -m` form is 69 ms end to end. `DJANGO_SETTINGS_MODULE` has to be in
`environment:` because a healthcheck is a separate process and `manage.py` only sets it
inside its own.

## Raise `stop_grace_period` to 30 seconds

The shutdown arithmetic moved. Joining the consumer thread is bounded by
`REDIS_TIMEOUT + 1` — eleven seconds at the defaults, where 3.0 used
`BLPOP_TIMEOUT + 1` and gave it six, which was shorter than the worst call the consumer
makes. Add `DRAIN_TIMEOUT` (five) and the event log's flush (five) and the total is
**21 seconds**, against 16 before.

A grace period shorter than that turns a graceful stop into a kill, which is now the case
that duplicates messages rather than losing them. `30s` leaves room; raise
`DRAIN_TIMEOUT` if your sends spend long in the rate limiter, and raise the grace period
with it.

## Re-silence checks if you had to

`W010` and `W011` are gone, replaced by `I001` and `I002` with the same meanings. They
report as *information* now: an ephemeral hostname and an unrouted log alias are both
conditions a system check can see but cannot judge from where it stands, and as warnings
they failed `manage.py check --fail-level WARNING` in containers that owned no in-flight
list at all. If either id is in `SILENCED_SYSTEM_CHECKS`, update it — a silenced id that
no longer exists is dead but harmless, so nothing will tell you.

`E030` also refuses `REDIS_TIMEOUT` below **2** rather than below 1: at 1 the blocking
pop's deadline equals the socket's, and every idle pop raises instead of returning empty.

# From 2.x to 3.0

## The `telegram_bot` package name is gone

2.0 renamed the package and kept `telegram_bot` as a deprecated shim; 3.0
removes it. A project that upgrades without touching `INSTALLED_APPS` fails at
startup with `ModuleNotFoundError: No module named 'telegram_bot'`, which is the
loudest this could reasonably be.

```python
INSTALLED_APPS = ['django_redis_aiogram']
```

```python
from django_redis_aiogram import bot, conf, redis_conn
from django_redis_aiogram.client import TelegramBot
```

## Deploy the bot container before the web tier

3.0 nests a queued call under an envelope so it can carry a correlation id. The
3.0 consumer reads both shapes, so a backlog written by 2.x drains — but a **2.x
consumer handed a 3.0 payload** calls the Telegram method with `__envelope__` as
a keyword, raises, logs it and swallows it. The message is gone, with nothing
left to redeliver.

So: upgrade and restart the container running `start_tgbot` **first**, then the
web and Celery processes. A brief window where the new consumer reads old
payloads is fine; the reverse is not.

## Run migrate

The package ships one table now, and `migrate` creates it whether or not you
turn the event log on. Creating it later on a live database is the more
expensive order, so do it with the upgrade — see **[[Event-log|Event log]]**
before switching the feature on.

## Everything else that moved

`keyspace` delivery is gone: remove `'DELIVERY': 'keyspace'`, which check `E009`
now refuses rather than ignoring. The module-level string constants that
aliased enum members are gone too — import the member and interpolate `.value`.

If you are still on 1.x, do the 2.x section below first — the shim exists only
in 2.x, so 1.x to 3.0 is one jump with no compatibility layer to lean on.

# From 1.x to 2.x

If the Redis queue holds messages written by 1.x at the moment you deploy, set
`'ALLOW_PICKLE': True` for the upgrade window — unpickling queue data is code
execution, so it is refused by default. Remove the setting once the queue has
drained.

## Requirements

Python 3.10–3.14, Django 5.2+, aiogram 3.30+, redis 6.2+. Django 4.2 reached
end of life, and aiogram 3.30 needs Python 3.10.

## 1. Rename the app and the imports

```python
INSTALLED_APPS = ['django_redis_aiogram']
```

```python
from django_redis_aiogram import bot, conf, redis_conn
from django_redis_aiogram.client import TelegramBot
```

`TelegramBot` moved out of `telegram_bot.telegram_bot`, and the settings module
is `django_redis_aiogram.settings`. The package exports `bot` and `conf`, which
would otherwise shadow submodules of the same name. In 2.x the old name kept
working through a shim; in 3.0 it does not exist at all.

## 2. Drop placeholder tokens

The package no longer builds a bot or connects to Redis at import time, so a
project without credentials boots and tests normally. If you added something
like this to keep `manage.py test` working, delete it:

```python
# no longer needed
TG_BOT_KEY = os.getenv('TG_BOT_KEY') or '0:placeholder'
```

Instead, switch the bot off where it does not belong:

```python
TELEGRAM_BOT = {'ENABLED': os.getenv('RUN_BOT') == '1'}
```

or per container with `DJANGO_REDIS_AIOGRAM_ENABLED`. See **[[Deployment]]**.

## 3. Move parse_mode onto the bot

1.x had no way to reach aiogram's `DefaultBotProperties`, so projects injected
`parse_mode` into every call:

```python
# before
def default_kwargs(function):
    return {
        'send_message': {'parse_mode': 'HTML'},
        'send_photo': {'parse_mode': 'Markdown'},
    }.get(function, {})
```

```python
# after
TELEGRAM_BOT = {
    'DEFAULT_BOT_PROPERTIES': {'parse_mode': 'HTML'},
}
```

`DEFAULT_KWARGS` stays for what bot properties cannot express, such as a
default caption.

## 4. Use the public router

```python
dispatcher.include_router(bot.router)  # was bot._router
```

## 5. Prefer bot.send()

```python
bot.send(chat_id=chat_id, text=text)
```

It queues from your app and calls Telegram directly inside the bot container.
`enqueue` and `send_raw` still work.

## 6. Drain the 1.x queue, then drop the flag

If you needed `'ALLOW_PICKLE': True` for the upgrade window, the order in which
you close it again matters — a 1.x producer keeps writing pickled payloads:

1. upgrade or stop **every** producer: web, celery, anything calling
   `enqueue`
2. wait for the queue **and** every in-flight list to reach zero:
   `LLEN <REDIS_MESSAGES_KEY>` and `LLEN` on each
   `<REDIS_MESSAGES_KEY>:processing*` key — on Redis 6.2+ a message being sent
   sits in one of those, not in the queue
3. only then remove the setting

`ALLOW_PICKLE` controls reads: `True` accepts a pickled payload, `False`
refuses it. So removing it while an old producer is still running means its
messages are written and then refused on read. On Redis 6.2+ the consumer
leaves each refused message in its in-flight list and says so in the log, so
setting `ALLOW_PICKLE` back and restarting the worker delivers them; without
`LMOVE` they are gone. Either way it is the code-execution door, so close it as
soon as step 2 holds.

## 7. Re-silence checks if you had to

Ids moved from `telegram_bot.EXXX` to `django_redis_aiogram.EXXX` — and in 4.0 to
`django_aiogram.EXXX`, which is the prefix to silence if you are reading this page on the way
from 1.x to 4.0 rather than stopping at 2.0.

## Behavior that changed by itself

| | 1.x | 2.0 |
| --- | --- | --- |
| Import without credentials | breaks the project | fine |
| Delivery | keyspace expiry events | `BLPOP`, no server config needed |
| Redis database | hardcoded to 0 | taken from `REDIS_URL` |
| Queue format | pickle | JSON; pickle refused unless opted in |
| Crash mid-send | message lost | redelivered on the next start (Redis 6.2+) |
| FSM state | lost on restart | stored in Redis |
| Rate limiting | retry after refusal | paced under the published limits |
| System checks | could never fail | actually validate |
| Logging | root logger | `django_redis_aiogram`, structured fields |
| Retries exhausted | silent drop | logged, and raised if configured |

2.x let you keep the old mechanism with `'DELIVERY': 'keyspace'`. 3.0 removed
it — see the section above.

## Verifying the upgrade

```shell
python manage.py check
python manage.py test
```

Then, in a shell on a non-bot process:

```python
from django_redis_aiogram import bot  # the name at this hop; it changes again at 4.0

bot.enabled  # False where you disabled it
bot.send(chat_id=YOUR_ID, text='upgrade check')
```

and confirm the bot container logs `message sent`.
