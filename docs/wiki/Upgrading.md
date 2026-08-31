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
transport whose driver is not installed is refused by a system check at startup rather than by
an `ImportError` on the first send — in the processes that send. A process with `ENABLED=0` is
not asked for a driver, so a disabled one still reaches `E047` only if you enable it; if it
reads a queue depth it needs the driver regardless, because those reads are not gated.

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

### One import inside the event log moved

Only for a project that builds an `Event` itself — a custom recording seam, a test that
asserts on one, a metrics receiver that names the type:

```python
from django_aiogram.eventlog.records import Event, as_identifier
```

They were in `django_aiogram.eventlog.recorder`, which is now the queue and the writer
thread and nothing else. The shapes that cross that queue travel further than it does —
every seam builds one, the writer reads one, receivers are handed one — so they live beside
each other in their own module. Same objects, same fields; `recorder`, `EventRecorder` and
`events_recorded` did not move.

### Two paths you wrote down yourself

Everything above is inside your own imports, where a rename fails loudly the moment it is wrong.
These two live in Django's settings and in your `urls.py`, where nothing imports them until
something needs them:

| where | 3.x | 4.0 |
| --- | --- | --- |
| `DATABASE_ROUTERS` | `django_redis_aiogram.dbrouter.TelegramEventLogRouter` | `django_aiogram.eventlog.dbrouter.TelegramEventLogRouter` |
| `urls.py` | `django_redis_aiogram.webhook.telegram_webhook` | `django_aiogram.consumer.webhook.telegram_webhook` |

**`manage.py check` catches the first one.** `E048` reports any `django_redis_aiogram.` entry in
`DATABASE_ROUTERS`: for the router in the table above it names the replacement, and for any other
path from that distribution it says the distribution is gone — which is all it can honestly say
about a path this package never shipped. Without the check the router is imported on the first
query that needs routing, which is a request rather than a deployment step, and the failure is
Django's own `ImportError` naming a module rather than the fix.

**It cannot catch the second.** Nothing in this package can read your `urls.py` — the URL
configuration is Django's, resolved at request time, and a check that tried to import it would be
importing your whole project. So that row is this page's job, and a stale path there answers 404
on the webhook while the bot looks healthy: the process is up, the consumer is sending, and
Telegram's delivery attempts go nowhere. If you serve a webhook, change both.

## Move the event log's rows, or leave them

The table is `django_aiogram_event` now, and `migrate` creates it empty. The old
`django_redis_aiogram_event` is left exactly where it is: nothing reads it and nothing
drops it.

`I003` says so on every `manage.py check` while that table is there, and names the command that
copies its rows across.

**The two tables' indexes are named apart**, which is what lets the old one stay: on PostgreSQL
and SQLite index names are unique per schema rather than per table, so this release names its own
`dja_event_*` where 3.x used `drai_event_*`. (MySQL scopes them per table and would not have
minded either way.) Nothing to do about it — it is why `migrate` succeeds with both tables
present — but a dashboard or a hand-built index that names one of the old four is describing the
old table, and will keep describing it for as long as you keep it. It copies: the old table is left exactly as it is, including any row whose
id was already taken. With the app's own `migrate` already run:

```shell
python manage.py tgbot_move_events
```

It copies by primary-key range in bounded chunks — `--chunk`, `--sleep`, `--max-chunks`,
`--dry-run`, and `--database` for a log on its own alias — so a table sized by traffic takes as
many nights as it takes rather than one long statement. Both tables share the primary key, so an id
already in the new table is never inserted again: a run you stop resumes at the first id the
destination does not have, and a second run copies nothing. It is safe to run after the bot has
been writing for a while — and if one of its rows holds an id an old row also has, that old row is
left where it is and reported rather than overwritten.

Doing it by hand is still a reasonable choice on a small table **that nothing has written to yet**.
Both tables carry the same primary key, so a destination holding even one row the bot wrote after
`migrate` makes this `INSERT` fail on a duplicate key — and the ids it collides with are exactly the
history you are trying to keep. Past that point the command is the path: it copies the ids the
destination does not have and reports the ones it could not take.

On an empty destination it takes two statements rather than one:

```sql
INSERT INTO django_aiogram_event (id, created_at, correlation_id, kind, function, chat_id, user_id,
    message_id, update_id, worker, attempt, duration_ms, error_code, error, detail, short_id)
SELECT id, created_at, correlation_id, kind, function, chat_id, user_id,
    message_id, update_id, worker, attempt, duration_ms, error_code, error, detail, ''
FROM django_redis_aiogram_event;

-- PostgreSQL only, and not optional: explicit ids do not advance the sequence
SELECT setval(pg_get_serial_sequence('django_aiogram_event', 'id'),
              coalesce(max(id), 1), max(id) IS NOT null)
FROM django_aiogram_event;
```

`short_id` is 4.0's and the old table does not have it, so the copy writes an empty one: those rows
show as `(not backfilled)` in the admin until `manage.py tgbot_backfill_short_ids` fills them. It
cannot simply be left out of the list — `migrate` adds the column with a default and then drops the
default, so an `INSERT` that omits it is refused.

Name every column rather than writing `SELECT *`, which agrees with itself until either table
changes: a mismatched count is rejected, and a matching count in a different order is accepted with
every value one column to the side. And without the `setval`, PostgreSQL accepts the copy and
refuses the *bot's* next write with a duplicate key: the sequence is still where `migrate` left it.

The three-argument form is deliberate, and it is the one Django's own `sequence_reset_sql` emits —
which is what the command runs, so the two say the same thing. On a destination that ended up empty
the two-argument version is handed `NULL`, and `setval` is strict: measured on PostgreSQL 17.11 it
returns `NULL` and moves nothing, leaving the sequence wherever it already was. `coalesce` supplies
the 1 instead, and the third argument says that 1 has not been handed out yet, so an empty table is
left about to issue 1 rather than silently keeping an older position.

Then drop the old table when you are satisfied — `DROP TABLE django_redis_aiogram_event` is yours
to run, and this package will never run it for you.

## Fill in the short id, or leave it

`migrate` adds one column to the event log: `short_id`, twelve characters of the correlation id in
an alphabet a person can read out. It is what the admin's thread column shows and what its search
box takes — the column used to show the correlation id's first eight characters, which are a clock
rather than an identifier and which the search refused when they were typed back.

Rows written from now on carry their own. Rows already in the table have none until the backfill
walks them, in bounded chunks, with the arguments the other jobs take:

```shell
python manage.py tgbot_backfill_short_ids --dry-run
python manage.py tgbot_backfill_short_ids
```

Leaving it unrun is a legitimate choice and nothing breaks: those rows read `(not backfilled)` in
the admin and still link to the rest of their thread. A run you stop resumes — a row with no code is
a row still to do — and running it again after it finishes writes nothing.

Rows moved from 3.x arrive with no code, whether the command copied them or you did, so run this
after the move rather than before it.

**`0003_short_id` builds an index**, and on a table sized by traffic that build holds a lock for
its duration. The column itself is cheap — PostgreSQL adds it without rewriting the table — so it
is the index that decides whether this migration wants a window. There is no
`CREATE INDEX CONCURRENTLY` recipe here on purpose: Django generates that index's name, and a
hand-built one has to match it exactly. `python manage.py sqlmigrate django_aiogram 0003` prints
the statements this release will run, name included, which is the honest source for anyone
building it by hand.

## Choose the transport explicitly

Nothing is detected from what happens to be installed. Name the broker you want, and the
absence of its driver is a startup complaint rather than a runtime surprise.

A 3.x project that changes nothing here keeps the transport it had: `BROKER` defaults to the
Redis list, and `REDIS_URL`, `REDIS_MESSAGES_KEY`, `REDIS_TIMEOUT` and `BLPOP_TIMEOUT` mean
exactly what they meant. **[[Settings]]** lists what each transport declares, and there is a page
each — **[[Redis-list|Redis list]]**, **[[Redis-Streams|Redis Streams]]**, **[[RabbitMQ]]**,
**[[Kafka]]** — for what one guarantees and what it needs running.

### Switching transport is a drain, not a setting

Nothing moves messages from one transport to another, and nothing can: a queued message is
addressed to the queue it is in. So the switch has an order, and getting it wrong loses whatever
was in flight.

1. Stop the producers, or set `ENABLED=0` in them. They are what keeps the old queue filling.
2. Let the bot container finish the old queue, and **stop the workers before you believe the
   numbers**. `bot.queue_depth()` answers for the whole queue, but `bot.inflight_depth()`
   answers for the process that asks it: zero from a shell proves that *shell* holds nothing.
   On the Redis list every worker has its own list, so with more than one worker running there
   is no single reading that means "nothing is in flight anywhere".

   So: stop every worker first, then check. `manage.py tgbot_healthcheck --stranded` reports
   what sits under each worker name on the Redis list, and `bot.inflight_depth('<name>')` asks
   about one of them by name — a question only the two Redis transports can answer, since the
   other two raise `WorkerDepthUnavailableError` rather than a misleading zero. On RabbitMQ and
   Kafka nothing is left behind by a stopped worker —
   the broker requeues, the group replays — so stopping them *is* the drain, and what returns to
   the old queue has to be finished by a worker still on the old transport.

3. **Recover anything stranded, before the next step makes it unreachable.** A count from
   `--stranded` identifies messages; it does not move them. `manage.py tgbot_reclaim --worker
   <name>` puts them back at the front of the queue, and it needs a worker still configured for
   the **old** transport to take them — so run it, start one worker, and let the queue and its
   in-flight list reach zero before going on. Once step 4 has moved every process, that list is
   a key nothing reads: the messages are still in Redis and nothing this package runs will look
   at them again.
4. Change `BROKER` and the settings the new transport declares, in **every** process. A producer
   left on the old transport writes to a queue nothing reads any more, and it will not complain:
   both configurations are valid, they are simply not the same queue.
5. Start the bot container first, then the producers. The reverse order queues messages nobody
   is reading yet, which is harmless but indistinguishable from a broken deployment while you
   watch it.

Step 4 is the one worth checking twice. `manage.py check` refuses a `BROKER` whose driver is
missing and a required setting that is empty, so a half-configured process usually fails at
startup rather than silently — but a process still naming the *old* transport is a correct
configuration, and nothing can tell it apart from one that meant it.

### `DELIVERY` takes a dotted path

`'blpop'` is `'django_aiogram.consumer.delivery.BlpopDelivery'` now — or drop the key, which is
the same thing, since that is the default. The old word was the name of a Redis command that three
of the four transports never issue, and the setting had exactly one legal value; it selects a class
now, so a consumer you write goes there. `E009` refuses `'blpop'` by name and says what to write,
rather than reporting that a Redis command is not a dotted path.

`DeliveryKind` is gone with it: an enum of one member is not a choice. If you imported it to spell
the value, write the path.

**[[Delivery]]** has what a subclass must do, and the six rules that are each a defect this
package has already had.

### The shutdown budget follows it too

`stop_grace_period` is computed from a table on **[[Deployment]]**, and its first row used to be
`REDIS_TIMEOUT + 1` for every deployment. It is the timeout of the transport you chose now —
`RABBITMQ_TIMEOUT` or `KAFKA_TIMEOUT` where those apply — because that is the bound the consumer
thread actually sits inside, and a join shorter than it lets a consumer outlive the shutdown that
joined it.

Nothing to change if you keep the defaults: all four default their own timeout to ten, so the
total is the 21 seconds it was. It matters the moment you raise one, and the setting to raise
alongside it is the grace period, not `REDIS_TIMEOUT`.

One consequence for RabbitMQ, and the only thing here that changes without you touching a setting:
`RABBITMQ_TIMEOUT` now wins over a `blocked_connection_timeout` written into `RABBITMQ_URL`, where
the URL used to win. Both of the numbers above are computed from the setting, so a URL overriding it
left the take cap and the join describing a deadline no call carried — measured, a URL saying 60
against a setting of 2 left the join at 3 seconds while a blocked publish could sit for a minute. If
you set it in the URL, move the number to `RABBITMQ_TIMEOUT`; every other pika parameter in the URL
is still yours, `socket_timeout` included, and that one does not bound a take.

## Rename what used to say Redis

Five public names said Redis, in the four entries below, for two different reasons — and the
table separates them.

**Two were routing names.** `send_redis` and `asend_redis` said where a message went, and where
it goes is a setting now — so they are gone, renamed rather than aliased, because an alias that
outlives the release it was written for is a second name for the same thing for ever.

**Three were client-access names** — `bot.redis_conn`, and the package's `get_redis` and
`redis_conn`, which share a row below because they move in one import. Those *moved* rather than
went: one transport's client is
not the package's business to export from its front door, but the object is unchanged and still
importable from the module that owns it.

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

bot.enabled  # must be True for the send below to do anything
bot.queue_depth()  # crosses the broker boundary, sends nothing
bot.enqueue(chat_id=YOUR_ID, text='upgrade check')  # `enqueue` replaces `send_redis` in 4.0
```

Read the depth first. It is the only step that proves the transport is configured *and*
reachable without putting a message on the queue, and it reaches the broker regardless of
`ENABLED` — so it verifies the broker on the web tier you keep from sending, which is where you
are most likely to be standing.

The send is the opposite: `ENABLED=0` makes `enqueue` a no-op that still returns an id, so on a
disabled process it proves nothing and the `message sent` line below never comes. Run it
somewhere the flag is on, or set it for the length of the check.

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
`send_redis` and `send_raw` still work — `send_redis` is the name at this hop; the 4.0 section
above lists what it is called now.

## 6. Drain the 1.x queue, then drop the flag

If you needed `'ALLOW_PICKLE': True` for the upgrade window, the order in which
you close it again matters — a 1.x producer keeps writing pickled payloads:

1. upgrade or stop **every** producer: web, celery, anything calling
   `send_redis`
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
