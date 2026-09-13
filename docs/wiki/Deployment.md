# Deployment

One container runs the bot. Every other process just queues messages.

```yaml
# docker-compose.yml
services:
  back:
    image: ${IMAGE}
    command: gunicorn core.wsgi:application -b 0:8000
    env_file: .env

  celery_worker:
    image: ${IMAGE}
    command: celery -A core worker -l info
    env_file: .env

  telegram_bot:
    image: ${IMAGE}
    command: python manage.py start_tgbot
    restart: always
    # the Redis list's requirement, and only its: that transport keys the in-flight
    # list on this name, and without `hostname:` Docker invents a new one for each
    # container it creates — see Redis-list. One name is one worker: scale this
    # service and every replica resolves to this hostname, shares one in-flight list
    # and reclaims what the others are still sending. Give each replica its own
    # WORKER_NAME to run more than one. On the other three transports this line is
    # optional and nothing strands without it
    #
    # `deploy.replicas` on THIS service is a data-loss bug on the Redis list: every
    # replica resolves to this hostname, shares one in-flight list, and reclaims what
    # the others are still sending — the same message goes to a real person twice. To
    # run more than one worker, declare a service per worker with its own name, or
    # move to a transport that needs no identity
    hostname: telegram-bot-1
    env_file: .env
    environment:
      DJANGO_AIOGRAM_ENABLED: 1
    depends_on: [redis]

  redis:
    image: redis:7-alpine
    restart: always
```

That is the default transport. The shape does not change for the other three — one bot
container, everything else queueing — only the service it depends on and the settings that
name it.

**Redis Streams** needs no new service: the same server, a different data structure — provided
that server is **7.0 or newer**, which the list does not require. Below it the transport refuses
on first use rather than reporting a queue depth it cannot compute; see **[Redis Streams](Redis-Streams.md)**.

```yaml
    environment:
      DJANGO_AIOGRAM_BROKER: django_aiogram.broker.redis_streams.RedisStreamsBroker
      DJANGO_AIOGRAM_REDIS_STREAM_KEY: telegram-bot
```

**RabbitMQ.** `hostname:` becomes optional — the broker requeues what a dropped channel held,
so nothing is keyed on a name.

```yaml
  telegram_bot:
    # …as above, and
    # `service_healthy`, not the bare list: `depends_on` alone waits for the container
    # to start, and RabbitMQ answers connections a while before it will accept a publish
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      DJANGO_AIOGRAM_ENABLED: 1
      DJANGO_AIOGRAM_BROKER: django_aiogram.broker.rabbitmq.RabbitMQBroker
      # the same password as below, percent-encoded: `pika` parses this with
      # `URLParameters`, so an `@`, `/`, `:` or `#` in a generated password splits the
      # URL somewhere nobody meant and the failure looks like a wrong credential
      DJANGO_AIOGRAM_RABBITMQ_URL: amqp://bot:${RABBITMQ_PASSWORD_URLENCODED}@rabbitmq:5672/
      DJANGO_AIOGRAM_RABBITMQ_QUEUE: telegram-bot

  rabbitmq:
    image: rabbitmq:4
    restart: always
    # a user that is not `guest`: the default account is refused from anywhere but
    # localhost, and a queue anything untrusted can write to is a queue that chooses
    # which Telegram call the bot makes — see SECURITY.md
    environment:
      RABBITMQ_DEFAULT_USER: bot
      RABBITMQ_DEFAULT_PASS: ${RABBITMQ_PASSWORD}
    healthcheck:
      test: ['CMD', 'rabbitmq-diagnostics', '-q', 'ping']
      interval: 10s
```

Two variables, one secret: the broker wants the password as it is, the URL wants it
percent-encoded. Derive the second rather than typing it twice —

Both go in `.env` beside the compose file, because that is where Compose reads interpolation
values from — a shell variable is not visible to it, and the substitution would be an empty
password whose failure looks like a wrong credential.

**Single-quote the raw value there.** Compose interpolates `.env` values, so a password
containing `$` becomes a different password before RabbitMQ ever sees it, while the encoded copy
still stands for the original — an authentication failure with both halves looking correct.
Measured on Compose v5.3.1:

| in `.env` | what the container gets |
| --- | --- |
| `RABBITMQ_PASSWORD='p$X-s'` | `p$X-s` — preserved |
| `RABBITMQ_PASSWORD="p$X-s"` | `pzz-s` — `$X` expanded |
| `RABBITMQ_PASSWORD=p$X-s` | `pzz-s` — the same |

A password containing a **single quote** cannot go in `.env` at all: Compose refuses the file
with `unexpected character "'" in variable name`, and there is no escape for it. Generate one
without, rather than looking for the quoting that works.

Encode the value by handing it to this and pasting the result:

```shell
python3 -c 'import getpass, urllib.parse
print(urllib.parse.quote(getpass.getpass("password: "), safe=""))'
```

It **prompts** rather than taking the password from anywhere. Five earlier versions of this
recipe were cleverer and each was wrong differently: a shell variable Compose cannot see,
`os.environ` the script does not inherit, a `.env` parser that URL-encoded the quotes Compose
would have stripped, and then an argument — which put the secret in shell history and in `ps`.
Prompting has no argv, no history, no file to parse and no quoting rules to agree with, and it
encodes exactly the characters you type.

— because two hand-written values drift, and the drift shows up as an authentication failure
that points at the credential rather than at the encoding. `env_file: .env` is a different
mechanism and does not help here: it hands variables to the *container*, while `${...}` in the
compose file is substituted before that, from Compose's own environment and `.env`.

**Kafka.** Read **[Kafka](Kafka.md)** before this one rather than after: ordering is per partition and a
refusal replays a run of messages, and neither is something to discover in production.

```yaml
  telegram_bot:
    # …as above, and
    # no `service_healthy` here: the image ships no healthcheck, so this waits only for
    # the container to start. A publish before the broker is ready raises after
    # `KAFKA_TIMEOUT` rather than blocking — which matters for the *producers*, since the
    # bot container consumes. `restart: always` covers the bot; a web tier that queues on
    # boot wants a retry of its own
    depends_on: [kafka]
    environment:
      DJANGO_AIOGRAM_ENABLED: 1
      DJANGO_AIOGRAM_BROKER: django_aiogram.broker.kafka.KafkaBroker
      DJANGO_AIOGRAM_KAFKA_BOOTSTRAP: kafka:9092
      DJANGO_AIOGRAM_KAFKA_TOPIC: telegram-bot

  kafka:
    image: apache/kafka:4.0.0
    restart: always
    # the advertised listener is the whole configuration: with the image's default the
    # broker answers `localhost:9092`, which is itself from inside the container, and a
    # client elsewhere retries into a refusal loop rather than failing
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9094
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9094
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
```

A single-broker Kafka with replication factor 1 is a development recipe. It is here because the
listener configuration is the part everybody loses an afternoon to, not because one broker is a
production answer.

## Upgrading to 3.0: order matters, once

**Run `manage.py migrate` first.** The package shipped one table from 3.0 — a second, the
schedule table that an `eta` writes to, arrived in 4.1 — and the event log's is created whether
or not you turn the log on.

**Then deploy the bot container before the web tier.** 3.0 nests a queued call
inside an envelope. The new consumer reads the old flat shape, so a backlog
drains across the upgrade — the reverse does not hold: a 2.x consumer handed a
new payload calls the Telegram method with `__envelope__` as a keyword, raises,
logs it and swallows it, and the message is gone with nothing to redeliver.

Both are one-time concerns. After 3.0 the order is whatever you like **while the deployment
has one bot**, and 5.0 keeps it that way: a queued message names the bot it is for, and that
arrived as a field the reader ignores where it does not know it rather than as a new envelope
version, so nothing is lost in either direction.

**A second bot changes that.** A 4.1 consumer cannot read the field, so it delivers every
message through the one bot it has — including one queued for the other, which then goes out
under the wrong token, to a chat that bot may not be in. Nothing is dropped and nothing
raises; it is simply the wrong bot. So: upgrade the consumers to 5.0 first, and start queueing
for a second bot only once they are all there.

Note the absence of `ports:` on `redis`. Nothing outside the compose network
reaches it, which is why no password appears here. Publish that port and Redis
needs `requirepass` and a `REDIS_URL` carrying the credentials — the queue is a
list of Telegram API calls, and whoever can write to it can send as your bot.

Note what is **not** set: `back` and `celery_worker` leave `ENABLED` alone.
They queue messages, and `ENABLED=0` would make those calls no-ops — the
messages would vanish with a debug line and nothing else. The flag is for
processes that must not **send** — not to Telegram, not into the broker: image builds, a
migration container, CI. Not "reach nothing at all": the depth reads answer either way, on
purpose. See below.

## The jobs nothing runs for you

Three commands do work no request path does, so a deployment that never schedules them is a
deployment where that work never happens:

| command | what waits on it |
| --- | --- |
| `manage.py tgbot_prune_events` | the event log's size. `W006` warns while `EVENT_LOG_RETENTION_DAYS` is unset |
| `manage.py tgbot_dispatch_scheduled` | every send made with an `eta`. Without it a scheduled message waits for ever |
| `manage.py tgbot_intents` | every check and webhook change asked for in the admin. Without it those actions are recorded and never carried out. `--watch` is the always-on form; a person is waiting, so seconds is the useful interval |

**The commands that are deliberately not on that list**, each for its own reason.

`manage.py tgbot_replay` is run by a person, after an incident, with `--dry-run` first — see
**[Troubleshooting](Troubleshooting.md#telegram-was-down-what-did-we-lose-and-can-it-be-sent-again)**.
Scheduling it would mean re-sending failures nobody has looked at.

`manage.py tgbot_rewrap_tokens` is run when the storage the tokens are kept in changes: after turning an encrypting `TOKEN_STORAGE` on, which leaves the rows written
before it in plain, and after rotating its key. Not on a schedule — it has nothing to do until
one of those two things happens — see **[Tokens](Tokens.md)**.

`manage.py tgbot_prune_queues` belongs on neither list, because that depends on your
`REMOVED_QUEUE_POLICY`. A queue per client is what keeps one client's backlog off
another's, and it is also how a deployment leaks: a client goes, their bot's row goes, and a
Redis key, an AMQP queue or a consumer group stays for ever. The command finds the queues no
bot points at and does what the policy says:

| policy | what happens |
| --- | --- |
| `park` (default) | the queue is reported and nothing is removed. An operator decides, which is right where a client may come back and the messages may still be worth reading |
| `hold` | the queue is removed once it is empty, and reported while it is not |
| `drop` | the queue is removed, with whatever is still in it |

Under `park` it is safe to schedule and tells you what is accumulating; under `drop` it
deletes, so run it by hand or schedule it knowing that. `--dry-run` says what would happen,
`--queue` bounds a run to the queues you name — and a name is refused rather than skipped
where a bot still points at it *or* where no row has it at all: filtered out, a typo would
look like a cleanup that completed while the queue you meant is still there. A bot that is merely **switched off still counts**: a client paused for a
month has not given up their backlog.

Under `hold` the transport decides emptiness in **one step** — the read and the delete
together — because a producer can publish between a depth read and a delete, and a caller
that checked for itself would delete the message it had just been told about. A message a
consumer has *taken and not settled* counts as held: somebody is still sending it.

Two transports say they cannot prove that, and say so rather than guessing:

- **Kafka** cannot remove a queue at all. Deleting a topic is an administrative act against
  the cluster, not something a producer may take, so the line names the topic for whoever owns
  it — under every policy, and without reaching the cluster to find out.
- **RabbitMQ** refuses `hold` only. AMQP's own `if_empty` counts *ready* messages, so a queue
  whose one message is an unacknowledged delivery in another container reads as empty and
  would be deleted with the message; nothing here can see that delivery. Use `drop` when you
  know what is sending.

From cron, or as a container of its own:

```yaml
  scheduler:
    image: your-image
    command: python manage.py tgbot_dispatch_scheduled --loop --interval 5
    # no token and no bot here: a due row already holds the bytes the queue wants, so this
    # container needs the database and the broker and nothing else
    environment:
      DATABASE_URL: ${DATABASE_URL}
      REDIS_URL: ${REDIS_URL}
```

Several movers are safe **while a claim is live**: each row is claimed by a compare-and-set
update, so two racing for one produce a winner and a loser. Delivery here is at-least-once,
like every transport this package carries — a claim is a lease, and a publish that outlives
its own lease can be joined by a mover taking the row back, which sends the message twice.
Nothing fences a call already in flight to another system, so **keep `--lease` comfortably
above the deadline the transport puts on one call** (`REDIS_TIMEOUT`, `RABBITMQ_TIMEOUT`,
`KAFKA_TIMEOUT`); the mover warns when it is not, and the warning is a warning rather than a
guard.

`--grace` keeps a mover that was down for a day from delivering a day of stale messages at
once, `--max-attempts` gives up on a row the broker keeps refusing, `--limit` bounds one pass,
and `--dry-run` says what is waiting without claiming anything.

Under `--loop`, a pass that filled `--limit` goes straight round again rather than sleeping,
because a full batch means there is a backlog behind it. Full is counted in rows *claimed*,
not rows published — a batch that was all dropped past its grace, or all refused by the
broker, is still a full batch, and a backlog of those would otherwise clear at one `--limit`
per `--interval`.

## What ENABLED=0 turns off

- no router autodiscovery, so those modules are never imported
- no system checks registered — **unless the event log is on**, which is enough on
  its own to register all of them, bot settings included
- every send becomes a no-op that builds neither a bot nor a connection:
  `send`, `enqueue`, `send_raw`, `send_many` and the `await` forms `asend`,
  `aenqueue`, `asend_many`. Each still returns what it would have returned — the
  correlation id for one message, and one id per chat from `send_many` and
  `asend_many` — so a caller storing ids beside its own rows behaves the same here
- `start_tgbot` reports why and exits

The queue readers are the exception, and worth knowing before a monitor calls one:
`queue_depth()` and `inflight_depth()` are **not** no-ops here. They are reads rather
than sends, so a disabled process still needs whatever its transport connects with —
`REDIS_URL` on the two Redis brokers, `RABBITMQ_URL`, `KAFKA_BOOTSTRAP` — and the
driver behind it. Without the setting they raise `ImproperlyConfigured`; without the
driver, `BrokerDependencyError`. `manage.py check` asks a disabled process for neither.

`inflight_depth()` has a second limit on two of the four: on RabbitMQ and Kafka the
count is **process-local**, so anywhere else it answers zero — correctly, and uselessly.
The reasons differ. RabbitMQ does track unacknowledged deliveries, but per *channel*, and a
client sees its own; asking about another's means the management HTTP API, which is a second
way of talking to the broker for a number the contract defines as this worker's. Kafka has
nothing to ask at all: an offset is either committed or not, and "taken but not settled"
exists only in the process holding it.

Neither maps that work to a **name** this package chose, so passing a name there raises
`WorkerDepthUnavailableError` — the caller's own included, since there is nothing for a name to
match. The unnamed call is the one that answers. See **[Delivery](Delivery.md)** for which
transport keeps what.

So a disabled process needs no token, and needs its broker reachable only if something
asks it for a depth.

`ENABLED` is parsed rather than tested for truthiness — `'false'`, `'no'`,
`'off'` and `0` all disable the bot, and an unparseable value raises rather
than being read as enabled.

## The restart: always trap

A clean exit still counts as a crash under `restart: always`, so a disabled
`start_tgbot` would restart forever. Either keep the container out of the
default set:

```yaml
  telegram_bot:
    profiles: [bot]
```

or park it:

```yaml
    command: python manage.py start_tgbot --idle
```

`--idle` blocks until a signal instead of returning.

## Health and shutdown

`SIGTERM` unwinds cleanly: polling stops, the consumer thread is joined, then
`close()` drains the sends still in flight and shuts the aiogram session, the FSM
storage, the loop and the transport — on Kafka that last one flushes the producer and leaves the
consumer group, which is what keeps a restart from waiting out the session timeout before
anything is delivered again. Last, the messages that drain delivered are acknowledged. The
acknowledgement comes last because the drain is what finishes those sends, and the loop
that would otherwise have acknowledged them stopped at the join — without this step a
graceful stop would leave them to be sent again. Give the container enough grace period
to finish an in-flight send:

```yaml
    stop_grace_period: 30s
```

The grace period has to cover the waits shutdown makes, in order. For the bot
container, which is what this table is about:

| wait | bounded by | default |
| --- | --- | --- |
| joining the consumer thread | the transport's own deadline + 1 — `REDIS_TIMEOUT`, `RABBITMQ_TIMEOUT` or `KAFKA_TIMEOUT` | 11s |
| draining in-flight sends | `DRAIN_TIMEOUT` | 5s |
| flushing the event log | `recorder.STOP_TIMEOUT` | 5s |

So 21 seconds at the defaults, and `30s` leaves room.

A process that serves the **webhook** spends more inside `close()` alone, because it
has updates and a loop thread of its own to let go of: up to `DRAIN_TIMEOUT` waiting on
updates in flight, then up to five seconds joining the loop thread, then
`DRAIN_TIMEOUT` again draining sends — 15 seconds at the defaults rather than 5. If your
web tier calls `bot.close()` on shutdown, size its grace period on that. Raise `DRAIN_TIMEOUT` if
your sends spend long in the rate limiter — before 3.1.0 it was hardcoded at five
seconds and no grace period could buy more. Watch the other direction too:
raising the transport's timeout raises the join, and a grace period shorter than the sum
means Docker sends `SIGKILL` partway through, which is exactly the crash the
in-flight list exists to survive.

## Serving under ASGI

Nothing here is required. A Django process under ASGI can call `bot.send()` and
it works — it simply writes to a socket on the thread serving requests, and on the
first call that includes a connect bounded by the configured transport's own timeout.
`bot.asend()` is the same message without blocking that thread: the connect and its
timeout still happen, it just yields while they do. See
**[Sending messages](Sending-messages.md)**.

One thing is worth knowing rather than discovering. The async client belongs to
the loop that created it, so each loop gets its own, and only that loop may close
it. If your server has a lifespan hook, close it there:

```python
from django_aiogram import bot


# an ASGI lifespan shutdown, or django-ninja's
async def shutdown():
    await bot.aclose()
```

That closes the async client for the loop calling it, and nothing else — the
worker's `close()` is a different thing and belongs in the bot container.

A server with one loop for its whole life will not miss it: the connection is
closed when the process exits either way, perhaps with a `ResourceWarning`. It
matters where a process runs **many** loops — `asyncio.run` once per job in a
Celery task, a management command, a script. There each loop takes its own client,
and only closing it releases the connection while the loop that owns it still
exists. Nothing accumulates if you skip it — the registry drops clients whose loop
has closed — but the sockets stay open until then, and the close is untidy rather
than clean.

## hiredis, if the consumer is busy

```shell
pip install 'django-aiogram[hiredis]'
```

Only on the two Redis transports; the other drivers do their own parsing and this extra does
nothing for them.

redis-py parses replies in Python unless `hiredis` is present, and then in C. Nothing
in this package needs it and nothing changes if it is absent — it is an extra rather
than a dependency because the shape of the win is narrow: it pays on a consumer
reading a message at a time off a queue all day, and buys a web tier that only ever
pushes almost nothing. Install it in the bot container if you have measured the
parsing and not before.

## What is this deployment actually serving?

`manage.py check` reads the settings, and a client connected through your own interface is a
**row** — so for a deployment whose bots arrive at run time, the checks cannot see them at all.
Two commands can:

```shell
python manage.py tgbot_bots
python manage.py tgbot_queues
```

`tgbot_bots` lists every bot the providers resolve with the things that decide what it does:
where it came from (a settings section or a row), its **profile digest**, the queue it
publishes to, its mode, whether it is switched on, and which container holds its lease. The
digest is the answer to the question grouping raises — twenty bots configured alike should
show **one** digest between them, and a deployment that quietly built twenty groups is paying
for twenty transports. `--bot` narrows it, `--all` includes the bots that are switched off,
`--json` is the form a script reads. It prints the digest rather than the settings behind it,
because those hold `REDIS_URL` and its password.

Four commands take the bots or the queue they are meant for: `--bot` on `tgbot_replay`,
`tgbot_prune_events` and `tgbot_dispatch_scheduled`, `--queue` on `tgbot_reclaim` (and
`tgbot_webhook` has had `--bot` since the webhook work). Each defaults to what a single-bot
deployment already had, and a name `tgbot_reclaim` cannot find among the declared queues is
refused rather than read as an empty in-flight list — which is exactly what a drained queue
looks like.

**`tgbot_move_events` and `tgbot_backfill_short_ids` take neither**, and that is not an
omission: one copies a 4.x feed into the 5.x table and the other fills a column in it. Both
are one-time migrations over the whole table, addressed by `--database` rather than by bot,
and a half-migrated feed is worse than an unmigrated one.

`tgbot_queues` lists the declared queues with their pool, their depth, what is in flight and
how long ago something said it was consuming them — and names the queues that hold messages
with **nobody reading them**, which is the failure that otherwise looks like a slow bot. It
asks the transport, so it reaches the network; `--no-depth` is the form that does not, and a
queue whose transport cannot be reached reads `?` rather than `0`, because an unreachable
broker is not an empty queue. `--queue` and `--pool` narrow it.

## Is it working?

`docker ps` answers the wrong question: the process being up says nothing about
the consumer thread, which can be dead while polling continues.

```shell
python manage.py tgbot_healthcheck
```

Exit 0 and a line on stdout when healthy, non-zero with the reason on stderr
otherwise. It checks two things, and asks the transport both: the consumer reported
in recently, and the queue is not piling up. A warning — a stranded in-flight list is
the one it has — goes to stderr *without* changing the verdict, so a healthy probe
can write to both streams and still exit 0.

### Several queues in one container

```shell
python manage.py tgbot_healthcheck --queue client-a --queue client-b

# the module form calls no django.setup(), so it needs the settings module in its environment
DJANGO_SETTINGS_MODULE=myproject.settings python -m django_aiogram.healthcheck \
    --queue client-a --queue client-b
```

Each named queue is probed through a transport built for *it*, one line each, and the worst
answer decides — a container serving five clients is healthy or not per client, and a report
that summed the depths would hide the one queue filling up behind four empty ones.

The verdict for a named queue is not the one for this container's own, deliberately:

| what the probe finds | what it says |
| --- | --- |
| more waiting than `--max-queue` allows | **fails**, whether or not anything is consuming it — that limit is read first. `0` turns the check off rather than allowing nothing, which is also what `HEALTHCHECK_MAX_QUEUE` means |
| messages waiting and no live consumer | **fails** — they are going nowhere and nobody is coming |
| empty and no live consumer | **warns** — a queue declared for a client who has not written yet is waiting, not broken |
| a live consumer, within the limit | healthy, with the depth and how old the consumer's last word is |

This is the question a multi-queue deployment has no other way to ask — and *who* answers it
is the transport's business, not this package's. A Redis list has nothing that knows a
consumer exists, so the consumer writes a heartbeat key with a TTL; a Redis stream's consumer
group already records when each member last spoke, so nothing is written and nothing expires;
RabbitMQ and Kafka report that liveness is not observable from outside at all, and a named
queue on those is judged by its depth alone. `manage.py tgbot_queues` reads the same answer for
a person rather than for a container, and its **consumer** column says `tracked` for exactly
that case.

**The command form also names the quarantined bots**, with the reason a supervisor wrote — a
probe that says *healthy* while three clients' bots are quarantined is answering a narrower
question than the person reading it asked. It does not change the verdict: a revoked token is
fixed by a person, not by a restart. The module form says nothing about them on purpose, and
that is the next paragraph.

It opens no client of its own to do it, which is why it runs on all four transports:
the driver is an extra, and a probe that imported redis-py could not start on an
image built for Kafka or RabbitMQ.

### What it can see, per transport

The verdict is the same shape everywhere. What is *observable* is not, because
liveness is the transport's answer and only two transports write one down.

| Transport | The consumer | The depth | Stranded in-flight lists |
| --- | --- | --- | --- |
| Redis list | the heartbeat key its consumer writes, per worker | `LLEN` on the queue | yes — a `SCAN` over `<REDIS_MESSAGES_KEY>:processing:*` |
| Redis Streams | `XINFO CONSUMERS`: how long ago any member of the group last spoke, which a blocking read that finds nothing refreshes | entries not yet acknowledged by the group | no — the pending list belongs to the *group*, so any name can reclaim it and there is nothing stranded to find |
| RabbitMQ | not observable from outside: the broker tracks its own consumers, and it says so instead of guessing | messages ready in the queue | no — unacknowledged deliveries belong to a channel, and the broker returns them itself when it drops |
| Kafka | the same | the lag on the committed offsets | no — an uncommitted offset is replayed to whoever takes the partition |

The last column is the sweep, not the bookkeeping: the group does record which consumer
holds each pending entry — `manage.py tgbot_reclaim` uses exactly that — but a *stranded*
list is a thing only the Redis list can have, because only there is unsettled work parked
under a worker name that nothing else will come back for. On the other three a name is not
needed to recover the work, which is what `needs_identity` says, and the probe skips a scan
whose keys cannot exist rather than reporting a reassuring zero.

`consumer not observable from outside` in a healthy line is the second column, not a
missing consumer: on those two transports a worker that dies gives its work back
without anybody asking, so there is nothing for a probe to notice. The depth is the
signal there — set `HEALTHCHECK_MAX_QUEUE` and a wedged consumer shows up as a
backlog.

On the Redis list, the consumer writes `<REDIS_MESSAGES_KEY>:heartbeat:<worker>` every
`HEARTBEAT_INTERVAL` seconds, with a TTL of three times that — so one missed
refresh is not a failure, but a dead thread stops looking alive on its own. The
key is per worker, named like the in-flight list, so each container answers for
itself.

**What the probe does not see: the database.** It reads the transport and nothing else, on
purpose — it has to answer in milliseconds and without `django.setup()`. So a bot whose
*inbound* half is broken can pass it, and one class of that is worth knowing about because it
happened: a database restart used to leave every handler raising `InterfaceError` while the
consumer went on sending and the probe went on passing. Since 4.0 every update is bracketed with
`close_old_connections()`, which is what Django does around a request and what a bot worker never
had, so that particular failure recovers by itself on the next update. A handler that raises for
its own reasons still will, and the probe will still pass: aiogram logs it and the update stays
unhandled, so watch your handler logs rather than the exit code for that.

```yaml
  telegram_bot:
    command: python manage.py start_tgbot
    environment:
      # required: a healthcheck is a separate process, and `manage.py` only sets this
      # inside its own — so without it here the probe cannot read your settings at all
      DJANGO_SETTINGS_MODULE: core.settings
    healthcheck:
      test: ['CMD', 'python', '-m', 'django_aiogram.healthcheck']
      interval: 30s
      timeout: 5s
      start_period: 30s
      retries: 3
```

**Not `manage.py tgbot_healthcheck`, and this matters more than it looks.** That
command still exists and still works; what it also does is `django.setup()`, which
populates the app registry and runs every `AppConfig.ready()` in *your* project before
it reads a single Redis key. In one measured project — twenty apps, one of them
registering adapters in `ready()` — that was 17.9 seconds against 0.01 seconds of
actual probing, so Docker killed the probe at every timeout and the container read
`unhealthy` for the best part of an hour while the bot was fine. The number that would
have to go in `timeout:` is not this package's to know, because what it covers is your
`INSTALLED_APPS`.

The `python -m` form reads your settings module and stops there: measured at 69 ms
end to end, interpreter startup included.

**`DJANGO_SETTINGS_MODULE` has to be in the container's environment**, which is the one
thing this form needs and the management command does not. The conventional `manage.py`
sets it with `os.environ.setdefault(...)` *inside its own process*, and a healthcheck is
a different process — so a container that runs `manage.py` quite happily may still not
export it. Without it the probe answers `cannot read the settings: …` and exits 1, which
is honest but permanently unhealthy. It is in the `environment:` block above for that
reason.

Use the management command when a person is looking at the output. It additionally scans
for stranded in-flight lists and reports which delivery guarantee is in force, neither of
which can change the verdict, and both of which are the expensive part: the sweep is up to
twenty `SCAN` rounds plus an `LLEN` per list it finds, over a keyspace often shared with a
cache backend, and the guarantee is a write. Nobody reads either twice a minute.
`--stranded` and `--guarantee` turn them on for the `python -m` form too.

**Both are Redis-only**, and they are the only part of the probe that is. They build a
client of their own, so on a transport with no redis-py the guarantee reads `unknown` and
the sweep warns that it did not finish, naming the reason — the verdict never depended on
either. Where in-flight work has no worker name to be keyed on, the sweep is not attempted
at all and says nothing: that is the table above's last column, and a line on every probe
run about a question the transport does not have is worse than silence.

`start_period` matters: the first heartbeat is written when the consumer's loop
first turns, so a container checked immediately after start has nothing to show
yet.

To fail when work is backing up rather than only when the worker is gone, set a
queue limit — as a setting, or per invocation, where the flag wins:

```shell
python manage.py tgbot_healthcheck --max-queue 1000 --max-age 25
```

`--max-age` has a ceiling it cannot be argued out of: three `HEARTBEAT_INTERVAL`s, because
that is the TTL the consumer writes the key with. A heartbeat can never be *observed*
older than that — the key is gone — so a larger limit only ever refuses with the same
line, and the probe says as much when you give it one. To tolerate a longer silence, raise
`HEARTBEAT_INTERVAL` and the ceiling moves with it.

A disabled process is not unhealthy: with `ENABLED=0` the command says so and
exits 0, since nothing is meant to be running there.

## Metrics: scrape every process that does something

The shipped exporter fills a registry from the event feed —
`pip install 'django-aiogram[prometheus]'` and `connect()` in an `AppConfig.ready()`, see
**[Event log](Event-log.md#metrics-without-the-table)**. What matters here is *where*: each
process exports what it does, and the two halves do different things.

| process | the kinds it produces |
| --- | --- |
| web, Celery, anything that queues | `outbound.queued`, `outbound.scheduled` |
| the bot container | `outbound.consumed`, `outbound.sent`, `outbound.retried`, `outbound.failed`, `outbound.dropped`, every `inbound.*`, `fsm.transition`, `queue.*` |
| the mover (`tgbot_dispatch_scheduled`) | `outbound.queued` for a row it published, `outbound.dropped` for a publish that failed, a row past `--grace` and a row past `--max-attempts` |

So a scrape configuration that names only the web tier reports zero sends for ever, and one
that names only the bot reports that nothing is ever queued. Both are the exporter working.

`log.dropped` is the row that says recording itself fell behind, and it is exempt from
`EVENT_LOG_KINDS` for that reason — alert on it, because while it is non-zero every other
number here is missing some.

Under gunicorn with several workers, `prometheus_client` needs `PROMETHEUS_MULTIPROC_DIR` and
the multiprocess collector in whatever view serves `/metrics`, the same as for any other
metric in that process. The exporter adds no requirement of its own, and no server: it fills
a registry and stops there.

## The event log and your database

With `EVENT_LOG` on, every process that records owns **one more database
connection** — the writer thread's, which nothing but the writer closes. Size
the pool for it: a gunicorn worker is a process, so four of them open eight
connections rather than four. A `CONN_MAX_AGE` of 0 costs the writer nothing
extra, because it holds its own connection rather than borrowing the request's.

`EVENT_LOG_SYNC` writes on the calling thread instead, which keeps the count at
one per worker — and makes every send wait for the database, which is why
`W009` warns about it. It is for tests.

Point it somewhere else if the traffic warrants: `EVENT_LOG_DATABASE` names any
alias in `DATABASES`, and the writer and the admin both use it explicitly, so
the feature works with or without the router installed. See
**[Event log](Event-log.md)**.

The bot container needs `DATABASES` reachable too. It is the same Django
project, but a different place on the network — a database that only the web
tier can reach records `outbound.queued` and never the `outbound.sent` that
says the message actually arrived.

## Scaling

One bot container is normally enough — Telegram's limits bind long before the
consumer does. Several are safe if you want the redundancy: the pop is atomic,
so a queued message goes to exactly one worker.

On Redis 6.2+ a message is moved to a per-worker processing list while it is
being sent, and stays there until the send has actually finished; a replacement
worker reclaims what it left behind **when it resolves the same identity**, which
is `WORKER_NAME` or, without one, the hostname. Given that, delivery is
**at-least-once**, so a crash mid-send can produce a duplicate — and each worker
needs an identity of its own, since the list is per worker. A replacement under a
different name strands the old list instead: `I001` reports the risk, and
`manage.py tgbot_reclaim --worker <name>` is the way back. Before 3.1.0 the
message was removed as soon as the send was *scheduled*, which meant polling mode
did not have that guarantee at all.
Waiting for the send is something the handler opts into: `bot.send_raw`, which
this command uses, does — a handler of your own taking only `**kwargs` is still
acknowledged when it returns. Older servers lack `LMOVE` and fall back to plain
pops, which is **at-most-once**: a kill between the pop and the call loses that
one message — unless `REQUIRE_CRASH_SAFE` is on, in which case the command
refuses to start at all rather than run that way. A send that *fails* is
acknowledged and logged either way, never redelivered for ever. See
**[Delivery](Delivery.md)**.

Do not run two containers polling the **same token**, though. Telegram allows
only one `getUpdates` consumer per bot, and the second will fight the first for
updates — which is what the polling lease is for: containers agree through a row,
and `MAX_BOTS_PER_WORKER` is what splits the bots between them.

### Receiving and sending are two jobs

`start_tgbot` does both by default, which is what a small installation wants and what it has
always had. At scale they do not scale together, so each half can be turned off:

```shell
python manage.py start_tgbot --no-updates    # consume the queues; never call getUpdates
python manage.py start_tgbot --updates-only  # receive updates; consume nothing
```

- **`--no-updates`** is the sender: the shape a webhook deployment wants, where updates arrive
  in the web tier and this process exists to send, and the shape a sender pool wants at any
  scale. A loop still turns in it, because the consumers hand their sends to one.
- **`--updates-only`** is the receiver. Give its probe `--no-consumer` as well:

  ```shell
  python manage.py tgbot_healthcheck --no-consumer
  # the module form reads the settings itself, so it needs the variable `manage.py` sets
  DJANGO_SETTINGS_MODULE=core.settings python -m django_aiogram.healthcheck --no-consumer
  ```

  Without it the probe asks whether a consumer is turning, finds no heartbeat — nothing in that
  container writes one — and restarts a container that is doing exactly what it was told. The
  transport is still read either way: a receiver has to *send* what its handlers produce, so a
  queue it cannot reach is a real failure. The command warns about this at startup.
- Both flags together are refused: a container that neither receives nor consumes would sit
  there looking alive, answering the probe, and doing nothing. So is `--updates-only` in
  **webhook mode** — there the updates arrive over HTTP in whatever serves the webhook, so
  this process receives nothing anyway and consuming is all it was doing.

Shutdown is unchanged — whatever is running stops together, and it preserves whichever
guarantee your transport gives: at-least-once where there is an in-flight list, at-most-once
on a Redis server without `LMOVE` (see **Crash safety** above). Splitting the roles changes
neither, which is why `close()` and the drain stay one story.

### One container, several queues

A queue is the isolation boundary: give a client one of their own and their
backlog is theirs. A container is then told which queues to consume, the way
Celery's worker is told with `-Q`:

```shell
python manage.py start_tgbot --queues default,vip
python manage.py start_tgbot --pools vip
```

- **`--queues`** names them. Each has to be declared, in
  `TELEGRAM_BOT_DEFAULTS['QUEUES']` or as a `TelegramQueue` row; a name that is
  not is refused rather than consumed, because a container reading a queue
  nobody publishes to looks healthy and delivers nothing. The exception is a
  database that could not be *read* — down, or not migrated here: the table's
  answer is then unknown, so nothing is refused and the container serves what it
  was told. A deployment with no database at all is not that case, and its
  settings are the whole declaration.
- **`--pools`** names the *labels* on those rows instead, and that is the one
  Celery has no equivalent for: a queue created after this container started is
  served with no redeploy, which is what enumeration cannot do when clients
  arrive at run time. No globs — a glob would include a queue by the accident of
  its name. A pool that holds no queues refuses the run.

  Re-read every `BOT_REFRESH_INTERVAL` while the container runs, which is what
  makes "no redeploy" true: a queue added to a pool starts being consumed within
  that interval, and one moved out of it stops. A pass that could not read the
  table leaves the consumers as they are, and one queue that cannot be consumed
  is one queue — the container keeps serving the rest, and the next pass tries it
  again.
- Given both, the container serves the union, each queue once.
- Given neither, it serves the one queue its settings name, which is every
  deployment before 5.0.

**A budget per queue, whatever shape the consumers run in.** That is the point
rather than an implementation detail: a backlog on one queue is a backlog on one
queue, and `MAX_IN_FLIGHT` is applied to each of them separately. A queue at its
bound simply stops being read from until one of its sends finishes; the others
keep being read.

**What it costs depends on the transport.** RabbitMQ, Kafka and Redis Streams
read several queues over the connection they already have — several
`basic_consume` on one channel, one `subscribe` naming several topics, one
`XREADGROUP` naming several streams — so twenty queues on any of them are
**one** connection and **one** consumer thread. A queue arriving there is told
to the consumer that is already running, so the clients it was already serving
are not paused.

That is per **lane**, and a lane is the queues whose settings agree on
everything but which queue they name — the same arithmetic that decides what a
set of bots shares, which **[Multiple bots](Multiple-bots.md)** is the page for: the transport, the server, the serializer,
`MAX_IN_FLIGHT`. Queues that disagree cannot share a connection and get a
consumer each, which is the same arithmetic the runtime groups bots by. A
container whose twenty client queues are configured alike — the ordinary case,
since they differ by name and nothing else — is one connection.

A crash-safe Redis list cannot: `BLMOVE` takes one source, and reading several
keys would mean `BLPOP`, which loses the message between the pop and the send.
There a container serving twenty queues holds twenty connections and twenty
threads, so serve them from a few containers by pool rather than all from one.

## Not using containers

Nothing here is docker-specific. Run `python manage.py start_tgbot` under
systemd or supervisor; the web and worker services need no extra environment,
since only one process should run `start_tgbot` in the first place.
