# Settings

Everything lives under `TELEGRAM_BOT` in `settings.py`. Scalar values can also
come from `DJANGO_AIOGRAM_<NAME>`; Django settings take precedence.

All of it is validated by `manage.py check` — in processes where the bot is
enabled **or** the event log is on. A container with `ENABLED=0` and the log
recording still registers every rule, including the ones about the bot's own
settings. The credential warnings stay silent there — `W001` and `W002` are gated on
the bot being enabled, as the table below says — but the log's own rules are not:
measured on `{'ENABLED': False, 'EVENT_LOG': True}`, that process reports `W005`,
`W006` and `I001`. Plain `manage.py check` exits 0 on all three, and the
`--fail-level WARNING` this documentation recommends for CI fails on the two
warnings. Only a process with `ENABLED` and `EVENT_LOG` both switched off registers
nothing.

`E047` is split along the same line, which matters to a base install: since 4.0 no
transport driver is a dependency of this package, and a process that reaches no
transport is not asked to install one. Measured on a disabled process installed
without `django-aiogram[redis]`: no `E047`, and `manage.py check` still exits 0. A
`BROKER` naming something that is not a transport is still reported there, because
that name is equally wrong in the worker that does send.

## Credentials

| Setting | Default | Description |
| ------- | ------- | ----------- |
| `TOKEN` | `''` | Telegram bot token |
| `REDIS_URL` | `''` | Redis connection URL, including the database index |

Neither is required for the project to boot.

## Which processes run the bot

| Setting | Default | Description |
| ------- | ------- | ----------- |
| `ENABLED` | `True` | Whether this process sends — to Telegram, or into the broker. The depth reads answer regardless |
| `AUTODISCOVER` | `True` | Import `<app>.<MODULE_NAME>` on startup |
| `MODULE_NAME` | `'tg_router'` | Module to look for in each installed app |

**Every boolean setting here is parsed, not tested for truthiness**: `'false'`,
`'no'`, `'off'` and `0` all mean false, wherever a boolean is accepted. Anything
unparseable raises `ImproperlyConfigured` rather than being read as true — which
matters because the environment can only give you a string, so `'false'` under a
bare truthiness test would mean the opposite of what it says. `RAISE_EXCEPTION`
was the last setting read that way, and no longer is. See **[[Deployment]]**.

## Bot behavior

| Setting | Default | Description |
| ------- | ------- | ----------- |
| `DEFAULT_BOT_PROPERTIES` | `{}` | Passed to aiogram's `DefaultBotProperties` |
| `DEFAULT_KWARGS` | `lambda fn: {}` | Per-function extras the above cannot express |
| `FSM_STORAGE` | `'redis'` | `'redis'`, `'memory'`, or a dotted path |
| `MAX_RETRIES` | `10` | Retries after a Telegram rate-limit refusal |
| `RAISE_EXCEPTION` | `False` | Let `send_raw` propagate failures |

`DEFAULT_BOT_PROPERTIES` accepts every field aiogram defines: `parse_mode`,
`disable_notification`, `protect_content`, `allow_sending_without_reply`,
`link_preview`, `link_preview_is_disabled`, `link_preview_prefer_small_media`,
`link_preview_prefer_large_media`, `link_preview_show_above_text`,
`show_caption_above_media`. A misspelling fails at `manage.py check`.

```python
TELEGRAM_BOT = {
    'DEFAULT_BOT_PROPERTIES': {
        'parse_mode': 'HTML',
        'link_preview_is_disabled': True,
    },
}
```

`DEFAULT_KWARGS` covers what bot properties cannot, such as a default caption:

```python
def default_kwargs(function: str) -> dict:
    return {'send_photo': {'caption': 'Photo'}}.get(function, {})
```

## Updates

These decide how updates reach the bot. They have nothing to do with the queue,
which carries outbound messages in both modes — see **[[Webhook]]**.

| Setting | Default | Description |
| ------- | ------- | ----------- |
| `MODE` | `'polling'` | Where updates come from: `'polling'` or `'webhook'` |
| `WEBHOOK_URL` | `''` | Where Telegram posts updates; required when `MODE` is `'webhook'` |
| `WEBHOOK_SECRET` | `''` | Required with `WEBHOOK_URL`; the view compares it with the header Telegram echoes |
| `WEBHOOK_ALLOWED_UPDATES` | `()` | Update types to receive; empty means Telegram's default set |

## Queue

| Setting | Default | Description |
| ------- | ------- | ----------- |
| `DELIVERY` | `'django_aiogram.consumer.delivery.BlpopDelivery'` | Dotted path to the consumer class. Your own `Delivery` subclass goes here — see **[[Delivery]]** |
| `BROKER` | `'django_aiogram.broker.redis_list.RedisListBroker'` | Which transport carries messages, by dotted path. Nothing is inferred from what happens to be installed: a name whose driver is missing is a system check with the `pip install` line, not an `ImportError` on the first send. Each broker declares its **own** settings — see the table below — and the other queue rows here belong to the package rather than to a transport. A broker with no sensible default for where a message goes marks that setting required and refuses at startup without it |
| `REDIS_MESSAGES_KEY` | `'TELEGRAM_BOT_MESSAGE'` | List holding queued calls — the Redis list transport's own |
| `WORKER_NAME` | hostname | Names this worker's in-flight list on the Redis list transport, and nothing on the other three — see **[[Redis-list]]** |
| `BLPOP_TIMEOUT` | `5` | How often the consumer checks for shutdown; capped at `min(HEARTBEAT_INTERVAL, floor(<the transport timeout>) - 1)`, never below 1 |
| `DRAIN_TIMEOUT` | `5` | Seconds `close()` gives in-flight sends to finish before canceling them |
| `MAX_IN_FLIGHT` | `0` | Sends the consumer leaves in flight before it stops taking messages; `0` is no bound |
| `REQUIRE_CRASH_SAFE` | `False` | Refuse to start where a message cannot survive the worker being killed mid-send |
| `REDIS_TIMEOUT` | `10` | Seconds a single Redis call may take before the server counts as gone |
| `HEARTBEAT_INTERVAL` | `10` | Seconds between the consumer's reports; the key lives three times as long, which is also the most `--max-age` can observe |
| `HEALTHCHECK_MAX_QUEUE` | `0` | Longest queue still considered healthy; the check fails only above it, and `0` disables it |
| `SERIALIZER` | `'json'` | `'json'` or `'pickle'` — see **[[Serialization]]** |
| `ALLOW_PICKLE` | `False` | Let the reader accept pickled payloads. Needed to *read* them at all, and needed alongside `SERIALIZER: 'pickle'` to write them. Unpickling queue data is code execution, so only on a queue nothing untrusted can write to |

### What each transport declares

`BROKER` names one of these, and only that one's settings are read. A setting belonging to a
transport you are not using is reported by `W003` as a key nothing reads, which is what it is.

| Transport | Settings | Required |
| --- | --- | --- |
| `django_aiogram.broker.redis_list.RedisListBroker` | `REDIS_URL`, `REDIS_MESSAGES_KEY`, `REDIS_TIMEOUT`, `BLPOP_TIMEOUT` | none — the list has a default name |
| `django_aiogram.broker.redis_streams.RedisStreamsBroker` | `REDIS_URL`, `REDIS_STREAM_KEY`, `REDIS_STREAM_GROUP`, `REDIS_TIMEOUT`, `BLPOP_TIMEOUT` | **`REDIS_STREAM_KEY`** |
| `django_aiogram.broker.rabbitmq.RabbitMQBroker` | `RABBITMQ_URL`, `RABBITMQ_QUEUE`, `RABBITMQ_PREFETCH`, `RABBITMQ_TIMEOUT` | **`RABBITMQ_URL`**, **`RABBITMQ_QUEUE`** |
| `django_aiogram.broker.kafka.KafkaBroker` | `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC`, `KAFKA_GROUP`, `KAFKA_TIMEOUT` | **`KAFKA_BOOTSTRAP`**, **`KAFKA_TOPIC`** |

#### Redis list

The Redis list declares four settings, gathered here because the other three transports have
their own sections. They also appear above, spread across the tables they landed in when Redis
was the only transport — `REDIS_URL` under **Credentials**, the other three under **Queue** — and
splitting those into what the package owns and what a transport does is its own change.

| Setting | Default | Description |
| ------- | ------- | ----------- |
| `REDIS_URL` | `''` | Where the server is. Shared with the Streams transport and with the FSM storage, which is why it also sits under **Credentials** above |
| `REDIS_MESSAGES_KEY` | `'TELEGRAM_BOT_MESSAGE'` | The list, and the prefix the in-flight and heartbeat keys derive from |
| `REDIS_TIMEOUT` | `10` | The deadline on any single call. `E030` refuses anything below 2, because the pop has to sit a second inside it |
| `BLPOP_TIMEOUT` | `5` | How often the blocking take is interrupted to check for shutdown, capped at `min(HEARTBEAT_INTERVAL, floor(<the transport timeout>) - 1)`, never below 1 — the transport's timeout being `REDIS_TIMEOUT`, `RABBITMQ_TIMEOUT` or `KAFKA_TIMEOUT`, whichever `BROKER` names. Floored because two of those accept fractions, so a `2.6` deadline leaves a whole second inside it rather than 1.6. `W004` says so and names the bound that binds |

#### Redis Streams

| Setting | Default | Description |
| ------- | ------- | ----------- |
| `REDIS_STREAM_KEY` | **required** | The stream `XADD` writes to. No default on purpose: one would sit a suffix away from `REDIS_MESSAGES_KEY`, and `XADD` against a key holding a list answers `WRONGTYPE` on the first send rather than at startup. `E047` asks for it before anything runs |
| `REDIS_STREAM_GROUP` | `'django-aiogram'` | The consumer group every worker joins, so they share the stream instead of each reading all of it. Defaulted, because unlike the key there is nothing another transport could collide with |

#### RabbitMQ

| Setting | Default | Description |
| ------- | ------- | ----------- |
| `RABBITMQ_URL` | **required** | `amqp://user:pass@host:5672/vhost`. No default: it carries credentials and a host, and neither is worth baking in |
| `RABBITMQ_QUEUE` | **required** | The durable queue messages are published to and consumed from. No default, for the reason `REDIS_STREAM_KEY` has none — where messages go is not something to guess |
| `RABBITMQ_PREFETCH` | `0` | `basic_qos` prefetch, where 0 means the broker does not limit how many unacknowledged messages a consumer may hold. Left unlimited on purpose: `MAX_IN_FLIGHT` already bounds that, and a prefetch below it would stall the consumer — it would hold its limit of unacknowledged sends and be given nothing more until one finished |
| `RABBITMQ_TIMEOUT` | `10` | Seconds a call may sit on a *blocked* connection before it gives up. RabbitMQ blocks a publisher's connection under memory or disk pressure, and pika leaves this unset — measured — so a blocked connection would hold every synchronous call on it for ever, and `publish` runs on request threads. This wins over a `blocked_connection_timeout` written into `RABBITMQ_URL`, which it did not before 4.0: the consumer's take is capped by this number and the shutdown join is derived from it, so a URL overriding it left both describing a deadline no call carried. Every other pika parameter in the URL is still yours — `socket_timeout` included, which does not bound a take. Has to be a positive finite number, and `E047` refuses anything else by name — a `0` here used to reach pika as `10` while `W004` and the consumer's cap read it as `0` |

A publish here is **confirmed and mandatory**: the broker answers before `send()` returns, and
a message that cannot be routed raises instead of vanishing into an exchange. That matches what
the Redis transports already do — `RPUSH` answers with the new length — and it costs what the
guarantee costs: measured, 323–393µs against 15–20µs for the same publish with only the confirm
taken off. Most of that is the disk rather than the round trip — the same publish without
persistence is 135–173µs. A Redis list publish, measured the same way and on the same machine, is
120–147µs.

Those two are **not divided here**, and that is deliberate: they come from different scripts run
at different times, so there is no pair of numbers from one run to divide. What they support is an
ordering — this is the most expensive publish of the four, above Kafka's 166–295µs — and the
ordering is also the only part that survives a change of footing. A *native* Redis publish was
measured at 14–19µs in 3.1.0, and a multiple against that baseline would read quite differently
from one against this.

Nothing here needs `WORKER_NAME`. An unacknowledged message returns to the queue when the
channel that held it drops, which is what a worker being killed does to it — so there is no
in-flight list, `reclaim()` has nothing to do, `tgbot_reclaim` refuses, and `I001` stays quiet.
The healthcheck reports the consumer as **not observable from outside**, because RabbitMQ tracks
its own consumers and this package writes nothing about them.

**Redis 7.0 or newer**, for this transport only — the package floor stays 6.2 for the list.
`XINFO GROUPS` grew the `lag` field in 7.0 and it is the only exact answer to how many
messages are waiting; measured, 6.2 has no such field, and Redis has no command that counts a
range. The broker probes for the field on first use and refuses by name without it rather than
reporting a number that would drive `HEALTHCHECK_MAX_QUEUE` wrongly.

**Never trim this stream by length.** `XADD MAXLEN` and `XDEL` remove exactly the entries an
unfinished send leaves unacknowledged. Measured: trim past a pending entry and `XPENDING`
still reports it while `XAUTOCLAIM` hands the id back in its *deleted* list — the message is
gone and no consumer can replay it. `XDEL` additionally costs Redis the ability to answer
`lag` at all — a depth read then refuses instead of guessing. That one is temporary:
measured, the count returns once the group has read through to the end of the stream, so it
is unavailable exactly while there is a backlog, and `XSETID` restores it at once. The broker's own `trim()` stops at the oldest unacknowledged entry.

#### Kafka

| Setting | Default | Description |
| ------- | ------- | ----------- |
| `KAFKA_BOOTSTRAP` | **required** | `host:port[,host:port…]`. No default, for the reason the AMQP URL has none |
| `KAFKA_TOPIC` | **required** | The topic messages are produced to and consumed from |
| `KAFKA_GROUP` | `'django-aiogram'` | The consumer group every worker joins, so they share the partitions instead of each reading all of them |
| `KAFKA_TIMEOUT` | `10` | Seconds any single call may take — the socket timeout, and how long a publish waits for the broker's acknowledgement before it reports a refusal. Between `0.01` and `300`, which is what librdkafka accepts for the setting this becomes; outside that it is refused at startup rather than by the driver |

**Kafka settles a position, not a message**, and that is the difference to understand before
choosing it. Committing offset N says every message below N has been dealt with. So a consumer
holding several sends at once — which `MAX_IN_FLIGHT` allows — cannot settle them in whatever
order they finish: this broker commits the highest **contiguous** prefix and holds anything
settled out of order until the gap below it closes. Nothing is lost, and a burst of slow sends
delays the commit rather than skipping it.

The same shape has a sharper edge on `release`. There is no per-message nack in Kafka, so
giving a message up means rewinding to its offset — and that record, together with every later
one in its partition, is delivered again. **Build idempotency on your own business key**, which
the delivery page recommends generally and which matters most here.

**A publish waits for the broker** — 166 to 295µs for one message, across eleven runs. On the
same footing a Redis list publish is 120–147µs and RabbitMQ's confirmed one 323–393, which puts
this between them; the numbers are not divided against each other, for the reason the AMQP
section above gives. `produce()` itself answers in 0.2µs because librdkafka's own thread does the
I/O, and returning there would be a weaker promise than `RPUSH` already makes. Automatic topic
creation is the broker's setting, not this package's: with it off, a missing topic is a refusal at
publish time.

Nothing here needs `WORKER_NAME`. A consumer that dies stops heartbeating, the group
rebalances, and its partitions go to another member from the last committed offset — so
`reclaim()` has nothing to do, `tgbot_reclaim` refuses, and the healthcheck reports the
consumer as not observable from outside.

## Rate limits

| Setting | Default | Description |
| ------- | ------- | ----------- |
| `RATE_LIMIT` | see below | Proactive pacing, or `None` to disable |

```python
TELEGRAM_BOT = {
    'RATE_LIMIT': {
        'overall_per_second': 30,
        'per_chat_per_second': 1,
        'group_per_minute': 20,
    },
}
```

See **[[Rate-limits|Rate limits]]**.

## Event log

Off by default. Turning it on records what the bot did into one append-only
table, which needs `manage.py migrate` and a retention job — see
**[[Event-log|Event log]]** before you enable it.

| Setting | Default | Description |
| ------- | ------- | ----------- |
| `EVENT_LOG` | `False` | Record events at all. Gates both the writing and the admin |
| `EVENT_LOG_KINDS` | `()` | Which kinds to keep; empty means every kind this version knows. Naming any also opts out of the kinds a later release adds, and filters `events_recorded` receivers as well as rows. `log.dropped` is exempt either way — it is the record that recording fell behind |
| `EVENT_LOG_PAYLOAD` | `'summary'` | `'none'`, `'summary'` (argument names and sizes) or `'full'` (message bodies) |
| `EVENT_LOG_MAX_PAYLOAD_BYTES` | `8192` | Cap on the JSON column; `0` stores no payload at all |
| `EVENT_LOG_REDACT_KEYS` | see `defaults.py` | Payload keys whose values are blanked before a row is written |
| `EVENT_LOG_BUFFER_SIZE` | `1000` | Events held in memory while the writer is behind; a full buffer drops the event rather than making a send wait |
| `EVENT_LOG_BATCH_SIZE` | `200` | Rows per `bulk_create` |
| `EVENT_LOG_FLUSH_INTERVAL` | `1` | Seconds before a partial batch is written anyway |
| `EVENT_LOG_RETENTION_DAYS` | `0` | Days a row is kept; `0` keeps them for ever. Read by `manage.py tgbot_prune_events`, never on the write path |
| `EVENT_LOG_DATABASE` | `''` | A `DATABASES` alias for the log; empty means the default one |
| `EVENT_LOG_SYNC` | `False` | Write on the calling thread instead of the writer's. Tests only |

`EVENT_LOG_KINDS` and `EVENT_LOG_REDACT_KEYS` are settings-only, like
`WEBHOOK_ALLOWED_UPDATES`: a tuple has no textual form the environment could
carry, and a string from it would be read one character per item.

## Check ids

Errors are `django_aiogram.EXXX`, warnings `django_aiogram.WXXX`,
and information `django_aiogram.IXXX`. An error refuses the boot; a warning
fails `manage.py check --fail-level WARNING`, which is what a CI step or an
entrypoint usually runs; information fails neither, and is there for conditions
this package can see but cannot judge from inside a check — `I001` and `I002`
below are both of that kind, because a system check cannot tell which process it
is running in or look inside a database router.

They moved from `telegram_bot.EXXX` in 2.0 — update `SILENCED_SYSTEM_CHECKS`
if you silenced any. An id is never reused once its setting is gone, so an
entry naming a retired one is dead but harmless.

| Id | Meaning |
| -- | ------- |
| `W001` / `W002` | `TOKEN` / `REDIS_URL` empty while the bot is enabled |
| `W003` | `TELEGRAM_BOT` contains unknown keys |
| `W004` | `BLPOP_TIMEOUT` is **above** the ceiling the consumer applies — `min(HEARTBEAT_INTERVAL, floor(<the transport timeout>) - 1)`, never below 1 — so the take is silently shortened to it. Equal to the ceiling is not warned about and is not shortened. The hint names whichever of the two binds, and the transport term is the one `BROKER` names rather than always `REDIS_TIMEOUT` |
| `E001`–`E003`, `E017` | a boolean setting holds something that cannot be read as true or false. `ENABLED` and `AUTODISCOVER` are read while the app loads, so in practice those two refuse the boot with the same message before `check` runs at all |
| `E004`–`E007`, `E009`–`E011` | a string setting is wrong, or not one of the allowed values |
| `E012`, `E014` | an integer setting is wrong or below its minimum |
| `E015` / `E016` | `DEFAULT_KWARGS` not callable / `DEFAULT_BOT_PROPERTIES` not a mapping |
| `E018` | unknown key in `DEFAULT_BOT_PROPERTIES` |
| `E019` | `FSM_STORAGE` is not `redis`, `memory` or a dotted path |
| `E020` | `RATE_LIMIT` is malformed |
| `E021` | `WORKER_NAME` is not a string |
| `E022` | `SERIALIZER` is `pickle` while `ALLOW_PICKLE` is `False` |
| `E023` | `HEARTBEAT_INTERVAL` is wrong or below 1 |
| `E024` | `HEALTHCHECK_MAX_QUEUE` is wrong or negative |
| `E025` / `E026` | `WEBHOOK_URL` / `WEBHOOK_SECRET` is not a string |
| `E027` | `WEBHOOK_URL` is set without a secret or is not https, or `MODE` is `webhook` with no URL |
| `E028` | `MODE` is not `polling` or `webhook` |
| `E029` | `WEBHOOK_ALLOWED_UPDATES` is not a list, or names an update type Telegram does not have |
| `E030` | `REDIS_TIMEOUT` is wrong or below 2 — the pop has to sit one second inside it |
| `E031`, `E042` | `EVENT_LOG` / `EVENT_LOG_SYNC` cannot be read as true or false. `EVENT_LOG` is read while the app loads, so it too refuses the boot first |
| `E032`, `E035` | `EVENT_LOG_KINDS` / `EVENT_LOG_REDACT_KEYS` is not a list or tuple of strings |
| `E033` | `EVENT_LOG_PAYLOAD` is not `none`, `summary` or `full` |
| `E034`, `E039` | `EVENT_LOG_MAX_PAYLOAD_BYTES` / `EVENT_LOG_RETENTION_DAYS` is wrong or negative |
| `E036`–`E038` | `EVENT_LOG_BUFFER_SIZE` / `EVENT_LOG_BATCH_SIZE` / `EVENT_LOG_FLUSH_INTERVAL` is wrong or below 1 |
| `E040` | `EVENT_LOG_DATABASE` is not a string |
| `E041` | `EVENT_LOG_DATABASE` names an alias that is not in `DATABASES` |
| `E043` | `REDIS_URL` sets `decode_responses` while `ALLOW_PICKLE` is `True` |
| `E044` | `DRAIN_TIMEOUT` is not a finite number, or is negative |
| `E045` | `MAX_IN_FLIGHT` is not an integer, or is negative |
| `E046` | `REQUIRE_CRASH_SAFE` cannot be read as true or false |
| `E047` | `BROKER` is unusable. Reported whatever `ENABLED` says: it is empty; it names something that is not a broker; it names one that declares no `CALL_TIMEOUT_OPTION` — the option bounding one of its calls, which `W004` quotes and the consumer caps its reads by; or that option holds something the transport refuses, meaning anything but a positive finite number of seconds, and whatever narrower range the transport documents. That last finding stands aside where the option has a rule of its own that is already reporting the value — `REDIS_TIMEOUT` has `E030`, so one value never draws two errors. The name and the deadline are judged before the driver is looked for, so a process that has not installed the extra yet still hears about them. Gated on the bot being enabled, like `W001` and `W002`: the driver behind it is not installed — the hint carries the `pip install` line for that extra — and its own required settings are unset. A process that never reaches a transport is not asked to install a driver, while a name or a deadline is as wrong in the web tier as in the worker |
| `E048` | `DATABASE_ROUTERS` names any `django_redis_aiogram.` path, which 4.0 renamed. The router we shipped is named against its replacement, `django_aiogram.eventlog.dbrouter.TelegramEventLogRouter`; any other path from that distribution is reported as gone, since this cannot invent a replacement for something it never had |
| `I001` | `WORKER_NAME` is empty **and** the hostname is one Docker generated, so a replacement container gets a different name — which strands whatever the old container was sending. Information rather than a warning because a check cannot tell a consumer from a web process, and every container without `hostname:` matches; `start_tgbot` warns for itself at startup |
| `I002` | `EVENT_LOG_DATABASE` names an alias and nothing in `DATABASE_ROUTERS` that this check can read sends this app there, so a plain `migrate` may not create the table — `migrate --database=<alias>` still would. Information rather than a warning: a router of your own returning that alias is equally correct, and this cannot see inside one |
| `W005` | the log is on while its database has no engine, so every event is dropped |
| `W006` | the log is on with `EVENT_LOG_RETENTION_DAYS` at 0, so nothing ever deletes a row |
| `W007` | `EVENT_LOG_BATCH_SIZE` is above `EVENT_LOG_BUFFER_SIZE`, so the batch can never fill |
| `W008` | `EVENT_LOG_KINDS` names a kind nothing records |
| `W009` | `EVENT_LOG_SYNC` is on, so a send waits for the database |
