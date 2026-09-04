# API

The shared instance is what you normally use:

```python
from django_aiogram import bot
```

`bot` is a `TelegramBot`. Building one needs no credentials — everything that
does appears on first use — so importing it anywhere is safe, including in a
process that never talks to Telegram.

`import django_aiogram` costs about 0.17 ms, because the package resolves its
exports on attribute access — it was roughly a millisecond and a half before 3.1.0. Naming `bot` is what loads aiogram and
the pydantic stack under it (~900 ms), so `from django_aiogram import bot`
pays that once, at the moment of import. Put it in the modules that send —
router modules, the views and tasks that call `bot.send()` — and a process that
imports none of them never loads aiogram at all.

`ENABLED=0` covers the package's own boot: no autodiscover, so no `tg_router`
module is imported, and no checks are registered. It does not un-import
anything. A module that names `bot` at import time still loads aiogram in a
disabled process — the send is a no-op, the import is not free. Move it inside
the function if that matters.

## What the instance holds

| | What it is | Built |
| --- | --- | --- |
| `bot.bot` | the aiogram `Bot` | on first use; raises if `TOKEN` is empty |
| `bot.dispatcher` | the aiogram `Dispatcher`, with the configured FSM storage | on first use |
| `bot.loop` | the event loop this bot's work runs on | on first use, one per instance |
| `bot.router` | the `Router` the decorators register on | with the instance |
| `bot.max_retries` | how many times a rate-limited send is retried | from `MAX_RETRIES`, or the constructor |
| `bot.enabled` | whether this process should send at all; the depth reads below are the exception | read per access |
| `bot.rate_limiter` | the limiter for this token, shared with any other instance holding it | on first use |
| `bot.is_worker` | whether this process is the one polling Telegram | read per access |

These are public. 1.x code drives them directly — running the loop by hand, feeding the
dispatcher — and that keeps working; `tests/test_public_surface.py` fails if any of them
disappears. Reusing the connection through `bot.redis_conn` is the one that stopped: 4.0 removed
it, and `django_aiogram.redis.redis_conn` is the same object in the module that owns it.

## Sending

| | |
| --- | --- |
| `bot.send(function='send_message', **kwargs)` | queue it, or call Telegram directly inside the bot container |
| `bot.enqueue(...)` | always queue |
| `bot.send_raw(...)` | always call Telegram from this process |
| `bot.send_many(chat_ids, function='send_message', *, chunk_size=100, **kwargs)` | queue rather than call, one message per chat, a chunk per round trip |
| `bot.outcome(correlation_id)` | what became of the message that id names, read from the event log |
| `await bot.aoutcome(correlation_id)` | as `outcome`, without blocking the loop |
| `bot.cancel_scheduled(correlation_id)` | delete the rows this id still has waiting; returns how many. A positive count is not a promise that nothing went out — see below |

Every form that *queues* takes `eta=<datetime>` — aware under `USE_TZ`, naive without it, and
the other way round refused rather than guessed at — `send`, `enqueue`, `send_many` and
their awaiting twins, which writes the call to the schedule instead of the queue, including
inside the bot container where `send` would otherwise call Telegram directly. `send_raw` does
not: it calls Telegram from this process, and there is nothing to schedule.
`manage.py tgbot_dispatch_scheduled` is what publishes a row once it is due. See
**[Sending messages](Sending-messages.md#sending-it-later)**.

`function` must name a Telegram API method aiogram exposes; anything else raises
`ValueError` before it reaches the queue. See **[Sending messages](Sending-messages.md)**.

`send`, `enqueue` and `send_raw` return a **correlation id** — a `uuid.UUID`
that ties every row about that message together, whichever process wrote it.
It is not one per message and not an idempotency key: a handler's replies inherit
the id of the update that caused them, so one id can cover several messages.
Deduplicate on a key your own domain owns.
`send_many` returns one per chat, in the order the chats were given. Store it
beside your own model if you want to join your records to the feed later. Each of
them also accepts one as a keyword argument, and a handler replying to an update
inherits that update's id without passing anything:

```python
identifier = bot.send(chat_id=chat_id, text='hello')
Receipt.objects.create(order=order, telegram_correlation_id=identifier)
```

With `TRANSACTIONAL` on, a send that takes the queue route and is made inside
`transaction.atomic()` returns its id immediately and reaches the broker when the block
commits. Only that route: the table above has `bot.send()` calling Telegram directly inside
the bot container, and `bot.send_raw()` doing so everywhere. Neither makes a queue write, so
there is nothing for the setting to hold back. See
**[Sending messages](Sending-messages.md#inside-a-transaction)**. The awaiting twins below
never wait for a commit: a coroutine holds its own database connection, so there is no
transaction of the caller's for them to see.

Before 3.0 they returned `None`, so gaining a return value broke no call site. The 4.0
renames did — see **[Upgrading](Upgrading.md)** for each old name against what to call instead.

### From code already on an event loop

| | |
| --- | --- |
| `await bot.asend(...)` | as `send`, without the blocking socket write |
| `await bot.aenqueue(...)` | as `enqueue` |
| `await bot.asend_many(...)` | as `send_many` |

Same signatures, same rows, and the same correlation id — resolved on the caller's
context before the first `await`, so a handler's replies still inherit the id of
the update that caused them.

The difference is not *where* the write happens. `redis.asyncio` writes on the
same thread the loop is running on; it just yields while waiting instead of
holding that thread, so under ASGI the thread goes on serving other requests
rather than sitting on a socket. Reach for these from an async view or an async
task. Note that only the waiting yields: `asend_many` iterates the chats and
serializes each chunk between its awaits, and that part is ordinary CPU work on
the loop's thread. A fan-out large enough to matter belongs in a task, not in a
request.

**On the Redis transports**, each loop gets its own client, because `redis.asyncio`
connections are loop-affine. `await bot.aclose()` closes the one belonging to the loop
that calls it, and it is worth calling from a lifespan shutdown if your server has one —
it is the only path that closes the connection on the loop it belongs to, which is the
only loop that may close it. Without it the connection stays open until the client is
collected, and Python may say so with a `ResourceWarning`.

RabbitMQ and Kafka have no such client: their drivers are synchronous, so the awaiting
half borrows a thread and publishes through the same connection the synchronous half
uses. `aclose()` is a no-op there — it closes a Redis client those deployments never
open — and the connection it did not close belongs to the process rather than to any
loop. Nothing about the async API changes; what changes is that there is nothing for a
lifespan shutdown to release. **[Deployment](Deployment.md)** has
the shutdown recipe.

### Queue introspection

| | |
| --- | --- |
| `bot.queue_depth()` | messages waiting for a worker; one read, of whichever transport `BROKER` names |
| `bot.inflight_depth(worker=None)` | messages one worker is part-way through sending |
| `await bot.aqueue_depth()` / `await bot.ainflight_depth(...)` | the same read, without holding the loop |

`aget_redis()` and `aclose_redis()` in `django_aiogram.redis` are **not** part
of this surface, deliberately: the async client is one per running loop, and its
lifetime belongs to `bot.aclose()` rather than to a caller. Reach the queue through
the four methods above; if you hold a client of your own, you own closing it on the
loop that made it.

These four are reads rather than sends, so `ENABLED=0` does not turn them into
no-ops the way it does every send: they still connect, and without whatever the
configured transport connects with — `REDIS_URL` for the two Redis brokers, the
equivalent for the others — they raise `ImproperlyConfigured` rather than
answering zero, and `BrokerDependencyError` when it is the driver that is absent
rather than the setting. A monitor that runs in a disabled process still needs that setting, and the
driver: `manage.py check` does not ask a disabled process for either.

`inflight_depth` with no argument answers for the calling process on three of the four
transports; naming another is how a monitor reads what a worker that is gone was still holding.

**Redis Streams is the exception, and deliberately.** Unnamed, it answers for the whole consumer
group rather than for this consumer, because a stream's pending list belongs to the group: after a
crash the unsettled work is whoever's picks it up, so "how much is unsettled" is the question with
an answer. Name a consumer to narrow it to that one's share. A monitor reading the unnamed number
there is reading the group's backlog, which is usually what it wants — but it is not one worker's. The key scheme
behind them is this package's business — an exporter should not have to reproduce
`<REDIS_MESSAGES_KEY>:processing:<worker>` by hand.

**Naming one is answerable on the Redis transports only.** The list keeps a key per worker and a
stream group records the consumer each entry went to, so either can be asked about a name.
RabbitMQ knows unacknowledged deliveries as a *channel*'s and Kafka knows uncommitted offsets as a
*member*'s, so both raise `WorkerDepthUnavailableError` rather than return a number — zero being
the answer that would stop somebody looking. On those two, what a dead worker held is already back
in `queue_depth()`: the broker returns an unacknowledged message when the channel drops and the
group replays an uncommitted offset, with nothing to reclaim by hand.

Each returns an `int` — a length at the moment it was read, not a correlation id
and not a reservation. A depth read and then acted on is already out of date, so
these answer "is the backlog growing" rather than "how many will this worker send".

## Handlers

One decorator per aiogram observer, all registering on `bot.router`:

`message`, `edited_message`, `channel_post`, `edited_channel_post`,
`inline_query`, `chosen_inline_result`, `callback_query`, `shipping_query`,
`pre_checkout_query`, `poll`, `poll_answer`, `my_chat_member`, `chat_member`,
`chat_join_request`, `error`.

```python
@bot.message(F.text == '/start')
async def start(message):
    await message.answer('hi')
```

Arguments pass straight through to aiogram, so filters behave exactly as they do
there. See **[Handlers](Handlers.md)**.

## Running and stopping

```python
bot.start_polling()  # attaches the router, then blocks on long polling
bot.close()  # drains in-flight sends, releases the storage, session and loop
```

`close(drain_timeout=None)` waits for sends still pacing behind the rate limiter,
cancels whatever outlasts the wait with a warning, then releases the FSM storage's
own Redis client, the bot's HTTP session and the loop. A closed instance builds
itself again on next use.

The wait defaults to `DRAIN_TIMEOUT`, five seconds, and passing a number overrides
it for that call. It was a hardcoded five before 3.1.0, which `start_tgbot` never
passed — so a deployment could raise `stop_grace_period` all it liked and never buy
the drain a second more. Set the setting rather than the argument: the arithmetic on
**[Deployment](Deployment.md)** adds it up for you.

`start_tgbot` does both around the delivery consumer; you only need them when
running the bot yourself.

## A second instance

```python
own = TelegramBot(max_retries=3)
```

`TelegramBot(max_retries=None, loop=None)` — pass a loop to put its work on one
you already run.

Each instance builds its **own** aiogram `Bot`, HTTP session, dispatcher and,
unless you hand it one, event loop. What they share is the token, which comes
from settings either way, and therefore the rate-limit budget: `get_rate_limiter`
is keyed by token, because Telegram counts per bot and two limiters would let one
bot send at twice the rate. The Redis connection is shared too — it is
process-wide, not per instance.

Polling from more than one instance on the same token is not supported by
Telegram itself: one `getUpdates` consumer per bot.

Prefer the shared `bot`. A fresh instance inside a task or a request means a
fresh event loop and HTTP session that nothing closes — see **[Sending messages](Sending-messages.md)**.

## Module level

```python
from django_aiogram import TelegramBot, bot, conf, __version__
```

`conf` reads `settings.TELEGRAM_BOT` on first access, falls back to
`DJANGO_AIOGRAM_<NAME>` for scalars, and resets itself on `override_settings`.

**`get_redis` and `redis_conn` left this list in 4.0**, along with `bot.redis_conn`. They are
Redis's, and a package that carries four transports should not export one transport's client
from its front door. Nothing about them changed otherwise — same objects, same laziness, one
connection — so code that wants Redis for its own keys imports them from the module that owns
them:

```python
from django_aiogram.redis import get_redis, redis_conn
```

## Values the settings accept

Some settings offer a **fixed set of choices**, and every one of those exists as an enum member,
so a project can import the value instead of spelling the string:

```python
from django_aiogram.config.enums import StorageKind, UpdateMode

TELEGRAM_BOT = {
    'FSM_STORAGE': StorageKind.REDIS,
    'MODE': UpdateMode.POLLING,
}
```

| | Members |
| --- | --- |
| `SerializerKind` | `JSON`, `PICKLE` |
| `StorageKind` | `REDIS`, `MEMORY` |
| `UpdateMode` | `POLLING`, `WEBHOOK` |
| `RateLimitKey` | the three `RATE_LIMIT` keys |
| `SerializationTag` | the `__model__`-style markers a queued payload carries |
| `EventKind` | what an event log row can be: `outbound.*`, `inbound.*`, `fsm.transition`, `queue.*`, `log.dropped` |

They are `(str, Enum)`, so a member compares equal to its string and works
anywhere the string does. `choices(SerializerKind)` gives the plain-string set,
which is what the system checks validate for **these** settings.

**The settings that name an implementation are not among them.** `BROKER` and `DELIVERY` take a
dotted path — to a `Broker` and to a `Delivery` subclass — so their sets are open by design, and
their checks judge a path rather than a membership. `DeliveryKind` is gone in 4.0 for exactly that
reason: it had one member, `BLPOP`, and an enum of one is not a choice. There is no replacement to
look for; write the path, and see **[Delivery](Delivery.md)**.

The values are **frozen**: queued payloads and stored settings carry them, so a
member may be renamed but never revalued.

## The event log

```python
from django_aiogram.eventlog.events import failure_kinds, kind_choices, register_kind
from django_aiogram.eventlog.outcomes import Outcome, SentMessage, aoutcome, outcome
from django_aiogram.models import TelegramEvent
from django_aiogram.eventlog.signals import events_recorded
```

`TelegramEvent` is the append-only feed: one row per thing that happened, insert
only, and `kind` is an unconstrained `CharField` whose legal values live in a
Python registry rather than in the schema. `register_kind(code, label,
failure=False)` adds your own; `kind_choices()` and `failure_kinds()` are what
the admin filters read.

Querying it is ordinary ORM work, and the indexes are built for two questions —
one message's history, and what a chat has seen:

```python
TelegramEvent.objects.filter(correlation_id=identifier).order_by('id')
TelegramEvent.objects.filter(chat_id=chat_id).order_by('-id')[:50]
```

The first of those two has an answer written for it. `bot.outcome(identifier)` — or
`outcome(identifier)`, the function behind it — returns an `Outcome`: a `state` of `sent`, `failed`,
`pending` or `unknown`, the `message_id` and `chat_id` Telegram gave, `at`, `error`,
`attempt`, and `sent` for every message recorded under the id. `bot.aoutcome()` is the
awaiting twin. See **[Event log](Event-log.md#what-became-of-one-message)** for what each
state means and what `unknown` does not mean.

`state` is an `OutcomeState`, which is `(str, Enum)` like the rest — so `== 'sent'` and
`is OutcomeState.SENT` are the same test. It is not a value any setting accepts, which is
why it is here rather than in the table above.

It reads the feed, so it needs `EVENT_LOG` on **and** an `EVENT_LOG_KINDS` that is empty or
keeps the four kinds a correct outcome requires. Either one missing raises
`OutcomesUnavailableError` at the call rather than answering `unknown` for ever — the log
being off is reported as itself, and the missing kinds are named only once it is on, because
a set of kinds means nothing while nothing is recorded.

**That guard is about the reading process only.** The rows are written by whichever process
*sent* the message, and it reads its own settings — so a bot container with the log off, or
with a narrower `EVENT_LOG_KINDS`, leaves the call answering `unknown` with nothing to
refuse. Both processes have to be configured to record the deciding kinds.

A row written since the column arrived also carries a **short id**: the same
message in twelve characters a person can read out, which is what the admin's
thread column shows and what its search box takes. Rows already in the table when
it arrived hold `''` until the backfill named below walks them.

```python
import uuid

from django_aiogram.eventlog.events import normalise_short_id, short_id

short_id(uuid.UUID('a615799d-dce6-42bc-af47-22c6ccf2c525'))  # 'YHS2RV6F5H95'
normalise_short_id('yhs2-rv6f 5h95')  # the same code
normalise_short_id('hello')  # '', not a code
```

`short_id` is a pure function of the correlation id — the low 60 bits in
Crockford's base32, the alphabet without `I`, `L`, `O` and `U` — so a log
pipeline or a support script computes it without touching the database, and
`TelegramEvent.short_id` holds the same value. `normalise_short_id` is the
reading half: it ignores case, spaces and hyphens, folds `I`, `L` and `O` onto
what they are heard as, and answers `''` for anything that is not a code —
including a `U`, which the alphabet drops without giving it an alias.

The column is stored rather than computed, because base32 of a truncated id has
no inverse a `WHERE` clause could use. It is not unique: sixty random bits make a
collision unlikely rather than impossible, so a search returns every row a code
matches and `correlation_id` stays the identifier. Rows already in the table when
the column arrives have none until `manage.py tgbot_backfill_short_ids` walks
them — see **[Event log](Event-log.md)**.

`events_recorded` is the metrics seam: a `django.dispatch.Signal` fired once per
batch with the `Event` objects in it, from the event writer's own thread — except
under `EVENT_LOG_SYNC` and at shutdown, where there is no writer thread to run on:
there they run on the thread that recorded the event, or the one that called
`recorder.stop()` — which is not `bot.close()`, documented above, but the event
writer's own shutdown. `EVENT_LOG_SYNC` only takes effect with the log on, so a
receiver-only process still gets the writer thread whatever that flag says. It fires whether or not `EVENT_LOG` is on, which is
the point: counting what the bot does and keeping a row for it are separate
decisions. `Event`'s field names are pinned by `tests/test_public_surface.py` and
are therefore API.

Nothing here is imported unless you import it: `models.py` pulls no aiogram, so
a migration container pays nothing for it, and `signals.py` pulls neither aiogram
nor the ORM so a metrics module can import it at settings time. See
**[Event log](Event-log.md)**.

## Errors

```python
from django_aiogram.exceptions import DjangoRedisAiogramError
```

| | Raised when | Carries |
| --- | --- | --- |
| `DjangoRedisAiogramError` | base of everything this package raises | |
| `SerializationError` | a payload cannot be encoded, or cannot be decoded | |
| `UnknownApiMethodError` | a call names something that is not a Telegram API method | `function` |
| `LoopUnavailableError` | there is no event loop this call can use; also a `RuntimeError` | |
| `ShuttingDownError` | the bot is closing, so the send was refused rather than queued for a loop that will not run it — a webhook view answers 503 on this, and Telegram redelivers | |
| `LoopThreadNotStartedError` | the loop exists but nothing is turning it, so a hand-off would never be stepped | `timeout` |
| `MalformedEnvelopeError` | a queued payload is not a shape any version of this package wrote | |
| `WorkerDepthUnavailableError` | `inflight_depth` was asked about a *named* worker on a transport that knows unsettled work by channel or by group member rather than by name | `broker`, `worker` |
| `DeliveryNotConfiguredError` | `DELIVERY` names nothing, names a 3.x word, or names something that is not an **instantiable** `Delivery` subclass — `Delivery` itself and one that leaves `run()` abstract are refused too. Also a `ValueError`, which is what 3.x raised | `path` |
| `UnknownEnvelopeVersionError` | a queued payload was written by a newer version than this consumer reads | `version` |
| `UnknownInputFileKindError` | a queued payload names an input file kind this version cannot rebuild | `kind` |
| `UnknownModelError` | a queued payload names a class that is not an aiogram type | `name` |

The **Carries** column is the part a caller may act on. Everything is in the message as well,
but an attribute is a decision a program can make: which method was refused, how long the loop
was waited for, how far ahead the writer of a payload is. Nothing else is kept — a value the
caller already has, or one it can read out of its own settings, stays in the sentence.

The transports refuse a publish in the same shape as each other, and each carries the pair.
`QueueRefusedError` from RabbitMQ carries `queue` and `reason`. `ProduceRefusedError` from Kafka
carries `topic` and `reason`. The reason is what the broker or the driver said rather than a
sentence this package composed, and both classes are `BrokerError`, importable from the
transport's own `exceptions` module.

Catching `DjangoRedisAiogramError` catches all of them. The two you are likely
to name keep the bases they had before the family existed —
`UnknownApiMethodError` is still a `ValueError`, and the serializer errors are
still `SerializationError` — so existing `except` clauses keep working.
Configuration problems remain Django's `ImproperlyConfigured`, since that is
what `manage.py check` and Django's own machinery expect.
