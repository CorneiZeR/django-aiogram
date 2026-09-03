# Event log

An optional table recording what the bot did: a message queued, delivered,
retried or dropped, an update received, an FSM transition, a payload refused.
It exists to answer the question the structured log cannot once it has rotated —
*this user says they never got the message; did we send it?*

**It is not a replacement for [Logging](Logging.md).** Above roughly ten thousand events a
second a database is the wrong tool and a log shipper is the right one. Below
that, a table you can query and join against your own models is worth the write.

```python
TELEGRAM_BOT = {
    'EVENT_LOG': True,
    # part of turning it on, not an afterthought: nothing on the write path deletes
    # anything, so `W006` warns while this is 0, which is also the default — and a project running
    # `manage.py check --fail-level WARNING` in CI, which this documentation recommends,
    # gets a red build for a log that grows for ever. See Pruning below
    'EVENT_LOG_RETENTION_DAYS': 30,
}
```

Then `python manage.py migrate`. The table is created by `migrate` whether or
not the flag is on; nothing reads or writes it until you turn it on.

## One row is one thing that happened

| Column | What it holds |
| ------ | ------------- |
| `created_at` | when the event happened, stamped by whoever recorded it |
| `correlation_id` | ties the stages of one message together |
| `short_id` | the same thread in twelve characters a person can read out — empty on rows that predate the column, see below |
| `kind` | which of the kinds below |
| `function` | the aiogram method, when there is one |
| `chat_id`, `user_id`, `message_id`, `update_id` | the identifiers Telegram issued |
| `worker` | which container recorded it |
| `attempt`, `duration_ms` | how many tries, and how long |
| `error_code`, `error` | why it failed |
| `detail` | everything kind-specific, as JSON |

Rows are **inserted and never updated**. That is what lets two processes write
the same message's history with no coordination: the web process writes
`outbound.queued`, the bot container writes `outbound.sent`, and they line up
through `correlation_id` without either holding a foreign key.

It also means enabling the log in the bot container but not in the web processes
gives you `sent` rows with no `queued` rows to match. That is not a bug.

## The kinds

| Kind | When |
| ---- | ---- |
| `outbound.queued` | a send was written to the queue — a Redis list or stream, an AMQP queue, a Kafka topic |
| `outbound.consumed` | the bot container read it off the queue. Not the same as settled: on Kafka an uncommitted offset is redelivered to whoever takes the partition, and on Redis Streams and RabbitMQ unacknowledged work comes back — `outbound.sent` is the row that says the work is done. The exception is a Redis server without `BLMOVE`, where the list transport's read *is* the removal and a crash mid-send loses the message; the consumer says so at startup |
| `outbound.sent` | Telegram accepted it |
| `outbound.retried` | Telegram refused it with a rate limit; backing off |
| `outbound.failed` | the call raised |
| `outbound.dropped` | it had not completed when the row was written: serialization failed, queueing refused it, retries were exhausted, or shutdown canceled it. `detail.stage` says which. Not the same as *never sent* — a queueing error can follow a write that landed, and a cancellation can land after Telegram took the request |
| `inbound.received` | an update arrived, by polling or webhook |
| `inbound.handled` | the handlers finished |
| `inbound.failed` | a handler raised |
| `fsm.transition` | a chat's state changed |
| `queue.undecodable` | a payload could not be decoded |
| `queue.rejected` | a payload named something that is not a Telegram API method |
| `log.dropped` | the writer fell behind and lost events — the gap, recorded |

`outbound.dropped` is the one worth reading twice. Four different things end up
under that one kind, they are not equally recoverable, and `detail` together with
`error_code` is what separates them:

| Row says | What happened | Send it again? |
| --- | --- | --- |
| `detail.stage: serialising` | the payload could not be encoded, so it never left the process | yes — Redis never saw it |
| `detail.stage: queueing` | the write to Redis raised | **not certainly** — an `RPUSH` that raised may have been applied and only its reply lost |
| `detail.max_retries: N` | Telegram refused it with a rate limit N times over | it was delivered nowhere and nothing will retry it, so yes — but expect the same refusal |
| `error_code: NotScheduled` | it was never scheduled, or was canceled at shutdown; `error` says which | **usually not** — see below |

`NotScheduled` is the one that will bite an operator. A send that came off the
queue is *deliberately* left in the worker's in-flight list when shutdown cancels
it, so a restart under the same worker identity reclaims and delivers it —
re-sending by hand duplicates. A send made directly with `send_raw`, from your own
code rather than from the queue, was never in that list, and nothing will ever
retry it. The rows tell them apart: a queued message has `outbound.queued` and
`outbound.consumed` rows under the same `correlation_id`, and a direct `send_raw`
has neither. See **[Delivery](Delivery.md)** for the guarantee that makes the first case work.

The stages matter most after a broadcast: `send_many` loses the ids of the failing
chunk with the exception, so these rows are the only list of which messages went
missing, and the stage is the only thing that says whether re-sending them would
duplicate.

Register your own:

```python
from django_aiogram.eventlog.events import register_kind

ORDER_NOTIFIED = register_kind('shop.order.notified', 'Order notified')
```

Adding a kind is **not** a schema change — the column is an unconstrained
`CharField` and the registry lives in Python. Register at module scope in
something your `tg_router` imports, or the kind will be missing from that
container's admin filter. Namespace as `<app>.<noun>.<verb>` and keep the total
in the tens: `kind` leads an index, and that index is only worth having while
its cardinality stays low.

## What became of one message

`bot.send()` answers with a correlation id, and the reply Telegram gave is produced in the
bot container — a different process from the one that queued the call. So the `message_id`
was out of reach of whoever sent the message, which is the id an edit or a delete needs.

It has been in the table all along. `bot.outcome()` is what asks:

```python
from django_aiogram import bot

identifier = bot.send(chat_id=CHAT_ID, text='Working…')
...
answer = bot.outcome(identifier)
if answer.state == 'sent':
    bot.send('edit_message_text', chat_id=answer.chat_id, message_id=answer.message_id, text='Done')
```

Four states, and the last two are not the same question:

| `state` | what the feed holds | what it means |
| --- | --- | --- |
| `sent` | an `outbound.sent` row | Telegram accepted the call. `message_id` and `chat_id` are set where it produced a message — a `send_chat_action` has neither, and `answer.sent` still holds an entry for it. A `send_media_group` produces several, and all of them are there |
| `failed` | `outbound.failed`, or a drop the row says is the end | it will not arrive; `error` says why |
| `pending` | `queued`, `consumed`, `retried`, or a drop that may still be delivered | on its way — ask again, within a bound of your own |
| `unknown` | no outbound row an outcome is decided from | nothing has been recorded **yet**, or nothing ever will be. Other rows may exist under the id — a handler's `inbound.*` share it — and none of them decides an outcome |

**Not every `outbound.dropped` row is a failure**, and the difference matters because a
caller told `failed` re-sends. That kind is written from three places:

| the row says | state | why |
| --- | --- | --- |
| `detail.max_retries` | `failed` | Telegram kept refusing and the message was acknowledged |
| `detail.stage` is `serialising` | `failed` | the payload never left the process; re-sending is safe |
| `detail.stage` is `queueing` | `pending` | the publish raised and **may still have been applied** — this is the one not to re-send |
| `NotScheduled`, with an `outbound.queued` row | `pending` | a shutdown refused it without acknowledging, so the next start reclaims it |
| `NotScheduled`, with no `queued` row | `failed` | a send that took the direct route — `send_raw` anywhere, or `send` inside the bot container — was never on a queue, so nothing will reclaim it |

`sent` wins over anything written after it, because an id is not one per message: a
handler's replies inherit the id of the update that caused them, so a later `retried` may
belong to a different message under the same id. `answer.sent` is every message recorded
under it, newest first, and `message_id`, `chat_id` and `at` read the newest — which is the
answer when one `send()` produced one message.

One call can also be several messages on its own: `send_media_group` answers with a list, so
its row keeps every id and `answer.sent` holds one entry each, in the order Telegram posted
them. `answer.message_id` is then the first of the album rather than a choice between them.

**Where an id does name several, the state is about the id and not about one of them**, and
the feed holds nothing finer to narrow it with. The case that shows: one message queued and
another sent directly, both dropped by a shutdown — the queued one is reclaimed by the next
start and the direct one is not, and the rule above can only see that *something* under this
id was queued. It answers `pending` for both, which is the side that cannot duplicate a
message: a caller waiting for a send that is already finished loses a wait, where one told
`failed` about a message the next start will deliver re-sends it and the chat gets two. Pass
an explicit `correlation_id` per send where you need the states apart.

**`unknown` has several causes and the feed cannot tell them apart.** The message has not
got there yet; or the writer dropped the event under pressure, which the section below
explains and a `log.dropped` row marks; or `EVENT_LOG_RETENTION_DAYS` has pruned it; or —
the one that looks least like a misconfiguration — **the process that sent the message does
not record outcomes.** The bot container reads its own `TELEGRAM_BOT`, so one with the log
off or with a narrower `EVENT_LOG_KINDS` writes no row for a message it delivered perfectly
well, and the refusal below cannot fire for a configuration this process cannot see.

So treat `unknown` as *not yet*, give up after a bound of your own, and check the sending
container's settings before concluding anything —
**[Troubleshooting](Troubleshooting.md#botoutcome-says-unknown-for-a-message-that-was-delivered)**
walks the list. This is a feed, not a receipt.

**A configuration that cannot answer refuses instead**, rather than reporting `unknown` for
ever — the one word that means *not yet*. `EVENT_LOG` off is one. The other is an
`EVENT_LOG_KINDS` that leaves out any of the four kinds a **correct** outcome requires.
Six kinds are read; these four are the ones whose absence changes an answer rather than
sharpening it:

The call refuses, so none of the rows below is something you can observe — each is what the
refusal is *for*, which is why it demands the kind rather than answering without it:

| kind | what answering without it would do |
| --- | --- |
| `outbound.sent` | there would be no result at all |
| `outbound.failed` | a refused message would read `unknown`, so a caller polling for an end never reaches one |
| `outbound.dropped` | the same, for the drops above |
| `outbound.queued` | not a missing answer but a **wrong** one: the shutdown-drop rule reads this row, so without it a message the next start will deliver would be reported `failed` — and a caller acts on `failed` by re-sending |

`outbound.consumed` and `outbound.retried` are the other two read, and are **not** required.
They can only ever produce `pending`, so leaving them out moves an in-flight message to
`unknown` — a different word for the same instruction — and costs precision rather than
correctness.

Either refusal is an `OutcomesUnavailableError` at the call, naming every kind that is
missing at once. It is checked when asked rather than at boot on purpose: narrowing
`EVENT_LOG_KINDS` is a reasonable thing to do in a project that never reads an outcome, and
a system check cannot tell whether this one does.

`await bot.aoutcome(identifier)` is the same read without blocking the loop, and
`django_aiogram.eventlog.outcomes.outcome()` is the function behind both for code that has
no bot to hand.

## Nothing waits for the database

Recording hands the event to a bounded in-memory queue; one background thread
drains it in batches. A send never waits on the database, and a database that is
slow or down costs dropped rows, never dropped messages.

When the queue is full the event is dropped, counted, and reported at most once
a minute as `the event log is falling behind`. When the writer catches up it
records a `log.dropped` row, so the gap is visible in the data and not only in
the log.

A row the database refuses on its own — a constraint, a column too small — is counted
the same way, so a batch that lands 39 of 40 rows leaves a `log.dropped` behind it
rather than a silent hole. The count survives a gap row that itself cannot be written: it
is taken off the counter before the row is written and given back if that row does not
land — raised or refused alike — so the next successful flush reports it. Taking it off
first is also what stops two flushes reporting the same hole, since a worker draining by
hand and the writer thread can both be mid-flush at once.

`EVENT_LOG_BUFFER_SIZE`, `EVENT_LOG_BATCH_SIZE` and `EVENT_LOG_FLUSH_INTERVAL`
size it. A batch larger than the buffer can never fill, so `W007` says so.

**A broadcast is where this bites.** `send_many` records one `outbound.queued` row
per message, the same as sending them one at a time — but it removes the pacing
the sequential round trips used to give the writer, so fifty thousand chats arrive
as fifty thousand events in a few seconds rather than spread over minutes. Raise
`EVENT_LOG_BUFFER_SIZE`, or narrow `EVENT_LOG_KINDS`, before the first large one.
The messages are never at risk; the rows about them are.

What is lost: on `SIGKILL`, a worker timeout or `os._exit()`, whatever is in the
queue and in the current batch. At the defaults that is under a second of events
plus up to 200 rows. A clean `SIGTERM` loses nothing. This is an event feed, not
a ledger — if you need durability across a kill, the thing that already gives it
to you is the queue, on any transport that is crash-safe. The exception is a Redis
server without `BLMOVE`, where the list transport says so in the log and delivery drops
to at-most-once; **[Delivery](Delivery.md)** has the guarantee per transport.

## Metrics, without the table

The same events reach a `django.dispatch.Signal`, so a project can count what the
bot does without keeping a row for any of it:

```python
# metrics.py, imported from your AppConfig.ready()
from prometheus_client import Counter

from django_aiogram.eventlog.signals import events_recorded

SENDS = Counter('telegram_events', 'django-aiogram events', ['kind', 'function'])


def count(sender, events, **kwargs):
    for event in events:
        SENDS.labels(kind=event.kind, function=event.function or 'none').inc()


events_recorded.connect(count, dispatch_uid='metrics.telegram')
```

Receivers get `events`: a tuple of `Event`, whose field names are the same ones the
table's columns carry and are pinned as public API. A signal rather than a setting
naming a dotted path, because there is then no path to get wrong, no check id for
it, and no question about what happens when the import fails.

With `EVENT_LOG` on, the write is attempted before a receiver sees the batch, so
nothing a receiver does can change a row that was written — which is why they get
the real objects rather than copies. The `detail` dict inside one is an ordinary
dict, shared with the other receivers, so treat it as read-only.

*Attempted*, not guaranteed, and only when the log is on at all. A write that failed
still publishes, because a database being down is exactly when someone is watching a
dashboard; with the log off there is no write to attempt and a receiver is the only
thing the batch reaches. Either way, a batch arriving is not evidence that a row
exists for it.

Four things about it are worth knowing before you rely on it, and three of them
surprise people:

**It fires with `EVENT_LOG` off.** The table and the metrics are separate
decisions. Connect a receiver, leave the log off, run no migration for it: the
events still arrive. Turn the log on as well and both happen.

**Payload summaries are the only part of `detail` the log gates.** With the log
off, `detail` still carries whatever the recording seam measured itself: a send's
`duration_ms`, a retry's `retry_after`, a queueing failure's `stage`, a gap's
`dropped` count. What is missing is the *summarized arguments* — redacting
credentials out of a payload, walking it and bounding it costs tens of
microseconds, and a counter keyed on `kind` and `function` needs none of it. If
your receiver needs message bodies, it needs the log on too, and then
`EVENT_LOG_PAYLOAD` decides what is in there.

**`EVENT_LOG_KINDS` filters this as well, with one exemption.** It is one answer to
"which events does this deployment care about", not two — so a receiver sees
exactly the kinds the table would have kept. `log.dropped` is exempt in both
directions: it is the record that recording itself fell behind, and a deployment
that filtered it out would read the hole as quiet traffic rather than as a gap.
The table has always been exempt for that row; receivers are exempt with it.

**Connect during app loading.** The update middleware and the FSM storage wrapper
are built once, and whether to build them is decided then. A receiver connected
after the first update arrives will not see updates in that process. An
`AppConfig.ready()` is where Django says signal receivers belong, and it is early
enough.

### Where it runs, and what that costs

The rule is **whichever thread flushed the batch publishes it**, and normally that is
the event writer's own — with `EVENT_LOG` on, after that batch's own write has been
attempted. So a slow receiver delays neither a send nor the rows it just saw, only
later batches and how long the writer takes to stop. Never delaying a send is the
whole reason this is not a settings hook calling into your code from the send path.

Three other threads can flush a batch, so three other threads can publish one:

* whatever calls `recorder.drain_once()`, which exists so a test can drive the real
  flush path on its own thread

* under `EVENT_LOG_SYNC`, on the thread that recorded the event, after its write
  attempt. That flag only takes effect with the log on — there is nothing to insert
  synchronously otherwise — so the write is always attempted there, and may still
  fail. It is a testing setting, and receivers running inside the send path is one
  more reason to keep it one
* at shutdown, on whichever thread called `stop()`, for whatever the writer had not
  drained by then. Those are published rather than dropped because they are the last
  events before the process goes, and there is no writer left to hand them to

A receiver that raises is logged as `an events_recorded receiver raised` and costs
neither the other receivers their batch nor the database its rows. `send_robust` is
most of that: Django hands the exception back instead of letting it end the writer.

Not all of it. Django's failure logging reads `receiver.__qualname__`, which a *callable
instance* does not have — so for that shape `send_robust` raises rather than containing
anything, measured on Django 6.1. Two things answer that: connecting a receiver gives it a
name, so the dispatch does not break in the first place, and this package catches the
exception anyway. Without the catch it would be counted as a failed database write, which is
the one story in the log that would send you to the wrong place entirely.

That used to have a consequence the catch could not reach: `send_robust` names the failing
receiver in its own log line, looking the name up *inside* its `except`, and a callable
instance has none — so the lookup raised out of the dispatch and **receivers after the
offending one never saw that batch**. Connecting a receiver names it now, from its class, so
the loop survives a receiver that raises — for every receiver that can be named, which is
every function, every bound method and every ordinary instance.

Where the name cannot be set — a class with `__slots__`, a read-only property — connecting
logs a warning saying so, because that receiver can still end a dispatch. Everything else is
unchanged: the rows were written first, and a receiver that raises is still contained.

Two honest notes about `prometheus_client` in particular. Its `labels()` and
`inc()` both take locks, and in multiprocess mode an increment is an mmap write —
cheap, but not free, and it happens once per event in the batch. And `outbound.sent`
is recorded inside the `start_tgbot` worker, which serves no HTTP at all: the
exporter has to be stood up **in that container**, or the numbers you scrape from
the web tier will only ever cover queueing.

No new dependency comes with this. `django.dispatch` is Django, and nothing here
imports `prometheus_client` or knows it exists.

## Message bodies are not stored by default

`EVENT_LOG_PAYLOAD` is `'summary'`: argument names and text lengths, not the
text. Set it to `'full'` to store bodies, and treat that as the personal-data
decision it is. `'none'` stores no payload at all.

Credentials are stripped from `detail` and `error` either way. That matters more
than it sounds: the bot token is in the API URL, aiogram puts the URL in its
exception messages, and those messages are what an `error` column holds.

## The admin, and who may see what

With the flag on and `django.contrib.admin` installed, the feed appears in the
admin as a read-only list. Add, change and delete are refused outright — rows
leave through `tgbot_prune_events`, not one at a time.

Two permissions decide what a reader sees, and each gates something real:

Django's four stock permissions are created as usual. One is added, because
Django has no equivalent for it:

| Permission | Grants |
| ---------- | ------ |
| `view_telegramevent` | the list and the detail page: when, which kind, which chat, which method, the error code |
| `view_telegramevent_payload` | the `detail` and `error` columns — message bodies under `EVENT_LOG_PAYLOAD: 'full'`, and exception text |

That split is the point: support needs to see that a message went out and when,
without reading what it said. There are no field-level permissions in Django, so
there is no way to express it with the stock four.

`add`, `change` and `delete` exist but the admin refuses all three, since the
feed is append-only and rows leave through `tgbot_prune_events`.

```python
from django.contrib.auth.models import Group, Permission

support = Group.objects.create(name='Telegram support')
support.permissions.add(Permission.objects.get(codename='view_telegramevent'))

operators = Group.objects.create(name='Telegram operators')
operators.permissions.set(
    Permission.objects.filter(
        content_type__app_label='django_aiogram',
        codename__in=['view_telegramevent', 'view_telegramevent_payload'],
    )
)
```

Permissions live wherever `django.contrib.auth` does. If the log is on another
alias, leave `auth` where it is — the router in this package moves only its own
app, and dragging `auth` along would move your users with it.

What the admin deliberately does not do, because the table is sized by traffic:
no full result count, no date drilldown (its truncation is a scan no index can
serve), and no substring search — the three searchable columns are matched
exactly, so each uses its index. Sorting is limited to three columns:
`created_at`, `kind` and `chat_id`. Sortable, not merely indexed: `short_id` has an index too, for
the exact search, and is deliberately not sortable because ordering messages by a random code
answers nothing. The other headers are not links, and an `?o=`
naming one of them — from a bookmark, or a link shared before this restriction — is
dropped rather than honoured, because ordering the whole table by `worker` is a
sequential scan and a sort on every page.

Paging counts at most **10 000 rows**, inside a `LIMIT`. The number is exact for
the filtered views people actually read and stops growing past the cap, so the
deepest pages are unreachable; at that depth the answer is a filter, not another
page. Django would otherwise run `COUNT(*)` over the whole filtered queryset on
every page load.

A `LIMIT` only stops early if the rows arrive already ordered, which is why the
kind index is on `(kind, -id)` — the same `-id` the changelist orders by. Before
3.1.0 it was `(kind, -created_at)`, so every filtered page sorted in a temporary
b-tree first and the bound above was not true. The list also leaves `error` and
`detail` in the table: it renders neither, and between them they are most of what
a row weighs. The detail page asks for them back.

### The short id

A row written since the column arrived carries a twelve-character code in `short_id`, and that is
what the thread column shows and what the search box takes. Rows older than the column hold `''`
until the backfill below fills them, which is the one exception to everything this section says. It is the low 60 bits of the correlation id in Crockford's base32 —
the alphabet without `I`, `L`, `O` and `U`, so nothing on screen is confusable with anything else
on screen.

That column used to show the correlation id's first eight characters. Those are a clock, and a
coarse one: a UUIDv7 opens with a 48-bit millisecond, and eight hex characters are that clock's
top 32 bits — `ms >> 16`, measured on a real id — so they step once every 2**16 ms, 65.5 seconds.
Every message inside one of those steps wore the same label, and typing it back found nothing,
because Django refuses a partial UUID before the query is built. The prefix named the
minute-and-a-bit it happened in; the code names the message.

Reading one back is deliberately forgiving. Case, spaces and hyphens are ignored, and `I`, `L` and
`O` fold onto `1`, `1` and `0`, so a code read out over a call still lands when somebody says "oh"
for a zero. `U` is the one the alphabet drops without folding — Crockford leaves it out to make an
accidental obscenity less likely, not because it looks like `V` — and folding it would turn a
mistyped code into a different valid one instead of into nothing.

A term that is not a code is not searched for as one. A number a `BIGINT` can hold goes to
`chat_id`, a full UUID to `correlation_id`, and everything else — a longer number among them, since
asking for one is an error from the backend rather than an empty page — is answered with nothing
rather than handed to the database. A twelve-character term of nothing but digits is a legal code
and a plausible chat id, and the chat id wins: codes of only digits are about one in a million,
while chat ids that long are ordinary.

It is **stored rather than computed**, which is the whole reason the column exists: base32 of a
truncated id has no inverse a `WHERE` clause can use, so a computed code could only be searched by
scanning the table. Twelve indexed bytes, measured at +57 bytes a row with the index. Sixty random
bits make a collision unlikely rather than impossible, so the column is not unique — the search
returns every row a code matches, and `correlation_id` stays the identifier.

### Filling it in on history

`migrate` adds the column empty. Filling it is a command rather than a data migration, for the
same reason the prune is a command: `migrate` runs inside a deploy, and a table sized by traffic
cannot be paced, stopped or resumed there.

```shell
python manage.py tgbot_backfill_short_ids
```

The same arguments as the others — `--chunk`, `--sleep`, `--max-chunks`, `--database`, `--dry-run`
— and one chunk per transaction. A row with no code is a row still to do, so there is no watermark
to keep: a run you stop leaves the rows it committed done and the next one continues, and a run
over a finished table reports that and writes nothing.

Until it finishes both states are on the page at once. Rows written since `migrate` carry their
code; older ones read `(not backfilled)` and still link to the rest of their thread by correlation
id.

## Growth, and the job that bounds it

Budget roughly **0.3 kB per event** including indexes. A million events a day is
about 0.3 GB a day, so thirty days of retention is around 9 GB. Storing message
bodies pushes the per-event figure up, so measure rather than trust it once
`EVENT_LOG_PAYLOAD` is `'full'`.

Nothing on the write path deletes anything. Set `EVENT_LOG_RETENTION_DAYS` and
schedule the command; `W006` warns while it is `0`, which is its default, because the
feature is not finished without it:

```shell
python manage.py tgbot_prune_events
```

It deletes by primary-key range in bounded chunks, one transaction each, so it
never holds a long lock and never competes with the inserts arriving at the
other end of the table. `--sleep` paces it for replicas, `--max-chunks` bounds a
nightly run, `--dry-run` reports without deleting.

On PostgreSQL the space returns via autovacuum rather than immediately; after a
large first prune, a plain `VACUUM` (never `FULL`, which takes an exclusive
lock) is the follow-up.

**Do not put a `ForeignKey` on `TelegramEvent`.** It breaks Django's fast-delete
path, and every prune then has to fetch primary keys first.

### Rows written before 4.0

The table is `django_aiogram_event` now, and `migrate` creates it empty. Rows written by 3.x are
in `django_redis_aiogram_event`, where nothing reads them and nothing drops them — `I003` says so
on every `manage.py check` until they are moved or the table is gone.

```shell
python manage.py tgbot_move_events
```

The same shape as the prune above, and the same arguments: `--chunk`, `--sleep`, `--max-chunks`,
`--database`, `--dry-run`. It copies by primary-key range, one transaction per chunk, naming every
column rather than `SELECT *`: a mismatched column count is rejected, while a matching count in a
different order is accepted with every value one column to the side. Naming them fails on the first
and makes the second impossible.

The names come from what the two tables share, asked of the database — the old one is frozen at
3.x's shape while this one moves on. A column introduced only in 4.0 is written with the model's
default, which for `short_id` is empty, so moved history arrives ready for
`tgbot_backfill_short_ids` above.

**Stopping it is safe, and so is running it after the bot has been writing.** Both tables share the
primary key, so an id already in the new table is either a row the command copied or a row this
release wrote — either way it is not inserted again, and each chunk copies only the ids that are
not there yet. A killed run resumes at the first id the destination does not have, a finished one
copies nothing, and running it twice is not a way to duplicate history.

Starting above the destination's *highest* id would be cheaper and wrong: the bot writes to the new
table from the first message after `migrate`, so a destination that has been written to at all
would skip every old row beneath its highest id and report a completed move.

**An id can be taken.** A row this release wrote may hold an id an old row also has, and nothing can
put both under one primary key — so that old row is left where it is and the command says how many
such rows there are, on every run, including the one that copies nothing. That last part is the one
that matters: "every id is present, nothing is left to copy" is what somebody reads before dropping
the old table, and it has to be followed by the rows that are only in it.

The count is taken across the whole old table rather than the ranges being copied, because a
destination that already holds low ids puts the resume point above them — no chunk would ever look —
and it is taken after the copy, so a row that lands while a chunk is running is counted too. A row
is told from a copy of itself by comparing every column: two rows can share an id, a timestamp and a
kind and still differ in the payload or the chat, and the answer decides whether you can drop a
table.

Comparing them, and deciding what the history is worth, is yours: this will not renumber your rows
to make a total tidy.

A row that lands *while a chunk is being copied* is the same collision arriving late, and the chunk
is retried rather than lost — each retry excludes what landed. It is likeliest before the first run,
when the sequence is still where `migrate` left it and the bot is drawing the very ids the old table
used, so the quiet night is worth choosing for more than the load.

It also moves the sequence past the ids it inserted. That step is what a hand-written
`INSERT ... SELECT` leaves out, and on PostgreSQL its absence is not visible until the *bot's* next
write fails on a duplicate primary key.

Dropping `django_redis_aiogram_event` afterwards is yours to do. This package will never run it
for you.

## A separate database

```python
TELEGRAM_BOT = {'EVENT_LOG_DATABASE': 'logs'}
DATABASE_ROUTERS = ['django_aiogram.eventlog.dbrouter.TelegramEventLogRouter', ...]
```

The writer and the admin name the alias explicitly, so the log lands in the
right database even with no router installed. The router is what makes `migrate`
create the table there. Put it **first** in `DATABASE_ROUTERS` — Django takes
the first non-`None` answer — and run `migrate --database=logs`.

`W005` fires when the log is on and its database has no engine, and `E041` when
the alias is not in `DATABASES` at all. Both matter because the writer runs on a
thread nobody is watching: without them the failure is a log line in a container
nobody reads.

## Partitioning, and what this package will not do

The package will not shard or partition the table. What it does is stay
partitionable, which is a real property and not a consolation: no foreign keys
in or out, no constraints, no unique index other than the primary key, and only
inserts, selects and range deletes.

That is exactly the set of properties an operator needs to take the table over
out of band. The sketch below is for an **empty** table — do it right after
`migrate`, before the log is switched on:

```sql
ALTER TABLE django_aiogram_event RENAME TO django_aiogram_event_old;

CREATE TABLE django_aiogram_event (
    LIKE django_aiogram_event_old INCLUDING DEFAULTS INCLUDING IDENTITY
) PARTITION BY RANGE (created_at);            -- no primary key on the parent

CREATE TABLE django_aiogram_event_2026_08
    PARTITION OF django_aiogram_event FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');
CREATE INDEX ON django_aiogram_event_2026_08 (id);   -- per partition, per index

DROP TABLE django_aiogram_event_old;
```

`INCLUDING IDENTITY` is not decoration: Django creates the primary key as
`GENERATED BY DEFAULT AS IDENTITY` rather than a `serial` with a default, and
`INCLUDING DEFAULTS` alone copies nothing for it — every insert that omits `id`
then fails on the not-null constraint.

Three things that sketch does **not** do, and that a table with rows in it
needs: `LIKE` copies neither the indexes nor the primary key, so each partition
needs its own; a partition has to exist for every date
that will be written, past and future, or the insert fails outright; and the
existing rows have to be moved with an `INSERT ... SELECT` from the old table
before it is dropped. Work all three out for your data before running anything.

Retention then becomes `DROP TABLE` on a whole partition — instant, no dead
tuples. All of this is **unsupported**: `migrate` must not touch this app on
that database afterward. Django's migrations have no representation for
`PARTITION BY`, and both PostgreSQL and MySQL require every unique key to
contain the partition column, which an auto-incrementing primary key cannot
express.
