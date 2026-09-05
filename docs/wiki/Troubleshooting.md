# Troubleshooting

## Nothing is delivered, no errors anywhere

Check the bot container is actually running the bot:

```shell
docker compose logs telegram_bot | grep 'delivery started'
```

No such line means either `ENABLED` is off in that container, or the command
never got past startup. `manage.py check` will say which.

## `manage.py check` fails with `E009` after upgrading

`DELIVERY` is a dotted path since 4.0, so the words that used to go there no longer
do. `'blpop'` is now `'django_aiogram.consumer.delivery.BlpopDelivery'` — or drop the
key, which is the same thing by default. `'keyspace'` was the 1.x mechanism, removed
in 3.0, and there is nothing to write instead. `E009` names both by name rather than
reporting "not a dotted path", because a settings file being upgraded has one of them
in it.

The check fails rather than falling back silently, because a consumer that quietly
changes is worse than one that refuses to start. What `E009` cannot tell you is whether
a *plausible* path imports and is a `Delivery`: resolving it would make every
`manage.py check` import aiogram. `start_tgbot` settles that before it starts a thread.

## A send hangs instead of failing

`REDIS_TIMEOUT` (10 seconds by default) bounds both connecting and waiting for an
answer, so a Redis that accepts the connection and then stops responding raises
`redis.exceptions.TimeoutError` rather than holding the request thread.

redis-py only started applying a read deadline of its own in 8.0. On 5.x, 6.x
and 7.x a stalled server blocks the caller until the process is killed, which is
why the package sets the deadline itself rather than relying on the client.

## Messages pile up in the queue

```python
from django_aiogram import bot

bot.queue_depth()  # messages waiting for a worker
bot.inflight_depth()  # what this worker is part-way through sending — on Redis Streams, the group
```

Those two answer for whichever transport `BROKER` names. From a shell there is no
one command, because each transport counts differently — for the Redis list:

```shell
redis-cli -n <db> llen TELEGRAM_BOT_MESSAGE
```

`TELEGRAM_BOT_MESSAGE` is the default `REDIS_MESSAGES_KEY`; if you set your own, it
is that key here and in every Redis path below. On a stream it is `XLEN`, on AMQP the queue's
message count, on Kafka the lag of the consumer group — which is the reason to prefer
`queue_depth()` in anything you keep, an exporter especially. It asks the same question on every
transport, and it reads a scheme that is ours to change.

Expect the shell figure and `queue_depth()` to disagree on Kafka while sends are in flight, and
do not go looking for a bug: raw group lag counts every record past the committed offset,
including the ones this process has taken and not yet settled, and `queue_depth()` subtracts
those. The gap is what the worker is holding, so it closes as the sends finish. Offsets settle a
contiguous prefix there, so the gap can also outlast the sends that caused it — see
**[Delivery](Delivery.md)**.

`inflight_depth()` is **not** the same everywhere, and the difference decides where you can ask
it. The Redis list keeps its in-flight messages on the server under the worker's name and Redis
Streams keeps them in the group's pending list, so either can be read from anywhere — a monitor
in the web tier included. On RabbitMQ and Kafka the count is process-local instead, for
two different reasons: AMQP tracks unacknowledged deliveries per channel and a client sees its
own, so another's is a management-API question rather than a protocol one, while Kafka has
nothing to ask at all — an offset is either committed or not. Asked anywhere else it
answers **zero**, correctly and uselessly. On those two, run it in the bot container or read the
broker's own tooling.

The same split decides whether you may **name** a worker. `inflight_depth('some-worker')` reads
that worker's in-flight work on the Redis pair; on RabbitMQ and Kafka **any** name raises
`WorkerDepthUnavailableError` — the calling worker's own included — because the unsettled work
there belongs to a channel or a group member rather than to a name this package chose, so there is
nothing for a name to match. That refusal is deliberately not a zero: on those
two transports what a dead worker held is already back in `queue_depth()` — the broker returns an
unacknowledged message when the channel drops, the group replays an uncommitted offset — so the
number to look at is the queue's, and there is nothing to reclaim by hand.

A growing list does not by itself mean the consumer is stopped: producers can
simply be outpacing it, and `MAX_IN_FLIGHT` deliberately holds intake back while
sends are outstanding. Check the heartbeat and the in-flight list below before
concluding the worker is down — see above for one that genuinely is. Messages
wait in the queue until a worker takes them. On Redis 6.2+ a taken message sits in
`<key>:processing:<worker>` until the send has finished, and a restart under the
same **worker identity** reclaims it: at-least-once, so a crash mid-send can
duplicate a send. That identity is `WORKER_NAME` when it is set and the hostname
otherwise — so on a platform that gives each container a fresh hostname, a
recreated worker looks like a different one and leaves the old list untouched.
Set `WORKER_NAME` to something stable wherever hostnames change.

All of that holds for the worker `start_tgbot` runs; a handler of your own is
only held that way if it takes an `on_complete` keyword.

That list is expected to be non-empty while sends are in flight, and an entry
stays until its send finishes or shutdown cancels it. `MAX_IN_FLIGHT` bounds how
many sends the consumer leaves outstanding, and so how far the list can run
ahead. Without `LMOVE` it is at-most-once, unless `REQUIRE_CRASH_SAFE` is on —
then the worker refuses to start rather than deliver that way. A send that
exhausted `MAX_RETRIES` is logged and acknowledged, not redelivered.

## The container is unhealthy while the probe says `healthy`

The probe was killed by Docker's `timeout`, so its exit code never arrived — `docker
inspect` shows `ExitCode: -1` and a growing `FailingStreak` beside a last line that
reads `healthy: consumer 6s old, 0 queued`. The age is what the *transport* says: a
Redis list has a heartbeat key the consumer writes, and a stream reads it off its
consumer group, which is why the line says `consumer` rather than naming one of them.
A transport that cannot answer at all says `consumer not observable from outside`, and
that is a pass rather than a failure — there is nothing to look at, so the depth is the
whole verdict.

It happens when the healthcheck runs `manage.py tgbot_healthcheck`, because a
management command runs `django.setup()` first: the whole of your `INSTALLED_APPS`,
every `AppConfig.ready()`, before the first Redis call. In one project that was 17.9
seconds. Use the form that does not:

```yaml
    environment:
      DJANGO_SETTINGS_MODULE: core.settings   # a healthcheck is a separate process
    healthcheck:
      test: ['CMD', 'python', '-m', 'django_aiogram.healthcheck']
      timeout: 5s
```

If the probe answers `cannot read the settings: …` instead, that variable is the thing
missing: `manage.py` sets it inside its own process, so a container running it may
never export it.

See **[Deployment](Deployment.md)**. Raising `timeout:` also stops the killing and leaves your whole
Django app being imported twice a minute to read two keys.

## What the healthcheck's refusals mean

Every line below is what the probe writes to stderr before exiting 1, from either form.
Grepping one out of `docker inspect` should land here.

| The line | What it is telling you |
| --- | --- |
| `the broker is unreachable: …` | The transport could not be addressed or refused the connection. Covers a missing or malformed `REDIS_URL`, an unreadable `REDIS_TIMEOUT`, and a broker that is genuinely down or has dropped the connection mid-probe. This line was `redis is unreachable` up to 3.1 — it moved when the probe stopped pinging Redis before every check, which is what lets it run at all on a transport that is not Redis. The old wording survives in one place: `--guarantee` and `--stranded` build a Redis client of their own, and when that fails they log `redis is unreachable` as `tg_reason` rather than refusing — the guarantee then reads `unknown`, and the sweep says it did not finish |
| `… needs the '…' package, which is not installed. Install it with: pip install "django-aiogram[…]"` | `BROKER` names a transport whose driver this image does not carry. Every driver is an extra since 4.0, so an image built for one transport and pointed at another gets this — the line names the extra that fixes it. Both forms of the probe report it; neither tracebacks |
| `TELEGRAM_BOT['…'] is not a number: …` | `HEARTBEAT_INTERVAL` or `HEALTHCHECK_MAX_QUEUE` holds something `int()` refuses. `manage.py check` reports these as `E023`/`E024`, but the container form never runs it — that is the point of it — so it says so itself |
| `cannot read the settings: …` | `DJANGO_SETTINGS_MODULE` is missing from the container's environment, or names a module that does not import. See the section above |
| `no heartbeat has been written: nothing within Ns, or the consumer never started` | Redis list: the key is absent — the consumer never ran, died before its first beat, or has been silent longer than the key's TTL. If the line adds that a limit over the TTL cannot be observed, `--max-age` is set above `3 × HEARTBEAT_INTERVAL` and is doing nothing |
| `no consumer has joined the group: nothing within Ns, or the consumer never started` | Redis Streams: the group exists and nothing has ever read from it. Nothing is written for this transport — the group's own record of when each member last spoke is the signal — so this means no worker has started, not that a key is missing |
| `the consumer last reported Ns ago, over the Ns limit` | The transport can see the consumer and it has been quiet too long. What was measured depends on which one: the Redis list reads the heartbeat key its consumer writes on every pass, and Redis Streams reads how long ago any member of the consumer group last spoke — which a blocking read that finds nothing refreshes, so an idle queue is not a stale consumer. Either way the worker is wedged, gone, or slower than `HEARTBEAT_INTERVAL × 3` |
| `the heartbeat is not a timestamp` | Redis list: something else writes to that key. Give the worker its own `REDIS_MESSAGES_KEY`, or its own database |
| `could not read the consumer liveness: …` | `PING` answered and the next command did not: a failover in between, a replica that cannot serve the key, or `decode_responses` in a URL shared with a cache backend meeting bytes it cannot decode |
| `could not read the queue length: …` | The same, one command later |
| `N messages are queued, over the limit of N` | Work is backing up. `HEALTHCHECK_MAX_QUEUE` or `--max-queue` is what set that number; see **Messages pile up in the queue** above |

Three lines are not refusals and do not change the exit code:

| The line | What it is telling you |
| --- | --- |
| `N message(s) are in flight under other worker names …` | Written to stderr while still exiting 0, and only with `--stranded`. Another worker may be sending them this second; if it is gone, `manage.py tgbot_reclaim --worker <name>` requeues them. `at least N` means the bounded sweep stopped early, so the count is a floor — and the next line says why |
| `the scan for stranded in-flight lists did not finish: …` | The sweep was asked for and could not walk the whole keyspace: no client (an install with no redis-py, or an unusable `REDIS_URL`), a key it could not decode, or the twenty-`SCAN`-round bound. Without this line a zero would be indistinguishable from a sweep that never ran. Where the transport keys no in-flight work on a worker name at all — Redis Streams, RabbitMQ, Kafka — the sweep is not attempted and nothing is written |
| `disabled in this process; nothing to check` | `ENABLED` is off here, so nothing is meant to be running and nothing is wrong. Exit 0, and deliberately not colored as a success |

Two more reach the log rather than the output — both mean the probe declined to answer
that part rather than fail the container over it:

- `could not scan for stranded in-flight lists`
- `could not establish which delivery guarantee is in force`

## The webhook answers 503, or every update 403s

**503** means the view refused the update rather than handling it, so Telegram will
redeliver — which is what you want. Four reasons, each with its own log line:

- `webhook received an update while the bot is disabled` — `ENABLED` is off here
- `webhook is not configured to serve updates` — `MODE` or `WEBHOOK_SECRET` cannot be
  read: an unknown mode and an empty secret each raise `ImproperlyConfigured`, and this is
  the view answering for it rather than raising through. The secret is only read once the
  mode says to serve, so a polling deployment is told it polls instead
- `webhook received an update while this deployment polls` — `MODE` is not `webhook`, so
  a worker is polling and this process must not also feed the dispatcher
- `webhook cannot build the bot` — building it raised `ImproperlyConfigured`; a
  missing or malformed `TOKEN` is the common example, not the only one
- `webhook refused an update` — nothing ran it: the process is shutting down, its loop was
  already closed by an earlier `close()`, or the loop's own thread had not started yet.
  The closed-loop case is worth knowing about in a web worker that stays up — something
  closed the bot and requests kept arriving

**403** means the `X-Telegram-Bot-Api-Secret-Token` header did not match
`WEBHOOK_SECRET`. Check that the value you registered with `manage.py tgbot_webhook set`
is the one the process now reads — rotating the setting without re-registering gives
exactly this. The comparison is on bytes, so a secret outside ASCII is compared like any
other: a matching one passes, and a mismatched one gets this 403 rather than a traceback.

**400** means the body did not parse as an update. Something other than Telegram is
posting to that URL.

## Handlers never fire

```python
from django_aiogram import bot

len(bot.router.observers['message'].handlers)
```

Zero means autodiscovery did not find them. Usual causes:

- the file is not called `tg_router.py` (or `MODULE_NAME` says otherwise)
- the app is not in `INSTALLED_APPS`
- `AUTODISCOVER` or `ENABLED` is off in that process

If a router raises while importing, the error surfaces at startup — it is not
swallowed. 1.x did swallow it, so a typo there disabled the whole file
silently.

## The project will not start without a token

It should. 2.0 does not build a bot, and no release since connects to the broker at import
time. If it still fails, something in *your* code is touching `bot.bot`, `send_raw` or a depth
read at import time — those are the points that genuinely need credentials. `bot.redis_conn` used
to be on that list and is gone in 4.0. `django_aiogram.redis.redis_conn` is the same object, and
importing it costs nothing — it is a proxy, and only *using* one, `redis_conn.ping()` say, opens
the connection that needs credentials.

Placeholder tokens are no longer necessary; drop them.

## FSM state is lost on restart

`FSM_STORAGE` is `'redis'` by default. If you set it to `'memory'`, state lives
in the process and does not survive. 1.x had no storage at all, so this is
often left over from then.

## Duplicate messages

Check whether two bot containers are polling the same token — Telegram allows
one `getUpdates` consumer per bot.

Each message goes to one worker on every transport — an atomic pop, a consumer group handing
an entry to one member, a broker delivering once — but that is ownership, not exactly-once.
Delivery is at-least-once wherever the transport can recover a message it handed out — every
one here except a Redis list without `LMOVE`, which is at-most-once and loses rather than
duplicates. So the question is which duplicates *this* transport produces:

| Transport | Where a duplicate comes from |
| --- | --- |
| **[Redis list](Redis-list.md)** | a worker killed mid-send, reclaimed by the next start **that resolves the same name** — the in-flight list is keyed on it, so a replacement with a fresh hostname strands the message instead. And two workers sharing a `WORKER_NAME` share one list, so each reclaims what the other is still sending — give every worker its own name, and one it keeps |
| **[Redis Streams](Redis-Streams.md)** | an entry idle longer than the liveness TTL is claimed by another consumer, so a worker slow enough to look dead has its message taken |
| **[RabbitMQ](RabbitMQ.md)** | a dropped channel requeues everything it held unacknowledged |
| **[Kafka](Kafka.md)** | **a run rather than a message.** One refusal rewinds its partition, so that record and every later one in it are delivered again; a kill replays from the last committed offset, which is at least what was in flight and can be more — records that had already finished stay behind a commit gap and come back with it |

The Kafka row is the one to read before assuming a bug. Nothing there is per-message, so a
handler that is not idempotent on its own business key will send innocent messages twice.

## A message went out for a transaction that rolled back

`bot.send()` writes to the broker where it is called, so an `atomic()` block that raises
after a send leaves the message queued and its row gone. Set `TRANSACTIONAL` to `True` to
hold the write until the commit — see
**[Sending messages](Sending-messages.md#inside-a-transaction)**.

It covers the queue route only. Inside the bot container `send` calls Telegram itself, and
`send_raw` does so from anywhere: there is no queue write to hold back, so a message sent
that way still goes out when the block rolls back.

With it already on and the message still going out, check which connection the block is on:
the setting watches `default`, and a transaction opened on another alias is not one it can
see. An alias configured `AUTOCOMMIT: False` is the other case — a manually managed
transaction does not run commit hooks when it ends, `atomic()` blocks inside it included, so
the write is made immediately and the log says so once: *publishing without waiting for a
commit*.

## `bot.outcome()` says `unknown` for a message that was delivered

`unknown` means none of the outbound rows an outcome is decided from has been recorded
against that correlation id — other kinds may well exist under it, a handler's `inbound.*`
among them, and none of those decides anything. Several things produce that:

- **The row has not been written yet.** The writer batches, so allow
  `EVENT_LOG_FLUSH_INTERVAL`.
- **The event was dropped under pressure.** That leaves a `log.dropped` row behind it; look
  for one around that time.
- **`EVENT_LOG_RETENTION_DAYS` has pruned it.**
- **The process that *sent* the message does not record outcomes.** This is the one that
  looks least like a configuration problem, because the reading process is configured
  correctly — see below.

**Two processes, two settings files, and the reading one cannot see the other's.** Getting
`unknown` rather than `OutcomesUnavailableError` says only that **this** process could
record an outcome: `EVENT_LOG` on, and `EVENT_LOG_KINDS` empty or keeping all four of
`outbound.sent`, `outbound.failed`, `outbound.dropped` and `outbound.queued` —
**[Event log](Event-log.md#what-became-of-one-message)** says what each is for. The bot
container reads its own `TELEGRAM_BOT`, so the refusal cannot fire for *its* configuration:
that is [the event log writes nothing](#the-event-log-writes-nothing) below, item 3, and the
half-a-story it describes is exactly this symptom seen from the reading end. Nor does the
refusal say the writer succeeded; that is what the `log.dropped` row above is for.

So check the sending container's settings before concluding the message never went out.

Reading `unknown` for everything, on a project with `EVENT_LOG_DATABASE` set, means the
query is going to the wrong alias — `outcome()` uses the log's, so this is a sign of a
hand-written query rather than of this one.

## A scheduled send never goes out

Nothing on the write path publishes one, so check first that
`manage.py tgbot_dispatch_scheduled` is actually scheduled — from cron, or running with
`--loop`. `--dry-run` answers the question directly: it counts what is due, what is not due
yet and what a mover has claimed, and claims nothing itself.

Then, in the order these bite:

- **The mover refuses to start** where `ENABLED` is false, because a scheduled send would
  have nowhere to go. It says so and claims nothing.
- **`--grace` dropped it.** A row more than that many seconds overdue is recorded as a drop
  with `TooLate` rather than sent late; the feed says which and by how much. `--grace 0` is
  the default and refuses nothing — however overdue a row is, it goes out.
- **The broker kept refusing it and the mover gave up.** After `--max-attempts` failed
  publishes, five by default, the row is deleted with a `TooManyAttempts` drop naming the
  count in `detail.attempts`. `--max-attempts 0` retries without end instead — the row's own
  counter has no ceiling to reach, while the drop event's `attempt` column stops at 32767.
- **A row is gone and there is no drop row for it at all.** Two causes, both by design. A
  mover killed between publishing and deleting leaves the row for its lease to lapse, and the
  next pass publishes it again — a duplicate rather than a drop, which is what at-least-once
  means here. And a mover whose lease lapsed *while* it was publishing no longer owns the row,
  so it records no `TooManyAttempts` or `TooLate` about it: the mover that does own it decides
  what happens, and a drop row from the one that lost the race would be a claim about a
  message the winner may have delivered. Look for the `outbound.sent` under the correlation id
  before looking for a drop.
- **A row is claimed and still there.** Wait one `--lease` (300 seconds by default) and
  another mover takes it back: a claim is a lease, not a deed, because a mover that died
  holding a row would otherwise strand the message for ever. What that costs is a second
  copy where the mover died *after* publishing, which is the trade this package makes
  everywhere — at-least-once, never silent loss, **with a finite lease**. A publish that
  outlives its own lease is the same exposure from the other end: nothing fences a request
  already in flight to another system, so keep `--lease` comfortably above the deadline the
  transport puts on one call. The mover says so in the log when it is not. Under `--lease 0`
  a claim never lapses, so a mover that died holding one leaves that row where it is until
  somebody clears `claimed_at`; that is the exception, and it is the reason the default is
  not zero.

  Whether to expect a drop row depends on which happened. A publish that *failed* records
  one and says why; a mover that was **killed** between publishing and deleting records
  nothing at all, so do not go looking for a row that explains it. `--lease 0` trusts a
  claim for ever, and then a crash does need an operator.
- **The row is in the wrong database.** The schedule is *not* routed to
  `EVENT_LOG_DATABASE`; it lives with the project's own tables. A project that pointed a
  router at this app by label before 4.1 should check `migrate` created
  `django_aiogram_scheduled` where the mover reads it.

## The bot ignores ENABLED

`ENABLED` is parsed, so `'false'` disables. If a value cannot be parsed you get
`ImproperlyConfigured` rather than a silent fallback. Both the app startup and
the send path read it the same way.

## Sends are slow

That is likely the pacing in **[Rate limits](Rate-limits.md)** doing its job: one
message per second to the same chat, 20 per minute to a group. Verify with `RATE_LIMIT`
set to `None`; if it speeds up, tune the numbers rather than removing them, or
Telegram will start refusing.

## Telegram was down; what did we lose, and can it be sent again

The feed answers the first half: `outbound.failed` and `outbound.dropped` rows over the
window, which the admin filters by kind and by time. `manage.py tgbot_replay` answers the
second, from the same rows:

```shell
python manage.py tgbot_replay --since 2026-09-04T10:00 --dry-run
python manage.py tgbot_replay --since 2026-09-04T10:00 --limit 50
```

`--dry-run` first, always: this is the one command in the package whose mistake is measured in
messages people receive. It prints the call it would make for each row, and the reason for
each row it will not — **and it answers what the live run would answer**, including the
failures already replayed and the messages whose several endings are one message. It is read
instead of the live run, so a difference between them would be the worst kind of bug this
command could have.

**Both endings are selected by default, and the reason is not guessable from the names.**
Rate-limit exhaustion — the case this section is named after — is recorded as
`outbound.dropped` with `detail.max_retries`, not as `outbound.failed`, so a default of
`outbound.failed` alone would replay the smaller half and leave an operator concluding the rest
was fine. `--kind` narrows to one when you know which half you are looking at.

`outbound.dropped` covers more than a loss, which is why the default can include it safely —
three of its shapes are read rather than trusted:

- **past `--grace`** (`TooLate`) — skipped, because the deployment already decided not to send it.
- **never acknowledged** (`NotScheduled`) — skipped, because the message is still the queue's. A
  worker refusing a send at shutdown does not acknowledge it, so the transport hands it back when
  the container comes up; replaying it would be the second copy. The same code also covers a
  direct `send_raw` refused the same way, which *is* lost — but that one was never queued, so
  nothing recorded its arguments and it cannot be replayed either way.
- **a queue write that failed** (`detail.stage`) — refused for want of arguments, because the row
  that records it is written before the payload reaches a transport and the `outbound.queued` row
  never happened.

**Most refusals are honest ones, and they say which.** A replay needs the arguments the send
was made with, and the feed records a *description* of them — so:

- `the arguments were recorded as omitted rather than in full` — a photo, a document, or any
  value the log replaces with a marker. Nothing to replay from.
- `... as truncated ...` — the arguments did not fit the column and were capped.
- `a value was redacted, and redaction is one-way` — a token-shaped value or a
  `EVENT_LOG_REDACT_KEYS` key was blanked on the way in. Anywhere in the text, not only as the
  whole of a value: a body reading `the token is ***, keep it` is refused, and so is one that
  merely writes `***` for emphasis. The second is the cost of the first.
- `... keys is the cap, and a mapping at it cannot be told from one cut to it` — fifty is
  where the log stops keeping keys or items, and a structure sitting exactly there might be
  whole or might be cut. Refused, because the alternative is a call sent with items missing.
- `no outbound.queued or outbound.scheduled row carries its arguments` — the failure row names
  the function and the chat, and the arguments live on the row the *producer* wrote. With
  `EVENT_LOG_PAYLOAD: 'none'` there never was one; after `tgbot_prune_events` there is not one
  any more, however recent the failure.
- `it was sent in the end, so nothing was lost` — the ending selected is not the end of that
  message's story. A mover that failed three times and published on the fourth leaves three
  drop rows and an `outbound.sent`, and Telegram has the message.
- `the deployment discarded it on purpose (TooLate), so this is not a loss` — `--grace` refused
  that message deliberately; replaying it would be the outage twice.
- `the worker never acknowledged it (NotScheduled), so the queue redelivers it on restart` — the
  send was refused while the container was shutting down and the message stayed in flight on the
  transport. Restart the worker; it comes back on its own.
- `it has been replayed already; the row joining them says so` — an `outbound.replayed` row
  names this failure, from this run, an earlier one, or another run that finished while this one
  was walking. **This is what makes the command
  re-runnable**: the selection is bounded, so five hundred failures are walked a hundred at a
  time, and without it every run would replay the same oldest hundred and never reach the
  rest.

So `EVENT_LOG_PAYLOAD: 'full'` is what makes replay possible, and it is not a guarantee: it is
decided per row, not per setting. Set it before you need it — a failure recorded under
`'summary'` cannot be upgraded afterwards.

Each replay is a **new** message with a new correlation id, and an `outbound.replayed` row
joins it to the one it stands in for (`detail.replay_of`). That row is what stops the *next*
run selecting the same failure, so the command refuses to run at all when `EVENT_LOG_KINDS`
excludes `outbound.replayed`, and says so per message if the feed would not take the row.
Unlike every other row this package writes, it is written synchronously and its answer is
read: the recorder drops rather than waits, which is right in the send path and wrong for the
one row that prevents a duplicate.

Run it again for the next hundred, and again: `--limit` counts the messages a run *sends*
rather than the rows it reads, so each run walks past what the last one did and reaches the
next. The report keeps the two apart — `replayed 100; refused 3; skipped 100` — because a skip
is a message that needed nothing, and a refusal is one somebody has to decide about. `--limit` is 100 by
default because a slipped date range would otherwise empty a month of failures into the queue;
`--limit 0` is the deliberate unbounded mode, and a negative number is refused as the typo it
is.

That is not the same as idempotence, and the difference matters in three ways: the guard is a
*row*, so a replay whose join row the feed refused is offered again — the run says which, in
the report and in the log — and a failure replayed by something other than this command is not
known to it.

**Two runs at once are safe.** Each failure is claimed in `django_aiogram_replay_claim` before
its message is queued, and that column is unique, so the second run is refused by the database
rather than by a read it would have to trust. It says so per row: *another run holds it
(`<worker>`, since `<time>`)*.

What is left is one narrow case, and it is the mover's trade in the same words. A claim is
released when the queue write fails, so only a run that *died* between claiming and queueing
leaves one behind — and `--claim-lease` (an hour by default) is how long the next run believes
it before taking over. Taking one over can send a second copy, because the message may have
reached the queue in the instant before that process went; the log says
*taking over a replay claim from a run that did not finish* when it happens.

## `ModuleNotFoundError: No module named 'telegram_bot'`

The 1.x package name was a deprecated shim in 2.x and is gone in 3.0. The
package is `django_aiogram`: use it in `INSTALLED_APPS`, import from it,
and note that `TelegramBot` lives in `django_aiogram.producer.client` while the
settings module is `django_aiogram.config.settings`. See **[Upgrading](Upgrading.md)**.

## The event log writes nothing

In order of how often it is the answer:

1. `TELEGRAM_BOT['EVENT_LOG']` is off. It is off by default, and `record()`
   returns before it reads anything else.
2. `migrate` has not run. The writer logs `no such table` once per batch and
   drops what it held; after five failures in a row it suspends for a minute
   rather than hammering the database, and records a `log.dropped` row for the
   gap once it gets through again.
3. The process you are looking at is not the one that records. `outbound.queued`
   is written by whichever process queued the message — through `enqueue` or
   `aenqueue`, and `send`/`asend` outside the worker reach one of those —
   `outbound.sent` by the bot container. Enabling the log in one and not the other gives you half a
   story, and that is not a bug.
4. `EVENT_LOG_KINDS` is set and excludes what you are looking for. The list is
   **inclusive**: naming anything drops everything unnamed, including kinds a
   later release adds. `W008` warns when it names a kind this version does not
   know.
5. Nothing has been flushed yet. The writer batches on a timer, so a test that
   asserts immediately needs `recorder.flush()`. See **[Testing](Testing.md)**.

`manage.py check` catches the configuration half of this: `W005` if the log is
on with no database configured, `E041` if `EVENT_LOG_DATABASE` names an alias
that does not exist.

## The admin page is missing

The changelist is registered in `ready()` only when **both** are true:
`EVENT_LOG` is on, and `django.contrib.admin` is in `INSTALLED_APPS`. It is
above the `ENABLED` gate on purpose — reading the feed is not talking to
Telegram, so a web tier with `ENABLED=0` still shows it.

The flag is read per request as well, so turning it off hides the page without
a restart. If the app shows but every row 403s, the user is missing
`view_telegramevent`; if the rows show but `detail` and `error` are absent,
that is `view_telegramevent_payload` doing its job. See
**[Event log](Event-log.md)**.

## Getting more detail

Merge this logger into your existing `LOGGING`, keeping your own `version` and
`handlers`:

```python
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {'console': {'class': 'logging.StreamHandler'}},
    'loggers': {
        'django_aiogram': {'handlers': ['console'], 'level': 'DEBUG'},
    },
}
```

See **[Logging](Logging.md)** for the fields each event carries.
