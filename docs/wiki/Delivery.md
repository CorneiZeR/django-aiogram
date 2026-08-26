# Delivery

`bot.send()` puts a serialized call on the queue. The bot container takes it off
and makes the call.

Which queue is `BROKER`'s answer — see **[[Settings]]** — and this page is about
what the consumer does with it, which is the same either way: take one message,
send it, settle it, and leave it unsettled if the send did not happen. Where a
transport's own machinery shows through, the section says which transport it is
talking about.

This page is about outbound messages: how a queued `bot.send()` reaches
Telegram. Which way *updates* arrive — polling or webhook — is a separate
choice, described in **[[Webhook]]**; the queue works the same under both.

## One transport at a time, and where to read about it

`BROKER` names it, and only that one's settings are read. What each guarantees, what it costs
and what it needs from you differs enough to be worth a page each:

| Transport | Crash safety | Needs a worker name | Recovery is |
| --- | --- | --- | --- |
| **[[Redis-list]]** | at-least-once wherever `BLMOVE` is available, at-most-once without it — a capability, not a version | **yes** — the in-flight list is keyed on it | a command you run |
| **[[Redis-Streams]]** | at-least-once, and **Redis 7.0+ or it refuses to run** | no | a clock: idle entries are claimed |
| **[[RabbitMQ]]** | at-least-once, the broker's doing | no | automatic: a dropped channel requeues |
| **[[Kafka]]** | at-least-once, the group's doing | no | automatic: an uncommitted offset replays |

A payload's size is the transport's business too, and the ceilings are not comparable: Kafka's
default is around a megabyte while the Redis and AMQP ones are orders larger. This package
configures none of them, so each page names the setting that governs its own — and the reason it
matters at all is `BufferedInputFile`, which queues a file's *bytes*.

The rest of this page is what the consumer does with any of them.

`DELIVERY` is a separate and much smaller choice: it names the consumer *class*, and `'blpop'` is
its only value. The `keyspace` consumer 1.x used — write a key with a TTL, react to its expiry
event — was removed in 3.0, because it needed `CONFIG SET notify-keyspace-events`, which managed
providers refuse, and nothing could be delivered before the TTL elapsed. Settings that still say
`'keyspace'` fail `E009` rather than falling back quietly. The name has outlived its accuracy —
that consumer now asks a broker rather than issuing `BLPOP` — and renaming it is a settings
migration rather than a rename, so it waits.

## Running more than one worker

The consumer takes each message once, and every transport is responsible for that
being true: the Redis list relies on `BLMOVE` and `BLPOP` being atomic, and Redis
Streams on a consumer group handing an entry to one member. Running several bot
containers is safe, though a single one handles a lot: the limits in
**[[Rate-limits|Rate limits]]** bind long before the consumer does.

## Crash safety

A message taken and not yet sent has to survive the worker being killed, and each transport
records that its own way. The guarantee is **at-least-once** — so a crash can cause a duplicate
send — with two exceptions, one per layer.

The transport's: a Redis list on a server without `BLMOVE` falls back to plain pops and drops to
at-most-once. That is the row the table above marks, and `REQUIRE_CRASH_SAFE` refuses to start
there.

The handler's: a handler that does not take `on_complete` is settled the moment it returns, so
whatever it does after that is outside the guarantee on **every** transport. `bot.send_raw` takes
it and `manage.py start_tgbot` uses it, so a normal worker is covered; see below for the contract
and why a handler of your own keeps the older behaviour.

What an operator has to do about the guarantee differs by transport even where the guarantee
does not.

**Redis list.** On Redis 6.2+ the message is moved to `<queue>:processing:<worker>`
while it is being sent and removed once the send has actually finished. A worker
killed mid-send leaves it there, and the next start reclaims it **if it resolves
the same worker name** — the list is keyed on that name, so a replacement container
with a fresh hostname strands it instead. That is what `I001` reports and what
`manage.py tgbot_reclaim --worker <name>` is the way back from.

**Kafka.** A committed offset is the *next* record to read, so it says that every offset below
it in that partition is settled rather than saying anything about one message. A consumer that
holds several sends at once — which `MAX_IN_FLIGHT` allows — therefore commits only the highest
**contiguous** prefix per partition: settle the second while the first is still in flight and
nothing is committed for that partition until the first finishes, because committing the second
would claim the first as done. A worker killed at that moment loses nothing: replay starts *at*
the committed offset, because that offset is the next record to read rather than the last one
dealt with — so what
comes back is that record and everything above it in the partition, which with `MAX_IN_FLIGHT`
above two is more than the pair that caused the gap. Partitions are independent: a gap in one
does not hold up commits in another.

The same shape has a sharper edge on a refusal. There is no per-message nack, so giving one up
rewinds that partition to the offset, and **the released record together with every later one
in that partition** is delivered again — the other partitions carry on untouched. A handle the
rewind *reached* stops being settleable — one naming its offset or a higher one — because the
delivery it named no longer exists; a send that finishes afterwards is reported and settles
nothing, and its message comes back with the rest. Handles below the rewind are untouched and
still this worker's to settle, which is why the broker records where each rewind went rather
than how many there have been. **Build
idempotency on your own business key** — the advice below is not decoration on this transport.

**RabbitMQ.** The least of any of them: an unacknowledged message returns to the
queue when the channel that held it drops, and a worker being killed drops its
channel by dying. So there is no in-flight list, `reclaim()` has nothing to do and
says so by answering nothing at all, `tgbot_reclaim` refuses, and `I001` stays
quiet. A publish is confirmed and mandatory, so a message the broker will not take
raises rather than disappearing into an exchange.

**Redis Streams.** The consumer group records every delivery before the consumer
sees it, and unsettled entries sit in the group's pending list rather than under a
name. So a worker's name buys nothing here: any consumer reclaims what a dead one
held, a replacement container with a fresh hostname strands nothing, `I001` stays
quiet, and `tgbot_reclaim` refuses because there is nothing for `--worker` to
select. A worker that comes back — or one already running — picks the work up once
the old one has been quiet longer than the liveness TTL.

Before 3.1.0 it was removed when the *handler returned*, and `send_raw` returns
as soon as the coroutine is scheduled. In polling mode that meant the message
left the in-flight list before Telegram had seen it, and this guarantee was not
true: a `docker stop` with a backlog delivered what the drain had time for and
lost the rest, with nothing left to redeliver. Webhook mode always had it,
because there the consumer drives the send to completion itself.

Duplicates after a kill are therefore real now where they were not before.
**Build idempotency on your own business key**, not on `correlation_id`: a
handler's replies inherit the id of the update that caused them, so it is not
one per message.

### It follows the handler, not the queue

Waiting for the send is something the *handler* opts into, by taking an
`on_complete` keyword. `bot.send_raw` does, and `manage.py start_tgbot` uses it,
so a normal worker has the guarantee above.

A handler of your own that takes only `**kwargs` — the shape every recipe on
**[[Testing]]** uses — keeps the pre-3.1.0 semantics exactly: it is acknowledged
the moment it returns, which is at-most-once if it goes on to do the sending
somewhere else. That is deliberate, so existing handlers are not silently made to
hold messages they never release. Take `on_complete` and call it when your send
finishes if you want the message held until then.

`MAX_IN_FLIGHT` bounds how many sends the consumer will leave outstanding before it stops
taking messages. The default, `0`, is no bound. Worth setting on any worker that sees large
backlogs, though what it saves you differs: on the **[[Redis-list|Redis list]]** each
acknowledgement scans the in-flight list, so an unbounded one makes draining quadratic; on
**[[Kafka]]** it is how many records a single gap can hold back, because offsets settle a
contiguous prefix.

`REQUIRE_CRASH_SAFE` refuses to start where the transport cannot promise at-least-once, rather
than running degraded and saying so only in a log line. Only one transport can be in that
position — a Redis list whose server does not answer `LMOVE`, which is anything before 6.2 but
also a 6.2+ connection failed over to one without it — and the check happens before the
consumer thread starts, because a failure inside it would kill the thread and leave the process
polling updates with nothing draining the queue.

**Only the Redis list needs a worker identity**, and that is the sharpest practical difference
between the transports. There the in-flight list is keyed on the worker's name, so a name that
does not survive the container strands whatever it was sending, and two workers sharing a name
reclaim each other's work. `manage.py tgbot_reclaim`, `I001` and the stranded-list report all
exist for that one transport; the other three answer for themselves and those tools refuse.
**[[Redis-list|Redis list]]** has the whole story, including what Docker does to a hostname.

Handler errors are not crashes: a message whose send *failed* is acknowledged
and logged, not redelivered forever.

`Delivery.dispatch()` is what decides that, and its return value is the whole contract: `True`
means the consumer settles the message — however this transport settles one — and `False` means
it does not, leaving it for a later delivery. Withholding the acknowledgement only saves the
message where the transport recorded it as taken *before* handing it over. Every transport here
does, with one exception: a Redis without `LMOVE` has already popped the message, so there
`False` and `True` come to the same thing and it is gone.

`False` comes back for four reasons, in two kinds. Three leave the message for
somebody else: a pickled payload refused because `ALLOW_PICKLE` is off, which is
the one failure a change of configuration can undo; an envelope written by a
**newer version than this consumer understands**, which is the deploy-order case —
roll the consumers first and it drains; and a handler raising `CancelledError`,
whose outcome is **unknown** — at shutdown usually, though the `except` is
unqualified, so any cancellation counts. Unknown rather than "reached nothing": a
send can be cancelled after Telegram has already taken the request, so the message
is kept for a redelivery that may turn out to be a duplicate. That is the trade
this release makes everywhere — losing a message is worse than sending it twice, and
handlers are asked to be idempotent for exactly this.

The fourth is not a refusal at all: a handler that accepted `on_complete` *signals* completion
through it, and the consumer settles the message on its next turn. That is what makes
at-least-once true — where the message is still recoverable. On a Redis without `LMOVE` it is
already gone when the handler is called, so deferring the acknowledgement defers nothing and
that server stays at-most-once.

All three refusals rest on the message still being recoverable, as the paragraph above says: on
a Redis without `LMOVE` it was already popped, so none of them recovers it and `False` only means
this consumer will not delete it twice. Everything
else — undecodable bytes, a method that is not Telegram API, a handler that
raised before it scheduled anything — returns `True`, because redelivering it
would only fail again.

## What happens to a broken message

A payload that cannot be decoded is logged and dropped; the consumer moves on.
A handler that raises is logged the same way. Neither stops the worker.

## Shutting down

`SIGTERM` — what `docker stop` sends — unwinds polling, stops the consumer,
closes the aiogram session and the FSM storage. Messages already in the list
stay there for the next start.
