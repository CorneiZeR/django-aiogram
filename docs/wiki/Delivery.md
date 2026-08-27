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

## `DELIVERY`: which consumer runs

`DELIVERY` is a **dotted path** to a `Delivery` subclass, the way `BROKER` is a path to a
`Broker` one. The default is `'django_aiogram.consumer.delivery.BlpopDelivery'`, the consumer
this package ships, and a project that changes nothing gets it.

Until 4.0 the setting accepted exactly one string, `'blpop'` — the name of a Redis command that
three of the four transports never issue, since the consumer asks the broker and the broker
reaches for `basic_get`, a stream read or a poll. So the setting named one transport's mechanism
while offering no choice at all. It is a path now, which makes the name accurate and the setting
useful in the same change. The `keyspace` consumer 1.x used — write a key with a TTL, react to
its expiry event — was removed in 3.0: it needed `CONFIG SET notify-keyspace-events`, which
managed providers refuse, and nothing could be delivered before the TTL elapsed. `E009` names
both old words against what to write instead.

### Writing your own

Subclass `Delivery` and implement `run()`. Everything else is provided, and the provided parts
are the ones that are easy to get wrong:

```python
from django_aiogram.consumer.delivery import Delivery


class BatchedDelivery(Delivery):
    """Takes in batches, settles each message as its send finishes."""

    def run(self) -> None:
        while not self.stopping:
            self.heartbeat()
            self.collect()  # settle what finished while we blocked
            taken = self.broker.take(timeout=5)  # or take_nowait, in a loop of your own
            if taken is None:
                continue
            if self.dispatch(taken.payload, taken.handle):
                self.acknowledge(taken.handle)
        self.collect()  # again: a send that finished during the last read is still unsettled
```

Four rules, and each is a defect this package has already had:

* **`run()` must return when `stop()` is called.** `self.stopping` is the flag;
  `start_tgbot` joins the thread with a deadline taken from the transport's own timeout, and a
  consumer that outlives its join goes on to acknowledge a message the bot has already refused.
* **Acknowledge only what `dispatch` says is done.** `dispatch` returns `False` when the send is
  still in flight — a handler taking `on_complete` settles the message itself, later, from the
  producer's thread. Acknowledging on `False` is the at-most-once bug 3.1.0 removed.
* **Call `collect()` every turn, and once more after the loop.** Sends that finished while the
  read was blocking report themselves into a queue only the consumer thread drains, so a
  completion that arrives during the *last* read is settled by nothing unless `run()` drains on
  its way out. Skip either and a graceful stop redelivers what it had already sent.
* **Do the transport's I/O on your own thread only.** The broker instance is process-global and
  each transport restricts what a foreign thread may touch; `heartbeat()`, `take()` and
  `acknowledge()` all belong to the thread `run()` is on.

`E009` checks the shape of the path at `manage.py check` and nothing more: resolving it would
import the consumer module, which imports the serializer, which imports aiogram — 883ms and
135 MiB on every `migrate` and `runserver`, measured. Whether the path imports, and imports a
`Delivery`, is settled by `start_tgbot` before it starts a thread, and a `DeliveryNotConfiguredError`
there names the setting and the value.

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

`SIGTERM` — what `docker stop` sends — unwinds polling, stops the consumer, closes the aiogram
session and the FSM storage, and then releases the transport: the queue's own connection, which
on Kafka means flushing the producer and leaving the consumer group. Messages already queued stay
there for the next start.

A process that never calls `bot.close()` — a web tier that only queues — releases it at exit
instead, through a hook armed the first time it builds a broker. Neither path is a substitute for
the other: the first runs while the process is still healthy and can report what it could not
flush, and the second is what covers a process with no shutdown code of its own.
