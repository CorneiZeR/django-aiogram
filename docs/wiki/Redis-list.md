# Redis list

This is the transport 3.x used, and it stays the default in 4.0 so that an upgrading project
changes its imports and nothing else. A Redis list holds the queue: `RPUSH` to publish, a
blocking pop to take.

```python
TELEGRAM_BOT = {
    'BROKER': 'django_aiogram.broker.redis_list.RedisListBroker',
    'REDIS_URL': 'redis://localhost:6379/0',
}
```

`pip install django-aiogram[redis]`. Nothing else is required: the list has a default name.

| Setting | Default | What it is |
| --- | --- | --- |
| `REDIS_URL` | — | where the server is; a query string on it overrides the timeouts below |
| `REDIS_MESSAGES_KEY` | `TELEGRAM_BOT_MESSAGE` | the list, and the prefix every other key here derives from |
| `REDIS_TIMEOUT` | `10` | the deadline on any single call |
| `BLPOP_TIMEOUT` | `5` | how often the blocking pop is interrupted to check for shutdown |

See **[[Settings]]** for the rest, and **[[Delivery]]** for what the consumer does with any
transport. This page is the part that is this one's alone.

## What it guarantees

**At-least-once wherever `BLMOVE` is available**, which is Redis 6.2 and above — but the
condition is the command, not the version number, and that distinction has teeth. `BLMOVE` takes
the message and records it in one round trip, so there is no instant where it has left the queue
and been written down nowhere.

Without it the consumer falls back to plain pops, says so in the log, and the guarantee drops to
**at-most-once**: a kill between the pop and the send loses that message. `REQUIRE_CRASH_SAFE`
refuses to start rather than running degraded quietly.

The fallback is a runtime downgrade rather than a version check, so it can happen to a server
that *was* 6.2+: a connection that lands on one without `LMOVE` after a reclaim already
succeeded — a failover to an older replica, say — downgrades then and there. Watch the log for
it rather than inferring safety from the version you deployed.

Ordering is the list's: first in, first out, one consumer at a time per message, because a
blocking pop is atomic. **Several bot containers are safe only if each resolves a different
name** — same name, one in-flight list, and each reclaims what the others are still sending. See
below.

## The in-flight list, and why the worker's name matters

This is the transport that needs an identity, and the only one that does. In-flight messages
live in `<REDIS_MESSAGES_KEY>:processing:<worker>` — keyed on the name — so reclaiming works
only when a restarted worker resolves the *same* name it had.

`<worker>` is `WORKER_NAME` when set, and otherwise the hostname. A container started without
`hostname:` gets a fresh twelve-character name from Docker every time it is **created**, so a
redeploy strands whatever the last container was sending in a list nothing will read again.
`I001` reports the case it can detect and `start_tgbot` warns at startup.

The other half is the collision: two workers resolving to the *same* name share one in-flight
list and each reclaims what the other is still sending. Copying a `WORKER_NAME` between
services gets you there as surely as sharing a host does.

`manage.py tgbot_reclaim --worker <name>` is the way back from a stranded list, and it is
deliberately manual — naming a worker is a human saying it is gone. Nothing here probes for
liveness, because a slow worker looks exactly like a dead one and taking its message back sends
it twice. `--dry-run` reports without moving, `--limit` bounds a run so a mistaken name costs
`n` messages rather than a list.

## Liveness

Nothing on the server knows a consumer exists, so the consumer writes one: a heartbeat key with
a TTL of three intervals, refreshed every `HEARTBEAT_INTERVAL`. A probe reads its age. That is
why `BLPOP_TIMEOUT` is capped at `min(REDIS_TIMEOUT - 1, HEARTBEAT_INTERVAL)` — a pop outlasting
the refresh lets the key go stale while the worker is perfectly healthy, and a pop asked to wait
longer than the socket will turns every idle round into an error. `W004` says so before
deployment, and names the bound that actually binds.

## What it costs

An `RPUSH` acknowledged by the server took 120 to 147 microseconds on one laptop against a
containerised Redis — the divisor the other transports' figures are quoted against. There is no
unconfirmed mode to compare it with: Redis is asked for no disk here, so a list publish *is* the
acknowledged one. `scripts/measurements` re-takes it.

Acknowledging is an `LREM`, which scans the in-flight list, so `MAX_IN_FLIGHT` earns its keep
here more than anywhere: an unbounded list turns draining a backlog into quadratic work.

## Where it shows through

- `manage.py tgbot_healthcheck` reports messages under *other* worker names, so a stranded pile
  is visible. The container probe leaves that sweep off unless you pass `--stranded`: `SCAN`
  walks the whole keyspace, and the count is a floor rather than a total.
- A refusal is a no-op. There is nothing to nack — the message is already in the in-flight list,
  and leaving it there *is* the refusal.
- `bot.inflight_depth()` reads that list off the server, so any process can ask it — and naming
  another worker is how a monitor sees what a dead one left. Only two of the four transports can
  answer that from outside the worker; this is one.
- **What a payload may weigh** is `proto-max-bulk-len` on the server, and this package sets
  nothing: whatever your Redis allows is what a message may be. Generous by default, and the
  reason it can matter at all is `BufferedInputFile` — queueing one puts the file's bytes in the
  payload, so a document sent that way is a queue entry of that size. Prefer `FSInputFile` or
  `URLInputFile` where you can; both queue a reference rather than the contents.
- `decode_responses` in a `REDIS_URL` shared with a cache backend meets bytes it cannot decode.
  `E043` refuses that combination when pickle is allowed, because the failure lands inside
  redis-py before any code here runs.
