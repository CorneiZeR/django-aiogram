# Scaling

One bot is one connection and a container. A thousand bots is a different question, and the
answer is not "the same thing, bigger": the two ways of receiving updates cost different
things per bot, and only one of them is free.

What a set of bots *is* — aliases, identities, what they share — is
**[Multiple bots](Multiple-bots.md)**; where they come from at run time is
**[Dynamic bots](Dynamic-bots.md)**. This page is what each of them costs.

## Polling stops well before a thousand bots

Long polling costs, **per bot**:

- one long-lived HTTP connection to Telegram, held open for as long as the wait;
- one task on the process's loop, awake whenever that bot has updates;
- one lease row, renewed every pass — see **[Deployment](Deployment.md)**.

The shared `AiohttpSession` every bot in a process uses defaults to a connector limit of
**100 connections**. Past that, bots wait for each other's `getUpdates` to return: not an
error, just latency nobody asked for, distributed unevenly and invisible from outside. Raising
the limit moves the wall rather than removing it — the file descriptors, the loop's own
scheduling and Telegram's per-bot limits are all still there.

So: **polling is for tens of bots, not thousands.** It needs no web tier, no public
certificate and no reachable hostname, which is why it is the default and why it is right for
one bot or twenty.

## Webhooks cost nothing per bot

A webhook has no per-bot connection at all. Telegram posts each update to a URL and the
request lands in whatever serves it — normally the web tier that is already there, already
scaled, already behind a load balancer. Adding the thousandth bot costs one `setWebhook`
call and one row.

The URL carries the bot's identity and each bot has its own secret, so the view knows which
bot's handlers an update belongs to and one client's leaked secret cannot post as another.
**[Webhook](Webhook.md)** has the shape of it and the reconciliation pass.

What a webhook deployment still needs a long-running process for is *sending*: the queue is
drained by `start_tgbot`, and `--no-updates` is the flag that says this container only sends.

## The shape at each size

| bots | updates | why |
| --- | --- | --- |
| 1 | polling | nothing to serve, nothing to certify |
| up to ~20 | polling | comfortable inside the connector limit, one container |
| ~20 to ~100 | polling with leases, or webhook | `MAX_BOTS_PER_WORKER` splits the bots across containers; the connector limit is the ceiling to watch |
| past ~100 | **webhook** | the per-bot connection is the thing that does not scale, and a webhook does not have one |

The numbers are not thresholds the package enforces — nothing refuses to poll a hundred bots.
They are where the cost changes shape.

## What scales with the bots, whatever the mode

- **Queues.** One queue for everybody is one backlog for everybody: give a client their own
  and `MAX_IN_FLIGHT_PER_BOT` bounds what one of them can hold. A container serves several
  queues with `--queues` or `--pools`, and a queue a client leaves behind is removed by
  `manage.py tgbot_prune_queues`.
- **Rate limits.** Telegram's are per bot, so the budgets are too.
- **The event log.** Every send writes rows, so it grows with the bots and not with the
  containers; `EVENT_LOG_RETENTION_DAYS` and `manage.py tgbot_prune_events` are what bound it.
- **The database.** A bot is a row, and the supervisor reads them on a watermark, so a
  thousand bots is one aggregate per pass rather than a thousand.
