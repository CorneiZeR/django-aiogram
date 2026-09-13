# Dynamic bots

For the deployment whose bots arrive while it runs: a client connects their own bot through
your interface, and a container somewhere starts serving it seconds later without a redeploy.

Everything on **[Multiple bots](Multiple-bots.md)** still holds — identity, aliases,
resolution, grouping. What changes is *where the bots come from*.

## Providers

`BOT_PROVIDERS` names them by dotted path, in order:

```python
TELEGRAM_BOT_DEFAULTS = {
    'BOT_PROVIDERS': (
        'django_aiogram.runtime.providers.from_settings',
        'django_aiogram.runtime.providers.from_database',
    ),
}
```

`from_settings` reads `TELEGRAM_BOTS` and is the default on its own. `from_database` reads the
`TelegramBot` table. Where two sources name one identity the **first keeps it**, and the
collision is logged — so a bot pinned in settings cannot be taken over by a row.

A provider of your own is a callable answering with resolved bots, and it has one obligation:

> **Say what you read, or raise.** Never answer "nothing" to mean "I could not look."

A read that raises leaves the running set exactly as it is. A read that answers *nothing* is
held for one pass before it is believed, because twenty bots disappearing at once is far more
often a query against the wrong database than twenty clients leaving together. A deployment
that really did remove its last bot converges one interval later.

## The rows

| Table | What it holds |
| --- | --- |
| `TelegramBot` | one bot: its identity, its token, which profile and queue it points at, its sparse overrides, and whether it is on |
| `TelegramBotProfile` | settings a group of bots shares, so twenty clients on one plan are one row |
| `TelegramQueue` | a declared queue and the pool that serves it |
| `TelegramBotLease` | which container is polling which bot |

Overrides are **sparse and by key presence**, the same rule the settings dicts follow: a column
per setting would need a migration per setting, and `NULL` cannot mean both "inherit" and
"`RATE_LIMIT` is off".

Nothing reads any of it until a provider is configured. A project running its bots from
`settings.py` writes no rows at all.

## How a change arrives

Two ways, and the poll is the one that makes it arrive *at all*:

- **`BOT_REFRESH_INTERVAL`** (30s) — every pass re-reads the whole desired set and compares. A
  pass over an unchanged table costs one aggregate per table.
- **A control message**, published on the transport when a row is saved — and published on the
  caller's *commit*, so a save the transaction rolls back announces nothing.

The message says **"read again"**, never "set the token to X". That is what makes a duplicate
free, a loss cost one interval, and a reordered pair harmless. It is the same reason the
supervisor is level-triggered: nothing acts on what changed, everything compares what is.

The admin never talks to Telegram inside a request for the same reason — see *Intents* below.

## When a bot will not start

One bot's failure is its own: it is quarantined with a reason and the pass carries on to the
others. What the failure *was* decides how long it is believed:

| Fate | What it is | What happens |
| --- | --- | --- |
| `revoked` | 401 — the token was revoked in BotFather, or the bot deleted | **No retry.** Only a new token brings it back, and a timer never will |
| `conflict` | 409 — something else is polling this token, usually a deploy whose old process has not exited | Retried; it clears itself |
| `transient` | 429, a closed socket, a 5xx | Retried with backoff — the ordinary weather |
| `unknown` | anything else: a broker that refused, a profile naming an unusable transport | Retried, because a guess is not evidence |

The reason and the moment it may be tried again are written on the row, which is what the admin
shows and what `manage.py tgbot_bots` prints.

**A revoked token is the client's problem, and your project is what tells them.** Two signals
carry it:

```python
from django.dispatch import receiver

from django_aiogram.runtime.lifecycle import bot_quarantined, bot_recovered


@receiver(bot_quarantined)
def tell_the_client(sender, bot_id, fate, reason, until, **kwargs):
    if fate == 'revoked':
        email_whoever_owns(bot_id, reason)


@receiver(bot_recovered)
def stop_telling_them(sender, bot_id, **kwargs):
    clear_the_warning(bot_id)
```

`until` is `None` where nothing will retry it, which is exactly the case worth an email.

## When a client deletes their bot

Telegram answers 401 and the bot is quarantined as `revoked`, with no retry: that is the same
path as a rotated token, and it is deliberate — the row stays, so the client reconnecting means
writing a new token rather than starting again.

What their **queue** costs you is a separate decision, because a queue per client is how a
deployment leaks — one Redis key, AMQP queue or consumer group per client that ever existed.
`manage.py tgbot_prune_queues` is the sweep, and `REMOVED_QUEUE_POLICY` says what it may do:
`park` (the default) leaves the queue and reports it, `hold` removes it once it is empty, `drop`
removes it with whatever is still in it. Nothing acts on this on its own.

## Intents: what the admin asks for

Checking a token, registering a webhook, removing one — none of those may happen inside an HTTP
request, because five hundred selected bots would be five hundred round trips with a person
watching a spinner. So the admin writes the *asking* into the row, and a process with an event
loop answers it:

```console
$ python manage.py tgbot_intents --watch
```

Run it beside the bot containers. An intent is a person waiting for an answer, so the useful
interval is seconds. The asking stays on the row until an answer is written — a worker that
died holding one loses it to whoever asks next, rather than taking the only record of it away —
and the answer is written only by the claim still held, so a slow worker cannot overwrite the
answer to a question somebody asked after it. The result is one line a person can read, with
the token redacted.

## Which container polls which bot

Polling is exclusive per token: two processes on one bot is a 409 for whoever was there first.
So a bot is held by a **lease**, renewed on every pass:

| Setting | Default | What it decides |
| --- | --- | --- |
| `MAX_BOTS_PER_WORKER` | `0` | How many bots one polling process may hold; `0` is as many as it is given |
| `BOT_LEASE_SECONDS` | `90` | How long a lease is believed. `W011` reports one that is not comfortably longer than `BOT_REFRESH_INTERVAL` |

A container that dies loses its bots once `BOT_LEASE_SECONDS` has passed, and another process
may take them then. **An expired lease is not a revocation**: nothing reaches into the old
container, so one that carries on — or comes back from a pause — polls the same token until a
reconciliation pass stops it, and Telegram answers 409 to whichever of them asked second. Webhooks need none of this — there is nothing to hold — and past
a hundred bots they are the only shape that works. **[Scaling](Scaling.md)** has the numbers.

## The credential

A row holds a token, and the shipped storage writes it as it was given: what protects it then is
the database's own access control and the `view_telegramevent_payload`-style permission the
model declares. **Treat a dump of that table as a dump of every client's credential.**
`TOKEN_STORAGE` behind the `[crypto]` extra encrypts it at rest, and
`manage.py tgbot_rewrap_tokens` moves a table that already has tokens in it.
**[Tokens](Tokens.md)** is the page.

## When it does not work

- `manage.py tgbot_bots` — every bot that resolved, including the ones in rows, with its
  profile, queue, state and lease. `manage.py check` cannot see those: the checks read settings.
- `manage.py tgbot_bots --json` — the same for a script.
- `manage.py tgbot_queues` — depth and whether anything is consuming.
- `manage.py tgbot_intents` — what is outstanding, and what came back.
