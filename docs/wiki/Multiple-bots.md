# Multiple bots

One bot is the small case of several. A project that writes a token and nothing else has one
bot called `default`, and everything on this page still describes it — it is simply a set of
one.

```python
import os

TELEGRAM_BOT_DEFAULTS = {'REDIS_URL': os.environ['REDIS_URL']}
TELEGRAM_BOTS = {
    'default': {'TOKEN': os.environ['TELEGRAM_TOKEN']},
    'support': {'TOKEN': os.environ['SUPPORT_TOKEN'], 'RATE_LIMIT': {'overall_per_second': 5}},
}
```

`TELEGRAM_BOT_DEFAULTS` is what every bot inherits and each section overrides what it names —
**by naming it**, not by its value: `'RATE_LIMIT': None` is a bot with no limit, not a bot
falling back to the shared one. That is the same rule at every level, and it is why overrides
are a dict rather than a column per setting.

## An alias is a name, an identity is the bot

A section's key is an **alias**: a name the project chose, which it may change. A bot's
**identity** is the number in front of the colon in its token, read without asking Telegram:

```
123456789:AAH... → 123456789
```

That number is the one thing that holds still. A token is a credential a project rotates and an
alias is a label it renames; the identity survives both, so it is what a queued message carries,
what the event log records, what a metric is labelled with and what the admin finds a bot by.

A token with no identity in it is still usable — it is just anonymous: `E052` reports it, and a
message it queues names no bot, so a consumer delivers it through the bot it has. That is the
4.x shape, and it is what makes a rolling upgrade work.

## Reaching a bot

```python
from django_aiogram import bot, bots

bot.send(chat_id=42, text='through the default bot')
bots['support'].send(chat_id=42, text='through the support bot')
bots.by_id(123456789).send(chat_id=42, text='through whoever that is')
```

`bots` is a `Mapping`, so `len(bots)`, `for alias in bots` and `'support' in bots` all work, and
`bot` is exactly `bots['default']` — the same object, not a copy. Each is built on first use and
kept: a bot holds the aiogram `Bot`, its share of the loop and the sends a shutdown has to
drain, so a second object for one alias would drain half of them.

`bots.by_id()` also asks the providers, which is what a webhook update needs: a client's bot is
a row rather than a section, and by the time an update names it there is no section to find it
under. See **[Dynamic bots](Dynamic-bots.md)**.

## What a section may not say

Some settings configure the **process**, not a bot, so a section naming one is refused by
`E053` rather than quietly deciding for everybody:

`AUTODISCOVER`, `MODULE_NAME`, `WORKER_NAME`, `FSM_STORAGE`, `BOT_PROVIDERS`,
`BOT_REFRESH_INTERVAL`, `MAX_BOTS_PER_WORKER`, `BOT_LEASE_SECONDS`, `QUEUES`,
`REMOVED_QUEUE_POLICY`, `METRICS_PER_BOT`, `TOKEN_STORAGE`, `TOKEN_ENCRYPTION_KEYS`,
`EVENT_LOG` and every `EVENT_LOG_*` one.

`ENABLED` is **not** among them: one bot can be switched off while the rest keep running.

## Handlers are the process's, state is the bot's

There is one handler tree in a process and one dispatcher driving it, because a `Router` cannot
be attached to two dispatchers — so every bot in the process serves the same handlers, and a
handler that has to know which bot it is answering for reads it from the update:

```python
@router.message(Command('whoami'))
async def whoami(message: Message, bot: Bot) -> None:
    await message.answer(f'you are talking to {bot.id}')
```

The FSM store is one per process too, and its keys carry the bot's identity — so one person
talking to two of your bots has **two** conversations, not one shared muddle. Measured: without
that, both bots' states land on `fsm:5:5:state` and each answers with the other's.

## What twenty bots cost

Bots configured alike share what a connection is: the broker, the consumer thread, the HTTP
session. What they must agree on to share it is computed from their **resolved settings** — the
transport and its options, the queue, the serializer, `MAX_IN_FLIGHT` — and is called a
*profile*. Nothing configures it, and nothing should: it is what the settings already say.

`manage.py tgbot_bots` is where you see it:

```console
$ python manage.py tgbot_bots
bot_id  alias    source    profile   queue  mode     enabled  lease
111111  default  settings  5aeada77  —      polling  True     —
222222  support  settings  5aeada77  —      polling  True     —
333333  vip      settings  b1cbfd81  vip    polling  True     —
3 bot(s) in 2 group(s)
```

`default` and `support` share a profile although their rate limits differ — a limit is paced
per bot and costs nothing to share a transport with. `vip` names a queue of its own, so it gets
a transport of its own: two bots on one queue would read each other's messages.

**Two groups is the number to read.** Twenty bots that quietly built twenty groups is twenty
connections, and nothing else in the system would say so.

A rate limit is per bot for the same reason the profile ignores it: Telegram's limits are the
bot's, not the deployment's. See **[Rate limits](Rate-limits.md)**.

## A queue of its own, for a client who must not wait

`QUEUE` is a bot's setting, and a bot that names one publishes there and is consumed from
there:

```python
TELEGRAM_BOTS = {
    'default': {'TOKEN': os.environ['TELEGRAM_TOKEN']},
    'vip': {'TOKEN': os.environ['VIP_TOKEN'], 'QUEUE': 'vip'},
}
TELEGRAM_BOT_DEFAULTS = {'QUEUES': ('vip',)}
```

A queue nothing declares is refused where the transport for it is built, because a message
published to a queue nobody consumes is a send that succeeds and arrives nowhere. Declare it in
`QUEUES` or as a `TelegramQueue` row. **[Deployment](Deployment.md)** has which container
serves what, and **[Scaling](Scaling.md)** has the arithmetic.

A queue of its own does not mean a connection of its own. Queues whose settings agree on
everything but their name are read by **one** consumer over one connection wherever the
transport can do it — RabbitMQ, Kafka and Redis Streams can, a crash-safe Redis list cannot —
and each keeps its own in-flight budget inside that. So twenty clients on twenty queues is
twenty backlogs and one connection, not twenty of each.

## Everything a message carries says which bot

A queued message names its bot on the wire **where the bot has an identity to name** -- which
is every token Telegram issues, and not a token `E052` has already reported -- so a container
serving several delivers each through the right one. One that names nobody is delivered through
the consumer's own bot, which is what a 4.x payload is and what makes the upgrade rolling. The same identity reaches the event log (`bot_id`), the structured
logs (`tg_bot_id`), the Prometheus labels (behind `METRICS_PER_BOT`, which is off by default —
twenty bots is twenty label values) and the admin's filters.

It is a *field* rather than a new envelope version, which is what lets the web tier and the bot
container be deployed in either order during the upgrade: a 4.1 consumer handed a 5.0 payload
delivers it through the one bot it has. **Before you add a second bot**, every consumer has to
be at 5.0 — see **[Upgrading](Upgrading.md)**.

## Testing a project with several

```python
with capture_sends(bot='support') as sent:
    notify_everyone()

assert sent.kwargs == [{'chat_id': 42, 'text': 'the support bot only'}]
```

`capture_sends()` with no argument captures every bot and `sent.for_bot('support')` sorts them
out afterwards; a narrowed capture refuses a bot it is not watching rather than answering with
an empty list. **[Testing](Testing.md#several-bots)** has the rest.

## When it does not work

- `manage.py check` — `E050` to `E059` are about this: a `TELEGRAM_BOT` left over from 4.x, a
  section that is not a mapping, a process-scoped setting inside one, a token with no identity,
  two bots on one identity, a queue nothing declares.
- `manage.py tgbot_bots` — what actually resolved, including the bots that live in rows, which
  `check` cannot see.
- `manage.py tgbot_queues` — the queues, their depth, and whether anything is consuming them.
