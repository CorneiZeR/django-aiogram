# AI assistants

Coding agents integrate this package the way a person would — by reading the
docs — and they get the same handful of things wrong, usually because 1.x
material is still the first thing a search finds. This page is the brief to hand
them.

## Paste this in

Everything an assistant needs to wire the package into a Django project
correctly, short enough to sit in a system prompt or a `CLAUDE.md` /
`AGENTS.md` / `.cursor/rules` file:

```text
Project uses django-aiogram 4.x. Rules:

- Import the shared instance: `from django_aiogram import bot`. Never
  construct TelegramBot() per task or per request — that builds an event loop and
  an HTTP session nothing closes.
- To send from anywhere (view, task, signal): `bot.send(chat_id=..., text=...)`.
  It queues on whichever transport `BROKER` names outside the bot container, and
  calls Telegram directly inside it. Pass another method by name: bot.send('send_photo', chat_id=..., photo=...).
  Only Telegram API methods aiogram exposes are accepted.
- From async code, await `bot.asend(...)` instead: `send()` writes to a socket on
  the thread the loop is running on. Same arguments, same returned id.
- To reach many chats, call `bot.send_many(chat_ids, text=...)`, which is
  synchronous. From async code, `await bot.asend_many(chat_ids, text=...)`: the
  coroutine queues nothing until it is awaited. Both queue a chunk per round trip
  and return one id per chat, and both queue inside the bot container too, where
  `send` would call Telegram directly. With `ENABLED=0` neither writes anything
  and you still get the ids, the same as `send`. They speed up queueing only —
  the rate limits still pace delivery.
- Handlers go in <app>/tg_router.py and are registered with decorators on the
  shared bot: @bot.message(F.text), @bot.callback_query(...). They are ordinary
  async Django code; use afirst()/sync_to_async for the ORM.
- Bot-wide defaults such as parse_mode belong in
  TELEGRAM_BOT['DEFAULT_BOT_PROPERTIES'], not in every send call.
- Settings live in the TELEGRAM_BOT dict; scalars can come from
  DJANGO_AIOGRAM_<NAME> environment variables.
- The package is safe to import with no TOKEN and no Redis. Do not add
  placeholder credentials to make imports work, and do not guard imports in
  try/except.
- `from django_aiogram import bot` loads aiogram (~900 ms), which is the
  cost of sending and is paid once. Import it in the modules that send, not in a
  package `__init__` that every process loads.
- Only the container running `manage.py start_tgbot` runs the bot. Do not set
  DJANGO_AIOGRAM_ENABLED=0 on web or Celery processes: it turns their
  sends into no-ops and the messages are dropped.
- Queued payloads are JSON. Keep SERIALIZER='json'. Writing pickle takes BOTH
  SERIALIZER='pickle' and ALLOW_PICKLE=True — the flag alone only lets the
  reader accept pickle, so a payload JSON cannot describe is still refused at
  the point it is queued. It is the escape hatch for exactly those payloads,
  and turning it on means whoever can write to the queue can execute code
  in the bot container, so do it only on a queue nothing untrusted can write to.
- A queued send cannot raise in the caller. Failures are logged by the worker.
  Use bot.send_raw with RAISE_EXCEPTION only when the caller must see the error.
- To send later, pass an aware datetime as `eta` to any form that **queues** —
  send, enqueue, send_many and their awaiting twins, but never send_raw, which
  reaches Telegram from this process and has nothing to schedule:
  `bot.send(chat_id=..., text=..., eta=timezone.now() + timedelta(hours=1))`. It
  writes a row and publishes nothing, so the deployment must run
  `manage.py tgbot_dispatch_scheduled` — from cron or with `--loop` — or the
  message waits for ever. `bot.cancel_scheduled(identifier)` calls it off while it
  is still waiting. Do not reach for Celery's countdown for this; do not pass a
  naive datetime, which is refused.
- To edit or delete a message you queued, read its `message_id` back with
  `bot.outcome(identifier)` — the id `send()` returned is a correlation id and not
  Telegram's. Pass an explicit `correlation_id=uuid4()` to any send whose own
  outcome you will use: inside a handler the id is inherited from the update, so
  every reply shares it and `outcome()` answers about the newest of them. For a
  send_media_group read `answer.sent`, which holds an entry per message of the
  album; `answer.message_id` is only its first.
- Branch on the outcome's state before acting on it, all four of them. Only
  `sent` may be edited, and check `message_id` and `chat_id` are not None even
  then — a call that produced no message, such as send_chat_action, is `sent`
  with neither. `failed` means stop. `pending` and `unknown` mean ask again
  later, within a bound of your own: `unknown` can be permanent, so past that
  bound treat it as unresolved rather than polling on. Do not write a loop that
  blocks a request waiting for it.
- It needs EVENT_LOG=True and an EVENT_LOG_KINDS that is either empty or keeps
  outbound.sent, outbound.failed, outbound.dropped and outbound.queued — **in the
  start_tgbot container as well as in the process that asks.** The rows are
  written by whichever process sent the message, and the refusal can only speak
  for the settings it can see: a bot container with the log off leaves the
  lookup answering `unknown` with nothing to refuse.
- `python manage.py check` validates the settings; treat its E0xx/W0xx output as
  the spec.
- Run `python manage.py migrate` after upgrading. The package ships two tables,
  created whether or not you turn the event log on.
- bot.send() returns a correlation id. Store it next to your own model if you
  want to join your records to the event log later.
- For metrics, connect a receiver to `events_recorded` from
  `django_aiogram.eventlog.signals` in an AppConfig.ready(). Do not invent a settings
  hook: there is none. It fires with EVENT_LOG off, so metrics need no table and no
  migration, and the exporter must run in the start_tgbot container because that is
  where send outcomes are recorded.
- The event log is off by default (TELEGRAM_BOT['EVENT_LOG']). Turning it on
  needs a retention job — `manage.py tgbot_prune_events` — or the table grows
  without bound. Message bodies are not stored unless EVENT_LOG_PAYLOAD='full',
  which is a personal-data decision, not a verbosity one.
```

## Prompts that work

**Add a notification.** *"In `orders/views.py`, notify the reviewer over Telegram
when an order is approved. Use `bot.send` from `django_aiogram` so the
request does not wait on Telegram, and add a test that asserts the message was
queued — see the Testing page of the django-aiogram wiki for the fakeredis
recipe."*

**Add a handler.** *"Add `support/tg_router.py` with a `/status` command that
answers with the caller's open ticket count. Register it with `@bot.message`
from `django_aiogram`, keep the ORM access async, and do not touch
`INSTALLED_APPS` — autodiscover imports `tg_router` from every installed app."*

**Set up the containers.** *"Add a `telegram_bot` service to
`docker-compose.yml` running `python manage.py start_tgbot`, restarting always,
depending on redis, sharing the same image and `.env` as `back`. Give it a
healthcheck running `python -m django_aiogram.healthcheck` with
`DJANGO_SETTINGS_MODULE` in its `environment:` — the probe is a separate process, and
`manage.py` only sets that variable inside its own process. Not
`manage.py tgbot_healthcheck` in a healthcheck: it runs `django.setup()` first and
Docker kills it at the timeout. Leave `DJANGO_AIOGRAM_ENABLED` unset on the other services
— they queue messages."*

**Turn on the event log.** *"Run `manage.py migrate` first, then enable
`TELEGRAM_BOT['EVENT_LOG']` in django-aiogram — a process that starts
recording before the table exists drops everything it records until someone
notices. Then set `EVENT_LOG_RETENTION_DAYS` and schedule
`manage.py tgbot_prune_events` daily. Leave `EVENT_LOG_PAYLOAD` at its default
so message bodies stay out of the table, and grant support only
`view_telegramevent`."* See **[Event log](Event-log.md)**.

**Migrate an older project.** *"This project imports `telegram_bot`, which
`django-redis-aiogram` 3.0 removed, and that distribution is now `django-aiogram`.
Move it to `django_aiogram` 4.x following the wiki's Upgrading page, newest section
first: rename it in `INSTALLED_APPS`, replace the imports with the 4.0 layout, move
`parse_mode` into `DEFAULT_BOT_PROPERTIES`, drop the placeholder token from settings,
and use `bot.router` instead of `bot._router`."* See
**[Upgrading](Upgrading.md)**.

**Debug delivery.** *"Messages are queued but never arrive. Check in this order:
is the `start_tgbot` container running and is `ENABLED` true there, does
`bot.queue_depth()` grow — it asks whichever transport `BROKER` names, where a
`redis-cli llen` answers for the list transport alone — and what does the
`django_aiogram` logger say. The wiki's Troubleshooting page lists the
causes per symptom."*

## What assistants get wrong

Each of these has been seen in real integrations, and each is a 1.x habit:

| Mistake | Why it happens | What to do instead |
| --- | --- | --- |
| A placeholder `TOKEN` in settings so imports work | 1.x built the bot at import time and crashed without one | Nothing. Since 2.0 it imports fine with no credentials |
| `parse_mode` in every `send` call | 1.x had no other way | `DEFAULT_BOT_PROPERTIES` once |
| `DJANGO_AIOGRAM_ENABLED=0` on web and Celery | it reads like "do not run the bot here" | Leave it unset; only `start_tgbot` runs the bot |
| `TelegramBot()` inside a task | the shared instance looks stateful | Import `bot` |
| `bot._router` | it was private for a long time | `bot.router` |
| `try/except` around the import | defensive habit from the crashing version | Import it plainly |
| Expecting a queued send to raise | the call looks synchronous | The worker logs it; use `send_raw` if the caller must know |
| `SERIALIZER: 'pickle'` "for keyboards" | true in 1.0.4, false since aiogram 3 | JSON round-trips keyboards, media and files |

## Working on this package, not with it

`AGENTS.md` in the repository root is the brief for that: layout, the commands
CI runs, and the invariants that have dedicated tests. Anything an agent changes
here needs a test that fails when the change is reverted.
