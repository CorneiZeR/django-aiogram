# Sending messages

```python
from django_aiogram import bot

bot.send(chat_id=CHAT_ID, text='hello')
```

`send()` picks the route: inside the bot container it calls Telegram directly, anywhere else
it queues the call through whichever transport `BROKER` names. Callers do not have to know which
process they are in.

Both routes are gated on `ENABLED`. A disabled process reaches neither Telegram nor the broker
and returns the correlation id anyway, so a caller storing it beside its own row gets the same
value whether or not this deployment sends.

Any Telegram API method aiogram exposes works — pass its name first. The name
is checked against that allowlist, so a queued payload cannot reach anything
else on the bot:

```python
bot.send('send_photo', chat_id=CHAT_ID, photo=URL, caption='look')
bot.send('send_chat_action', chat_id=CHAT_ID, action='typing')
```

## Choosing the route yourself

| Method | Behavior |
| ------ | --------- |
| `send()` | direct in the bot container, queued elsewhere |
| `enqueue()` | always queue |
| `send_raw()` | always call Telegram from this process |

`send_raw` from a web process that does not serve the webhook builds its own event
loop and HTTP session. That works, but it does not share the bot's rate-limit budget.
Prefer `send()`. A process that *does* serve the webhook is the case below.

**Whether it waits changed in 3.1.0, in a web process that also serves the
webhook.** A process serving the webhook gives the loop a thread of its own from
the first update it handles, and `send_raw` hands work to a running loop rather
than driving it. So from that point on it *schedules* and returns, where before
it drove the loop and blocked until Telegram answered.

What that costs is the exception: a send that fails after the retries used to
raise into your view under `RAISE_EXCEPTION`, and now appears in the log instead.
A process that never serves the webhook is unaffected — with no thread running
the loop, `send_raw` still drives it and waits.

From inside a **handler** it could never wait — a handler already runs on that
loop, so it can only schedule. What 3.1.0 changes there is that the scheduled
send now *runs*: before, nothing stepped it until the next update arrived, or
`close()`, or never.

If you need the answer, `await` the aiogram call yourself, or send from a process
that does not serve the webhook.

## From an async view, and to many chats

```python
async def notify(request):
    await bot.asend(chat_id=CHAT_ID, text='done')


def announce(chat_ids):
    return bot.send_many(chat_ids, text='we are back')


async def announce_from_async(chat_ids):
    return await bot.asend_many(chat_ids, text='we are back')
```

`asend` is `send` for code already on an event loop. **Outside the bot container**, where it
queues, the synchronous one writes to a socket on the calling thread — which under ASGI is the
thread serving requests, and on the first call that includes a connect bounded by whatever
timeout the transport `BROKER` names declares. Inside the worker both take the direct route
instead: `send_raw` schedules onto the bot's loop and returns, the first connection is to
Telegram rather than to a broker, and no `BROKER` setting is involved.

So the two routes share their ids and their event rows and share no socket: one writes to the
queue `BROKER` names, the other to Telegram. That is the whole of the difference, and it is why
`send()` deciding for you is the point rather than a convenience.

`send_many` queues one message per chat, a chunk of them per round trip, and
returns an id per message in the order the chats were given. `asend_many` is its
loop-friendly twin, and the case for it is stronger than for `asend`: a fan-out
writes once per chunk and serializes every payload, so the synchronous one holds
the calling thread that much longer. What `asend_many` moves off the way is the
waiting, not the work — it still serializes each chunk on the loop's own thread
between its awaits, so a broadcast big enough to notice belongs in a task rather
than in a request.

Two things it does **not** do. It does not speed up delivery — the rate limits
still pace what leaves for Telegram, so fifty thousand chats is about half an hour
at the default thirty a second. And it makes event-log overflow *worse*, because
the pacing that sequential round trips gave the writer is gone: raise
`EVENT_LOG_BUFFER_SIZE`, or narrow `EVENT_LOG_KINDS`, before broadcasting. See
**[Event log](Event-log.md)**.

A chunk that fails records a drop for its own messages and raises. Earlier chunks
are already queued and their ids are lost with the exception, which is why those
rows exist rather than leaving you to work out how far it got.

## Keyboards

```python
from aiogram import types

markup = types.InlineKeyboardMarkup(
    inline_keyboard=[
        [types.InlineKeyboardButton(text='Approve', callback_data='approve:42')],
        [types.InlineKeyboardButton(text='Open', web_app=types.WebAppInfo(url=URL))],
    ]
)

bot.send(chat_id=CHAT_ID, text='Review this', reply_markup=markup)
```

Keyboards survive the queue intact, including through a JSON round trip.

## Files

`file_id` and URLs are the cheapest thing to send, and always safe to queue:

```python
bot.send('send_photo', chat_id=CHAT_ID, photo='https://example.test/a.png')
bot.send('send_document', chat_id=CHAT_ID, document=EXISTING_FILE_ID)
```

Actual uploads work too:

```python
from aiogram.types import BufferedInputFile, FSInputFile, URLInputFile

bot.send('send_document', chat_id=CHAT_ID, document=FSInputFile('/app/media/report.pdf'))
bot.send('send_photo', chat_id=CHAT_ID, photo=BufferedInputFile(data, filename='chart.png'))
```

`FSInputFile` carries a path, so the file has to exist in the **bot container**
too — share a volume, or send bytes with `BufferedInputFile`.

## Sending it later

`eta` puts a send in a table instead of on the queue, and a mover publishes it when the time
comes:

```python
from datetime import timedelta

from django.utils import timezone

identifier = bot.send(
    chat_id=CHAT_ID,
    text='Your appointment is tomorrow',
    eta=timezone.now() + timedelta(hours=23),
)
```

Nothing happens until `manage.py tgbot_dispatch_scheduled` runs, so **schedule that** — from
cron, or with `--loop` in a container of its own. Until it does, the row is visible with
`--dry-run` and the feed carries an `outbound.scheduled` row saying what is waiting and for
when.

There is no admin page for the schedule: `--dry-run` is how you look at what is waiting, and
`bot.cancel_scheduled(identifier)` is how you take something out of it. The feed is where the
history lives, as it does for everything else here.

`bot.cancel_scheduled(identifier)` calls it off and answers with how many rows it removed.
Zero means nothing was waiting — which is also the answer once a mover has claimed the row,
because by then the message is on its way and deleting the row would not stop it.

**A count can be partial where an id names more than one waiting send.** `send_many` is not
that case: it gives every chat its own id. It happens where a caller passed one explicit
`correlation_id` to several scheduled sends, or where a handler's replies inherited the
update's — then some rows may be claimed and some not, and the number is only what went. Pass
an id per send if you need to call them off one at a time.

**Why a table and not the transport.** Of the four only RabbitMQ can delay a message, and
only with a plugin or a dead-letter detour; a Redis list, a stream and a Kafka topic cannot.
Building on the one that almost can would make `eta` work on a quarter of the deployments,
against everything `BROKER` promises — so the wait sits above the transport, and a row that
comes due becomes an ordinary queued message on whichever one is configured.

Four things worth knowing before you use it:

- **A scheduled send is a database write**, on the caller's own connection. So it rolls back
  with the transaction that made it and needs nothing from `TRANSACTIONAL` — and the awaiting
  twins block on it, because Django has no asynchronous `bulk_create`.
- **The payload is frozen when you call.** A payload the project cannot serialize raises
  there rather than out of a mover hours later, and the bytes cannot drift if the settings
  change in between.
- **An `eta` already past comes due at once** rather than being refused: clock skew between a
  web tier and a database is not a caller's problem, and "as soon as possible" is a
  reasonable thing to schedule. It is still never faster than the mover's interval.
- **A naive datetime is refused** under `USE_TZ`. Django would attach the project's
  `TIME_ZONE` to it, and a message an hour early is not worth guessing at.

Inside the bot container `eta` schedules like anywhere else — it is the one case where `send`
does not call Telegram directly, because sending now cannot be what an `eta` meant.

## Editing a message you queued

`send()` hands back a correlation id, not a `message_id` — the reply Telegram gave belongs
to the bot container, and the caller is somewhere else. The id an edit needs is recorded
against that correlation id, and `bot.outcome()` reads it back:

```python
from uuid import uuid4


def start_the_import(job):
    # an id of its own, so the outcome is about this message and no other
    identifier = bot.send(chat_id=CHAT_ID, text='Import started…', correlation_id=uuid4())
    Job.objects.filter(pk=job.pk).update(telegram_correlation_id=identifier)
    # after the commit: called inside `atomic()`, `delay()` can start the task before the
    # update above has landed, and it would read the id the row held before -- None
    transaction.on_commit(lambda: finish_the_import.delay(job.pk))


@app.task(bind=True, max_retries=5)
def finish_the_import(self, job_pk):
    # the row, not the instance `update()` left behind: a queryset update writes to the
    # database and touches nothing in memory, so `job.telegram_correlation_id` on a stale
    # instance is whatever it held before -- usually None
    job = Job.objects.get(pk=job_pk)
    answer = bot.outcome(job.telegram_correlation_id)
    if answer.state == 'failed':
        return
    if answer.state != 'sent':
        # `pending` and `unknown` both mean ask again, and `max_retries` is the bound past
        # which the honest answer is unresolved rather than another poll
        raise self.retry(countdown=5)
    bot.send(
        'edit_message_text',
        chat_id=answer.chat_id,
        message_id=answer.message_id,
        text='Import finished',
    )
```

It needs `EVENT_LOG` on and an `EVENT_LOG_KINDS` that is empty or keeps the four kinds a
correct outcome requires, **in the bot container as well as here** — the rows are written by
whichever process sent the message, and the refusal can only speak for the process that
asks. A `log.dropped` row is the other reason a delivered message has none. A state of
`pending` or `unknown` means ask again — within a bound of your own, because `unknown` can
be permanent and after that bound the honest answer is *unresolved* rather than another
poll.

**Pass an explicit `correlation_id` to any send whose own outcome you will use.** Inside a
handler the id is inherited from the update, so every reply shares one and `outcome()`
answers about the newest of them — an edit built on that can reach the wrong message. A
`send_media_group` is the other multiple: one call, one row, and an `answer.sent` entry per
message, where `answer.message_id` is only the first of the album.
**[Event log](Event-log.md#what-became-of-one-message)** has the four states and what
`unknown` does not tell you. There is no waiting built in on purpose: blocking a request on
the bot container is what the queue exists to avoid.

## Inside a transaction

A send writes to the broker as it is called, and that write is not part of your
transaction:

```python
with transaction.atomic():
    order = Order.objects.create(...)
    bot.send(chat_id=CHAT_ID, text=f'Order {order.pk} accepted')
    charge(order)  # raises
```

The row is gone and the message is not. `TRANSACTIONAL` holds the **queue** write until the
commit, so the block above announces nothing when it rolls back:

```python
TELEGRAM_BOT = {'TRANSACTIONAL': True}
```

It is off by default because it moves when a message reaches the queue.
**[Settings](Settings.md#bot-behavior)** has what changes with it on — the event row waits
too, and a publish that fails after the commit cannot undo it. Without the setting, the
same guarantee is `transaction.on_commit(lambda: bot.send(...))` written at each call site.

The other route is unaffected, and deliberately: `send_raw` — and `send` inside the bot
container, which is the same thing by
[the rule above](#choosing-the-route-yourself) — calls Telegram rather than the broker, so a
handler replying to an update does not wait for anything to commit.

## Errors

Queued messages are delivered by the worker; failures are logged there, not
raised in your view. For direct calls, `RAISE_EXCEPTION` propagates them — but
only where `send_raw` still waits for the answer, which in a process that serves
the webhook it does not. See [Choosing the route yourself](#choosing-the-route-yourself)
above: there the failure reaches the log rather than the `except` below.

```python
from aiogram.exceptions import TelegramBadRequest

try:
    bot.send_raw(chat_id=CHAT_ID, text='**broken*', parse_mode='Markdown')
except TelegramBadRequest:
    ...
```

Telegram rate-limit refusals are retried up to `MAX_RETRIES`; exhausting them
logs an error and, with `RAISE_EXCEPTION`, re-raises — into the caller that was
waiting, so the same qualification applies. See **[Rate limits](Rate-limits.md)**
for staying under the limits in the first place.

## From Celery

Queue the call and let the bot container do the talking:

```python
@app.task
def notify(chat_id: int, text: str) -> None:
    bot.send(chat_id=chat_id, text=text)
```

Do not build a `TelegramBot()` per task — the shared `bot` is lazy and safe to
import anywhere; a fresh instance means a fresh event loop and HTTP session
that nothing closes.
