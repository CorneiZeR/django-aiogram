# Testing

Nothing happens at import time, so your suite needs neither a token nor a
reachable Redis. What it does need is a decision: are the sends part of what you
assert, or noise you want gone?

## Settings for tests

```python
# settings/test.py
TELEGRAM_BOT = {
    'FSM_STORAGE': 'memory',  # no Redis for dialogue state
}
```

That is enough. `TOKEN` and `REDIS_URL` may stay empty — they are only read when
something actually reaches Telegram or Redis.

Setting `'ENABLED': False` goes further: every send becomes a no-op, the `await`
and bulk forms included — though not `queue_depth()` and `inflight_depth()`, which
are reads and still want a reachable Redis. Convenient when Telegram is irrelevant to the suite,
wrong if any test asserts that a message was queued — those assertions would pass
over nothing, and they would pass over nothing *quietly*, because each call still
returns the id it would have used.

## `TRANSACTIONAL` and `TestCase`

If the project sets `TRANSACTIONAL` to `True`, a send made inside Django's `TestCase`
never reaches the broker: `TestCase` wraps each test in a transaction it rolls back, so
the commit the write is waiting for never comes. That is the setting working, not a bug — but an assertion
that a message was queued will fail, and it will fail in a way that reads like the code
under test never sent one.

Two ways out. `TransactionTestCase` (or `@pytest.mark.django_db(transaction=True)`) commits
for real, so the write happens where a deployment would make it. Or
`captureOnCommitCallbacks(execute=True)`, which runs the hooks the block queued and is
cheaper:

```python
with self.captureOnCommitCallbacks(execute=True):
    accept_order(order)
```

## Asserting that your code queued a message

`django_aiogram.testing` ships the helper, so a test names the call rather than the bytes:

```python
from django_aiogram.testing import capture_sends


def test_approval_notifies_the_reviewer():
    with capture_sends() as sent:
        approve(order)  # your code, which calls bot.send(...)

    assert sent.kwargs == [{'chat_id': 42, 'text': 'Order approved'}]
```

No server, no settings to arrange and nothing patched. For the duration of the block this
process's broker is an in-memory one, so every producer runs for real — `send`, `enqueue`,
`send_many`, their awaiting twins, and the mover behind `eta` — and the messages land where
the helper can read them.

Each record has names on it, and `correlation_id` is the same value `bot.send()` returned:

```python
from django_aiogram.testing import capture_sends


def test_the_reply_carries_the_id_the_caller_got():
    with capture_sends() as sent:
        identifier = notify(order)

    assert len(sent) == 1
    assert sent[0].function == 'send_message'
    assert sent[0].kwargs['chat_id'] == 42
    assert sent[0].correlation_id == identifier
```

`sent.of('send_photo')` narrows to one method, for a block that queues more than one kind.
Reading never consumes: assert what was queued and then run the consumer over the same
messages if that is what the case is about.

**Three things it does not catch**, each on purpose:

- `send_raw` reaches Telegram from the calling process and never queues.
- `'ENABLED': False` makes every send a no-op — the capture is empty, and each call still
  returns the id it would have used.
- `'TRANSACTIONAL': True` holds the write until the caller's transaction commits, so read the
  capture *after* the block rather than inside it, and see the section above about `TestCase`.

### As a fixture, or as a mixin

Neither half of the Django world is the assumed one. For pytest, register the plugin once:

```python
# conftest.py
pytest_plugins = ('django_aiogram.testing.plugin',)
```

and take `telegram_sends`, which captures the whole test:

```python
def test_approval_notifies(telegram_sends):
    approve(order)

    assert telegram_sends.kwargs == [{'chat_id': 42, 'text': 'Order approved'}]
```

For `TestCase`, mix `SendCaptureMixin` in **before** the case class, and read `self.sent`:

```python
from django.test import TestCase

from django_aiogram.testing import SendCaptureMixin


class ApprovalTests(SendCaptureMixin, TestCase):
    def test_the_reviewer_is_told(self):
        approve(self.order)

        assert self.sent.kwargs == [{'chat_id': 42, 'text': 'Order approved'}]
```

### The whole suite on an in-memory broker

Where the consumer is what a test drives — delivery, acknowledgement, reclaiming — point
`BROKER` at the shipped double and skip the capture entirely:

```python
# settings/test.py
TELEGRAM_BOT = {
    'FSM_STORAGE': 'memory',
    'BROKER': 'django_aiogram.testing.InMemoryBroker',
}
```

It is a real broker rather than a stub: publish, take, ack, release, reclaim and the depths,
held to the same contract as the four transports in this package's own conformance suite. Its
one option is `MEMORY_TIMEOUT` (1.0 by default), which bounds how long a `take` waits for a
message that has not arrived — there is no IO here for a deadline to be about.

Nothing is written down, so nothing survives the process, and each `override_settings` of
`TELEGRAM_BOT` starts from an empty queue. `crash_safe` is `False` and says so, which is what
stops it being mistaken for something to deploy.

For a fixture of your own that wants the same thing without the capture,
`django_aiogram.broker.registry.use_broker(broker)` is the seam underneath: it makes an
instance this process's broker for the length of a block, ahead of `BROKER` rather than
through it, so an `override_settings(TELEGRAM_BOT=...)` inside the block cannot take it away.

### Reading the queue by hand

The escape hatch, for a test that really does mean to assert on the wire format — and the only
way to do this before 4.1. Point the connection at [fakeredis](https://pypi.org/project/fakeredis/)
and read the list back. `loads` decodes a payload the same way the worker does, and `unpack`
reads the envelope it is wrapped in:

```python
import fakeredis
from django.test import override_settings

from django_aiogram import bot
from django_aiogram.wire.envelope import unpack
from django_aiogram.wire.serializers import loads


@override_settings(TELEGRAM_BOT={'REDIS_URL': 'redis://localhost:6379/0'})
def test_approval_notifies_the_reviewer(monkeypatch):
    server = fakeredis.FakeRedis()
    monkeypatch.setattr('django_aiogram.broker.redis_list.broker.get_redis', lambda: server)

    approve(order)  # your code, which calls bot.send(...)

    queued = [unpack(loads(raw)) for raw in server.lrange('TELEGRAM_BOT_MESSAGE', 0, -1)]
    assert [(call.function, call.kwargs) for call in queued] == [
        ('send_message', {'chat_id': 42, 'text': 'Order approved'}),
    ]
```

**Two costs, which are why the helper above exists.** `loads`, `unpack` and the transport's own
key are this package's internals, so a suite written this way is pinned to a wire format that
moves — envelope v1 accepts the 2.x shape precisely because it moved once. And the recipe is
Redis-shaped: on RabbitMQ or Kafka there is nothing here to copy.

3.0 nests the arguments under an envelope so a message can carry a correlation
id — see **[Event log](Event-log.md)**. Reading through `unpack` is what keeps a
test from having to know that: `call.correlation_id` is there if you want it,
and `bot.send()` returns the same value.

Patch `django_aiogram.broker.redis_list.broker.get_redis` — the name the transport looks
up. In 4.0 the producer hands a payload to a broker and the broker owns the connection, so
that is where a fake belongs; patching `django_aiogram.redis.get_redis` alone leaves the real
connection in place, and patching the producer no longer reaches the write at all.

**That covers the synchronous sends only.** On the Redis transports, and in a process
that queues rather than delivers, `asend`, `aenqueue`, `asend_many`, `aqueue_depth` and
`ainflight_depth` go through `aget_redis`, which keeps one client per running loop — so a
test that patched the synchronous name and then awaited one of these opened a real
connection. Patch the builder underneath it and the registry still runs for real:

Both qualifications matter when a fixture is copied. Inside the worker, `asend` calls
`send_raw` and reaches no broker at all, so a fake on `aget_redis` there watches a path the
test never takes. And RabbitMQ and Kafka have no async client to patch: their drivers are
synchronous, so the awaiting methods borrow a thread and use the same connection the
synchronous ones do — which is the connection the section above patches.

```python
import fakeredis
import fakeredis.aioredis
import pytest


@pytest.fixture
def fake_redis(monkeypatch):
    """One in-memory server behind both halves, sync and async."""
    server = fakeredis.FakeServer()
    client = fakeredis.FakeRedis(server=server)
    monkeypatch.setattr('django_aiogram.broker.redis_list.broker.get_redis', lambda: client)
    monkeypatch.setattr(
        'django_aiogram.redis.build_async_client',
        lambda: fakeredis.aioredis.FakeRedis(server=server),
    )
    return client
```

A fixture rather than four loose lines, because `monkeypatch` is one: copied into a
module as it stands, the calls above have no `monkeypatch` to reach and raise
`NameError`. Take `fake_redis` in the test and read the queue off its return value.

One `FakeServer` behind both, so a message queued through `asend` is visible to a
synchronous read of the queue.

This is the queue path, which is what a web or Celery process takes. In the *worker*
process `asend` calls Telegram directly through `send_raw` and never reaches
`aget_redis`, so a test asserting on the queue there should call `aenqueue`
explicitly rather than rely on the routing. This package's own `redis_server` fixture is exactly
this, and patches `aget_redis`'s *builder* rather than `aget_redis` itself for the
same reason the note above gives: patching the accessor leaves the thing under test
untested.

## Faking the send instead

When the payload is not the point, replace the call:

```python
from django_aiogram import bot


def test_approval_notifies(monkeypatch):
    sent = []
    monkeypatch.setattr(bot, 'send', lambda **kwargs: sent.append(kwargs))

    approve(order)

    assert sent == [{'chat_id': 42, 'text': 'Order approved'}]
```

## Testing a handler

Call it. A handler is an ordinary coroutine, and `Message` is a pydantic model
you can build:

```python
import asyncio
import datetime

from aiogram import types

from myapp.tg_router import start_handler


def a_message(text):
    return types.Message(
        message_id=1,
        date=datetime.datetime.now(datetime.timezone.utc),
        chat=types.Chat(id=42, type='private'),
        text=text,
    )


def test_start_greets():
    message = a_message('/start')
    replies = []

    async def answer(text, **kwargs):
        replies.append(text)

    # aiogram models refuse plain attribute assignment
    object.__setattr__(message, 'answer', answer)

    asyncio.run(start_handler(message))

    assert replies == ['Hello 42']
```

## Testing that the filters route

Feed a dispatcher an update when the filter is the thing under test. `a_message` is the builder from the section above:

```python
import asyncio

from aiogram import Bot, Dispatcher, F, Router, types


def test_only_text_reaches_the_echo():
    seen = []
    router = Router()

    @router.message(F.text == '/probe')
    async def probe(message):
        seen.append(message.text)

    dispatcher = Dispatcher()
    dispatcher.include_router(router)

    asyncio.run(dispatcher.feed_update(Bot(token='42:x'), types.Update(update_id=1, message=a_message('/probe'))))

    assert seen == ['/probe']
```

`Bot(token='42:x')` opens no connection on its own, and the handler above only
records — so nothing reaches Telegram.

**A handler that answers is a different matter.** `feed_update` hands the
handler a *copy* of the event bound to the bot, so patching `answer` on the
message you constructed has no effect: the copy carries the real one, and
`await message.answer(...)` performs an actual API call. Stub the bot's session
instead, which is the seam every reply goes through:

```python
from aiogram.client.session.base import BaseSession


class RecordingSession(BaseSession):
    def __init__(self):
        super().__init__()
        self.calls = []

    async def close(self):
        pass

    async def make_request(self, bot, method, timeout=None):
        self.calls.append(method)

    async def stream_content(self, *args, **kwargs):  # pragma: no cover - unused
        yield b''


def test_the_handler_answers():
    session = RecordingSession()
    fake = Bot(token='42:x', session=session)
    ...
    asyncio.run(dispatcher.feed_update(fake, update))

    assert [type(call).__name__ for call in session.calls] == ['SendMessage']
    assert session.calls[0].text == 'you have 3 open orders'
```

Calling the handler directly, as in the section above, avoids all of this and is
the better choice unless the routing itself is what you are testing.

To exercise your real routing, include `bot.router` instead of a fresh one. Mind
the order: aiogram stops at the first handler that matches, so a catch-all
`@bot.message()` registered earlier swallows everything after it — in a test as
much as in production.

## Testing the worker itself

You should not have to. If you want an end-to-end check, queue a message with
`enqueue`, then run the consumer once against fakeredis:

```python
from django_aiogram import bot
from django_aiogram.consumer.delivery import BlpopDelivery

# with get_redis patched to fakeredis as above
bot.enqueue(chat_id=42, text='hi')

handled = []
# the consumer also passes correlation_id and queued_at, which a handler
# standing in for send_raw can ignore
delivery = BlpopDelivery(handler=lambda function, **payload: handled.append((function, payload)))
delivery.consume_pending()  # drains the list without blocking

function, payload = handled[0]
assert function == 'send_message'
assert payload['chat_id'] == 42
assert payload['text'] == 'hi'
```

`consume_pending()` returns as soon as the list is empty, so it needs no thread
and no timeout.

## What this project's own tests do

`tests/conftest.py` patches every alias of `get_redis` at once, which is why a
single fixture covers the client, the delivery consumer and the helpers. If a
recipe here stops working, `tests/test_documented_recipes.py` fails — the
snippets above are executed, not just written down.
