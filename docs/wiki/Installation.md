# Installation

```shell
pip install 'django-aiogram[redis]'
```

Requires Python 3.10–3.14, Django 5.2+, aiogram 3.30+, and — for the Redis
transport — redis 6.2+.

**The transport driver is an extra**, so a deployment downloads the one it uses and
not the other three. `BROKER` names the transport and nothing is inferred from what
happens to be installed, so the two have to agree; when they do not, `manage.py check`
says so with the install line for the one you named:

```text
?: (django_aiogram.E047) TELEGRAM_BOT['BROKER'] names
   'django_aiogram.broker.redis_list.RedisListBroker', whose driver is not installed.
	HINT: pip install "django-aiogram[redis]"
```

A base `pip install django-aiogram` is a valid install — it imports, and every
`manage.py` command runs — it just cannot carry a message anywhere yet. A process
with `ENABLED` off is not asked for a driver, so a web container that only records the
event log needs no extra. A process that reads the queue depth does need one even when
disabled: those reads are not gated on `ENABLED`, and `manage.py check` cannot tell
which processes make them.

The redis floor is 6.2 because aiogram's `RedisStorage` asks for it, and
`FSM_STORAGE: 'redis'` is the default. On redis-py below 5.0.1 the storage
raises `AttributeError: 'Redis' object has no attribute 'aclose'`. redis-py 8 is
tested here and works, though aiogram's own optional extra stops at 7.

## Add the app

```python
# settings.py
import os

INSTALLED_APPS = [
    ...,
    'django_aiogram',
]

TELEGRAM_BOT = {
    'TOKEN': os.environ.get('TELEGRAM_BOT_TOKEN', ''),
    'REDIS_URL': os.environ.get('REDIS_URL', ''),
}
```

That is the whole minimum, and both values may be empty at startup. The
package needs them only when something actually reaches Telegram or Redis, so
tests, migrations and a build all run without them.

The package ships one table, so run migrations after adding it:

```shell
python manage.py migrate
```

The table is created whether or not you turn the event log on — `EVENT_LOG` is
off by default, and nothing is written until you set it. See
**[[Event-log|Event log]]**.

## Configure from the environment

Scalar settings can come from `DJANGO_AIOGRAM_<NAME>`:

```ini
# .env
DJANGO_AIOGRAM_TOKEN=123:abc
DJANGO_AIOGRAM_REDIS_URL=redis://redis:6379/0
DJANGO_AIOGRAM_ENABLED=0
```

Django settings win over the environment. Callables and mappings —
`DEFAULT_KWARGS`, `DEFAULT_BOT_PROPERTIES`, `RATE_LIMIT` — have no sensible
textual form and stay in `settings.py`.

## Run the bot

```yaml
# docker-compose.yml
services:
  telegram_bot:
    image: ${IMAGE}
    command: python manage.py start_tgbot
    restart: always
    env_file: .env
    depends_on: [redis]

  redis:
    image: redis:7-alpine
    restart: always
```

See **[[Deployment]]** for the whole file, and for turning the bot off in every
other process.

## Check the configuration

```shell
python manage.py check
```

Settings are validated: wrong types, unknown keys, misspelled bot properties
and impossible rate limits all fail here rather than at the first message.
Missing credentials are reported as warnings, not errors, so a build or a
migration container is not blocked by them.

Run it somewhere the bot is enabled: a process with `ENABLED` off registers no
checks, so it reports nothing either way — unless the event log is on there, which
registers all of them on its own, bot settings included.

## Next

* **[[Handlers]]** to answer messages
* **[[Sending-messages|Sending messages]]** to send them
