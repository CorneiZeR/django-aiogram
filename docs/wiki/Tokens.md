# Tokens

A token is the bot. Anyone holding it can read every message the bot receives and send as it,
and Telegram has no way to tell that apart from the real thing — so where the token is kept is
a decision, and since 5.0 it is one this package can be told about.

## Where a token comes from

Two places, and they are not the same kind of secret:

- **`TELEGRAM_BOT_DEFAULTS['TOKEN']` or a `TELEGRAM_BOTS` section** — a token in the
  deployment's own configuration, usually from an environment variable. Nothing here stores it;
  it lives wherever the settings do.
- **A `TelegramBot` row** — a bot a client connected through the project's own interface. This
  is the one a database dump carries.

## `TOKEN_STORAGE`

Every read and write of a row's token goes through one seam, so a project that turns
encryption on has no plaintext path left behind:

```python
TELEGRAM_BOT_DEFAULTS = {
    'TOKEN_STORAGE': 'django_aiogram.crypto.FernetTokenStorage',
    'TOKEN_ENCRYPTION_KEYS': [os.environ['TG_TOKEN_KEY']],
}
```

The default is `django_aiogram.tokens.PlainTokenStorage`, which writes the value as it was
given. That is a real answer rather than an oversight: where the database is already the trust
boundary, what protects the token is its access control and the admin permission on the
column — and a base install then never imports `cryptography`.

The encrypting one needs the extra:

```shell
pip install "django-aiogram[crypto]"
```

Both settings belong to the process rather than to a bot — a section that sets one is refused
by `E053`. `E062` reports a storage that cannot be built, and `E063` an encrypting storage
with no keys; both are read at boot, so `manage.py check` answers rather than the first row
that could not be read.

## Turning it on for a table that already has tokens

```shell
python manage.py tgbot_rewrap_tokens
```

The storage reads a value it did not write — every plain token from before — as it is, so the
order is a settings change, a deploy, and then this walk. Nothing is down in between, and a
row the walk has not reached yet is still readable by every process.

`--dry-run` says what it would write. A row whose column is already what the storage writes is
left alone, because `updated_at` is the watermark every supervisor polls and a table-wide bump
would have every container re-read every bot for nothing.

## Rotating a key

The keys are listed newest first: the first one encrypts, every one of them decrypts.

```python
TELEGRAM_BOT_DEFAULTS = {
    'TOKEN_STORAGE': 'django_aiogram.crypto.FernetTokenStorage',
    'TOKEN_ENCRYPTION_KEYS': [os.environ['TG_TOKEN_KEY_NEW'], os.environ['TG_TOKEN_KEY_OLD']],
}
```

1. Add the new key **in front** and deploy. Old rows still read.
2. Run `manage.py tgbot_rewrap_tokens`. Each row is rewritten under the new key.
3. Drop the old key from the list and deploy again.

Doing step 3 before step 2 is the one mistake that costs data: the rows written under the old
key stop being readable, and the bots in them are reported and left out rather than served —
one bot's credential is one bot's problem, and the healthy bots keep running. Put the key back
and run the walk.

A new key is Fernet's own:

```shell
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

## What is not a secret store

This encrypts a column. It does not hide the token from a process that serves the bot — that
process has to talk to Telegram — and it does not help if the key sits in the same dump as the
rows. Keep the key where the deployment's other secrets are.

Nothing writes a token into the event log: `wire/payloads.py` redacts token-shaped strings
anywhere in a recorded value, which covers the API URL aiogram puts into its exception
messages. See **[Event log](Event-log.md)**.
