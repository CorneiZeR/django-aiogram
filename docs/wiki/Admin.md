# Admin

For a deployment whose clients bring their own bot, the admin is the interface: bots are added
there, tokens rotated there, a noisy client throttled there, and a client who left switched off
there. Three models are registered — **bots**, **profiles** and **queues** — from
`AppConfig.ready()`, so nothing is read at import.

Registration needs no setting: a web container that sends nothing and records nothing still has
to be able to configure the bots the other containers serve.

## The token is write-only

The token field renders nothing and posts a new value. **Empty means keep**, so a credential can
be rotated by somebody who cannot read the one being replaced.

What a page shows instead is a mask — the identity, which is not a secret, and eight dots for the
half that is. The identity is read *from the token* rather than typed: it is what every queued
message, feed row and log line names a bot by, and a mis-keyed digit is a row that serves
somebody else's bot.

A new token is stored through `TOKEN_STORAGE`, like every other write — see
**[Tokens](Tokens.md)**.

## Fieldsets are permission boundaries

| section | what is on it |
| --- | --- |
| Bot | the label a person reads, and the identity |
| Credentials | the mask and the write-only token field. **Behind `view_telegrambot_token`** |
| Placement | the profile it inherits from, and the queue it publishes to |
| Limits, Updates, Delivery, Bot behaviour | one line per setting this bot may decide, collapsed until you open them |
| State | whether it is switched on, and what the supervisor last said about it |

`view_telegrambot_token` follows the precedent the event feed set with
`view_telegramevent_payload`: being allowed to switch a bot off is a different question from
being allowed to read what sends as it. Without it the credentials section is **not rendered at
all** rather than masked — a section that renders is a section that posts back, and a token field
left in place would let a user who cannot read the credential replace it.

## Overriding a setting is a decision, so it is a checkbox

Each setting on those four sections is a pair: a box that says **this bot decides it**, and
the value. Next to the box is what the bot would use if you left it alone, and which layer
that comes from:

```text
MAX_RETRIES     Inherited: 5 — from the profile 'vip'.
RATE_LIMIT      Inherited: {'overall_per_second': 30, ...} — from the deployment defaults.
```

The checkbox exists because `overrides` is sparse **by key presence**, not by value:
`RATE_LIMIT: None` is a real setting — it switches pacing off — so "inherit" cannot be spelled
as an empty field or a null. Unticking a box hands the setting back to the profile.

Values are judged by the package's own checks: a `MAX_RETRIES` of zero is refused on the page
because `E012` refuses it at boot. One law, two consumers — a second copy of the rules in the
form is how a page comes to accept a configuration the deployment then refuses to start with.

`TOKEN`, `QUEUE`, `ENABLED` and `DEFAULT_KWARGS` are not on those sections: the first three
have a place of their own on the page — the credentials section, the queue picker and the
switch — and the fourth is a callable, which no form can hold. Settings the *process* owns are
absent too, and `E053` is what reports one written into a row anyway.

## What the list is for

The bot changelist is meant to be scanned rather than opened row by row:

| column | the question it answers |
| --- | --- |
| pool | where this bot's work is done — the axis a container is started with |
| own settings | which settings this bot decides for itself, because a bot behaving unlike its neighbours is usually carrying an old override |
| serving | whether anything is serving it: switched on, and not quarantined |

Filters cover the switch, the profile, the queue and the pool; **Switch on** and **Switch off**
are actions, so five hundred clients are one statement rather than five hundred saves. A
profile or a queue shows how many bots are on it, as a link to exactly those bots.

## The request never talks to Telegram

Saving a row moves its watermark and publishes a notice; a supervisor reads it and acts, seconds
later. Nothing here calls `getMe` or `setWebhook`, which is what makes an action over five hundred
selected rows return immediately instead of holding a request open for five hundred round trips.

`quarantine_reason` and `quarantined_until` are the supervisor's own writing, read-only here: they
say what happened to a bot, and a person clearing the text would not clear the condition. The
switch a person reaches for is `enabled`.

Switching a bot on or off moves its `updated_at`, which is the watermark a supervisor polls —
so the change lands within a poll rather than at the next restart.
