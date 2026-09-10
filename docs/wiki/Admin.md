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
| Delivery and limits | this bot's own `overrides` — anything absent comes from its profile, then from the deployment's defaults |
| State | whether it is switched on, and what the supervisor last said about it |

`view_telegrambot_token` follows the precedent the event feed set with
`view_telegramevent_payload`: being allowed to switch a bot off is a different question from
being allowed to read what sends as it. Without it the credentials section is **not rendered at
all** rather than masked — a section that renders is a section that posts back, and a token field
left in place would let a user who cannot read the credential replace it.

## The request never talks to Telegram

Saving a row moves its watermark and publishes a notice; a supervisor reads it and acts, seconds
later. Nothing here calls `getMe` or `setWebhook`, which is what makes an action over five hundred
selected rows return immediately instead of holding a request open for five hundred round trips.

`quarantine_reason` and `quarantined_until` are the supervisor's own writing, read-only here: they
say what happened to a bot, and a person clearing the text would not clear the condition. The
switch a person reaches for is `enabled`.
