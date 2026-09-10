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

**Reading one is a page of its own.** Next to the mask is *show it*, which asks for a
confirmation and then writes a `bot.token_revealed` row into the event feed: the bot, and who
asked. So "who saw this token" has an answer during the incident where somebody has to ask it —
and a credential is never in a changeform, a browser history or a screenshot of one by accident.


## Fieldsets are permission boundaries

| section | what is on it |
| --- | --- |
| Bot | the label a person reads, and the identity |
| Credentials | the mask and the write-only token field. **Behind `view_telegrambot_token`** |
| Placement | the profile it inherits from, and the queue it publishes to |
| Limits, Updates, Delivery, Bot behaviour | one line per setting this bot may decide, collapsed until you open them |
| State | whether it is switched on, and what the supervisor last said about it |

Adding a bot needs that permission too: a new bot has no token to keep, so adding one *is*
setting a credential. And the field is absent from the form a user without it gets, not just
from the page — a section that is merely unrendered still posts back, and a token accepted from
somebody who may not read one is the bot given away.

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

Two settings *are* credentials — `WEBHOOK_SECRET`, and `REDIS_URL` because it carries the
broker's password — so what the page prints for them is `'set'` or `'not set'` and the layer,
never the value, and their pairs are only offered to a user who may see this bot's token.

The values are judged **together**, and against the profile this submission chose rather than the
one the row held: `W004` decides whether a `BLPOP_TIMEOUT` will be honoured from
`HEARTBEAT_INTERVAL` and the transport's deadline, so a setting judged one at a time — or under
the old profile — is refused while the configuration is right. A bot being added is resolved by
the identity in the token being submitted, because a bot's own environment variables are keyed
by it.

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

Renaming a queue reaches the bots pointed at it within a poll: the providers watch the three
tables a resolved bot reads, so the rename is a change they can see rather than one that waits
for a restart.

Filters cover the switch, the profile, the queue and the pool; **Switch on** and **Switch off**
are actions, so five hundred clients are one statement rather than five hundred saves. A
profile or a queue shows how many bots are on it, as a link to exactly those bots.

## The request never talks to Telegram

Saving a row moves its watermark and publishes a notice; a supervisor reads it and acts, seconds
later. Nothing here calls `getMe` or `setWebhook`, which is what makes an action over five hundred
selected rows return immediately instead of holding a request open for five hundred round trips.

## Edits that would strand a backlog are refused

Pointing a bot at a different queue leaves whatever is in the old one with nobody to take it: a
consumer serves the queues it was told about, and after the move nothing points there. So the
edit is refused while that queue still holds messages, and the message says what to do first —
let it drain, or switch the bot off and drain it deliberately.

Judged by the queue each configuration **resolves to**, not by the picker alone: a bot with no
queue of its own takes one from its profile or the deployment's defaults, and moving that one
strands a backlog like any other.

A queue the transport **cannot be reached** to read is not a queue with messages in it: the edit
goes through, because a page that refused every change while Redis blinked would be worse than
the rare mistake. That is the same trade the supervisor makes about a provider it could not read.
A transport that cannot be **built** — a `BROKER` naming nothing importable, a driver that is not
installed — is refused instead: nobody can read that queue at all, then or later.

**This is a guard, not a guarantee.** The depth read and the save are two steps, so a message
published between them lands in the queue the bot is leaving. Closing that would mean holding a
lock across every `bot.send()` in the deployment for an edit somebody makes twice a year. What
the check catches is the ordinary mistake — moving a client with a visible backlog — and the
order with no race in it is the one the message names: switch the bot off, let the queue drain,
then move it.

`quarantine_reason` and `quarantined_until` are the supervisor's own writing, read-only here: they
say what happened to a bot, and a person clearing the text would not clear the condition. The
switch a person reaches for is `enabled`.

Switching a bot on or off moves its `updated_at`, which is the watermark a supervisor polls —
so the change lands within a poll rather than at the next restart.
