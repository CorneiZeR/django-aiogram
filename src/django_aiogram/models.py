"""The tables this app ships: the append-only feed, and the operational state around it.

Django imports this on every ``django.setup()`` — before ``AppConfig.ready()``
and regardless of ``ENABLED`` — so it may not reach aiogram, directly or
otherwise, and may not read settings at import time.

One is the feed. Everything else is operational state -- mutable, and none of it routed to a
log database of its own, because a claim the software cannot enforce because the log lives in a
warehouse would be no claim at all:

* `TelegramScheduledSend`, the sends waiting for a time: a mover claims one, publishes it and
  deletes it.
* `TelegramReplayClaim`, one row per failure `tgbot_replay` is putting back. It exists because
  a unique constraint is the only claim that is atomic on all four databases this package
  supports.
* `TelegramBotProfile`, `TelegramQueue` and `TelegramBot`, which are what a project configures
  a bot in when it does not configure it in ``settings.py``.
* `TelegramBotLease`, which is how two containers agree on which of them polls a bot.

Everything after the feed's own class is about those. The feed itself is below.

Rows are inserted and never updated. The stages of one outbound message are
three rows sharing a ``correlation_id``: the web process writes the queued row
and the bot container writes the delivered one, with no coordination between
them and no foreign key either way. That only works because the feed is
insert-only, which is also what keeps pruning cheap and the table shardable.
"""

from django.db import models
from django.utils import timezone

from django_aiogram.eventlog.events import MAX_KIND_LENGTH, SHORT_ID_LENGTH


class TelegramEvent(models.Model):
    """One thing that happened to one message, update or handler."""

    id = models.BigAutoField(primary_key=True)
    # stamped by whoever recorded it: the writer batches, so auto_now_add would
    # record when the batch was flushed rather than when the thing happened
    created_at = models.DateTimeField(default=timezone.now)
    # time-ordered (UUIDv7), so this index appends rather than scattering
    correlation_id = models.UUIDField()
    # the same id, in twelve characters a person can read aloud and type back. Stored rather than
    # derived on the way out, because the point of it is the *search*: the code names 60 of the
    # random bits and cannot be turned back into a UUID, so without a column there is nothing to
    # filter an indexed column by. Indexed and not unique -- see `events.short_id` for why two rows
    # may share one, and the admin shows both rather than pretending
    short_id = models.CharField(max_length=SHORT_ID_LENGTH, blank=True, db_index=True)
    # no choices: see events.kind_choices for why the registry stays in Python
    kind = models.CharField(max_length=MAX_KIND_LENGTH)

    function = models.CharField(max_length=64, blank=True)
    chat_id = models.BigIntegerField(null=True, blank=True)
    user_id = models.BigIntegerField(null=True, blank=True)
    message_id = models.BigIntegerField(null=True, blank=True)
    update_id = models.BigIntegerField(null=True, blank=True)
    # the same name the in-flight list uses, so a row points at a container
    worker = models.CharField(max_length=128, blank=True)
    #: which bot the row is about, by the number in its token. Null for a row written before
    #: there was more than one, and for a bot whose token has no identity to read -- `E052`
    #: reports that. A plain column and not a relation: the feed takes no foreign keys, and
    #: the identity outlives any row describing the bot
    bot_id = models.BigIntegerField(null=True, blank=True)

    attempt = models.PositiveSmallIntegerField(default=0)
    duration_ms = models.PositiveIntegerField(null=True, blank=True)
    error_code = models.CharField(max_length=64, blank=True)
    error = models.TextField(blank=True)
    detail = models.JSONField(default=dict, blank=True)

    class Meta:
        """Portable everywhere: no constraints, no relations, four indexes."""

        db_table = 'django_aiogram_event'
        # by id, not created_at: id is the insert order and unique, so the admin
        # paginator gets a total order without a tie-breaker column
        ordering = ('-id',)
        verbose_name = 'telegram event'
        verbose_name_plural = 'telegram events'
        # Django's four stock permissions, plus the one it has no equivalent for:
        # there are no field-level permissions, and seeing that a message went
        # out is a different question from reading what it said
        permissions = (('view_telegramevent_payload', 'Can see event payloads and error text'),)
        # named explicitly and kept short: Oracle rejects an identifier over 30
        indexes = (
            models.Index(fields=('correlation_id',), name='dja_event_correlation'),
            # two consumers: the changelist's created_at sort header and the
            # prune watermark. Django appends -pk to make the sort deterministic,
            # which this cannot serve on its own — measured, the descending header
            # sorts only the last term, within rows sharing one created_at, and
            # the ascending one needs no sort at all. A pair of (created_at, -id)
            # indexes would remove even that, at the price of two more writes per
            # row on a table whose whole design is cheap inserts
            models.Index(fields=('-created_at',), name='dja_event_recent'),
            # by -id, matching `ordering`: on (kind, -created_at) every filtered
            # changelist sorted in a temp b-tree, page query and bounded count
            # alike, which is what made the count's documented bound untrue
            models.Index(fields=('kind', '-id'), name='dja_event_kind_id'),
            models.Index(fields=('chat_id', '-id'), name='dja_event_chat'),
            # by -id for the reason the kind index gives: it matches `ordering`, so a
            # changelist filtered to one bot pages without a sort
            models.Index(fields=('bot_id', '-id'), name='dja_event_bot'),
        )

    def __str__(self) -> str:
        """Name the event the way an admin row reads."""
        return f'{self.kind} {self.function}'.strip()


class TelegramScheduledSend(models.Model):
    """One send that is not due yet, and the payload it will go out as.

    **Why a table and not a transport feature.** Of the four transports only RabbitMQ can
    delay a message at all, and only with a plugin or a dead-letter detour; a Redis list, a
    stream and a Kafka topic cannot. Building on the one that almost can would make ``eta``
    a setting that works on a quarter of the deployments, which is the opposite of what
    ``BROKER`` promises. So the wait happens above the broker contract, and the moment a row
    comes due it becomes an ordinary queued message on whichever transport is configured.

    **The payload is stored serialized**, exactly as an immediate send would have written it.
    Two things follow. A payload the project cannot serialize raises where the call was
    written rather than out of a mover hours later; and the bytes cannot drift, so the
    consumer receives what the caller meant even if the settings changed in between.

    Not routed to ``EVENT_LOG_DATABASE``. The feed is a record and may live in a warehouse of
    its own; this is operational state a producer writes and a mover consumes, and it belongs
    with the caller's other writes -- which is also what makes a scheduled send inside
    ``atomic()`` roll back with the transaction, needing nothing from ``TRANSACTIONAL``.
    """

    id = models.BigAutoField(primary_key=True)
    created_at = models.DateTimeField(default=timezone.now)
    #: the same id the queued and delivered rows will carry, and the handle a cancellation
    #: names. Not unique, though **not** because of `send_many` -- that gives every chat its
    #: own, measured. Two rows share one where a caller passed an explicit `correlation_id`
    #: to more than one scheduled send, or where a handler's replies inherited an update's
    correlation_id = models.UUIDField(db_index=True)
    #: when it may be published. Indexed with `claimed_at`, which is the mover's only query
    due_at = models.DateTimeField()
    function = models.CharField(max_length=64)
    #: for the admin and for a drop row; the payload is the authority
    chat_id = models.BigIntegerField(null=True, blank=True)
    #: which bot this will go out as. The payload carries it too -- it is stamped into the
    #: envelope where the send was written -- and this column is what a mover serving one bot
    #: filters on. No index of its own: the mover's query is `claimed_at, due_at` and a
    #: leading `bot_id` would not serve it, so the flag that needs one brings it
    bot_id = models.BigIntegerField(null=True, blank=True)
    #: the envelope as `serialise` produced it, ready for `Broker.publish`
    payload = models.BinaryField()
    #: set by the mover that owns this row. A second mover skips a claimed row rather than
    #: waiting for it -- until the claim lapses, which is what `claimed_until` is for
    claimed_at = models.DateTimeField(null=True, blank=True)
    claimed_by = models.CharField(max_length=128, blank=True)
    #: when this claim stops being believed, or ``None`` for a claim that never lapses.
    #: **On the row rather than in a setting**, and that is the whole point: the mover's
    #: lease is a command flag, so a producer asking "may this be cancelled?" would have had
    #: to guess at it. The row says when it comes free, and `claim` and `cancel` read the
    #: same fact. ``None`` beside a set `claimed_at` means `--lease 0`: held until an
    #: operator says otherwise
    claimed_until = models.DateTimeField(null=True, blank=True)
    #: how many times a mover has tried to publish this and failed. Bounded by
    #: ``--max-attempts``, because a lease turns "the claim stays and nothing retries it"
    #: into "every lease, for ever" -- a payload the broker refuses permanently would
    #: otherwise write one more drop row per lease until somebody noticed.
    #:
    #: A ``BigInteger`` because ``--max-attempts 0`` retries without end, so this counter has
    #: no bound of its own to stop at. A ``SmallInteger`` stops at 32767 -- four months of a
    #: 300-second lease -- and then a database that enforces the column range refuses the
    #: increment and takes the whole pass down with it. There is no lease short enough or
    #: deployment long enough to reach the end of this one
    attempts = models.PositiveBigIntegerField(default=0)

    class Meta:
        """Portable everywhere, and one index: the query the mover runs."""

        db_table = 'django_aiogram_scheduled'
        ordering = ('due_at', 'id')
        verbose_name = 'scheduled telegram send'
        verbose_name_plural = 'scheduled telegram sends'
        indexes = (
            # the mover asks for unclaimed rows that are due, oldest first. `claimed_at`
            # leads because it is the more selective of the two once a backlog builds:
            # everything claimed is on its way out
            models.Index(fields=('claimed_at', 'due_at'), name='dja_scheduled_due'),
        )

    def __str__(self) -> str:
        """Name the row the way an admin list reads."""
        return f'{self.function} at {self.due_at:%Y-%m-%d %H:%M:%S}'


class TelegramReplayClaim(models.Model):
    """One failure ``manage.py tgbot_replay`` is putting back, claimed so that nothing else does.

    **A row with a unique constraint, because that is the only claim that is atomic on all four
    databases this package supports.** PostgreSQL has advisory locks and MySQL has ``GET_LOCK``;
    SQLite has neither, which is the same reason `producer/scheduling.claim` is a compare-and-set
    update rather than ``SELECT ... FOR UPDATE SKIP LOCKED``. Two runs racing for one failure
    therefore produce one ``INSERT`` and one ``IntegrityError``, everywhere.

    Without it the command read whether a failure had been replayed, queued the message, and
    then wrote the row saying so -- three steps two processes could interleave, which is a
    recovered message delivered twice. For a command whose whole purpose is repairing an
    incident, that is the wrong side of the at-least-once trade this package makes elsewhere.

    **The claim is not the audit row.** ``outbound.replayed`` in the feed is what a person reads,
    and the feed may live on ``EVENT_LOG_DATABASE`` -- a different database, unjoinable in SQL.
    This lives with the caller's own writes, like the schedule, so a claim is readable and
    enforceable without the log's alias being reachable at all.

    A claim survives a queue write that did not answer -- a process that died between claiming
    and queueing, or a ``publish`` that raised after the bytes went -- and that is what
    ``queued_at`` and the lease are for: the row says whether the message is known to have
    reached the queue, and one that never said so is retakeable after ``--claim-lease``. Retaking
    one may send a second copy -- the same trade, and the same arithmetic answer, as the mover's
    own lease.
    """

    id = models.BigAutoField(primary_key=True)
    #: the failure being replayed. Unique **with the bot**, so the second run to reach it is
    #: told by the database rather than by a read it has to trust
    correlation_id = models.UUIDField()
    #: which bot's failure this is. ``0`` where there is no identity to read rather than
    #: ``NULL``, and that is the whole of it: a unique index treats two NULLs as distinct on
    #: every database here, so a nullable column would let two runs claim one failure and send
    #: the message twice -- which is the one thing this table exists to prevent
    bot_id = models.BigIntegerField(default=0)
    claimed_at = models.DateTimeField(default=timezone.now)
    #: which process holds it, so a stale claim names something an operator can look at
    claimed_by = models.CharField(max_length=128, blank=True)
    #: when the replacement reached the queue, and ``None`` while it has not. The difference
    #: between "this failure is handled" and "somebody is handling it", which is what makes a
    #: crashed run recoverable without guessing
    queued_at = models.DateTimeField(null=True, blank=True)
    #: the id the replacement went out under, so the pair is joinable here as well as through
    #: the feed's ``detail.replay_of`` -- which is on the log's alias and may be elsewhere
    replacement_id = models.UUIDField(null=True, blank=True)

    class Meta:
        """One row per failure per bot, and no index beyond the one the constraint builds."""

        db_table = 'django_aiogram_replay_claim'
        ordering = ('claimed_at', 'id')
        verbose_name = 'telegram replay claim'
        verbose_name_plural = 'telegram replay claims'
        constraints = (models.UniqueConstraint(fields=('bot_id', 'correlation_id'), name='dja_replay_claim_once'),)

    def __str__(self) -> str:
        """Name the row the way an admin list reads."""
        state = 'queued' if self.queued_at else 'claimed'
        return f'{state} {self.correlation_id}'


class TelegramBotProfile(models.Model):
    """The settings a group of bots shares, as a row a person can edit.

    A profile is what :mod:`django_aiogram.runtime.profiles` computes an identity from, and
    this is where the values come from when they are not in ``settings.py``: a project running
    tens of bots on three configurations writes three of these and points every bot at one.

    **Sparse, by key presence.** ``overrides`` holds only what this profile decides; anything
    absent is inherited from the shared defaults. Not a column per setting and not ``NULL``
    meaning "inherit": ``RATE_LIMIT: None`` is a value -- it switches the limits off -- and a
    nullable column cannot say both. That is the same rule the settings dicts follow, and one
    rule is what keeps the three levels readable.
    """

    id = models.BigAutoField(primary_key=True)
    #: what a bot points at and an operator reads. Unique because it is the name in the bot's
    #: row rather than a description
    name = models.CharField(max_length=64, unique=True)
    #: only the settings this profile decides, keyed as the settings dicts are
    overrides = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    #: the watermark a reconciling supervisor reads: a change here is a change to every bot on
    #: this profile, and comparing one timestamp is cheaper than resolving them all
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        """Portable everywhere: one unique name, and the timestamp a supervisor polls."""

        db_table = 'django_aiogram_bot_profile'
        ordering = ('name',)
        verbose_name = 'telegram bot profile'
        verbose_name_plural = 'telegram bot profiles'
        indexes = (models.Index(fields=('-updated_at',), name='dja_profile_changed'),)

    def __str__(self) -> str:
        """Name the profile the way an admin list reads."""
        return self.name


class TelegramQueue(models.Model):
    """One declared queue, and the label that says which containers serve it.

    **Declared rather than conjured.** A queue a producer names and nobody consumes is where
    messages go to be forgotten, and a typo is all it takes -- which is why Celery's
    declare-on-publish is the one thing not copied from it. A name that is not here is refused
    where it was written.

    ``pool`` is the deployment axis and the queue name is not: a container is told which pools
    to serve, and pools are what stay stable while the queues under them come and go with the
    clients that need one of their own.
    """

    id = models.BigAutoField(primary_key=True)
    #: what the transport calls it -- a Redis list key, a stream, an AMQP queue, a Kafka topic
    name = models.CharField(max_length=255, unique=True)
    #: which set of containers serves it. Several queues share a pool; a container is started
    #: with the pools rather than with the queues, so a new client's queue needs no redeploy
    pool = models.CharField(max_length=64, default='default')
    created_at = models.DateTimeField(default=timezone.now)
    #: the watermark a reconciling supervisor polls, for the reason the profile's says -- and
    #: here for one of its own: a bot's `QUEUE` is read from the row it points at, so a queue
    #: renamed while nothing else moved has to be a change the providers can see
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        """Portable everywhere, and one index: the set a container asks for at startup."""

        db_table = 'django_aiogram_queue'
        ordering = ('pool', 'name')
        verbose_name = 'telegram queue'
        verbose_name_plural = 'telegram queues'
        indexes = (models.Index(fields=('pool',), name='dja_queue_pool'),)

    def __str__(self) -> str:
        """Name the queue and the pool it is served by."""
        return f'{self.name} ({self.pool})'


class TelegramBot(models.Model):
    """One bot a project configured at run time, rather than in ``settings.py``.

    What a settings section says, as a row: the token, which profile it takes its settings
    from, which queue it publishes to, and whether it is on. A project that adds bots through
    its own interface writes these, and a provider reads them —
    :mod:`django_aiogram.config.bots` is where the sections live and this is the other source.

    **The identity is the key, not the row's own id.** The number in front of the colon in the
    token is what a queued message, a feed row and a log line name a bot by, so it is what
    everything else joins on -- and it survives a token rotation, which is exactly when a row
    changes underneath.
    """

    id = models.BigAutoField(primary_key=True)
    #: the number in front of the colon in the token, and the only identity anything else
    #: uses. Unique, because two rows for one bot are two configurations for one Telegram
    #: account -- see `E051`, which reports the same thing about the settings
    bot_id = models.BigIntegerField(unique=True)
    #: what a person calls it: the client's name, the product's, whatever the admin shows
    label = models.CharField(max_length=128, blank=True)
    #: the credential, as ``TOKEN_STORAGE`` writes it. The shipped default writes it as it
    #: was given, and saying so is the point: what protects it then is the database's own
    #: access control and the permission below, which is why that permission exists at all.
    #: Treat a dump of this table as a dump of every bot's credential.
    #:
    #: A project that needs it encrypted at rest names the storage behind the ``[crypto]``
    #: extra instead, and `manage.py tgbot_rewrap_tokens` is what moves a table that already
    #: has tokens in it. Nothing reads this column directly -- both directions go through
    #: `django_aiogram.tokens`, so turning the storage on leaves no plaintext path behind
    token = models.TextField(blank=True)
    #: where its settings come from. ``None`` means the shared defaults and nothing else
    profile = models.ForeignKey(
        TelegramBotProfile,
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        related_name='bots',
    )
    #: where its messages are published. ``None`` means whatever its profile resolves to
    queue = models.ForeignKey(
        TelegramQueue,
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        related_name='bots',
    )
    #: only what this bot decides, over its profile. Sparse for the reason the profile's are
    overrides = models.JSONField(default=dict, blank=True)
    #: whether a supervisor should be serving it at all. The switch a person reaches for, as
    #: against the quarantine below, which the software sets
    enabled = models.BooleanField(default=True)
    #: why the software stopped serving it, if it did: a revoked token, a conflict on its
    #: updates. Empty while nothing is wrong. Set by the supervisor rather than by a person
    quarantine_reason = models.CharField(max_length=64, blank=True)
    #: when it may be tried again, or ``None`` for a quarantine that needs a person -- a token
    #: Telegram has refused is not going to start working on a timer
    quarantined_until = models.DateTimeField(null=True, blank=True)
    #: what an operator asked to have done to this bot, for a process with an event loop to
    #: do. Empty while there is nothing outstanding. **A request never talks to Telegram**: a
    #: page that called `getMe` for five hundred selected bots would hold the request open for
    #: five hundred round trips, so what the admin writes is the asking and something that
    #: already holds this bot answers it
    intent = models.CharField(max_length=32, blank=True)
    #: when it was asked, so an intent nothing has picked up is visible as one
    intent_asked_at = models.DateTimeField(null=True, blank=True)
    #: which process is carrying it out, and since when. A **lease** rather than a flag: the
    #: asking stays in `intent` until an answer is written, so a worker that died holding one
    #: loses it to whoever asks next instead of taking the only record of it with them. And a
    #: result is written only by the claim that is still held, so a slow worker cannot replace
    #: the answer to a question somebody asked after it
    intent_claim = models.CharField(max_length=64, blank=True)
    intent_claimed_at = models.DateTimeField(null=True, blank=True)
    #: what came back, in one line a person can read. Never the token: aiogram puts the API
    #: URL into its messages, so what is written here is redacted the way a feed row is
    intent_result = models.CharField(max_length=200, blank=True)
    #: when the answer was written, which is what makes a stale result readable as stale
    intent_done_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    #: the watermark a reconciling supervisor polls, for the reason the profile's says
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        """Portable everywhere, and the two questions a supervisor asks."""

        db_table = 'django_aiogram_bot'
        ordering = ('label', 'bot_id')
        verbose_name = 'telegram bot'
        verbose_name_plural = 'telegram bots'
        # Django's four stock permissions plus the one it has no equivalent for, exactly as the
        # feed does for its payloads: being allowed to switch a bot off is a different question
        # from being allowed to read the credential that sends as it
        permissions = (('view_telegrambot_token', 'Can see bot tokens'),)
        indexes = (
            models.Index(fields=('-updated_at',), name='dja_bot_changed'),
            # what a supervisor reconciles from: the bots it should be serving
            models.Index(fields=('enabled', 'bot_id'), name='dja_bot_serving'),
        )

    def __str__(self) -> str:
        """Name the bot the way an admin list reads, without printing the token."""
        return f'{self.label} ({self.bot_id})'.strip() if self.label else str(self.bot_id)


class TelegramBotLease(models.Model):
    """Which process is serving one bot's updates, so that only one of them is.

    Polling is exclusive: two processes calling ``getUpdates`` for one token get a 409 and
    half the updates each. This is how they agree, and it is the same mechanism
    `TelegramReplayClaim` and `producer.scheduling.claim` use for the same reason -- a
    compare-and-set against a unique row is the only claim that is atomic on every database
    this package supports. No ``SKIP LOCKED``, which SQLite does not have.

    **A lease rather than a lock.** A process that dies holding one would otherwise strand its
    bots for ever, so the claim expires and another process takes it. What that costs is a
    second poller for as long as the two overlap, which Telegram answers with a 409 -- the same
    arithmetic the mover's lease makes, and the reason the renewal interval belongs well inside
    the lease.

    Webhook deployments need none of this: an update arrives wherever the request landed.
    """

    id = models.BigAutoField(primary_key=True)
    #: the bot being served, by its identity. Unique: that is the claim
    bot_id = models.BigIntegerField(unique=True)
    #: which process holds it, so a stale lease names something an operator can look at
    holder = models.CharField(max_length=128)
    claimed_at = models.DateTimeField(default=timezone.now)
    #: when this lease stops being believed. Renewed well inside it by the holder; a process
    #: that stops renewing loses its bots to whoever asks next
    expires_at = models.DateTimeField()

    class Meta:
        """One row per bot, and the query a process runs to find what it may take."""

        db_table = 'django_aiogram_bot_lease'
        ordering = ('bot_id',)
        verbose_name = 'telegram bot lease'
        verbose_name_plural = 'telegram bot leases'
        indexes = (models.Index(fields=('expires_at',), name='dja_lease_expiry'),)

    def __str__(self) -> str:
        """Name the lease the way an operator reads it: who holds what, until when."""
        return f'{self.bot_id} held by {self.holder} until {self.expires_at:%Y-%m-%d %H:%M:%S}'
