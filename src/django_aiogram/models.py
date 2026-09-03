"""The two tables this app ships: the append-only feed, and the sends waiting for a time.

Django imports this on every ``django.setup()`` — before ``AppConfig.ready()``
and regardless of ``ENABLED`` — so it may not reach aiogram, directly or
otherwise, and may not read settings at import time.

`TelegramScheduledSend` is the second, and the only mutable row this app has: a mover
claims one, publishes it and deletes it. Everything below is about the feed.

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
