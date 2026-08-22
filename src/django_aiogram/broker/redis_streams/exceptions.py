"""What can go wrong with a stream that cannot go wrong with a list."""

from django_aiogram.broker.exceptions import BrokerError

__all__ = ('StreamLagUnknownError', 'StreamServerTooOldError')


class StreamServerTooOldError(BrokerError):
    """The server cannot answer how much is waiting, so this transport refuses it.

    ``XINFO GROUPS`` grew the ``lag`` field in Redis 7.0, and it is the only way to ask how
    many entries a group has not been delivered. Measured on 6.2.24 the field is absent
    altogether; Redis has no command that counts a range, so the alternative is an
    ``XRANGE`` scan of everything past ``last-delivered-id`` — and a ``depth()`` that drives
    ``HEALTHCHECK_MAX_QUEUE`` must not be an estimate.

    Refusing at first use rather than returning a number nobody should trust. The package
    floor stays 6.2 for the Redis list, which needs nothing newer.

    Probed rather than read off a version string: the question is whether *this* server
    answers with the field, and one ``XINFO GROUPS`` says so without depending on ``INFO``
    being available at all.
    """

    def __init__(self) -> None:
        """Name the field that is missing and the version that has it."""
        super().__init__(
            'The Redis Streams broker needs Redis 7.0 or newer: this server answers XINFO '
            'GROUPS without a `lag` field, so how many messages are waiting cannot be '
            'answered exactly. Use the Redis list broker, which works on 6.2, or upgrade '
            'the server.'
        )


class StreamLagUnknownError(BrokerError):
    """The server can no longer say how many messages are waiting.

    ``XINFO GROUPS`` answers ``lag`` with nil once entries have been removed from the middle
    of the stream: measured, one ``XDEL`` of an undelivered entry turns a lag of 4 into
    ``None`` and it stays that way for the life of the group. Redis is not being coy — it
    counts by arithmetic on entry ids, and a hole makes the arithmetic wrong.

    So this refuses rather than substituting a number. ``depth()`` drives
    ``HEALTHCHECK_MAX_QUEUE`` and the queue-depth warning, and a plausible-looking estimate
    there is worse than an outage: it reads as a healthy queue.

    The fix is an operator's, which is why the message names it: stop deleting entries, and
    restore countability with ``XSETID <key> <last-id> ENTRIESADDED <n>``. Trimming through
    this broker never causes it — see :meth:`RedisStreamsBroker.trim`, which stops at the
    oldest unacknowledged entry.
    """

    def __init__(self, key: str, group: str) -> None:
        """Name the stream and group, because a project may run more than one."""
        self.key = key
        self.group = group
        super().__init__(
            f'Redis cannot say how many messages are waiting in {key!r} for group {group!r}: '
            f'XINFO GROUPS reports no lag, which happens once entries have been deleted from '
            f'the stream. Restore it with XSETID, and do not XDEL or MAXLEN-trim this stream '
            f'— this broker trims only up to the oldest unacknowledged entry.'
        )
