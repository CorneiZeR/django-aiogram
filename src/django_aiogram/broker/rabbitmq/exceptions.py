"""What can go wrong reaching RabbitMQ that cannot go wrong reaching Redis."""

from django_aiogram.broker.exceptions import BrokerError

__all__ = ('QueueRefusedError',)


class QueueRefusedError(BrokerError):
    """The broker would not take the message, and said so before ``publish`` returned.

    This transport publishes with ``mandatory`` and publisher confirms on, so a message that
    cannot be routed comes back as an error rather than disappearing. That is a deliberate
    cost: measured, the confirmed, mandatory, persistent publish this transport makes takes
    323 to 393 microseconds against 18 to 20 for the same publish with only the confirm off.

    It is what keeps the promise the package already makes everywhere else. ``RPUSH`` answers
    with the new list length, so a Redis publish is acknowledged by the server before
    ``send()`` returns and a failure raises; ``basic_publish`` without confirms writes to a
    socket and returns, so a broker that died before persisting the message would lose it in
    silence. Nobody would find out until the message never arrived.
    """

    def __init__(self, queue: str, reason: str) -> None:
        """Name the queue and what the broker said, since neither is guessable from a trace.

        Both kept as attributes as well as formatted into the message, which is what the Kafka
        refusal does and for the same reason: a caller telling a missing queue from a broker out
        of resources should not have to parse English to do it.
        """
        self.queue = queue
        self.reason = reason
        super().__init__(
            f'RabbitMQ refused a message for queue {queue!r}: {reason}. The queue may not '
            f'exist, or the broker may be out of resources — a publish here is confirmed, so '
            f'this is the broker declining rather than a message quietly going nowhere.'
        )
