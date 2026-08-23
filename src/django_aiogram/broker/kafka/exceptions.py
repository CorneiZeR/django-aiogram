"""What can go wrong producing to Kafka that cannot go wrong elsewhere."""

from django_aiogram.broker.exceptions import BrokerError

__all__ = ('ProduceRefusedError',)


class ProduceRefusedError(BrokerError):
    """The broker did not confirm the message, and this transport waits for that.

    Kafka's producer answers locally: ``produce()`` appends to librdkafka's queue and returns,
    measured at 0.2 microseconds, and the broker's acknowledgement arrives later on a delivery
    callback. Returning at that first point would be a weaker promise than the rest of this
    package makes — ``RPUSH`` answers with the new list length before ``send()`` returns — so
    this transport waits, at 166 to 237 microseconds across repeated runs, and reports what came
    back.

    That makes Kafka the second slowest publish of the four, behind RabbitMQ's confirmed and
    persistent one at 323 to 393, and the number belongs in the documentation rather than in a
    footnote: it is the guarantee's price, not the driver's — though not only: measured over
    five runs, ``aiokafka`` waits 354 to 390 microseconds for the same acknowledgement, so this
    driver is the faster one as well.
    """

    def __init__(self, topic: str, reason: str) -> None:
        """Name the topic and what librdkafka said, since neither is in the traceback."""
        self.topic = topic
        super().__init__(
            f'Kafka would not accept a message for topic {topic!r}: {reason}. The topic may not '
            f'exist and automatic creation may be off, or the required acknowledgements may be '
            f'unavailable — this transport waits for the broker rather than returning once the '
            f'message is queued locally, so this is a refusal rather than a silent loss.'
        )
