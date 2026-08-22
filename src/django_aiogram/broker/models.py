"""The shapes a broker hands back. No behaviour, so every transport can build them."""

from typing import NamedTuple

__all__ = ('Liveness', 'Taken')


class Taken(NamedTuple):
    """One message off the queue, and the way back to it.

    ``payload`` is what the envelope is decoded from. ``handle`` belongs to the broker that
    produced it and to nothing else: it goes back to ``ack`` or ``release`` unread. A broker
    whose messages name themselves — a Redis list, where the value *is* the name — may put
    the payload in both.
    """

    payload: bytes
    handle: object


class Liveness(NamedTuple):
    """What a probe outside this process can say about the consumer.

    ``age`` is seconds since the consumer last said it was turning, or ``None`` where the
    transport tracks membership itself and nobody has to write it down — a probe reads
    ``reported`` first and asks the broker rather than a key it assumes exists.
    """

    reported: bool
    age: float | None
    detail: str = ''
