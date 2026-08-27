"""What goes wrong choosing or reaching a transport, as its own family."""

__all__ = (
    'BrokerDependencyError',
    'BrokerError',
    'BrokerNotConfiguredError',
    'WorkerDepthUnavailableError',
)


class BrokerError(Exception):
    """Anything about which transport is in use, or whether it can be used at all."""


class BrokerNotConfiguredError(BrokerError):
    """``BROKER`` names something that is not a broker, or names nothing."""


class WorkerDepthUnavailableError(BrokerError):
    """A transport that cannot say what a *named* worker holds.

    ``inflight_depth()`` with no argument answers for the caller on three of the four -- Redis
    Streams answers for the whole consumer group, deliberately, because a stream's pending list
    belongs to the group.

    Answering *by name* needs the unsettled work recorded under a name the server can be asked
    about, and only the Redis transports do that: the list keeps a per-worker key, and a stream
    group records the consumer each entry went to. RabbitMQ tracks unacknowledged deliveries per
    *channel* and a client sees its own; Kafka tracks uncommitted offsets in the process holding
    them. Neither is a name this package chose, so neither can be asked about one.

    A refusal rather than a zero, because zero is the answer that stops anybody looking. And the
    question is usually asked about a worker that has died, which on these two transports has an
    answer worth giving instead: the broker returns an unacknowledged message when the channel
    drops, and the group replays an uncommitted offset to whoever takes the partition — so what a
    dead worker held is already back in :meth:`depth`, with nothing to reclaim by hand.
    """

    def __init__(self, broker: str, worker: str) -> None:
        """Name the transport that refused and the worker that was asked about.

        The transport names itself with its own class name, so a project running a subclass
        reads the class it configured rather than the family it belongs to.

        Both kept, as every refusal in this package keeps what it was told: a monitor sweeping
        several worker names wants to know which one it just failed to read without parsing the
        sentence back apart.
        """
        self.broker = broker
        self.worker = worker
        super().__init__(
            f'{broker} cannot say how much the worker {worker!r} holds: unsettled work here '
            f"belongs to a channel or a group member rather than to a name. This worker's own "
            f'in-flight count is inflight_depth() with no argument, and work a dead worker held '
            f'is already back in depth() -- this transport returns it without a reclaim.'
        )


class BrokerDependencyError(BrokerError):
    """The named broker needs a driver that is not installed.

    Carries the install line rather than the import error, because the import error names a
    module and the reader needs the extra. Nothing is guessed from what happens to be
    importable, so this is the only place the difference is explained.
    """

    def __init__(self, broker: str, module: str, extra: str) -> None:
        """Say what was asked for, what is missing, and the one command that fixes it."""
        self.broker = broker
        self.module = module
        self.extra = extra
        super().__init__(
            f'{broker} needs the {module!r} package, which is not installed. '
            f'Install it with: pip install "django-aiogram[{extra}]"'
        )
