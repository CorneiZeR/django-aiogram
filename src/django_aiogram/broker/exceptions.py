"""What goes wrong choosing or reaching a transport, as its own family."""

__all__ = (
    'BrokerDependencyError',
    'BrokerError',
    'BrokerNotConfiguredError',
    'QueueMultiplexingUnavailableError',
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


class QueueMultiplexingUnavailableError(BrokerError):
    """A transport asked to read several queues over the connection it has for one.

    ``MULTIPLEXES`` is the capability, and it is answered per transport rather than assumed:
    RabbitMQ consumes several queues on one channel, Kafka subscribes to several topics on one
    consumer, and Redis Streams reads several streams in one ``XREADGROUP``. A crash-safe Redis
    list cannot -- ``BLMOVE`` takes one source -- so there a container serving three queues runs
    three consumers, which is what it has always done and what `Deployment.md` says the cost of.

    Raised rather than quietly reading one of them: a consumer that believed it was serving
    three queues and was in fact serving one would leave two backlogs with nobody on them, and
    nothing would say so.
    """

    def __init__(self, broker: str, queues: 'tuple[str, ...]', addressed: str = '') -> None:
        """Name the transport that refused, what it was asked for, and what it reads.

        All three, because the refusal has two readings and the message has to fit both: a set
        of queues is one, and *one* queue that is not the one this broker was built for is the
        other -- a consumer whose lane shrank to somebody else's queue, which reads as a
        configuration mistake rather than as multiplexing.
        """
        self.broker = broker
        self.queues = queues
        self.addressed = addressed
        asked = ', '.join(repr(one) for one in queues) or 'none'
        reads = f' It reads {addressed!r}.' if addressed else ''
        super().__init__(
            f'{broker} reads one queue per connection, and was asked for {asked}.{reads} '
            f'Serve one queue per consumer here -- which is what this transport does when '
            f'nothing asks it to multiplex -- or run a container per queue.'
        )
