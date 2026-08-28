"""The exceptions this package raises.

They live together, and build their own messages, so that call sites raise a
named domain error instead of a bare builtin with a formatted string.
"""

from django_aiogram.config.settings import SETTINGS_NAME


class DjangoRedisAiogramError(Exception):
    """Base class for every error this package raises."""


class LoopUnavailableError(DjangoRedisAiogramError, RuntimeError):
    """No event loop in this process can take an update right now.

    A refusal, not a handler failure: nothing has run, so the update is still
    Telegram's to redeliver. The webhook view answers a non-2xx for this and 200
    for everything else, which is the difference between a retry and a loop.

    Raised as one of the two below, so a caller can tell a shutdown from a fault.
    """


class ShuttingDownError(LoopUnavailableError):
    """The update arrived while this process was tearing the bot down."""

    def __init__(self) -> None:
        """Say which window this is, since it closes on its own."""
        super().__init__(
            'the bot is shutting down, so this update was not handled here. '
            'Telegram will redeliver it; another process, or this one after it '
            'restarts, will take it.',
        )


class LoopThreadNotStartedError(LoopUnavailableError):
    """The thread this process gives the loop did not start in time."""

    def __init__(self, timeout: float) -> None:
        """Name the deadline, which is the only number worth acting on.

        Kept as an attribute for that reason: the webhook view answers a non-2xx from the class
        alone, but a caller that wants to say *when* to come back has to read this number, and
        reading it out of the sentence is not something a caller should be asked to do.
        """
        self.timeout = timeout
        super().__init__(
            f'the event loop thread did not start within {timeout}s, and driving '
            'the loop from this thread instead would put two threads on one loop. '
            'The update was not handled here.',
        )


class DeliveryNotConfiguredError(DjangoRedisAiogramError, ValueError):
    """``DELIVERY`` names something that is not a consumer, or names nothing.

    Also a ``ValueError``, which is what 3.x raised for an unknown delivery: a project that
    wrote ``except ValueError`` around building a consumer keeps working, and gets a named
    class if it wants one.

    One class rather than one per way of being wrong, because the caller's move is the same in
    each -- fix the setting -- and only the message differs. `E009` reports what it can before a
    consumer is built, so this is the runtime backstop rather than the usual path: it is what
    `start_tgbot` raises for a path that reads correctly and does not resolve.
    """

    def __init__(self, path: object, detail: str) -> None:
        """Keep what was named, since that is what the caller has to change.

        The path is an attribute for the reason every refusal here keeps one: a management
        command reporting which setting to fix should not have to read it back out of English.
        """
        self.path = path
        super().__init__(f"{SETTINGS_NAME}['DELIVERY'] is {path!r}: {detail}")


class SerializationError(DjangoRedisAiogramError):
    """A payload could not be encoded or decoded."""


class UnknownApiMethodError(DjangoRedisAiogramError, ValueError):
    """A queued payload named something that is not a Telegram API method."""

    def __init__(self, function: str, method_count: int) -> None:
        """Name the rejected method and how many the Bot API actually has.

        The method is kept, because it is what was refused and it arrives from a queue as often
        as from a caller. The count is not: it is ``len(API_METHODS)`` of the installed aiogram,
        which anyone holding this exception can read for themselves.
        """
        self.function = function
        super().__init__(
            f'{function!r} is not a Telegram API method. Queued payloads may only '
            f'name one of the {method_count} methods aiogram exposes for the '
            f'Bot API; see the Serialization page.',
        )
