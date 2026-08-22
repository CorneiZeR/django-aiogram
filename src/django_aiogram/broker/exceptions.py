"""What goes wrong choosing or reaching a transport, as its own family."""

__all__ = ('BrokerDependencyError', 'BrokerError', 'BrokerNotConfiguredError')


class BrokerError(Exception):
    """Anything about which transport is in use, or whether it can be used at all."""


class BrokerNotConfiguredError(BrokerError):
    """``BROKER`` names something that is not a broker, or names nothing."""


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
