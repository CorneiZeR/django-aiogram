"""The bots this process talks through, one object per configured alias.

``django_aiogram.bot`` is the one a project with a single bot has, and ``bots['support']`` is
how a project with several reaches the rest. The same object comes back every time: each
carries the aiogram ``Bot``, the loop's share of the work and the in-flight sends that a
shutdown has to drain, so a second instance for one alias would drain half of them.

Built on first ask and never at import, like everything else here — a process that only runs
``manage.py check`` must not pay for aiogram, and ``tests/test_lazy_init.py`` fails if it does.
"""

import logging
import threading
from collections.abc import Iterator, Mapping
from typing import TYPE_CHECKING, Any

from django.core.exceptions import ImproperlyConfigured
from django.core.signals import setting_changed
from django.dispatch import receiver

from django_aiogram.config.bots import BOTS_SETTINGS_NAME, DEFAULT_ALIAS, aliases, record, records
from django_aiogram.config.settings import SETTINGS_NAME

if TYPE_CHECKING:
    from django_aiogram.config.bots import BotRecord
    from django_aiogram.producer.client import TelegramBot

__all__ = ('Bots', 'bots')

logger = logging.getLogger('django_aiogram')


class Bots(Mapping[str, 'TelegramBot']):
    """Every configured bot, by alias, as the objects a project sends through."""

    def __init__(self) -> None:
        """Hold nothing; the first lookup builds what it is asked for."""
        self._made: dict[str, TelegramBot] = {}
        self._lock = threading.Lock()

    def __getitem__(self, alias: str) -> 'TelegramBot':
        """Return the bot configured as ``alias``, or raise naming the ones there are."""
        found = record(alias)
        with self._lock:
            made = self._made.get(found.alias)
            if made is None:
                made = self._made[found.alias] = self._build(found.alias)
            return made

    @staticmethod
    def _build(alias: str) -> 'TelegramBot':
        """Build one bot, or hand back the process's own where the alias is the default.

        The default is `django_aiogram.bot`, and it has to be the *same object*: it is what a
        project's handlers were registered through and what a shutdown closes, so a second
        instance under the same alias would hold half of each.
        """
        # deferred: importing the client costs aiogram, and this module is reached by a
        # listing that may never send
        from django_aiogram.producer.client import TelegramBot  # noqa: PLC0415 - as above

        if alias == DEFAULT_ALIAS:
            from django_aiogram._singleton import bot  # noqa: PLC0415 - as above

            return bot
        return TelegramBot(record=record(alias))

    def by_id(self, bot_id: int) -> 'TelegramBot':
        """Return the bot a token's identity names, which is what the wire will carry.

        An alias is what a settings file writes and an identity is what a message names, so
        both have to resolve -- see `config.bots` for why the two are not the same thing.

        **The providers are asked too**, not only the settings: a client's bot is a row, and
        by the time a message or a webhook update names it there is no section to find it
        under. The settings come first, which is the same order `desired` resolves a conflict
        in, and the refusal names what it looked at.
        """
        for found in records():
            if found.bot_id == bot_id:
                return self[found.alias]
        for found in self._provided():
            if found.bot_id == bot_id:
                return self.for_record(found)
        known = ', '.join(str(found.bot_id) for found in [*records(), *self._provided()]) or 'none'
        msg = f'No bot is configured with the identity {bot_id}. Configured: {known}.'
        raise ImproperlyConfigured(msg)

    @staticmethod
    def _provided() -> 'tuple[BotRecord, ...]':
        """Every bot the providers can see, or none where they cannot be read.

        Contained, because this is reached from a webhook request and from a send: a database
        that blinked must not turn "which bot is this" into a traceback out of a view. The
        settings-configured bots above are answered whatever happens here.
        """
        # deferred: the providers reach the ORM, and this module is imported by a process
        # that may have no database at all
        from django_aiogram.runtime.providers import desired  # noqa: PLC0415 - as above

        try:
            return desired()
        except Exception:
            logger.exception('could not read the bots the providers see; going by the settings')
            return ()

    def for_record(self, found: 'BotRecord') -> 'TelegramBot':
        """Return the bot one resolved record describes, building it at most once.

        Keyed by the record's alias like everything else here -- a row's alias is its identity
        written out -- so a bot that arrives from the database is cached exactly as one from a
        settings section is, and a settings change drops both.
        """
        if found.alias == DEFAULT_ALIAS:
            # through the mapping, which hands back the process's own object: a second
            # instance under that alias would hold half the handlers and half the in-flight
            # sends, and a shutdown closes only the singleton
            return self[found.alias]
        # deferred: importing the client costs aiogram, and this module is reached by a
        # listing that may never send
        from django_aiogram.producer.client import TelegramBot  # noqa: PLC0415 - as above

        with self._lock:
            made = self._made.get(found.alias)
            if made is None:
                made = self._made[found.alias] = TelegramBot(record=found)
            return made

    def __iter__(self) -> Iterator[str]:
        """Iterate over the configured aliases, in the order the project declared them."""
        return iter(aliases())

    def __len__(self) -> int:
        """Return how many bots are configured."""
        return len(aliases())

    def __repr__(self) -> str:
        """Name the aliases without building any of them."""
        return f'<Bots {", ".join(aliases())}>'

    def forget(self) -> None:
        """Drop the built bots, so the next lookup builds them from the settings as they are.

        The default is kept: it is the process's own object, and a project's handlers are
        registered on it. Everything else is rebuilt, which is what makes a settings change in
        a test suite reach a bot it configured.
        """
        with self._lock:
            self._made = {alias: made for alias, made in self._made.items() if alias == DEFAULT_ALIAS}


bots = Bots()


@receiver(setting_changed, dispatch_uid='django_aiogram.runtime.registry')
def _forget_the_bots(**kwargs: Any) -> None:
    """Rebuild on the next ask when either dict a bot is configured in changes."""
    if kwargs.get('setting') in {SETTINGS_NAME, BOTS_SETTINGS_NAME}:
        bots.forget()
