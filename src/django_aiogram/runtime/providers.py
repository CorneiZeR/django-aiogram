"""Where the bots come from, and what a reader of them is allowed to say.

``BOT_PROVIDERS`` names them by dotted path, in order. Two ship: one reads ``TELEGRAM_BOTS``,
which is every project that knows its bots when it deploys, and one reads the `TelegramBot`
table, which is every project whose clients bring their own. A project with a source of its
own writes a third; it is a callable answering with :class:`~django_aiogram.config.bots.
BotRecord` objects and nothing more.

**A provider says what it read, or raises.** It never says "nothing" to mean "I could not
look": :func:`desired` lets the failure through and the supervisor keeps what it has, because
a database that blinked would otherwise deregister every bot in the process. That is the same
rule this package applies to a write that raised -- unknown is not refused -- read from the
other side.
"""

import logging
from typing import TYPE_CHECKING

from django.utils.module_loading import import_string

from django_aiogram.config.bots import records, resolve
from django_aiogram.config.settings import conf

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator

    from django_aiogram.config.bots import BotRecord

    #: what a provider is: something callable that answers with the bots it can see
    Provider = Callable[[], Iterable[BotRecord]]

__all__ = ('desired', 'from_database', 'from_settings', 'providers')

logger = logging.getLogger('django_aiogram')


def from_settings() -> 'tuple[BotRecord, ...]':
    """Return the bots ``TELEGRAM_BOTS`` describes, which is what a settings file knows."""
    return records()


def from_database() -> 'tuple[BotRecord, ...]':
    """Return the bots the `TelegramBot` table describes, resolved through their profiles.

    Three levels, as rows: the shared defaults, the profile's overrides, then the bot's own.
    The layers do the arithmetic -- see :func:`~django_aiogram.config.bots.resolve` -- so this
    function is a query and a name for each layer, and nothing else.

    **The alias is the identity written out.** A row has no alias to be known by, and the
    identity is what the wire and the feed name it by anyway; as a string of digits it is also
    a legal alias, so a finding about a bot from the database reads the same way as one about a
    section.

    Disabled rows are left out rather than reported as quarantined: a bot switched off is one a
    person decided not to serve, and the supervisor's job is to serve what it is given.
    """
    # deferred: importing models reaches the app registry, and this module is imported wherever
    # the settings are read -- including in a process that has none
    from django_aiogram.models import TelegramBot  # noqa: PLC0415 - as above

    found = []
    for row in TelegramBot.objects.filter(enabled=True).select_related('profile'):
        layers = []
        if row.profile is not None:
            layers.append((f'TelegramBotProfile({row.profile.name}).overrides', row.profile.overrides))
        layers.append((f'TelegramBot({row.bot_id}).overrides', {'TOKEN': row.token, **row.overrides}))
        found.append(resolve(str(row.bot_id), *layers))
    return tuple(found)


def providers() -> 'Iterator[Provider]':
    """Resolve every path in ``BOT_PROVIDERS``, in the order it names them.

    Refused by name rather than skipped: a provider that cannot be imported is a source of bots
    nobody is reading, and a deployment whose clients live in the database would serve none of
    them while looking healthy.
    """
    for path in conf['BOT_PROVIDERS']:
        yield import_string(path)


def desired() -> 'tuple[BotRecord, ...]':
    """Return every bot the providers can see, with the first to name an identity keeping it.

    Order decides a conflict, and it is the order ``BOT_PROVIDERS`` is written in: a bot in
    ``TELEGRAM_BOTS`` and in the table is one bot, and the settings win where they are named
    first. Reported rather than silently resolved, because two sources disagreeing about a
    token is a configuration somebody has to fix.

    A bot with no identity in its token is left out, and said once per read: nothing could
    address it -- `E052` reports the same thing about a section -- and serving it would put
    messages on a queue that name no bot at all.
    """
    seen: dict[int, BotRecord] = {}
    for provider in providers():
        for record in provider():
            identity = record.bot_id
            if identity is None:
                logger.warning(
                    'ignoring a bot whose token carries no identity',
                    extra={'tg_bot': record.alias},
                )
                continue
            if identity in seen:
                logger.warning(
                    'two sources configure one bot; keeping the first',
                    extra={'tg_bot_id': identity, 'tg_bot': seen[identity].alias, 'tg_other': record.alias},
                )
                continue
            seen[identity] = record
    return tuple(seen.values())
