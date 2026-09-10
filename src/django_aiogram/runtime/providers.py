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

**And an empty answer has to be said twice.** A provider that returned twenty bots and now
returns none is far more often a query that found the wrong database than twenty bots being
deleted at once, so the first such read is kept rather than acted on, and the second is
honoured. A deployment that really did remove its last bot converges one interval later,
which is the cheap half of that trade.
"""

import logging
import threading
from typing import TYPE_CHECKING, Any

from django.core.signals import setting_changed
from django.db.models import Count, Max
from django.dispatch import receiver
from django.utils.module_loading import import_string

from django_aiogram.config.bots import BOTS_SETTINGS_NAME, records, resolve
from django_aiogram.config.settings import SETTINGS_NAME, conf
from django_aiogram.tokens import TokenUnreadableError, read_token

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator

    from django_aiogram.config.bots import BotRecord
    from django_aiogram.models import TelegramBot as BotRow

    #: what a provider is: something callable that answers with the bots it can see
    Provider = Callable[[], Iterable[BotRecord]]

__all__ = ('desired', 'forget', 'from_database', 'from_settings', 'providers', 'switched_off')

logger = logging.getLogger('django_aiogram')

_lock = threading.Lock()
#: what the last read of the table found, with the watermark it was read at. A pass happens
#: every few seconds in every container, and resolving a bot is three layers of arithmetic per
#: row -- so an unchanged table is answered without reading the rows at all
_from_table: 'tuple[object, tuple[BotRecord, ...]] | None' = None
#: what each provider last said, by path, so an empty answer can be told from a first one
_last_said: 'dict[str, tuple[BotRecord, ...]]' = {}


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
    from django_aiogram.models import TelegramBot, TelegramBotProfile  # noqa: PLC0415 - as above

    global _from_table  # noqa: PLW0603 - one table per process, like the rows it caches
    mark = (
        TelegramBot.objects.aggregate(at=Max('updated_at'), n=Count('pk')),
        TelegramBotProfile.objects.aggregate(at=Max('updated_at'), n=Count('pk')),
    )
    with _lock:
        held = _from_table
    if held is not None and held[0] == mark:
        return held[1]

    read = _records(TelegramBot.objects.filter(enabled=True).select_related('profile'))
    with _lock:
        _from_table = (mark, read)
    return read


def _records(rows: 'Iterable[BotRow]') -> 'tuple[BotRecord, ...]':
    """Resolve every row, leaving out the ones whose stored token cannot be read.

    One bot's credential is one bot's problem: a key dropped before its rows were rewrapped
    must not turn a read of twenty bots into a read of none, which is what a refusal escaping
    here would do -- the supervisor would keep the running set and no new bot would arrive
    until somebody noticed.
    """
    found = []
    for row in rows:
        try:
            found.append(_resolved(row))
        except TokenUnreadableError:  # noqa: PERF203 - per row, because one bad row must not lose the rest
            # the message carries no ciphertext, which is why it can be logged at all
            logger.exception(
                'ignoring a bot whose stored token cannot be read',
                extra={'tg_bot_id': row.bot_id},
            )
    return tuple(found)


def _resolved(row: 'BotRow') -> 'BotRecord':
    """Resolve one row through its profile and its own overrides.

    ``provided=True``: this bot is a row, not a section, and the difference is not cosmetic. A
    section may legally be *named* `123456`, and a bot that re-resolved its own settings by
    alias would then read that section's token -- see `TelegramBot.settings`, and `I004`,
    which reports the collision at boot.
    """
    layers = []
    if row.profile is not None:
        layers.append((f'TelegramBotProfile({row.profile.name}).overrides', row.profile.overrides))
    # through the seam, always: a project that turned `TOKEN_STORAGE` on has no plaintext
    # path left, and reading the column directly here would have been one
    layers.append((f'TelegramBot({row.bot_id}).overrides', {'TOKEN': read_token(row.token), **row.overrides}))
    return resolve(str(row.bot_id), *layers, provided=True)


def switched_off() -> 'tuple[BotRecord, ...]':
    """Return the bots whose rows are switched off, which :func:`desired` leaves out.

    Not for serving -- a bot switched off is one a person decided not to serve -- but for the
    operator's own work on it: **its webhook still has to be deleted**, and deleting one needs
    that bot's token. Left unreachable, disabling a row would leave Telegram posting updates
    at a URL that answers 404 for ever, with nothing able to tell it to stop.
    """
    # deferred: the ORM, as in `from_database`
    from django_aiogram.models import TelegramBot  # noqa: PLC0415 - as above

    rows = TelegramBot.objects.filter(enabled=False).select_related('profile')
    return _records(rows)


def providers() -> 'Iterator[Provider]':
    """Resolve every path in ``BOT_PROVIDERS``, in the order it names them.

    Refused by name rather than skipped: a provider that cannot be imported is a source of bots
    nobody is reading, and a deployment whose clients live in the database would serve none of
    them while looking healthy.
    """
    for path in conf['BOT_PROVIDERS']:
        yield import_string(path)


def _read(path: str, provider: 'Provider') -> 'tuple[BotRecord, ...]':
    """Read one provider, holding back the first empty answer it gives after a full one.

    The count is what is compared, not the bots: a provider that swapped every bot for another
    twenty read something, and this is only about the answer that reads like a failure without
    being one.
    """
    got = tuple(provider())
    with _lock:
        held = _last_said.get(path)
        # `()` rather than `del`: the *next* empty read finds nothing held and is honoured, so
        # a deployment that really has no bots left converges on the pass after this one
        _last_said[path] = () if not got and held else got
    if not got and held:
        logger.warning(
            'a provider read no bots at all; keeping what it last said until it says so again',
            extra={'tg_provider': path, 'tg_bots': len(held)},
        )
        return held
    return got


def desired() -> 'tuple[BotRecord, ...]':
    """Return every bot the providers can see, with the first to name an identity keeping it.

    Order decides a conflict, and it is the order ``BOT_PROVIDERS`` is written in: a bot in
    ``TELEGRAM_BOTS`` and in the table is one bot, and the settings win where they are named
    first. Reported rather than silently resolved, because two sources disagreeing about a
    token is a configuration somebody has to fix.

    An empty answer is held back once -- see :func:`_read` -- and a failure is not caught here
    at all: the supervisor is what keeps the running set through it.

    A bot with no identity in its token is left out, and reported once for each read it
    appears in: nothing could address it -- `E052` reports the same thing about a section --
    and serving it would put messages on a queue that name no bot at all.
    """
    seen: dict[int, BotRecord] = {}
    # zipped rather than resolved again: the path is what a held answer is keyed by, and
    # `providers()` is what refuses one that cannot be imported
    for path, provider in zip(conf['BOT_PROVIDERS'], providers(), strict=True):
        for record in _read(path, provider):
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


def forget() -> None:
    """Drop what was read: the table's watermark and every provider's last answer."""
    global _from_table  # noqa: PLW0603 - as above
    with _lock:
        _from_table = None
        _last_said.clear()


@receiver(setting_changed, dispatch_uid='django_aiogram.runtime.providers')
def _forget_what_was_read(**kwargs: Any) -> None:
    """Drop both caches when a setting a resolved bot was built from moves.

    The watermark is over rows, and the layers under them are settings: a change to
    ``TELEGRAM_BOT_DEFAULTS`` moves every resolved value without touching a row, so a cache
    keyed only on the table would answer with the settings as they were.
    """
    if kwargs.get('setting') in {SETTINGS_NAME, BOTS_SETTINGS_NAME}:
        forget()
