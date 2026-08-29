"""Rules about the event log: where it is written, how, and what it costs.

The database it goes to and the router that sends it there, the batch and buffer sizes that decide
whether the writer keeps up, the retention window, and the table 3.x left behind.

`django.db` is imported inside the rules that need it, never at module scope: a check runs in every
process that runs `manage.py`, including the ones that never touch a database.
"""

from collections.abc import Collection

from django.conf import settings as django_settings
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string

from django_aiogram.config.checks.conditions import _the_log_is_on
from django_aiogram.config.checks.problems import Problem
from django_aiogram.config.checks.shapes import _setting
from django_aiogram.config.settings import SETTINGS_NAME, coerce_bool, conf
from django_aiogram.eventlog.events import known_kinds


def _kinds_this_version_records(key: str) -> list[Problem]:
    """Warn about a kind nothing writes: a typo here silently records nothing."""
    value = _setting(key)
    if not value or isinstance(value, (str, bytes)) or not isinstance(value, Collection):
        return []  # E032 owns the shape complaint
    known = known_kinds()
    unknown = sorted(repr(name) for name in value if isinstance(name, str) and name not in known)
    if not unknown:
        return []
    return [
        Problem(
            f'names kinds nothing records: {", ".join(unknown)}.',
            hint=f'Known kinds are: {", ".join(sorted(known))}.',
        )
    ]


def _a_configured_log_database(key: str) -> list[Problem]:
    """Resolve the alias here: the writer runs on a thread nobody is watching.

    An alias missing from DATABASES raises ConnectionDoesNotExist inside the
    writer thread, where the only trace is a log line in a container nobody
    reads and a queue that quietly fills and drops.
    """
    value = _setting(key)
    if not isinstance(value, str):
        return []  # E040 owns the type complaint
    alias = value.strip()
    if not alias:
        return []
    # deferred like the aiogram imports: a boot that records nothing must not
    # pay for the connection handler
    from django.db import connections  # noqa: PLC0415 - as above

    if alias in connections:
        return []
    return [
        Problem(
            f'names {alias!r}, which is not in DATABASES.',
            hint=f'Configured aliases are: {", ".join(sorted(connections))}.',
        )
    ]


def _somewhere_to_write_the_log(key: str) -> list[Problem]:
    """Warn, never error, when the log is on with no database behind it.

    A project may legitimately boot without one — this package's own suite does
    — so this must not be able to fail ``manage.py check``.

    The engine is what gets asked, not whether DATABASES is empty: Django fills
    an empty setting in with the dummy backend the first time anything touches
    connections, so by the time checks run the dict is never empty.
    """
    if not _the_log_is_on():
        return []
    # deferred for the same reason as the alias check above
    from django.db import DEFAULT_DB_ALIAS, connections  # noqa: PLC0415 - as above

    alias = str(conf.get('EVENT_LOG_DATABASE') or '').strip() or DEFAULT_DB_ALIAS
    if alias not in connections:
        return []  # E041 owns the missing alias
    engine = str(connections[alias].settings_dict.get('ENGINE') or '')
    if engine and engine != 'django.db.backends.dummy':
        return []
    return [
        Problem(
            f'is on while {alias!r} has no database engine, so every event is dropped.',
            hint=f'Configure a database, or leave {key} off in processes that have none.',
        )
    ]


def _a_log_the_rename_left_behind(_key: str) -> list[Problem]:
    """Say when 3.x's event log table is still sitting on the log's alias.

    Nothing is broken: the table this release writes to exists, and the rows in the old one are
    simply history nothing reads. But nothing points at them either -- the upgrade page mentions
    the move and a page is only read by whoever goes looking -- so a project upgrades, sees a
    working bot, and finds its history months later or not at all.

    Information rather than a warning, and always on: it is cheap, and this is what makes the
    command discoverable. Failing a build over it would be wrong twice over, since leaving the
    rows where they are is a legitimate choice and the check cannot tell that choice from an
    oversight.

    Asked of whichever alias the log resolves to, not of the default: with `EVENT_LOG_DATABASE`
    set, both tables live there, and a rule that looked at the default would report nothing on
    exactly the deployment that has the most rows to move.
    """
    from django_aiogram.eventlog.moving import (  # noqa: PLC0415 - reaches django.db, which a check must not import at module scope
        OLD_TABLE,
        old_table_is_present,
    )
    from django_aiogram.eventlog.writer import log_alias  # noqa: PLC0415 - as above

    alias = log_alias()
    try:
        present = old_table_is_present(alias)
    except Exception:  # noqa: BLE001 - a rule reports; it never fails, and a database that cannot be reached is not a finding
        return []
    if not present:
        return []
    return [
        Problem(
            f'{OLD_TABLE} is still on the {alias!r} database, holding rows this release does not read.',
            key='',
            hint=(
                'manage.py tgbot_move_events copies them into this release table in bounded '
                'chunks, and can be stopped and rerun. Dropping the old table afterwards is '
                'yours to do — see Upgrading.'
            ),
        )
    ]


def _a_routed_log_database(key: str) -> list[Problem]:
    """Say when the log is pointed at its own alias with nothing routing it there.

    ``EVENT_LOG_DATABASE`` names where the rows belong; ``TelegramEventLogRouter`` is
    what puts them there. Set the first and forget the second and every existing check
    passes — E040 sees a string, E041 sees a configured alias with a real engine, W005
    sees a database — while a plain ``migrate`` does not create the table on it and the
    writer logs ``no such table`` once per batch for ever. ``migrate --database=<alias>``
    still would, which is why this is information: someone may be doing exactly that.

    I002 rather than a warning, because a project may route this app by hand: a router
    of its own that returns the same alias is a legitimate way to do it, and this rule
    cannot see inside one. The hint says so too, and `Settings.md` lists it under the
    information ids.

    Compared through ``import_string`` so both spellings count. ``DATABASE_ROUTERS``
    accepts dotted paths and instances alike, and a project mixing the two — a path
    for ours, an instance for its own — is exactly the case a string comparison gets
    wrong.
    """
    if not _the_log_is_on():
        return []
    alias = str(_setting(key) or '').strip()
    if not alias:
        return []  # nothing was pointed anywhere, so nothing needs routing
    from django_aiogram.eventlog.dbrouter import TelegramEventLogRouter  # noqa: PLC0415 - no django.db at import

    for entry in getattr(django_settings, 'DATABASE_ROUTERS', ()) or ():
        candidate = entry
        if isinstance(entry, str):
            try:
                candidate = import_string(entry)
            except ImportError:
                continue  # a router Django itself will complain about
        if candidate is TelegramEventLogRouter or isinstance(candidate, TelegramEventLogRouter):
            return []
    return [
        Problem(
            f'is {alias!r}, and this check cannot see a router that sends this app there.',
            hint=(
                "Add 'django_aiogram.eventlog.dbrouter.TelegramEventLogRouter' to DATABASE_ROUTERS, "
                f'or leave {key} unset so the log uses the default database. A router of your own '
                'returning that alias is equally correct and is what this cannot read, which is why '
                'this is information rather than a warning.'
            ),
        )
    ]


#: paths a 3.x project wrote into its *own* settings, against what to write in 4.0. The router is
#: the only one a check can reach: nothing here can read a project's `urls.py`, so the webhook view
#: is documentation and **Upgrading** carries it


THREE_X_PATHS = {
    'django_redis_aiogram.dbrouter.TelegramEventLogRouter': ('django_aiogram.eventlog.dbrouter.TelegramEventLogRouter'),
}


def _a_router_this_release_still_has(_key: str) -> list[Problem]:
    """Report a `DATABASE_ROUTERS` entry that names 3.x, with the 4.0 path.

    The one moved path a check can reach. `DATABASE_ROUTERS` is Django's, and a project wrote our
    dotted path into it by hand, so the rename in 4.0 leaves a string nothing resolves -- and
    nothing here imports it, so nothing fails until the first query, which then fails with
    Django's own `ImportError` naming a module rather than the fix.

    Reported for **any** `django_redis_aiogram.` entry rather than only the router, and both halves
    of that matter: the known path gets its replacement named, and an unknown one still gets told
    that the distribution is gone, since guessing a replacement it never had would be worse than
    saying so.

    An error rather than a warning: this cannot work. The string names a package that a 4.0
    install does not have, and a router Django cannot import takes down the first query that needs
    routing -- there is no configuration in which this is deliberate.
    """
    problems = []
    for entry in getattr(django_settings, 'DATABASE_ROUTERS', ()) or ():
        if not isinstance(entry, str) or not entry.startswith('django_redis_aiogram.'):
            continue
        replacement = THREE_X_PATHS.get(entry)
        instead = (
            f'Write {replacement!r}.'
            if replacement
            else 'That distribution is gone in 4.0; the package is `django_aiogram` now.'
        )
        problems.append(
            Problem(
                f'names {entry!r}, which 4.0 renamed. {instead}',
                label='DATABASE_ROUTERS',
                hint=(
                    'The rename is mechanical for the router, and the webhook view in your '
                    "`urls.py` moved with it -- to 'django_aiogram.consumer.webhook."
                    "telegram_webhook'. Nothing here can read `urls.py`, so that one is only in "
                    'the Upgrading page.'
                ),
            )
        )
    return problems


def _a_log_that_is_pruned(key: str) -> list[Problem]:
    """Warn when nothing will ever delete a row, so the table only grows."""
    if not _the_log_is_on():
        return []
    try:
        days = int(_setting(key))
    except (TypeError, ValueError):
        return []  # E039 owns the type complaint
    if days > 0:
        return []
    return [
        Problem(
            'is 0 while the log is on, so nothing ever deletes a row.',
            hint='Set it and schedule `manage.py tgbot_prune_events`, or accept unbounded growth.',
        )
    ]


def _a_batch_the_buffer_can_hold(key: str) -> list[Problem]:
    """Warn when the batch can never fill, so the interval paces every write.

    The writer stops collecting at the buffer's size, which makes a larger batch
    not a bigger write but a partial one every flush interval.
    """
    try:
        batch = int(_setting(key))
        buffer = int(conf['EVENT_LOG_BUFFER_SIZE'])
    except (TypeError, ValueError):
        return []  # E036 and E037 own the type complaints
    if batch <= buffer:
        return []
    return [
        Problem(
            f'is {batch}, which the buffer caps at {buffer}.',
            hint=f"Raise {SETTINGS_NAME}['EVENT_LOG_BUFFER_SIZE'] above it, or lower this.",
        )
    ]


def _a_writer_that_does_not_block(key: str) -> list[Problem]:
    """Warn that synchronous recording puts a database round trip in the send.

    The whole design rests on recording never making a caller wait. This
    setting deliberately breaks that for tests, so the trade is stated rather
    than left to be discovered under load.

    Silent while the log is off, because `record()` returns before it ever
    reads this one: warning there would describe a cost nobody is paying.
    """
    if not _the_log_is_on():
        return []
    try:
        if not coerce_bool(_setting(key), f"{SETTINGS_NAME}['{key}']"):
            return []
    except ImproperlyConfigured:
        return []  # E042 owns the type complaint
    return [
        Problem(
            'is on, so every recorded event is written on the calling thread and a send waits for the database.',
            hint='Leave it off outside tests.',
        )
    ]
