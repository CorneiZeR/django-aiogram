"""Where 3.x's event log rows are, and how they get into 4.0's table.

The rename gave this app a new table and left the old one exactly where it is. Nothing reads it,
nothing drops it, and a project that wants its history has to move it -- which the upgrade page
described as one ``INSERT ... SELECT``. Honest, and the wrong shape on a table sized by traffic:
one statement, no pacing, no way to resume, and a sequence left behind.

This module is the part both halves of the answer share -- the check that notices the old table and
the command that empties it -- so neither can disagree with the other about which table, on which
alias, is the one being talked about.
"""

from django.db import connections

from django_aiogram.models import TelegramEvent

#: the table 3.x wrote to. A literal because it is history: no model names it any more, and the
#: string is the only thing left that does
OLD_TABLE = 'django_redis_aiogram_event'


def old_table_is_present(alias: str) -> bool:
    """Whether 3.x's table is still on this alias.

    Through ``introspection.table_names()`` rather than a query against the table, because a
    system check calls this: a rule that raises takes ``manage.py check`` down, and the whole point
    of the rule is to speak up on a deployment that has not finished upgrading.

    Every failure reads as "no" -- an alias that is not configured, a database that is not up, a
    connection this process is not allowed to open. A check cannot report what it cannot see, and
    the copy command asks the same question again before it moves anything.
    """
    if alias not in connections:
        return False
    try:
        with connections[alias].cursor() as cursor:
            return OLD_TABLE in connections[alias].introspection.table_names(cursor)
    except Exception:  # noqa: BLE001 - every failure to look is a "cannot see", never a finding
        return False


def shared_columns() -> tuple[str, ...]:
    """Name the columns to copy, one by one, from the model itself.

    Never ``SELECT *``. The two tables agree today, and would keep agreeing right up to the release
    that adds a column -- at which point ``SELECT *`` inserts the right values into the wrong
    places rather than failing. Named columns fail loudly instead, which is the answer a migration
    wants.
    """
    # `column` is `str | None` in the stubs because an abstract field has none; every field on a
    # concrete model has one, and an empty name would produce invalid SQL rather than wrong data
    return tuple(str(field.column) for field in TelegramEvent._meta.concrete_fields)  # noqa: SLF001 - _meta is Django's own API
