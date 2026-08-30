"""Where 3.x's event log rows are, and how they get into 4.0's table.

The rename gave this app a new table and left the old one exactly where it is. Nothing reads it,
nothing drops it, and a project that wants its history has to move it -- which the upgrade page
described as one ``INSERT ... SELECT``. Honest, and the wrong shape on a table sized by traffic:
one statement, no pacing, no way to resume, and a sequence left behind.

This module is the part both halves of the answer share -- the check that notices the old table and
the command that copies out of it -- so neither can disagree with the other about which table, on
which alias, is the one being talked about. Neither empties it: the rows stay, and dropping the
table is the operator's to do.
"""

from typing import Any

from django.db import connections

from django_aiogram.models import TelegramEvent

#: the table 3.x wrote to. A literal because it is history: no model names it any more, and the
#: string is the only thing left that does
OLD_TABLE = 'django_redis_aiogram_event'


def old_table_is_present(alias: str) -> bool:
    """Whether 3.x's table is still on this alias.

    Through ``introspection.table_names()`` rather than a query against the table, because asking
    for a table that is not there is the question, not an error to catch.

    **Failures are raised, not answered with "no".** Two callers ask this and they want opposite
    things from a database that cannot be reached: the check has to stay quiet, since a rule that
    raises takes ``manage.py check`` down; the command has to stop, since "there is no old table"
    and "I could not look" are the same sentence to a cron job and only one of them means the
    history has been moved. Swallowing it here gave both of them the check's answer, and the
    command then exited zero on a migration it had not done.

    An alias with no database configured is not a failure -- there is nothing there to have a
    table, and the command refuses an unknown alias by name before it gets here.
    """
    if alias not in connections:
        return False
    with connections[alias].cursor() as cursor:
        return OLD_TABLE in connections[alias].introspection.table_names(cursor)


def model_columns() -> tuple[str, ...]:
    """Every column 4.0's table has, in the order the model declares them."""
    # `column` is `str | None` in the stubs because an abstract field has none; every field on a
    # concrete model has one, and an empty name would produce invalid SQL rather than wrong data
    return tuple(str(field.column) for field in TelegramEvent._meta.concrete_fields)  # noqa: SLF001 - _meta is Django's own API


def _columns_of_the_old_table(alias: str) -> frozenset[str]:
    """Ask the database what 3.x's table actually has, rather than assuming it."""
    connection = connections[alias]
    with connection.cursor() as cursor:
        return frozenset(column.name for column in connection.introspection.get_table_description(cursor, OLD_TABLE))


def shared_columns(alias: str) -> tuple[str, ...]:
    """Name the columns to copy, one by one, from the two tables' agreement.

    Never ``SELECT *``. The old table is frozen at 3.x's shape and this one moves on, so a `SELECT *`
    breaks in whichever way the two shapes make available: a column count that no longer matches is
    rejected outright by every backend this package supports, and a count that still matches with
    the order changed is *accepted*, putting each value in the wrong column. Named columns fail on
    the first kind and make the second impossible, which is the answer a migration wants.

    **The model alone cannot name them.** It described both tables while they agreed, and the first
    column 4.0 added ended that: asking the old table for ``short_id`` is an error from the backend,
    not a column of nulls. So the list is the intersection, in model order, and what only this
    release has is handled by :func:`added_since`.
    """
    present = _columns_of_the_old_table(alias)
    return tuple(name for name in model_columns() if name in present)


def added_since(alias: str) -> tuple[tuple[str, Any], ...]:
    """Name the columns this release has and 3.x's table does not, each with the value to write.

    A copy cannot leave them out. ``migrate`` adds a column with a default and then drops the
    default, which is Django's way of not rewriting the table -- so the column ends up ``NOT NULL``
    with nothing behind it, and an ``INSERT`` that names every *other* column is refused by the
    database rather than filled in for you.

    The value is the field's own default, which is what the model would have written. For
    ``short_id`` that is empty, and empty is what the admin shows as ``(not backfilled)`` and what
    ``manage.py tgbot_backfill_short_ids`` looks for -- so history arrives in the state the tool for
    it already understands, rather than in one nothing knows how to finish.
    """
    present = _columns_of_the_old_table(alias)
    return tuple(
        (str(field.column), field.get_default())
        for field in TelegramEvent._meta.concrete_fields  # noqa: SLF001 - _meta is Django's own API
        if str(field.column) not in present
    )
