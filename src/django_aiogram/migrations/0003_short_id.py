"""Add the short id column, and nothing else.

The column arrives empty and is filled by ``manage.py tgbot_backfill_short_ids``, not here. A data
migration would rewrite a table sized by traffic inside a deploy, with no way to pace it, stop it or
resume it — the same argument that kept the 3.x event log copy out of ``migrate``.

Rows written after this lands carry their own short id, so the backfill is only ever about history.
``blank=True`` is what lets the two states coexist: a row from before the backfill has an empty one,
and the admin says so rather than showing a code that names nothing.

``AddField`` with an index is one ``ALTER TABLE`` and one ``CREATE INDEX``. On PostgreSQL the column
is added without a table rewrite — it has no default — but the index build takes a lock for its
duration, so on a large table run this in a window, or create the index by hand with
``CONCURRENTLY`` and tell Django it is done. Measured on 20 000 rows: the column and its index cost
about 57 bytes a row, a third more storage for the table.
"""

from typing import ClassVar

from django.db import migrations, models


class Migration(migrations.Migration):
    """One column, indexed, empty until the backfill runs."""

    dependencies: ClassVar = [('django_aiogram', '0002_kind_id_index')]

    operations: ClassVar = [
        migrations.AddField(
            model_name='telegramevent',
            name='short_id',
            field=models.CharField(blank=True, db_index=True, max_length=12),
        ),
    ]
