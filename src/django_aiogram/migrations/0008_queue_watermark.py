"""One column on the queue table: the watermark the providers poll it by.

A queue row decides a bot's ``QUEUE``, so a rename has to be a change a running container can
see -- and the reader compares ``max(updated_at)`` per table, which a table without the column
could not answer. ``auto_now`` fills it for the rows that already exist as this runs.

One ``ALTER TABLE`` on a table sized by the number of queues a deployment declared.
"""

from typing import ClassVar

from django.db import migrations, models


class Migration(migrations.Migration):
    """Add the queue watermark."""

    dependencies: ClassVar[list[tuple[str, str]]] = [
        ('django_aiogram', '0007_event_bot_index'),
    ]

    operations: ClassVar[list[migrations.operations.base.Operation]] = [
        migrations.AddField(
            model_name='telegramqueue',
            name='updated_at',
            field=models.DateTimeField(auto_now=True),
        ),
    ]
