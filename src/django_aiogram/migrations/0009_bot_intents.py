"""Four columns on the bot table: what an operator asked for, and what came back.

Nullable and blank throughout, and on a table sized by the number of clients rather than by
traffic -- so this is an ``ALTER TABLE`` per column on a few hundred rows, not the kind of
migration that needs a plan. No index: the reader is a process that already has the bot in
hand, and it asks by primary key.
"""

from typing import ClassVar

from django.db import migrations, models


class Migration(migrations.Migration):
    """Add the intent columns."""

    dependencies: ClassVar[list[tuple[str, str]]] = [
        ('django_aiogram', '0008_queue_watermark'),
    ]

    operations: ClassVar[list[migrations.operations.base.Operation]] = [
        migrations.AddField(
            model_name='telegrambot',
            name='intent',
            field=models.CharField(blank=True, max_length=32),
        ),
        migrations.AddField(
            model_name='telegrambot',
            name='intent_asked_at',
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='telegrambot',
            name='intent_done_at',
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='telegrambot',
            name='intent_result',
            field=models.CharField(blank=True, max_length=200),
        ),
    ]
