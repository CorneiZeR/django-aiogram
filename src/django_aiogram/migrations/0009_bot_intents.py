"""Six columns on the bot table: what an operator asked for, who is doing it, and the answer.

The claim is a lease -- a name and a moment -- for the reason the model's own comment gives: a
worker that died holding an intent must lose it rather than take the only record of it away.

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
            name='intent_claim',
            field=models.CharField(blank=True, max_length=64),
        ),
        migrations.AddField(
            model_name='telegrambot',
            name='intent_claimed_at',
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
