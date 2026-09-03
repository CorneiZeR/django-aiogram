"""Add the table sends wait in, which is the second and last one this app ships.

One ``CREATE TABLE`` and one ``CREATE INDEX``, on a table that starts empty — so unlike
``0003`` there is nothing to pace and no window to pick. It is safe on a running deployment:
nothing reads or writes the table until a caller passes ``eta``.

**It is created wherever the project's own tables go, not on ``EVENT_LOG_DATABASE``.** The
router routes the event log by model rather than by app label since 4.1, for the reason
`models.TelegramScheduledSend` gives: this is operational state a producer writes and a mover
consumes, and it belongs with the caller's other writes. Routed by app label, as it was
before, a project with a log database would have had this table created *only* there — and
nothing on ``default`` would have had one at all.

``payload`` is a ``BinaryField`` because it holds the envelope exactly as
``producer.queueing.serialise`` wrote it: bytes that go to ``Broker.publish`` unchanged when
the row comes due. On PostgreSQL that is ``bytea``, on MySQL ``longblob``, on SQLite ``BLOB``
— no size limit worth naming for a payload the queue already carries.
"""

from typing import ClassVar

import django.utils.timezone
from django.db import migrations, models


class Migration(migrations.Migration):
    """Create the schedule, with the one index the mover's query needs."""

    dependencies: ClassVar = [('django_aiogram', '0003_short_id')]

    operations: ClassVar = [
        migrations.CreateModel(
            name='TelegramScheduledSend',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('created_at', models.DateTimeField(default=django.utils.timezone.now)),
                ('correlation_id', models.UUIDField(db_index=True)),
                ('due_at', models.DateTimeField()),
                ('function', models.CharField(max_length=64)),
                ('chat_id', models.BigIntegerField(blank=True, null=True)),
                ('payload', models.BinaryField()),
                ('claimed_at', models.DateTimeField(blank=True, null=True)),
                ('claimed_by', models.CharField(blank=True, max_length=128)),
                ('claimed_until', models.DateTimeField(blank=True, null=True)),
                ('attempts', models.PositiveBigIntegerField(default=0)),
            ],
            options={
                'db_table': 'django_aiogram_scheduled',
                'ordering': ('due_at', 'id'),
                'verbose_name': 'scheduled telegram send',
                'verbose_name_plural': 'scheduled telegram sends',
                'indexes': [models.Index(fields=['claimed_at', 'due_at'], name='dja_scheduled_due')],
            },
        ),
    ]
