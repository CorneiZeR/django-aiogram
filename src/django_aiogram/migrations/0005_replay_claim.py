"""Add the row a replay claims, so two runs cannot recover one message twice.

One ``CREATE TABLE`` with one unique constraint, on a table that starts empty — safe on a
running deployment, and nothing reads or writes it until somebody runs ``tgbot_replay``.

**The unique constraint is the feature.** PostgreSQL has advisory locks and MySQL has
``GET_LOCK``; SQLite has neither, and this package supports all three plus whatever a project
points ``DATABASES`` at. A uniquely-constrained insert is the one claim that is atomic
everywhere — the same reasoning that made ``0004``'s mover claim a compare-and-set update
rather than ``SELECT ... FOR UPDATE SKIP LOCKED``.

Created where the project's own tables go, not on ``EVENT_LOG_DATABASE``, for the reason
``0004`` gives about the schedule: this is operational state a command writes and reads, and
a claim it cannot enforce because the log lives in a warehouse would be no claim at all.
`TelegramEventLogRouter` routes by model, so it keeps this off a *separate* log database and
says nothing about where else it goes.
"""

from typing import ClassVar

import django.utils.timezone
from django.db import migrations, models


class Migration(migrations.Migration):
    """Create the claim table, whose whole shape is one unique column."""

    dependencies: ClassVar = [('django_aiogram', '0004_scheduled_send')]

    operations: ClassVar = [
        migrations.CreateModel(
            name='TelegramReplayClaim',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('correlation_id', models.UUIDField(unique=True)),
                ('claimed_at', models.DateTimeField(default=django.utils.timezone.now)),
                ('claimed_by', models.CharField(blank=True, max_length=128)),
                ('queued_at', models.DateTimeField(blank=True, null=True)),
                ('replacement_id', models.UUIDField(blank=True, null=True)),
            ],
            options={
                'db_table': 'django_aiogram_replay_claim',
                'ordering': ('claimed_at', 'id'),
                'verbose_name': 'telegram replay claim',
                'verbose_name_plural': 'telegram replay claims',
            },
        ),
    ]
