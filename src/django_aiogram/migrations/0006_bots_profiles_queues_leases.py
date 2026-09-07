"""Add the tables a project configures its bots in, and tell three existing rows which bot.

Four ``CREATE TABLE``s on tables that start empty -- profiles, queues, bots and the polling
lease -- plus a column on each of the three tables that already had rows. Safe on a running
deployment: the new columns are nullable or defaulted, nothing reads them until a supervisor
or an admin is configured, and a project running one bot from ``settings.py`` never writes a
row here at all.

**The one change with a shape to it is the replay claim.** Its uniqueness moves from the
correlation id to the pair with the bot, because with more than one bot a single id can name a
message from each of them and the old constraint would have refused the second bot's replay as
already handled. And the column is ``0``-defaulted rather than nullable: a unique index treats
two NULLs as distinct on every database this package supports, so a nullable one would have
let two runs claim one failure -- which is the only thing that table exists to prevent.

Created where the project's own tables go rather than on ``EVENT_LOG_DATABASE``, for the
reason ``0004`` and ``0005`` give: all of it is operational state, and a claim or a lease the
software cannot enforce because the log lives in a warehouse would be neither.
"""

from typing import ClassVar

import django.db.models.deletion
import django.utils.timezone
from django.db import migrations, models


class Migration(migrations.Migration):
    """Create the four tables, and add the identity to the three that predate it."""

    dependencies: ClassVar = [('django_aiogram', '0005_replay_claim')]

    operations: ClassVar = [
        migrations.CreateModel(
            name='TelegramBot',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('bot_id', models.BigIntegerField(unique=True)),
                ('label', models.CharField(blank=True, max_length=128)),
                ('token', models.TextField(blank=True)),
                ('overrides', models.JSONField(blank=True, default=dict)),
                ('enabled', models.BooleanField(default=True)),
                ('quarantine_reason', models.CharField(blank=True, max_length=64)),
                ('quarantined_until', models.DateTimeField(blank=True, null=True)),
                ('created_at', models.DateTimeField(default=django.utils.timezone.now)),
                ('updated_at', models.DateTimeField(auto_now=True)),
            ],
            options={
                'verbose_name': 'telegram bot',
                'verbose_name_plural': 'telegram bots',
                'db_table': 'django_aiogram_bot',
                'ordering': ('label', 'bot_id'),
                'permissions': (('view_telegrambot_token', 'Can see bot tokens'),),
            },
        ),
        migrations.CreateModel(
            name='TelegramBotLease',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('bot_id', models.BigIntegerField(unique=True)),
                ('holder', models.CharField(max_length=128)),
                ('claimed_at', models.DateTimeField(default=django.utils.timezone.now)),
                ('expires_at', models.DateTimeField()),
            ],
            options={
                'verbose_name': 'telegram bot lease',
                'verbose_name_plural': 'telegram bot leases',
                'db_table': 'django_aiogram_bot_lease',
                'ordering': ('bot_id',),
            },
        ),
        migrations.CreateModel(
            name='TelegramBotProfile',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(max_length=64, unique=True)),
                ('overrides', models.JSONField(blank=True, default=dict)),
                ('created_at', models.DateTimeField(default=django.utils.timezone.now)),
                ('updated_at', models.DateTimeField(auto_now=True)),
            ],
            options={
                'verbose_name': 'telegram bot profile',
                'verbose_name_plural': 'telegram bot profiles',
                'db_table': 'django_aiogram_bot_profile',
                'ordering': ('name',),
            },
        ),
        migrations.CreateModel(
            name='TelegramQueue',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(max_length=255, unique=True)),
                ('pool', models.CharField(default='default', max_length=64)),
                ('created_at', models.DateTimeField(default=django.utils.timezone.now)),
            ],
            options={
                'verbose_name': 'telegram queue',
                'verbose_name_plural': 'telegram queues',
                'db_table': 'django_aiogram_queue',
                'ordering': ('pool', 'name'),
            },
        ),
        migrations.AddField(
            model_name='telegramevent',
            name='bot_id',
            field=models.BigIntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='telegramreplayclaim',
            name='bot_id',
            field=models.BigIntegerField(default=0),
        ),
        migrations.AddField(
            model_name='telegramscheduledsend',
            name='bot_id',
            field=models.BigIntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='telegramreplayclaim',
            name='correlation_id',
            field=models.UUIDField(),
        ),
        migrations.AddIndex(
            model_name='telegramevent',
            index=models.Index(fields=['bot_id', '-id'], name='dja_event_bot'),
        ),
        migrations.AddConstraint(
            model_name='telegramreplayclaim',
            constraint=models.UniqueConstraint(fields=('bot_id', 'correlation_id'), name='dja_replay_claim_once'),
        ),
        migrations.AddIndex(
            model_name='telegrambotlease',
            index=models.Index(fields=['expires_at'], name='dja_lease_expiry'),
        ),
        migrations.AddIndex(
            model_name='telegrambotprofile',
            index=models.Index(fields=['-updated_at'], name='dja_profile_changed'),
        ),
        migrations.AddField(
            model_name='telegrambot',
            name='profile',
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name='bots',
                to='django_aiogram.telegrambotprofile',
            ),
        ),
        migrations.AddIndex(
            model_name='telegramqueue',
            index=models.Index(fields=['pool'], name='dja_queue_pool'),
        ),
        migrations.AddField(
            model_name='telegrambot',
            name='queue',
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name='bots',
                to='django_aiogram.telegramqueue',
            ),
        ),
        migrations.AddIndex(
            model_name='telegrambot',
            index=models.Index(fields=['-updated_at'], name='dja_bot_changed'),
        ),
        migrations.AddIndex(
            model_name='telegrambot',
            index=models.Index(fields=['enabled', 'bot_id'], name='dja_bot_serving'),
        ),
    ]
