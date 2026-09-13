"""Index the feed by bot, in a migration of its own so it can be skipped without the rest.

One ``AddIndex``, and it is apart from ``0006`` for a reason an operator needs: this is the
only operation in the pair that touches a table whose size is set by traffic. Django builds an
index without ``CONCURRENTLY``, so on PostgreSQL this holds writes to
``django_aiogram_event`` for as long as the build lasts -- minutes on a large feed.

Nothing sending is affected: the recorder buffers and then drops rather than making a send
wait, so what it costs is log rows for the duration rather than messages.

On a feed large enough to care, apply ``0006``, fake **this** one, and create the index by
hand with ``CONCURRENTLY`` under the name below -- ``Upgrading.md`` has the two commands.
Faking ``0006`` instead would leave ``bot_id`` absent and every event insert refused, which is
why the two are not one migration.
"""

from typing import ClassVar

from django.db import migrations, models


class Migration(migrations.Migration):
    """Add the one index an operator may want to build for themselves."""

    dependencies: ClassVar = [('django_aiogram', '0006_bots_profiles_queues_leases')]

    operations: ClassVar = [
        migrations.AddIndex(
            model_name='telegramevent',
            index=models.Index(fields=['bot_id', '-id'], name='dja_event_bot'),
        ),
    ]
