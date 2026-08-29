"""Copy 3.x's event log rows into the 4.0 table, in bounded chunks.

Shaped like ``tgbot_prune_events`` and for the same reasons: a table sized by traffic is walked by
primary key range, one transaction per chunk, with a pause between them and a bound on how many a
single run does. So a nightly job has a blast radius and a big table takes as many nights as it
takes.

**Not a data migration.** ``migrate`` runs inside a deploy, and a copy that holds the deploy open
for as long as the table is large is not a thing an operator can pace, resume or stop. A check
(``I003``) says the old table is there and names this command; the operator picks the night.

**Resumable without duplicating.** Both tables share the primary key, so the destination's highest
id is the watermark and every chunk copies ids above it. A run killed halfway resumes where it
stopped and copies nothing twice. Not by ``created_at``: rows share timestamps, so a boundary
drawn there would either skip a row or copy it again.

Nothing is dropped. ``DROP TABLE django_redis_aiogram_event`` stays the operator's to run.
"""

import logging
import time
from argparse import ArgumentParser
from typing import Any

from django.core.management import BaseCommand, CommandError
from django.core.management.color import no_style
from django.db import connections, transaction
from django.db.models import Max

from django_aiogram.eventlog.moving import OLD_TABLE, old_table_is_present, shared_columns
from django_aiogram.eventlog.writer import log_alias
from django_aiogram.models import TelegramEvent

logger = logging.getLogger('django_aiogram')


class Command(BaseCommand):
    """Move the 3.x event log into this release's table, one bounded range at a time."""

    help = 'Copy rows from the 3.x event log table into the one this release writes to'

    def add_arguments(self, parser: ArgumentParser) -> None:
        """Declare the chunking, the two safety valves and the alias."""
        parser.add_argument(
            '--chunk',
            type=int,
            default=1000,
            help='width of the id range each transaction copies (default 1000)',
        )
        parser.add_argument(
            '--sleep',
            type=float,
            default=0.1,
            help='seconds between chunks. The valve for replica lag: with row-based binlogs every '
            'copied row is an event (default 0.1)',
        )
        parser.add_argument(
            '--max-chunks',
            type=int,
            default=0,
            help='stop after this many chunks, so a nightly run has a bounded blast radius. 0 means no limit',
        )
        parser.add_argument('--database', default=None, help='the alias to copy on; defaults to the configured one')
        parser.add_argument('--dry-run', action='store_true', help='report what would move, and move nothing')

    def handle(self, *args: Any, **options: Any) -> None:
        """Copy every row above the watermark, then leave the sequence able to accept an insert."""
        alias = options['database'] or log_alias()
        if alias not in connections:
            msg = f'no database is configured under the alias {alias!r}; DATABASES has {sorted(connections)}.'
            raise CommandError(msg)
        if not old_table_is_present(alias):
            self.stdout.write(f'No {OLD_TABLE} table on {alias!r}, so there is nothing to move.')
            return

        # the destination's own high-water mark, which is what makes a killed run resumable: the
        # rows below it are already here, and `id >` skips exactly those
        watermark = TelegramEvent.objects.using(alias).aggregate(highest=Max('id'))['highest'] or 0
        bounds = self._bounds(alias, watermark)
        if bounds is None:
            self.stdout.write(f'Nothing above id {watermark} in {OLD_TABLE}; the move is finished.')
            return

        moved = self._walk(alias, bounds, watermark, options)
        if options['dry_run']:
            self.stdout.write(f'would move {moved} events from {OLD_TABLE} into {alias!r}.')
            return
        self._reset_sequence(alias)
        self.stdout.write(f'moved {moved} events from {OLD_TABLE} into {alias!r}.')

    def _bounds(self, alias: str, watermark: int) -> tuple[int, int] | None:
        """Return the first and last id left to copy, or None when there are none."""
        table = connections[alias].ops.quote_name(OLD_TABLE)
        with connections[alias].cursor() as cursor:
            cursor.execute(f'SELECT MIN(id), MAX(id) FROM {table} WHERE id > %s', [watermark])  # noqa: S608 - the table name is this module's own constant, quoted by the backend
            low, high = cursor.fetchone()
        return None if low is None else (low, high)

    def _walk(self, alias: str, bounds: tuple[int, int], watermark: int, options: dict[str, Any]) -> int:
        """Copy each id range in turn, pausing between them."""
        low, high = bounds
        chunk = max(1, int(options['chunk']))
        limit = max(0, int(options['max_chunks']))
        pause = max(0.0, float(options['sleep']))
        moved = 0
        rounds = 0

        while low <= high:
            top = min(low + chunk - 1, high)
            copied = self._copy(alias, low, top, watermark, dry_run=options['dry_run'])
            moved += copied
            low = top + 1
            rounds += 1
            if limit and rounds >= limit:
                self.stdout.write(f'Stopped after {rounds} chunks; rerun to continue.')
                break
            # nothing was written, so there is nothing for a replica to catch up on
            if pause and copied and low <= high and not options['dry_run']:
                time.sleep(pause)
        return moved

    def _copy(self, alias: str, low: int, high: int, watermark: int, *, dry_run: bool) -> int:
        """Copy one id range, or count it. The watermark is repeated here on purpose.

        A concurrent writer cannot add rows to the old table -- nothing writes to it any more --
        but a *second run of this command* can, and the range bounds were computed before the walk
        started. Repeating `id > watermark` in the statement keeps every chunk idempotent on its
        own rather than only as part of the walk that planned it.
        """
        connection = connections[alias]
        old = connection.ops.quote_name(OLD_TABLE)
        new = connection.ops.quote_name(TelegramEvent._meta.db_table)  # noqa: SLF001 - _meta is Django's own API
        columns = ', '.join(connection.ops.quote_name(name) for name in shared_columns())
        window = 'WHERE id >= %s AND id <= %s AND id > %s'
        arguments = [low, high, watermark]

        with connection.cursor() as cursor:
            if dry_run:
                cursor.execute(f'SELECT COUNT(*) FROM {old} {window}', arguments)  # noqa: S608 - names quoted by the backend, values bound
                return int(cursor.fetchone()[0])
            with transaction.atomic(using=alias):
                cursor.execute(f'INSERT INTO {new} ({columns}) SELECT {columns} FROM {old} {window}', arguments)  # noqa: S608 - as above
                return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

    def _reset_sequence(self, alias: str) -> None:
        """Move the sequence past the ids just inserted, or the next insert collides.

        Explicit ids do not advance a PostgreSQL sequence, so a table copied into with the sequence
        still at zero accepts the copy and then refuses the first row the bot writes. Django's own
        `sequence_reset_sql` is what `loaddata` uses after loading explicit primary keys, and it
        speaks each backend's version of this -- `setval` on PostgreSQL, `sqlite_sequence` on
        SQLite, `AUTO_INCREMENT` on MySQL -- so this command does not have to.
        """
        connection = connections[alias]
        statements = connection.ops.sequence_reset_sql(no_style(), [TelegramEvent])
        if not statements:
            return
        with connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)
