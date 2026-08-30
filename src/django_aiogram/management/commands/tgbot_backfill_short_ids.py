"""Fill the short id on rows written before the column existed, in bounded chunks.

Shaped like ``tgbot_prune_events`` and ``tgbot_move_events``, and not a data migration for the same
reason as those: ``migrate`` runs inside a deploy, and rewriting a table sized by traffic there
cannot be paced, stopped or resumed.

**Resumable by what it is filling.** A row whose short id is empty is a row still to do, so the walk
asks for exactly those. A run that is killed leaves the rows it committed done, and the next one
picks up the rest without copying anything twice — there is no watermark to keep, because the
absence *is* the watermark.

Rows written after the migration carry their own short id, so this is only ever about history.
Nothing here computes an id differently from the writer: both call `events.short_id`.
"""

import logging
import time
from argparse import ArgumentParser
from typing import Any

from django.core.management import BaseCommand, CommandError
from django.db import connections, transaction

from django_aiogram.eventlog.events import short_id
from django_aiogram.eventlog.writer import log_alias
from django_aiogram.models import TelegramEvent

logger = logging.getLogger('django_aiogram')


class Command(BaseCommand):
    """Give the rows that predate the column the short id they would have been written with."""

    help = 'Fill the short id on event log rows written before it existed'

    def add_arguments(self, parser: ArgumentParser) -> None:
        """Declare the chunking, the two safety valves and the alias."""
        parser.add_argument('--chunk', type=int, default=1000, help='rows updated per transaction (default 1000)')
        parser.add_argument(
            '--sleep',
            type=float,
            default=0.1,
            help='seconds between chunks. The valve for replica lag: every updated row is an event '
            'with row-based binlogs (default 0.1)',
        )
        parser.add_argument(
            '--max-chunks',
            type=int,
            default=0,
            help='stop after this many chunks, so a nightly run has a bounded blast radius. 0 means no limit',
        )
        parser.add_argument('--database', default=None, help='the alias to fill; defaults to the configured one')
        parser.add_argument('--dry-run', action='store_true', help='report what would be filled, and fill nothing')

    def handle(self, *args: Any, **options: Any) -> None:
        """Walk the rows with no short id, filling each chunk in one transaction."""
        alias = options['database'] or log_alias()
        if alias not in connections:
            msg = f'no database is configured under the alias {alias!r}; DATABASES has {sorted(connections)}.'
            raise CommandError(msg)

        rows = TelegramEvent.objects.using(alias).filter(short_id='')
        remaining = rows.count()
        if not remaining:
            self.stdout.write(f'Every row in {alias!r} already has a short id.')
            return
        if options['dry_run']:
            self.stdout.write(f'would fill {remaining} rows in {alias!r}.')
            return

        filled = self._walk(alias, options)
        self.stdout.write(f'filled {filled} of {remaining} rows in {alias!r}.')
        if filled < remaining:
            self.stdout.write('Rerun to continue: a row without a short id is what this looks for.')

    def _walk(self, alias: str, options: dict[str, Any]) -> int:
        """Fill one chunk per transaction, pausing between them."""
        chunk = max(1, int(options['chunk']))
        limit = max(0, int(options['max_chunks']))
        pause = max(0.0, float(options['sleep']))
        filled = 0
        rounds = 0

        while True:
            with transaction.atomic(using=alias):
                # the ids first, then the update: `bulk_update` on a slice of a filtered queryset
                # would re-run the filter per chunk against rows this transaction is changing
                batch = list(TelegramEvent.objects.using(alias).filter(short_id='')[:chunk])
                if not batch:
                    return filled
                for row in batch:
                    row.short_id = short_id(row.correlation_id)
                TelegramEvent.objects.using(alias).bulk_update(batch, ['short_id'])
            filled += len(batch)
            rounds += 1
            if limit and rounds >= limit:
                self.stdout.write(f'Stopped after {rounds} chunks; rerun to continue.')
                return filled
            if pause:
                time.sleep(pause)
