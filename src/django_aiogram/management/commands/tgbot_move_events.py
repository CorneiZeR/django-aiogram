"""Copy 3.x's event log rows into the 4.0 table, in bounded chunks.

Shaped like ``tgbot_prune_events`` and for the same reasons: a table sized by traffic is walked by
primary key range, one transaction per chunk, with a pause between them and a bound on how many a
single run does. So a nightly job has a blast radius and a big table takes as many nights as it
takes.

**Not a data migration.** ``migrate`` runs inside a deploy, and a copy that holds the deploy open
for as long as the table is large is not a thing an operator can pace, resume or stop. A check
(``I003``) says the old table is there and names this command; the operator picks the night.

**Resumable without duplicating, and without assuming an empty destination.** Both tables share the
primary key, so an id already in the new table is either a row this command copied or a row this
release wrote -- and either way it must not be inserted again. Every chunk therefore inserts only
the ids that are *not there yet*, which makes each one idempotent on its own, and the walk starts at
the first old id the destination does not have.

The obvious cheaper rule -- start above the destination's highest id -- is wrong the moment the bot
has written anything, which it does from the first message after ``migrate``: every old row beneath
that id is skipped and the move reports itself complete. Checked against that shape rather than
reasoned about, and the case is in the suite.

Not by ``created_at`` either: rows share timestamps, so a boundary drawn there would skip a row or
copy it twice.

**An id can be taken.** A row this release wrote can hold an id an old row also has, and nothing can
insert both -- so that old row is reported rather than moved, and the command says how many it left.
Comparing them, and deciding what the history is worth, is the operator's call: this will not
renumber somebody's rows to make a total tidy.

Nothing is dropped. ``DROP TABLE django_redis_aiogram_event`` stays the operator's to run.
"""

import logging
import time
from argparse import ArgumentParser
from typing import Any

from django.core.management import BaseCommand, CommandError
from django.core.management.color import no_style
from django.db import IntegrityError, connections, transaction

from django_aiogram.eventlog.moving import OLD_TABLE, added_since, old_table_is_present, shared_columns
from django_aiogram.eventlog.writer import log_alias
from django_aiogram.models import TelegramEvent

logger = logging.getLogger('django_aiogram')

#: how often one chunk is retried when a row lands under an id it was about to copy. Small on
#: purpose: each retry excludes the ids that landed, so a chunk either converges immediately or is
#: racing something that will keep taking ids -- and then saying so beats retrying for ever
_COLLISION_ATTEMPTS = 3


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
        """Copy every row the destination does not have, then leave the sequence able to insert."""
        alias = options['database'] or log_alias()
        if alias not in connections:
            msg = f'no database is configured under the alias {alias!r}; DATABASES has {sorted(connections)}.'
            raise CommandError(msg)
        try:
            present = old_table_is_present(alias)
        except Exception as unreachable:
            # "there is no old table" and "I could not look" read the same to a cron job, and only
            # one of them means the history has been moved. `I003` swallows this because a check
            # must not fail; a command that did would report a migration it never made
            msg = f'could not look for {OLD_TABLE} on {alias!r}: {unreachable}'
            raise CommandError(msg) from unreachable
        if not present:
            self.stdout.write(f'No {OLD_TABLE} table on {alias!r}, so there is nothing to move.')
            return

        bounds = self._bounds(alias)
        if bounds is None:
            # the sequence too, on the way out. A run killed after its last chunk committed and
            # before the reset leaves the ids copied and the sequence behind them, and every rerun
            # would take this same path and skip the reset again -- so the deployment stays one
            # duplicate-key error away from its next write until somebody notices. Cheap, and
            # idempotent: it sets the sequence from the table's own maximum
            if not options['dry_run']:
                self._reset_sequence(alias)
            # what the message can say, and no more: an id that is present may hold the row this
            # command copied or the row this release wrote, and only whoever knows can tell
            self.stdout.write(f'Every id in {OLD_TABLE} is present in {alias!r}; nothing is left to copy.')
            self._report_stranded(alias, self._stranded(alias))
            return

        moved = self._walk(alias, bounds, options)
        verb = 'would move' if options['dry_run'] else 'moved'
        self.stdout.write(f'{verb} {moved} events from {OLD_TABLE} into {alias!r}.')
        # counted *after* the walk, and once: a row this release writes while a chunk is running
        # strands a source row the count could not have seen beforehand, and the operator reading
        # this line is the one deciding whether the old table can go
        self._report_stranded(alias, self._stranded(alias))
        if not options['dry_run']:
            self._reset_sequence(alias)

    def _stranded(self, alias: str) -> int:
        """Count old rows whose id is held by a *different* row in the destination.

        Asked of the whole table, not of the ranges being walked -- which is the difference between
        reporting a collision and never seeing one. The walk starts at the first id the destination
        lacks, so a row this release wrote before the first run strands the old rows *beneath* that
        id, and no chunk ever visits them. The move then reports a clean total and every later run
        says every id is present, which is exactly the state in which somebody drops the old table
        and loses the only copy of those rows.

        **Every column, not a sample of them.** Two rows can share an id, a timestamp and a kind and
        still be different rows -- a chat id, a payload, an error apart -- and a comparison that
        stopped at three columns would call that a copy, skip it, and report nothing. Since the
        answer decides whether an operator can drop a table, the comparison is the whole row, and a
        difference in any column is a difference. Built from the columns the two tables share, so it
        covers one added later without anybody remembering this query -- and a column only this
        release has cannot be a difference, since the old row has nothing there to differ from.
        Counting one as a difference would report every old row as stranded on the release that
        adds it.
        """
        connection = connections[alias]
        old = connection.ops.quote_name(OLD_TABLE)
        new = connection.ops.quote_name(TelegramEvent._meta.db_table)  # noqa: SLF001 - _meta is Django's own API
        # written out per column rather than with `IS DISTINCT FROM`, which MySQL does not have:
        # a null on one side and a value on the other is a difference, and two nulls are not
        differs = ' OR '.join(
            f'(n.{column} IS NULL AND o.{column} IS NOT NULL) '
            f'OR (o.{column} IS NULL AND n.{column} IS NOT NULL) '
            f'OR n.{column} <> o.{column}'
            for column in (connection.ops.quote_name(name) for name in shared_columns(alias) if name != 'id')
        )
        statement = f'SELECT COUNT(*) FROM {old} o JOIN {new} n ON n.id = o.id WHERE {differs}'  # noqa: S608 - names quoted by the backend, no values at all
        with connection.cursor() as cursor:
            cursor.execute(statement)
            return int(cursor.fetchone()[0])

    def _report_stranded(self, alias: str, stranded: int) -> None:
        """Say what could not be copied, on every path that reports anything at all.

        Not an error and not something this command may fix: nothing puts two rows under one
        primary key, and renumbering somebody's history to make a total tidy is a worse answer than
        naming what was left.
        """
        if not stranded:
            return
        self.stdout.write(
            f'{stranded} rows in {OLD_TABLE} have an id that a different row already holds in '
            f'{alias!r}, so they cannot be copied. Compare them before dropping {OLD_TABLE}.'
        )

    def _bounds(self, alias: str) -> tuple[int, int] | None:
        """Return the first id the destination does not have and the last id in the old table.

        The *first missing* id rather than the destination's highest, because those are different
        questions once the bot has written anything: the destination's highest says nothing about
        the old rows below it. This one is where a resumed run picks up, and it advances only as
        rows land -- which is what stops a nightly `--max-chunks` run from re-walking what the
        night before already did.
        """
        connection = connections[alias]
        old = connection.ops.quote_name(OLD_TABLE)
        new = connection.ops.quote_name(TelegramEvent._meta.db_table)  # noqa: SLF001 - _meta is Django's own API
        # every name here is this package's own and quoted by the backend; the only values in any
        # statement below are bound, which is what the rule is for
        statement = (
            f'SELECT MIN(o.id), (SELECT MAX(id) FROM {old}) FROM {old} o '  # noqa: S608
            f'WHERE NOT EXISTS (SELECT 1 FROM {new} n WHERE n.id = o.id)'
        )
        with connection.cursor() as cursor:
            cursor.execute(statement)
            low, high = cursor.fetchone()
        return None if low is None else (low, high)

    def _walk(self, alias: str, bounds: tuple[int, int], options: dict[str, Any]) -> int:
        """Copy each id range in turn, pausing between them, and return what moved."""
        low, high = bounds
        chunk = max(1, int(options['chunk']))
        limit = max(0, int(options['max_chunks']))
        pause = max(0.0, float(options['sleep']))
        moved = 0
        rounds = 0

        while low <= high:
            top = min(low + chunk - 1, high)
            copied = self._copy(alias, low, top, dry_run=options['dry_run'])
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

    def _copy(self, alias: str, low: int, high: int, *, dry_run: bool) -> int:
        """Copy one id range, and say how many rows landed.

        `NOT EXISTS` rather than a range above a watermark, and it is doing two jobs at once. It is
        what makes a chunk idempotent on its own, so a killed run, a rerun and a `--max-chunks`
        night all copy each row exactly once. And it is what keeps the insert from colliding with a
        row this release wrote: an id the destination already holds is left where it is, counted,
        and reported by the caller -- since nothing can put two rows under one primary key, and
        renumbering somebody's history to make a total tidy is not this command's decision.
        """
        connection = connections[alias]
        old = connection.ops.quote_name(OLD_TABLE)
        new = connection.ops.quote_name(TelegramEvent._meta.db_table)  # noqa: SLF001 - _meta is Django's own API
        shared = shared_columns(alias)
        # a column 3.x's table does not have takes the model's default, written as a bound value:
        # `migrate` drops the default it adds the column with, so leaving that column out of the
        # insert is refused by the database rather than filled in
        added = added_since(alias)
        names = (*shared, *(name for name, _ in added))
        columns = ', '.join(connection.ops.quote_name(name) for name in names)
        picked = ', '.join([*(f'o.{connection.ops.quote_name(name)}' for name in shared), *('%s' for _ in added)])
        # as in `_bounds`: names are this package's own and quoted, and the ids are bound
        window = f'FROM {old} o WHERE o.id >= %s AND o.id <= %s'
        missing = f'AND NOT EXISTS (SELECT 1 FROM {new} n WHERE n.id = o.id)'  # noqa: S608
        still_missing = f'SELECT COUNT(*) {window} {missing}'
        insert = f'INSERT INTO {new} ({columns}) SELECT {picked} {window} {missing}'
        window_arguments = [low, high]
        # the defaults sit in the select list, ahead of the range the window binds
        insert_arguments = [*(value for _, value in added), *window_arguments]

        if dry_run:
            with connection.cursor() as cursor:
                cursor.execute(still_missing, window_arguments)
                return int(cursor.fetchone()[0])

        # `NOT EXISTS` decides what to insert; it does not hold the ids it chose. A row this
        # release writes between that read and the commit takes one of them, and the insert ends as
        # a unique violation -- most likely of all *before* this command has run once, since the
        # destination's sequence is still where `migrate` left it and the bot is drawing the very
        # ids the old table used. So the chunk is retried, and each retry excludes one more id:
        # this converges rather than spinning, and a chunk that keeps failing says so instead of
        # ending the run with a traceback about a primary key
        for attempt in range(1, _COLLISION_ATTEMPTS + 1):
            try:
                with transaction.atomic(using=alias), connection.cursor() as cursor:
                    cursor.execute(insert, insert_arguments)
                    return cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
            # three attempts, each one a database round trip: the setup cost this rule measures is
            # nanoseconds against a network hop, and moving the handler out of the loop would mean a
            # second function whose only job is to be somewhere else
            except IntegrityError:  # noqa: PERF203
                if attempt == _COLLISION_ATTEMPTS:
                    msg = (
                        f'ids {low} to {high} kept colliding with rows being written to {alias!r} '
                        f'after {_COLLISION_ATTEMPTS} attempts. Move the rest while nothing is writing, '
                        f'or rerun: what landed is committed and will not be copied twice.'
                    )
                    raise CommandError(msg) from None
                logger.info('a chunk collided with a concurrent write and is being retried', extra={'tg_low': low})
        return 0

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
