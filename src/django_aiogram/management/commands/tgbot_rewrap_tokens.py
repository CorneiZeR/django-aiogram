"""Write every stored token back through the storage seam, under the key that writes now.

Two jobs, one walk. Turning ``TOKEN_STORAGE`` on for a table full of plain tokens, and
rotating the key of one that is already encrypted: both are "read it with whatever can read
it, write it with whatever writes now", which is what the seam does by itself.

**Nothing is down while it runs.** The encrypting storage reads every configured key and
writes with the first, and a value it does not recognise comes back as it is -- so a row this
command has not reached yet is still readable by every process. The order is: add the new key
in front, deploy, run this, then drop the old key.

A row whose token cannot be read is reported and left alone. That is the honest answer: a key
that was dropped too early is a mistake a person has to undo, and rewrapping such a row would
mean writing whatever unreadable bytes it holds back into it as a *token*.
"""

import logging
from argparse import ArgumentParser
from typing import Any

from django.core.management import BaseCommand
from django.db import transaction

from django_aiogram.tokens import TokenUnreadableError, read_token, store_token

logger = logging.getLogger('django_aiogram')


class Command(BaseCommand):
    """Rewrap the tokens in ``TelegramBot``, so the column holds what the settings say."""

    help = 'Store every bot token again through TOKEN_STORAGE, for turning it on or rotating a key'

    def add_arguments(self, parser: ArgumentParser) -> None:
        """Declare --bot, --batch-size and --dry-run."""
        parser.add_argument(
            '--bot',
            action='append',
            default=[],
            dest='bots',
            type=int,
            help='only this identity, however many times it is given. Defaults to every row.',
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=100,
            help='how many rows to read at a time. Defaults to 100.',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='say what would be rewrapped and write nothing.',
        )

    def handle(self, **options: Any) -> None:
        """Walk the rows and write back the ones whose column is not what the storage writes."""
        # deferred: the ORM, and a management command is imported by `manage.py help`
        from django_aiogram.models import TelegramBot  # noqa: PLC0415 - as above

        rows = TelegramBot.objects.all()
        if options['bots']:
            rows = rows.filter(bot_id__in=options['bots'])
        dry_run = options['dry_run']
        rewrapped = unchanged = unreadable = 0
        # by identity rather than by primary key, because that is what every message about a
        # bot names, and an operator reading this output has the identity in front of them
        for row in rows.order_by('bot_id').iterator(chunk_size=max(1, options['batch_size'])):
            outcome = self._one(row, dry_run=dry_run)
            rewrapped += outcome == 'rewrapped'
            unchanged += outcome == 'unchanged'
            unreadable += outcome == 'unreadable'
        self.stdout.write(
            f'{rewrapped} rewrapped, {unchanged} already stored as configured, {unreadable} unreadable'
            + (' (dry run: nothing written)' if dry_run else '')
        )

    def _one(self, row: Any, *, dry_run: bool) -> str:  # noqa: ANN401 - a row, deferred past the ORM import
        """Rewrap one row, and say which of the three things happened to it."""
        if not row.token:
            # a bot somebody is in the middle of configuring: there is nothing to wrap, and
            # writing an empty column back would move `updated_at` for no reason
            return 'unchanged'
        try:
            token = read_token(row.token)
        except TokenUnreadableError as refused:
            self.stderr.write(f'{row.bot_id}: {refused}')
            return 'unreadable'
        written = store_token(token)
        # a storage that writes what it already holds -- the plain one, always -- must not
        # move `updated_at`: every supervisor in the deployment polls that watermark, and a
        # table-wide bump would have every container re-read every bot for nothing.
        # Compared rather than trusted: the encrypting one writes a different ciphertext each
        # time, so this is only ever equal where nothing changed
        if written == row.token:
            return 'unchanged'
        if dry_run:
            return 'rewrapped'
        row.token = written
        with transaction.atomic():
            # the token alone: a row read a moment ago may have been edited since, and
            # `save()` would write back the settings as they were then
            row.save(update_fields=['token', 'updated_at'])
        logger.info('rewrapped a stored token', extra={'tg_bot_id': row.bot_id})
        return 'rewrapped'
