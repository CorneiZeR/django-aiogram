"""List the bots this deployment resolves, and what each of them will do.

``manage.py check`` cannot see a bot that lives in a row: the checks read settings, and a
client connected through a project's own interface is a `TelegramBot`. So for a deployment
whose bots arrive at run time this is the only way to answer *what will actually happen* --
which queue a bot publishes to, which group it shares a transport with, whether anything is
serving it, and why it is not.

**The grouping is the point.** Twenty bots configured alike collapse into one transport, one
dispatcher and one consumer thread, and the only way to know it worked is to see the same
profile digest against all twenty. A deployment that quietly built twenty groups is one that
pays for twenty connections, and nothing else would say so.

Read-only, and it reaches no network: the leases and the rows come from the database, the
settings from the providers. A bot whose token cannot be read is listed as such rather than
left out -- silence is what an operator would read as "no such bot".
"""

import json
from argparse import ArgumentParser
from typing import TYPE_CHECKING, Any

from django.core.management import BaseCommand

if TYPE_CHECKING:
    from django_aiogram.config.bots import BotRecord

#: what a bot with no lease shows, so the column is never empty
NOBODY = '—'
#: what the lease column shows when the table could not be read at all, which is not the same
#: answer as "nobody holds it"
UNREADABLE = '?'


class Command(BaseCommand):
    """Show every resolved bot with the things that decide what it does."""

    help = 'List the bots this deployment resolves, with their profile, queue, state and lease'

    def add_arguments(self, parser: ArgumentParser) -> None:
        """Declare --bot, --json and --all."""
        parser.add_argument(
            '--bot',
            action='append',
            default=[],
            dest='bots',
            type=int,
            help='only these identities, however many times it is given. Defaults to every bot.',
        )
        parser.add_argument(
            '--all',
            action='store_true',
            help='include the bots that are switched off, which are left out by default.',
        )
        parser.add_argument(
            '--json',
            action='store_true',
            help='one JSON object per line, for a script rather than a person.',
        )

    def handle(self, **options: Any) -> None:
        """Resolve the bots, then print what each of them resolves to."""
        found = self._bots(options)
        if not found:
            # a filter that matched nothing is not a deployment with no bots: the first sends
            # an operator to check their `--bot`, the second to check their configuration
            self.stdout.write(
                f'no bot matches --bot {", ".join(str(identity) for identity in options["bots"])}'
                if options['bots']
                else 'no bots are configured'
            )
            return
        held = self._leases()
        rows = [self._describe(record, held) for record in found]
        if options['json']:
            for row in rows:
                self.stdout.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            return
        self._as_a_table(rows)

    def _bots(self, options: dict[str, Any]) -> 'list[BotRecord]':
        """Every bot this deployment resolves, narrowed to the ones asked for."""
        # deferred: the providers reach the ORM, and a command module is imported by
        # `manage.py help`
        from django_aiogram.runtime.providers import desired, switched_off  # noqa: PLC0415 - as above

        found = list(desired())
        if options['all']:
            # the identities `desired` already answered for are not asked again: a section and
            # a switched-off row may name one bot, and `switched_off` applies none of the
            # precedence `desired` does -- so without this the listing would show that bot
            # twice, with different settings, and neither line would say which one wins
            seen = {record.bot_id for record in found}
            found += [record for record in switched_off() if record.bot_id not in seen]
        wanted = set(options['bots'])
        if wanted:
            found = [record for record in found if record.bot_id in wanted]
        return sorted(found, key=lambda record: (record.bot_id or 0, record.alias))

    def _leases(self) -> 'dict[int, str] | None':
        """Who holds each bot's lease, or ``None`` where the table could not be read.

        ``None`` rather than an empty mapping, and the difference is the column: *nobody holds
        this bot* and *nobody could look* are different answers, and printing the first for
        the second is how an operator concludes a bot is unserved while it is being served.

        A failure costs that column rather than the listing: a lease says which container is
        polling a bot, and a deployment on webhooks takes none at all.
        """
        from django.db import DatabaseError  # noqa: PLC0415 - as above
        from django.utils import timezone  # noqa: PLC0415 - as above

        from django_aiogram.models import TelegramBotLease  # noqa: PLC0415 - as above

        try:
            live = TelegramBotLease.objects.filter(expires_at__gt=timezone.now())
            return dict(live.values_list('bot_id', 'holder'))
        except DatabaseError as refused:
            self.stderr.write(f'could not read the bot leases: {refused}')
            return None

    @staticmethod
    def _describe(record: 'BotRecord', held: 'dict[int, str] | None') -> dict[str, Any]:
        """Say what one bot resolves to, in the terms an operator is asking about."""
        # deferred with the rest: the profile reads the transport's own options off `BROKER`
        from django_aiogram.runtime.profiles import profile_of  # noqa: PLC0415 - as above
        from django_aiogram.runtime.queues import named  # noqa: PLC0415 - as above

        identity = record.bot_id
        return {
            'bot_id': identity,
            'alias': record.alias,
            'source': 'row' if record.provided else 'settings',
            # the digest rather than the settings: two bots sharing one is the whole question,
            # and the settings themselves may carry a URL with a password in it
            'profile': profile_of(record).digest,
            # empty means the transport's own, which is what a single-queue deployment has
            'queue': named(record) or NOBODY,
            'mode': str(record['MODE']),
            'enabled': bool(record['ENABLED']),
            'lease': UNREADABLE if held is None else held.get(identity or 0, NOBODY),
        }

    def _as_a_table(self, rows: list[dict[str, Any]]) -> None:
        """Print the rows as columns wide enough for what is in them."""
        columns = ('bot_id', 'alias', 'source', 'profile', 'queue', 'mode', 'enabled', 'lease')
        widths = {name: max(len(name), *(len(str(row[name])) for row in rows)) for name in columns}
        self.stdout.write('  '.join(name.ljust(widths[name]) for name in columns))
        for row in rows:
            self.stdout.write('  '.join(str(row[name]).ljust(widths[name]) for name in columns))
        groups = {row['profile'] for row in rows}
        self.stdout.write(f'{len(rows)} bot(s) in {len(groups)} group(s)')
