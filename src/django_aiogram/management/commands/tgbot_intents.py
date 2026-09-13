"""Carry out what the admin asked for: check a token, register a webhook, remove one.

The admin writes the asking into the row because a request must not talk to Telegram -- five
hundred selected bots would be five hundred round trips inside one HTTP request. This is the
half that has an event loop.

Run it beside the bot containers, on a timer or with ``--watch``: an intent is a person waiting
for an answer, so the useful interval is seconds rather than minutes. Once a polling supervisor
runs inside ``start_tgbot`` it will do this in its own pass for the bots it holds, and this
command stays what a webhook deployment and an operator use.

Nothing here decides *what* to do -- :mod:`django_aiogram.runtime.intents` does, and the row is
what asked.
"""

import logging
import time
from argparse import ArgumentParser
from typing import Any

from django.core.management import BaseCommand

from django_aiogram.runtime.intents import carry_out

logger = logging.getLogger('django_aiogram')

#: how long ``--watch`` waits between passes when it found nothing. Short, because what it is
#: waiting for is a person watching an admin page for an answer
IDLE = 2.0


class Command(BaseCommand):
    """Answer the outstanding intents once, or keep answering them."""

    help = 'Carry out the checks and webhook changes asked for in the admin'

    def add_arguments(self, parser: ArgumentParser) -> None:
        """Declare --watch and --bot."""
        parser.add_argument(
            '--watch',
            action='store_true',
            help='keep going rather than making one pass. Ctrl-C stops it.',
        )
        parser.add_argument(
            '--bot',
            action='append',
            default=[],
            dest='bots',
            type=int,
            help='only these identities, however many times it is given. Defaults to every bot.',
        )

    def handle(self, **options: Any) -> None:
        """Make one pass, or keep making them until something interrupts."""
        if not options['watch']:
            self.stdout.write(f'{self._pass(options["bots"])} intent(s) carried out')
            return
        try:
            while True:
                if not self._pass(options['bots']):
                    time.sleep(IDLE)
        except KeyboardInterrupt:
            # the way an operator stops it, and not a failure: a partial pass leaves the
            # intents it did not reach in their rows, which is where they were
            self.stdout.write('stopped')

    def _pass(self, wanted: list[int]) -> int:
        """Carry out what the bots this run is about are waiting on.

        A read that failed leaves the intents where they are: this is the read-side rule the
        rest of the package keeps, and an intent is a row rather than a message -- nothing is
        lost by answering it a second later.
        """
        # deferred: the providers reach the ORM, and a command module is imported by
        # `manage.py help`
        from django_aiogram.runtime.providers import desired, switched_off  # noqa: PLC0415 - as above

        try:
            # the switched-off bots too: *delete this webhook* is exactly what a bot somebody
            # just turned off is waiting for, and `desired` leaves those out
            records = [*desired(), *switched_off()]
        except Exception:
            logger.exception('could not read the configured bots; nothing was carried out')
            return 0
        if wanted:
            records = [record for record in records if record.bot_id in set(wanted)]
        return carry_out(records)
