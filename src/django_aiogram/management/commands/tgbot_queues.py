"""List the queues this deployment declares, how deep they are, and who is reading them.

A queue per client is what keeps one client's backlog off another's, and it is also the thing
that goes wrong quietly: a queue nobody consumes fills up and every message in it is delivered
*eventually*, which looks exactly like a slow bot until somebody asks. The heartbeat keys the
consumers write are what can answer it, and this is where that answer is read.

**Depth is asked of the transport, so this reaches the network.** One connection per distinct
configuration rather than one per queue -- bots configured alike share a transport, and asking
twenty times would open twenty connections to answer one question. A queue whose transport
cannot be reached says so rather than reporting zero: an unreachable broker is not an empty
queue, and the difference is the whole point of looking.
"""

import json
from argparse import ArgumentParser
from typing import Any

from django.core.management import BaseCommand

#: what a column shows where the transport could not answer
UNKNOWN = '?'


class Command(BaseCommand):
    """Show the declared queues with their pool, their depth and their consumers."""

    help = 'List the declared queues, their depth and whether anything is consuming them'

    def add_arguments(self, parser: ArgumentParser) -> None:
        """Declare --queue, --pool, --json and --no-depth."""
        parser.add_argument(
            '--queue',
            action='append',
            default=[],
            dest='queues',
            help='only these queues, however many times it is given. Defaults to every declared one.',
        )
        parser.add_argument(
            '--pool',
            action='append',
            default=[],
            dest='pools',
            help='only the queues in these pools, however many times it is given.',
        )
        parser.add_argument(
            '--no-depth',
            action='store_true',
            help='do not ask the transport anything: names and pools only, over no network.',
        )
        parser.add_argument(
            '--json',
            action='store_true',
            help='one JSON object per line, for a script rather than a person.',
        )

    def handle(self, **options: Any) -> None:
        """Read the declared queues and say what is in them."""
        declared, readable = self._queues(options)
        rows = [self._describe(queue, pool, options) for queue, pool in declared]
        if not rows:
            # *none declared* and *nobody could look* are different answers, and saying the
            # first for the second sends an operator to look at their settings instead of at
            # the database that refused
            said = (
                'no queues are declared'
                if readable
                else 'no queues are declared in the settings, and the table could not be read'
            )
            self.stdout.write(said)
            return
        if options['json']:
            for row in rows:
                self.stdout.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            return
        self._as_a_table(rows)

    def _queues(self, options: dict[str, Any]) -> tuple[list[tuple[str, str]], bool]:
        """Every declared queue with its pool, narrowed to what was asked for.

        The settings' `QUEUES` are declared too and have no pool of their own: a deployment
        that names its queues in `settings.py` has no table to give them one, and calling that
        pool `default` would be inventing a fact.
        """
        # deferred: these reach the ORM and the settings, and a command module is imported by
        # `manage.py help`
        from django_aiogram.runtime.queues import in_settings  # noqa: PLC0415 - as above

        found: dict[str, str] = dict.fromkeys(in_settings(), '')
        readable = True
        try:
            from django_aiogram.models import TelegramQueue  # noqa: PLC0415 - as above

            found.update(dict(TelegramQueue.objects.values_list('name', 'pool')))
        except Exception:  # noqa: BLE001 - a listing that raised would say nothing at all
            # a table that is not migrated, or a database that blinked: the settings' own
            # queues are still worth listing, and a listing that raised would say nothing
            self.stderr.write('could not read the queue table; listing what the settings declare')
            readable = False
        wanted, pools = set(options['queues']), set(options['pools'])
        rows = [(name, pool) for name, pool in sorted(found.items()) if not wanted or name in wanted]
        if pools:
            rows = [(name, pool) for name, pool in rows if pool in pools]
        return rows, readable

    def _describe(self, queue: str, pool: str, options: dict[str, Any]) -> dict[str, Any]:
        """Say what is in one queue, and who is reading it."""
        row: dict[str, Any] = {'queue': queue, 'pool': pool or '—'}
        if options['no_depth']:
            return {**row, 'depth': UNKNOWN, 'in_flight': UNKNOWN, 'consumer': UNKNOWN}
        return {**row, **self._from_the_transport(queue)}

    def _from_the_transport(self, queue: str) -> dict[str, Any]:
        """Ask one queue's transport what it holds, or say it could not be asked."""
        from django_aiogram.broker.registry import broker_class  # noqa: PLC0415 - as above
        from django_aiogram.runtime.queues import settings_for  # noqa: PLC0415 - as above

        try:
            settings = settings_for(queue)
            broker = broker_class(settings).configured(settings)
        except Exception:  # noqa: BLE001 - whatever a driver raises, this is one queue's answer
            self.stderr.write(f'{queue}: the transport could not be built')
            return {'depth': UNKNOWN, 'in_flight': UNKNOWN, 'consumer': UNKNOWN}
        return {
            'depth': self._number(queue, broker.depth),
            'in_flight': self._number(queue, broker.inflight_depth),
            'consumer': self._consumer(broker),
        }

    def _number(self, queue: str, ask: Any) -> Any:  # noqa: ANN401 - a bound method of whichever transport
        """Ask the transport one number, or report that it would not answer.

        A queue that cannot be reached is not an empty one, and printing `0` for it is the
        one answer that would send an operator looking in the wrong place.
        """
        try:
            return ask()
        except Exception:  # noqa: BLE001 - as above; an unreachable transport is not an empty queue
            self.stderr.write(f'{queue}: the transport would not answer')
            return UNKNOWN

    @staticmethod
    def _consumer(broker: Any) -> str:  # noqa: ANN401 - whichever transport this queue is on
        """How long ago something said it was consuming this queue, as the transport sees it.

        Asked of the broker rather than read out of a key, for the reason
        `healthcheck._liveness_age` gives: a Redis list has nothing that knows a consumer
        exists and writes a marker, while a stream's group already records when each member
        last spoke. A transport that says liveness is not observable answers `tracked` rather
        than a number -- neither "alive" nor "nobody", which is the honest position.
        """
        try:
            report = broker.liveness()
        except Exception:  # noqa: BLE001 - as above
            return UNKNOWN
        if not report.reported:
            return 'tracked'
        return 'never' if report.age is None else f'{report.age}s ago'

    def _as_a_table(self, rows: list[dict[str, Any]]) -> None:
        """Print the rows as columns, and say which queues nobody is reading."""
        columns = ('queue', 'pool', 'depth', 'in_flight', 'consumer')
        widths = {name: max(len(name), *(len(str(row[name])) for row in rows)) for name in columns}
        self.stdout.write('  '.join(name.ljust(widths[name]) for name in columns))
        for row in rows:
            self.stdout.write('  '.join(str(row[name]).ljust(widths[name]) for name in columns))
        # the question this command exists for: a queue with messages and nobody reading it
        stranded = [
            row['queue']
            for row in rows
            # `never` is the transport saying it writes a marker and there is none: nobody
            # has consumed this queue since the marker's TTL. `tracked` and `?` are not that
            if row['consumer'] == 'never' and row['depth'] not in (0, UNKNOWN)
        ]
        if stranded:
            self.stdout.write(f'messages waiting with no consumer: {", ".join(stranded)}')
