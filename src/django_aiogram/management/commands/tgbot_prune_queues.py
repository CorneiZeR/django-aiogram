"""Remove what a client's queue leaves behind once no bot publishes to it.

A queue per client is the isolation that makes a shared deployment bearable, and it is also
how one leaks: a client goes, their bot's row goes, and their queue stays -- a Redis key, an
AMQP queue, a consumer group that nothing will ever read again. One per client that ever
existed.

**A command rather than a signal.** Removing a queue is a destructive act against a
transport, and a `post_delete` receiver in a web request is the wrong place to decide it: the
row may come back in the same minute, the queue may still hold messages a consumer was
sending, and nobody is watching the request that did it. This runs where an operator can read
what it did.

``REMOVED_QUEUE_POLICY`` is what it obeys, and the default is the safe one.
"""

import logging
from argparse import ArgumentParser
from typing import TYPE_CHECKING, Any

from django.core.management import BaseCommand, CommandError

from django_aiogram.config.enums import RemovedQueuePolicy
from django_aiogram.config.settings import SETTINGS_NAME, conf
from django_aiogram.runtime.queues import settings_for

if TYPE_CHECKING:
    from django_aiogram.broker.base import Broker
    from django_aiogram.models import TelegramQueue

logger = logging.getLogger('django_aiogram')


class Command(BaseCommand):
    """Find the queues no bot publishes to, and do what the policy says with them."""

    help = 'Remove the queues no bot publishes to any more'

    def add_arguments(self, parser: ArgumentParser) -> None:
        """Declare --policy, --queue and --dry-run."""
        parser.add_argument(
            '--policy',
            choices=sorted(policy.value for policy in RemovedQueuePolicy),
            default=None,
            help=(
                'what to do with a queue nothing publishes to. Defaults to TELEGRAM_BOT_DEFAULTS'
                "['REMOVED_QUEUE_POLICY']: 'park' leaves it and reports it, 'drop' removes it "
                "from the transport, 'hold' removes it only once it is empty."
            ),
        )
        parser.add_argument(
            '--queue',
            action='append',
            default=[],
            dest='queues',
            help='only this queue, however many times it is given. Refused where a bot still publishes to it.',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='say what would happen and change nothing.',
        )

    def handle(self, *args: Any, **options: Any) -> None:
        """Report every unreferenced queue, and remove the ones the policy says to."""
        policy = self._policy(options)
        for row in self._unreferenced(options['queues']):
            self._act_on(row, policy, dry_run=options['dry_run'])

    def _policy(self, options: dict[str, Any]) -> RemovedQueuePolicy:
        """Read the policy this run obeys, from the flag or the setting."""
        written = options['policy'] or str(conf['REMOVED_QUEUE_POLICY'] or '').strip()
        try:
            return RemovedQueuePolicy(written)
        except ValueError as refused:
            known = ', '.join(sorted(policy.value for policy in RemovedQueuePolicy))
            msg = f'{SETTINGS_NAME}["REMOVED_QUEUE_POLICY"] is {written!r}; it has to be one of {known}.'
            raise CommandError(msg) from refused

    def _unreferenced(self, only: list[str]) -> 'list[TelegramQueue]':
        """Return the queue rows no enabled bot names, in a stable order.

        A bot that is merely switched off still names its queue, and that is deliberate: a
        client paused for a month has not given up their backlog, and removing the queue under
        them would throw away messages nobody decided to throw away. Only a queue no row
        points at at all is unreferenced.
        """
        # deferred: the ORM, and a management command is loaded by `manage.py help`
        from django_aiogram.models import TelegramQueue  # noqa: PLC0415 - as above

        rows = TelegramQueue.objects.filter(bots__isnull=True).order_by('name')
        if only:
            wanted = {name.strip() for name in only if name.strip()}
            claimed = TelegramQueue.objects.filter(name__in=sorted(wanted), bots__isnull=False)
            still_used = sorted(claimed.values_list('name', flat=True))
            if still_used:
                msg = f'These queues still have bots publishing to them: {", ".join(still_used)}.'
                raise CommandError(msg)
            rows = rows.filter(name__in=sorted(wanted))
        return list(rows)

    def _act_on(self, row: 'TelegramQueue', policy: RemovedQueuePolicy, *, dry_run: bool) -> None:
        """Do what the policy says with one unreferenced queue, and say what that was."""
        from django_aiogram.broker.registry import get_broker  # noqa: PLC0415 - the transport, deferred

        broker = get_broker(settings_for(row.name))
        if not broker.removes_queues:
            # first, and before anything is *read*: the capability is a property and costs
            # nothing, while a depth is a call to a cluster that may not be reachable -- and
            # the answer here does not depend on it. A dry run says what a real one would do
            # rather than promising a removal this transport does not perform: Kafka, where a
            # topic is the cluster's to drop and not a producer's
            self.stdout.write(
                self.style.WARNING(f'{row.name}: this transport cannot remove a queue; remove it by hand.')
            )
            return
        # both, because either is a message: `depth` is what is waiting and `inflight_depth`
        # what a consumer has taken and not settled. Reported together, so a queue held back
        # says a number an operator can recognise
        held = broker.depth() + max(0, broker.inflight_depth() or 0)
        if policy is RemovedQueuePolicy.PARK:
            self.stdout.write(f'{row.name}: {held} message(s), parked — nothing publishes to it any more.')
            return
        if dry_run:
            plan = 'would be removed once empty' if policy is RemovedQueuePolicy.HOLD else 'would be removed'
            self.stdout.write(f'{row.name}: {held} message(s), {plan}.')
            return
        self._remove(row, broker, policy, held)

    def _remove(self, row: 'TelegramQueue', broker: 'Broker', policy: RemovedQueuePolicy, held: int) -> None:
        """Remove one queue from the transport and from the table, or say why it stayed.

        **The row is locked and its bots re-read here**, and that is not belt and braces: the
        candidates were chosen by a query, and a client's bot can be pointed at this queue in
        the seconds since. Removing it then destroys a live client's messages, and the row
        delete would fail anyway -- `TelegramBot.queue` is `PROTECT`. Locked and checked in
        the transaction that deletes, so the two cannot disagree.
        """
        from django.db import transaction  # noqa: PLC0415 - the ORM, deferred with the model

        from django_aiogram.models import TelegramQueue  # noqa: PLC0415 - as above

        with transaction.atomic():
            locked = TelegramQueue.objects.select_for_update().filter(pk=row.pk).first()
            if locked is None:
                self.stdout.write(f'{row.name}: gone already.')
                return
            if locked.bots.exists():
                self.stdout.write(f'{row.name}: a bot was pointed at it while this ran; left alone.')
                return
            if not broker.discard(if_empty=policy is RemovedQueuePolicy.HOLD):
                # under `hold` the transport is what decides emptiness, in one step: a
                # producer can publish between a depth read and a delete, so a caller that
                # checked for itself would delete the message it had just been told about
                self.stdout.write(f'{row.name}: held — it is not empty, or this transport cannot prove it is.')
                return
            locked.delete()
        logger.info('removed a queue nothing publishes to', extra={'tg_queue': row.name})
        self.stdout.write(f'{row.name}: removed, with {held} message(s).')
