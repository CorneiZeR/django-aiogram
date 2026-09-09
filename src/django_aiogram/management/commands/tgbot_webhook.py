"""Register, inspect or remove the Telegram webhook, for one bot or for every bot.

Telegram remembers the URL, not your settings file, so switching between polling
and webhooks means telling Telegram. `getUpdates` refuses to run while a webhook
is registered, which is why `delete` exists.

**`reconcile` is the one a deployment with many bots runs.** Only Telegram knows what it
will post to, so the pass asks -- `getWebhookInfo` per bot -- and gives each bot this
deployment serves the webhook it should have. Safe to run again: a bot already registered
costs one read.

It cannot repair the other direction. A bot Telegram is still posting to and this deployment
no longer serves needs that bot's token to deregister, and the row that held it is gone --
which is why `delete --bot <id>` belongs *before* the row is removed rather than after.

It paces itself, because a thousand bots starting at once is a thousand `setWebhook` calls
into an API with its own limits: `--pause` between calls and a jitter so two containers that
started together do not walk the list in step.
"""

import logging
import random
import time
from argparse import ArgumentParser
from typing import Any

from django.core.exceptions import ImproperlyConfigured
from django.core.management import BaseCommand, CommandError

from django_aiogram import bot
from django_aiogram.config.enums import UpdateMode
from django_aiogram.consumer.webhook import current_mode, registered, webhook_settings

logger = logging.getLogger('django_aiogram')


class Command(BaseCommand):
    """Tell Telegram where to post updates, or that it should stop."""

    help = 'Set, delete or show the Telegram webhook'

    def add_arguments(self, parser: ArgumentParser) -> None:
        """Declare the action, which bots it applies to, and how it paces itself."""
        parser.add_argument('action', choices=['set', 'delete', 'info', 'reconcile'])
        parser.add_argument(
            '--drop-pending',
            action='store_true',
            help='discard the updates Telegram queued while no webhook was registered',
        )
        parser.add_argument(
            '--bot',
            action='append',
            default=[],
            dest='bots',
            type=int,
            help=(
                'the identity of one bot, however many times it is given. Defaults to every '
                'configured bot for `reconcile`, and to this process own for the rest.'
            ),
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help=(
                'register even where Telegram already has the right URL. Telegram never '
                'reports the secret, so a rotated one looks like no change: this is how it '
                'is applied.'
            ),
        )
        parser.add_argument(
            '--pause',
            type=float,
            default=0.05,
            help=(
                'seconds between calls, jittered, so a deployment registering a thousand bots '
                'does not arrive as a thousand calls at once. 0 disables the pause.'
            ),
        )

    def handle(self, *args: Any, **options: Any) -> None:
        """Run the chosen action, and close the bot whichever way it ends."""
        if not bot.enabled:
            msg = (
                "the bot is disabled here (TELEGRAM_BOT_DEFAULTS['ENABLED'] or "
                'DJANGO_AIOGRAM_ENABLED); nothing was changed'
            )
            raise CommandError(msg)

        action = options['action']
        try:
            getattr(self, f'_{action}')(options)
        finally:
            bot.close()

    def _addressed(self, options: dict[str, Any]) -> tuple[Any, Any, 'int | None']:
        """Return the bot one single-bot action applies to, with its settings and identity.

        The process's own where no `--bot` is given, which is every deployment with one bot.
        More than one identity is refused rather than looped over: `set`, `delete` and `info`
        each say one thing about one bot, and `reconcile` is the action that walks a list.
        """
        asked = options['bots']
        if not asked:
            return bot, None, None
        if len(asked) > 1:
            msg = f'--bot may be given once for `{options["action"]}`; use `reconcile` to walk every bot.'
            raise CommandError(msg)
        from django_aiogram.runtime.providers import desired, switched_off  # noqa: PLC0415 - the ORM
        from django_aiogram.runtime.registry import bots  # noqa: PLC0415 - as above

        identity = asked[0]
        found = next((record for record in desired() if record.bot_id == identity), None)
        if found is None:
            # a switched-off row too, and only here: a bot nobody serves still has a webhook
            # to delete, and deleting one needs that bot's token. Unreachable, disabling a row
            # would leave Telegram posting at a URL that answers 404 for ever
            found = next((record for record in switched_off() if record.bot_id == identity), None)
            if found is not None:
                self.stdout.write(f'{identity} is switched off; acting on it anyway.')
        if found is None:
            msg = f'No bot with the identity {identity} is configured here.'
            raise CommandError(msg)
        return bots.for_record(found), found, identity

    def _set(self, options: dict[str, Any]) -> None:
        """Register the webhook Telegram should deliver to, warning if MODE disagrees.

        Setting one is what makes ``getUpdates`` start refusing, so a project still
        configured for polling is told plainly rather than left to read that from
        Telegram's error later.
        """
        if current_mode() != UpdateMode.WEBHOOK:
            self.stdout.write(
                self.style.WARNING(
                    f"TELEGRAM_BOT_DEFAULTS['MODE'] is '{current_mode()}': registering this webhook "
                    'stops getUpdates from working, so polling will fail until it is deleted'
                )
            )
        serving, settings, identity = self._addressed(options)
        arguments = webhook_settings(settings, identity)
        arguments['drop_pending_updates'] = options['drop_pending']
        serving.loop.run_until_complete(serving.bot.set_webhook(**arguments))
        self.stdout.write(self.style.SUCCESS(f'webhook set to {arguments["url"]}'))
        self.stdout.write('polling will refuse to start until this is deleted')

    def _delete(self, options: dict[str, Any]) -> None:
        """Unregister the webhook, which is what polling needs before it can start.

        **Run it before removing a bot's row, not after.** Deleting a webhook needs that
        bot's token, and once the row is gone this deployment has none -- so Telegram goes on
        posting updates to a URL that answers 404 for ever. `reconcile` cannot repair that
        either, for the same reason: there is nothing left to ask with.

        ``drop_pending_updates`` decides whether Telegram forgets what it queued while
        the webhook was down; the caller chooses, because that is a decision about
        duplicate work rather than about the webhook.
        """
        serving, _settings, _identity = self._addressed(options)
        serving.loop.run_until_complete(serving.bot.delete_webhook(drop_pending_updates=options['drop_pending']))
        self.stdout.write(self.style.SUCCESS('webhook deleted; polling can start again'))

    def _reconcile(self, options: dict[str, Any]) -> None:
        """Compare what Telegram has against what each bot should have, and repair it.

        **One direction only**, and the reason is the token: this walks the bots this
        deployment serves and gives each the webhook it should have. A bot Telegram is still
        posting to and this deployment no longer serves cannot be repaired from here at all --
        deregistering needs that bot's token, and the row that held it is gone. `delete --bot
        <id>` *before* removing the row is the other direction, and :meth:`_delete` says so.

        One bot's failure is its own: the pass carries on and says what it could not do, for
        the reason the supervisor gives about the same shape of work. A container that gave up
        on the first refusal would leave the rest unregistered.
        """
        from django_aiogram.runtime.providers import desired  # noqa: PLC0415 - the ORM, deferred

        wanted = [record for record in desired() if record.bot_id is not None]
        only = set(options['bots'])
        if only:
            wanted = [record for record in wanted if record.bot_id in only]
            missing = sorted(only - {record.bot_id for record in wanted})
            if missing:
                msg = f'These bots are not configured here: {", ".join(str(found) for found in missing)}.'
                raise CommandError(msg)
        for number, record in enumerate(wanted):
            if number:
                self._breathe(options['pause'])
            identity = record.bot_id
            if identity is None:  # pragma: no cover - filtered above, and mypy cannot see that
                continue
            self._reconcile_one(identity, record, options)

    def _reconcile_one(self, identity: int, record: Any, options: dict[str, Any]) -> None:  # noqa: ANN401
        """Bring one bot's webhook into line, and say what that took.

        Resolving the bot is inside the boundary, not before it: the providers are read again
        to build one, and a read that failed between the pass and this call would raise out of
        the loop and leave every later bot unregistered. One bot's failure is its own.
        """
        # deferred: the registry reaches aiogram and the providers, and a command module is
        # imported by `manage.py help`
        from django_aiogram.runtime.registry import bots  # noqa: PLC0415 - as above

        try:
            serving = bots.by_id(identity)
            arguments = webhook_settings(record, identity)
        except (ImproperlyConfigured, LookupError, KeyError) as refused:
            # a bot with no URL or no secret of its own is not registered under another bot's
            # -- `E027` reports the same thing at boot -- and a settings mapping missing a key
            # answers with `KeyError`, which is this bot's problem rather than the pass's
            self.stdout.write(self.style.WARNING(f'{identity}: not registered — {refused}'))
            return
        try:
            info = serving.loop.run_until_complete(serving.bot.get_webhook_info())
            if not options['force'] and registered(arguments, info):
                self.stdout.write(f'{identity}: already registered')
                return
            arguments['drop_pending_updates'] = options['drop_pending']
            serving.loop.run_until_complete(serving.bot.set_webhook(**arguments))
        except Exception as refused:  # noqa: BLE001 - one bot's failure is its own, see above
            logger.warning('could not reconcile a webhook', extra={'tg_bot_id': identity})
            self.stdout.write(self.style.WARNING(f'{identity}: failed — {type(refused).__name__}: {refused}'))
            return
        self.stdout.write(self.style.SUCCESS(f'{identity}: registered at {arguments["url"]}'))

    @staticmethod
    def _breathe(pause: float) -> None:
        """Wait a jittered fraction of a second, so a thousand bots are not a thousand calls.

        Jittered rather than fixed: two containers that started together would otherwise walk
        the same list at the same rate and arrive at the API in step, which is the stampede
        the pause exists to avoid.
        """
        if pause <= 0:
            return
        # not a secret, a delay: the jitter is about arrival times, not unpredictability
        time.sleep(pause * (0.5 + random.random()))  # noqa: S311 - see above

    def _info(self, options: dict[str, Any]) -> None:
        """Report what Telegram thinks the webhook is, which is the only authority on it.

        ``last_error_message`` is the field worth the command: a webhook can be
        registered and rejected on every delivery, and nothing local shows that.
        """
        serving, _settings, _identity = self._addressed(options)
        info = serving.loop.run_until_complete(serving.bot.get_webhook_info())
        if not info.url:
            self.stdout.write('no webhook registered; this bot is polled')
            return
        self.stdout.write(f'url: {info.url}')
        self.stdout.write(f'pending updates: {info.pending_update_count}')
        if info.last_error_message:
            self.stdout.write(self.style.WARNING(f'last error: {info.last_error_message} (at {info.last_error_date})'))
