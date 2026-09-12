"""Answer whether the bot container is doing its job.

`docker ps` says the process is up, which is not the same thing: the consumer
thread can be dead while polling continues, or Redis can be unreachable, and the
container stays "healthy" either way.

A wrapper, since 3.1.0, over :mod:`django_aiogram.healthcheck`. The decision
lives there so that ``python -m django_aiogram.healthcheck`` can make it without
``django.setup()`` — a management command populates the app registry and runs every
``AppConfig.ready()`` in the host project first, which is 17.89s in one measured
consumer against 0.01s of actual probing, and more than any Docker ``timeout`` can
honestly allow. This command is unchanged for anyone who has it in a compose file
today, and **Deployment** says why the other form belongs in a healthcheck.
"""

from argparse import ArgumentParser
from typing import Any

from django.core.management import BaseCommand, CommandError

from django_aiogram.broker.exceptions import BrokerDependencyError
from django_aiogram.healthcheck import add_limit_flags, check


class Command(BaseCommand):
    """Check Redis, the consumer's heartbeat and the queue length, in that order."""

    help = 'Exit 0 when the bot container is healthy, non-zero with a reason otherwise'

    def add_arguments(self, parser: ArgumentParser) -> None:
        """Declare the two limits, both of which default to a setting.

        Taken from the module that acts on them rather than restated here: a second copy
        of a flag is how one form ends up with a default the other does not have.
        """
        add_limit_flags(parser)

    def handle(self, *args: Any, **options: Any) -> None:
        """Report the first thing that is wrong, or that everything is fine.

        ``stranded`` and ``guarantee`` are asked for explicitly, because this command's
        output must not change for anyone who has it in a compose file — while the
        container-facing entry point leaves both off, since neither can alter the
        verdict and both are the expensive part of the probe.
        """
        try:
            report = check(
                max_queue=options['max_queue'],
                max_age=options['max_age'],
                stranded=True,
                guarantee=True,
                consumes=not options['no_consumer'],
                queues=options['queues'],
            )
        except BrokerDependencyError as error:
            # same refusal as the module form, in the shape a command reports with: BROKER
            # names a transport whose driver this install does not carry. A CommandError keeps
            # the install line on one line and the exit code non-zero, where a traceback out
            # of a probe says "unhealthy" without saying what to do about it
            raise CommandError(str(error)) from error
        if not report.ok:
            raise CommandError(report.message)
        # after the verdict, and only in this form: the quarantined bots are *rows*, and the
        # module form must not populate an app registry to answer a container's probe -- see
        # the note at the top of `healthcheck.py`. A command is already inside Django, so it
        # can say what a probe alone cannot
        for line in self._quarantined():
            self.stdout.write(self.style.WARNING(line))
        # plain when nothing was examined: a disabled process is not a healthy bot, and
        # this command has never colored that line green
        self.stdout.write(self.style.SUCCESS(report.message) if report.checked else report.message)
        for warning in report.warnings:
            self.stdout.write(self.style.WARNING(warning))

    @staticmethod
    def _quarantined() -> list[str]:
        """Name the bots a supervisor has stopped serving, with the reason it wrote.

        A probe that says *healthy* while three clients' bots are quarantined is answering a
        narrower question than the person reading it asked. It does not change the verdict:
        the container is doing what it can, and a revoked token is fixed by a person rather
        than by a restart.
        """
        # deferred: the ORM, and this command is imported by `manage.py help`
        from django.core.exceptions import ImproperlyConfigured  # noqa: PLC0415 - as above
        from django.db import DatabaseError  # noqa: PLC0415 - as above

        from django_aiogram.models import TelegramBot  # noqa: PLC0415 - as above

        try:
            held = TelegramBot.objects.exclude(quarantine_reason='').values_list('bot_id', 'quarantine_reason')
            return [f'bot {identity} is quarantined: {reason}' for identity, reason in held]
        except (DatabaseError, ImproperlyConfigured):
            # three deployments end up here and none of them is an unhealthy bot container: a
            # project whose bots live in `settings.py` and never migrated this table, one with
            # no database configured at all -- `DATABASES` empty is `ImproperlyConfigured`
            # rather than a database error -- and one whose database blinked. The verdict
            # above stands for all three
            return []
