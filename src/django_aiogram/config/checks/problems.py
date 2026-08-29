"""What a check reports, and the row that reports it.

The vocabulary the rest of this package speaks: one `Problem` is something wrong with one setting,
and one `Check` is the id it is reported under, the setting it guards, and the rule that decides.

Here rather than beside the rules because everything imports it and it imports nothing back --
`Check.run` reaches for the configured transport's options, and does it inside the method so this
module stays at the bottom of the stack.
"""

from collections.abc import Callable
from dataclasses import dataclass

from django.core.checks import CheckMessage, Error, Info
from django.core.checks import Warning as CheckWarning

from django_aiogram.config.defaults import DEFAULTS
from django_aiogram.config.settings import SETTINGS_NAME

#: the first letter of a check id decides how loudly it reports; see :class:`Check`
_LEVELS = {'E': Error, 'W': CheckWarning, 'I': Info}


_ID_PREFIX = 'django_aiogram'


@dataclass(frozen=True)
class Problem:
    """What a rule found: the tail of the message, and where it belongs.

    ``key`` names the setting to blame when it is not the one the check guards —
    one webhook rule reports against both WEBHOOK_URL and WEBHOOK_SECRET.

    ``label`` is for a setting that is not ours: 4.0 moved two module paths a project writes into
    *Django's* settings, and a rule about `DATABASE_ROUTERS` that introduced itself as
    ``TELEGRAM_BOT['...']`` would send the reader to the wrong file. Everything else keeps the
    prefix, since almost every rule here is about one of our own keys.
    """

    message: str
    key: str | None = None
    hint: str | None = None
    label: str | None = None


Validator = Callable[[str], list[Problem]]


@dataclass(frozen=True)
class Check:
    """One row of the registry: the id it reports under, the setting, the rule.

    The id's first letter picks the level. ``E`` is an error and fails
    ``manage.py check`` outright. ``W`` is a warning: it does not fail a plain ``check``,
    but it *does* fail ``check --fail-level WARNING``, which projects run in CI and in
    container entrypoints — so a warning has to be something the project can actually act
    on, in every process that runs checks. ``I`` is information: worth printing, but about
    a condition this check cannot decide from where it stands, so failing a build on it
    would be a guess.
    """

    code: str
    key: str
    validate: Validator

    def run(self) -> list[CheckMessage]:
        """Turn everything the rule found into Django check messages.

        A row about a **transport's** setting runs only where that transport is configured, and
        which kind a row is needs no flag: a key outside the package-wide table belongs to whichever
        broker declares it. So `E007` validated `REDIS_MESSAGES_KEY` as a string on a Kafka
        deployment that has no such setting, and would do the same for every option each new
        transport brings.

        An empty key means the row is about the settings dict as a whole, and those always run.
        """
        from django_aiogram.config.checks.transport import _broker_options  # noqa: PLC0415 - the module below this one

        if self.key and self.key not in DEFAULTS and self.key not in _broker_options():
            return []
        return [self._message(problem) for problem in self.validate(self.key)]

    def _message(self, problem: Problem) -> CheckMessage:
        """Label one problem with the setting it is about and this row's id."""
        key = self.key if problem.key is None else problem.key
        # an empty key means the check is about the settings dict as a whole, and a `label` means
        # it is about a setting of Django's rather than one of ours
        label = problem.label or (f"{SETTINGS_NAME}['{key}']" if key else SETTINGS_NAME)
        report = _LEVELS.get(self.code[0], Error)
        return report(f'{label} {problem.message}', hint=problem.hint, id=f'{_ID_PREFIX}.{self.code}')
