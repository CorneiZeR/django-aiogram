"""Making the bots this process serves match the bots it is configured to serve.

A controller rather than a startup step, and that is the whole shape of it: the providers say
what the deployment *wants* and this brings what is running into line with it, again and
again. A client connecting a bot through a project's own interface is then a row somebody
wrote, not a redeploy.

**A failed read is not a removal.** A provider that raises leaves the running set exactly as
it was, because the alternative is a database blinking and taking twenty bots off the air. It
is the read-side twin of the rule this package already states about writes -- a call that
raised is *unknown*, not refused -- and it is the one mistake this module exists to not make.

**Level-triggered, never edge-triggered.** Nothing here acts on "what changed"; every pass
reads the desired set whole and compares it. So a lost notification costs a few seconds, a
duplicate costs nothing, and a reordered pair cannot leave the process serving the wrong bots.
The push in :mod:`django_aiogram.runtime.control` says *something changed, read again* for
exactly this reason.

**One bot's failure is its own.** A token that cannot be parsed, a profile that names an
unusable transport: the bot is quarantined with a reason and a moment to try again, and the
pass carries on to the others. A supervisor that raised would take the healthy bots down with
the broken one.

**And a moment to try again is not always the right answer.** What the failure *was* decides
that, and :mod:`django_aiogram.runtime.lifecycle` is where it is decided: a revoked token
waits for a new one rather than for a clock.
"""

import logging
import math
import threading
import time
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING

from django.core.exceptions import ImproperlyConfigured
from django.utils import timezone

from django_aiogram.config.defaults import DEFAULTS
from django_aiogram.config.settings import conf
from django_aiogram.runtime.lifecycle import Fate, classify, flush, forget, remember, waits
from django_aiogram.runtime.providers import desired

if TYPE_CHECKING:
    from collections.abc import Callable

    from django_aiogram.config.bots import BotRecord

__all__ = ('Quarantined', 'Supervisor', 'serving')

logger = logging.getLogger('django_aiogram')

#: how long a bot that failed to start waits before it is tried again, and how far that grows.
#: A quarantine is not a punishment: most of the reasons are a configuration somebody is in the
#: middle of fixing, so the wait starts short. It grows because a token Telegram has refused
#: will not start working, and a retry per pass would be a log nobody can read
_FIRST_WAIT = 5.0
_LONGEST_WAIT = 300.0


@dataclass
class Quarantined:
    """One bot this process could not serve, and when it may be tried again."""

    reason: str
    #: monotonic, not wall clock: a supervisor sleeps and wakes across a clock that may be
    #: adjusted under it, and "five seconds from now" has to survive that. ``None`` is a
    #: quarantine no wait will end -- a revoked token -- and only a changed record clears it
    until: float | None
    #: the configuration that failed. A corrected token is a different record, and waiting out
    #: a backoff earned by the old one would leave an operator who fixed the problem watching
    #: nothing happen for up to five minutes
    record: 'BotRecord | None' = None
    #: how many attempts have failed, which is what makes the wait grow
    attempts: int = 1
    #: what the failure was, which is what decides whether anything will retry it
    fate: Fate = Fate.UNKNOWN

    def wait(self) -> float:
        """Return how long the next quarantine should last, doubling up to the ceiling.

        The ceiling is applied to the *exponent* rather than to the product, because the
        product is what overflows: a bot failing for long enough reaches an attempt where
        ``2 ** (attempts - 1)`` is no longer a float, and the ``OverflowError`` would escape
        into the pass and stop every bot after it -- which is the one promise this class is
        here to keep.
        """
        doublings = math.log2(_LONGEST_WAIT / _FIRST_WAIT)
        if self.attempts - 1 >= doublings:
            return _LONGEST_WAIT
        return float(min(_FIRST_WAIT * 2 ** (self.attempts - 1), _LONGEST_WAIT))


@dataclass
class Supervisor:
    """The bots this process is serving, and one pass that makes that the right set.

    ``start`` and ``stop`` are handed in rather than inherited, so the thing being reconciled
    is not this class's business: a polling process starts an update task, and a consumer
    starts nothing at all. That also makes the reconciliation testable without a bot.
    """

    start: 'Callable[[BotRecord], None]'
    stop: 'Callable[[int], None]'
    #: what is being served, by identity, with the record it was started from
    running: 'dict[int, BotRecord]' = field(default_factory=dict)
    #: the bots that failed to start, by identity
    quarantined: dict[int, Quarantined] = field(default_factory=dict)
    #: monotonic, for the same reason `Quarantined.until` is
    clock: 'Callable[[], float]' = time.monotonic
    #: whether serving a bot has to be exclusive, which for polling it does: two processes
    #: calling `getUpdates` for one token get a 409 and half the updates each. A consumer or a
    #: webhook process sets nothing here -- an update arrives wherever the request landed, and
    #: competing consumers on a queue are what every transport is for
    exclusive: bool = False
    #: every bot this process has held a lease on, so a shutdown releases them even when the
    #: bot was stopped earlier in the same pass
    holding: set[int] = field(default_factory=set)

    def reconcile(self) -> None:
        """Read what should be running and make it so, or leave everything as it is.

        The order is deliberate: stop what is gone before starting what is new, so a bot whose
        token was rotated is torn down and rebuilt rather than briefly served twice.
        """
        # first, and before anything can return early: a state write a database refused is
        # not written again by the work below, because both ends of it are steady states
        flush()
        try:
            wanted = {record.bot_id: record for record in desired() if record.bot_id is not None}
        except Exception:
            # the rule this module exists for: a provider that could not look has not said
            # there is nothing to serve
            logger.exception('could not read the configured bots; leaving the running set alone')
            return

        if self.exclusive:
            # after the read and before anything is started: a bot this process may not poll
            # is one it must not have running either, and a lease it lost while it was not
            # renewing is the same thing arriving from the other direction
            allowed = set(self._leased(wanted))
            self.holding |= allowed
            for identity in [held for held in wanted if held not in allowed]:
                if identity in self.running:
                    self._stop(identity, 'the lease went to another process')
                self.quarantined.pop(identity, None)
                del wanted[identity]

        for identity in [held for held in self.running if held not in wanted]:
            self._stop(identity, 'no longer configured')
        for identity in [held for held in self.quarantined if held not in wanted]:
            del self.quarantined[identity]

        for identity, record in wanted.items():
            if self.running.get(identity) == record:
                continue
            if identity in self.running:
                # the settings moved under a bot that is already running -- a rotated token,
                # a changed profile -- and the thing running was built from the old ones
                self._stop(identity, 'reconfigured')
            self._start(identity, record)

    def _start(self, identity: int, record: 'BotRecord') -> None:
        """Serve one bot, or quarantine it with the reason it could not be served."""
        held = self.quarantined.get(identity)
        # remembered before the branch below drops it: a corrected token is exactly the case
        # where the row still says quarantined and a person is waiting to see it stop saying so
        was_held = held is not None
        if held is not None and held.record != record:
            # the configuration changed under the quarantine, so the reason it was quarantined
            # for may be gone. Tried at once and from a fresh backoff
            del self.quarantined[identity]
            held = None
        if held is not None and (held.until is None or self.clock() < held.until):
            # `None` outlives every wait: nothing but a new token ends it, and a new token is
            # the changed record above rather than an elapsed clock
            return
        try:
            self.start(record)
        except Exception as refused:  # noqa: BLE001 - one bot's failure is its own; see the module docstring
            attempts = held.attempts + 1 if held is not None else 1
            fate = classify(refused)
            entry = Quarantined(
                reason=type(refused).__name__,
                until=None,
                record=record,
                attempts=attempts,
                fate=fate,
            )
            wait = entry.wait() if waits(fate) else None
            entry.until = None if wait is None else self.clock() + wait
            self.quarantined[identity] = entry
            # `error` rather than `exception`, and the class rather than the message: aiogram
            # puts the API URL into what it raises and the URL carries the token, so a
            # traceback here would ship every quarantined bot's credential to wherever the
            # logs go. `tg_error` is the class name for the reason `Logging.md` gives about
            # every other use of it -- a message is what carries a secret, and a class is what
            # an aggregator can group on. The row holds the same thing, and `tg_fate` is what
            # says whether anything will retry it
            logger.error(  # noqa: TRY400 - `exception` is the traceback, and the traceback is the leak
                'a bot could not be served; quarantining it and carrying on',
                extra={
                    'tg_bot_id': identity,
                    'tg_bot': record.alias,
                    'tg_attempts': attempts,
                    'tg_fate': fate.value,
                    'tg_error': type(refused).__name__,
                },
            )
            # the wall clock, and only here: a row is read by a person and by another process,
            # neither of which shares this one's monotonic clock
            until = None if wait is None else timezone.now() + timedelta(seconds=wait)
            remember(identity, fate, entry.reason, until)
            return
        self.running[identity] = record
        self.quarantined.pop(identity, None)
        if was_held:
            forget(identity)

    def _stop(self, identity: int, why: str) -> None:
        """Stop serving one bot, and let a failure to stop it not stop the pass.

        A teardown that raises leaves the entry gone regardless: the alternative is a bot this
        process believes it is serving and is not, which is the state nothing can recover from
        without a restart.
        """
        try:
            self.stop(identity)
        except Exception:
            logger.exception(
                'a bot refused to stop; dropping it from the running set anyway',
                extra={'tg_bot_id': identity, 'tg_reason': why},
            )
        finally:
            self.running.pop(identity, None)

    def _leased(self, wanted: 'dict[int, BotRecord]') -> 'tuple[int, ...]':
        """Return the bots this process holds a lease on, taking what it can.

        The ones it is already serving come first, so a pass does not hand a bot it holds to
        another container merely because the desired set grew past ``MAX_BOTS_PER_WORKER``.

        A failure to reach the leases leaves the running set alone, which is the rule this
        module already applies to a provider that could not look: the alternative is a
        database blinking and stopping every bot in the container.
        """
        # deferred: the leases reach the ORM, and this module is imported where there is none
        from django_aiogram.runtime.leases import claim  # noqa: PLC0415 - as above

        asked = [held for held in wanted if held in self.running] + [
            held for held in wanted if held not in self.running
        ]
        try:
            return claim(asked)
        except Exception:
            logger.exception('could not read the bot leases; keeping the bots already held')
            return tuple(held for held in asked if held in self.running)

    def released(self) -> None:
        """Stop every bot this process is serving and give up its leases.

        For a shutdown: leases lapse on their own, and waiting `BOT_LEASE_SECONDS` for them is
        a client's bot answering nothing for that long when another container was ready.
        """
        for identity in list(self.running):
            self._stop(identity, 'shutting down')
        if not self.exclusive:
            return
        from django_aiogram.runtime.leases import release  # noqa: PLC0415 - as above

        release(list(self.holding))

    def interval(self) -> float:
        """How long to wait before the next pass, never below a second."""
        try:
            return max(1.0, float(conf['BOT_REFRESH_INTERVAL']))
        except (TypeError, ValueError, ImproperlyConfigured):
            # a pass that refuses to be scheduled because a setting is unreadable is a
            # deployment that serves nothing, so the default answers instead -- the same trade
            # `looping.drain_budget` makes, and for the same reason
            logger.warning('BOT_REFRESH_INTERVAL is unreadable; falling back to the default')
            return float(DEFAULTS['BOT_REFRESH_INTERVAL'])


#: the one supervisor a process has, or ``None`` where nothing has started one
_serving: Supervisor | None = None
_lock = threading.Lock()


def serving(supervisor: Supervisor | None = None) -> Supervisor | None:
    """Read or set the supervisor this process is running, so a push can reach it."""
    global _serving  # noqa: PLW0603 - one per process, like the bots it serves
    with _lock:
        if supervisor is not None:
            _serving = supervisor
        return _serving
