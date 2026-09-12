"""The objects a profile owns, built once and shared by every bot that resolves to it.

Twenty bots configured alike hold one connection to the broker between them, not twenty. What
they share is what a :class:`~django_aiogram.runtime.profiles.Profile` decides; what stays
theirs is the token, the pacing and the aiogram ``Bot`` -- which costs almost nothing once its
HTTP session is shared.

Above the process and below the bot. The dispatcher, the handler tree and the FSM store are
one per *process* — ``config.defaults.PROCESS_SCOPED`` says why — and the token is one per
*bot*. This is the layer in between, and the transport is what lives in it.
"""

import atexit
import threading
from typing import TYPE_CHECKING

from django.core.signals import setting_changed
from django.dispatch import receiver

from django_aiogram.broker.registry import broker_class, overriding
from django_aiogram.config.bots import BOTS_SETTINGS_NAME
from django_aiogram.config.settings import SETTINGS_NAME
from django_aiogram.runtime.profiles import Profile, profile_of
from django_aiogram.runtime.queues import refuse_undeclared

if TYPE_CHECKING:
    from collections.abc import Mapping
    from typing import Any

    from django_aiogram.broker.base import Broker

__all__ = ('RuntimeGroup', 'close_groups', 'group_for', 'live_groups')


class RuntimeGroup:
    """Everything one profile's bots have in common, built on first use."""

    def __init__(self, profile: Profile, settings: 'Mapping[str, Any]') -> None:
        """Record what this group is for; nothing is connected until something asks."""
        self.profile = profile
        #: the settings this group was built from. One bot's, and any bot in the group would
        #: have done: they agree on everything a group reads, which is what put them here
        self.settings = settings
        self._broker: Broker | None = None
        #: set when the registry lets this group go. A group is handed out and used in two
        #: steps, so one can be retired in between -- and a transport built after that would
        #: belong to a group nothing holds
        self._gone = False

    @property
    def broker(self) -> 'Broker':
        """The one transport this group's bots publish to and consume from.

        An override from :func:`~django_aiogram.broker.registry.use_broker` wins over it and is
        never cached here: a capture is for the length of a block, and a group outlives one.
        """
        # this group's own settings, so a capture scoped to one queue answers for that queue
        # and leaves every other group on the transport it was configured with. Asked as the
        # group rather than as a bot: this transport is shared by every bot on the profile, so
        # a capture narrowed to one of them must not become the one the rest publish through
        held = overriding(self.settings, whole_group=True)
        if held is not None:
            return held
        # the registry's lock rather than one of this group's own, and that is what closes a
        # window rather than tidiness: `group_for` hands a group out and `close_groups` clears
        # the registry, so a build guarded only by this instance can put a transport into a
        # group nothing holds any more -- a connection no later shutdown can reach. One lock
        # over the choice *and* the build is what the broker registry has always done, and the
        # cost is the same: an uncontended acquisition on a path that ends in a socket
        with _lock:
            if self._gone:
                # asking for a group and asking it for a transport are two steps, and the
                # registry can be cleared between them -- so a build here would open a
                # connection in a group no shutdown can reach. The live group for these
                # settings is the answer, and it is one step away because this one is retired
                return group_for(self.settings).broker
            if self._broker is None:
                resolved = broker_class(self.settings)
                resolved.verify()
                # before the connection, and by name: a queue nothing declares is one nothing
                # consumes, so publishing to it is a send that succeeds and arrives nowhere
                refuse_undeclared(self.settings)
                # built *with* this group's settings, not merely chosen by them: an instance
                # reading `conf` would take its queue, its URL and its deadline from the
                # shared defaults, so a bot configured differently would be served by a
                # transport configured as everything else
                self._broker = resolved.configured(self.settings)
            return self._broker

    def close(self) -> None:
        """Release what this group holds. Safe to call when it holds nothing.

        Under the registry's lock, so a build cannot be part-way through: the transport this
        clears is the one that was there, and a build waiting behind it starts from nothing.
        """
        with _lock:
            current, self._broker = self._broker, None
            self._gone = True
            if current is not None:
                current.close()

    def __repr__(self) -> str:
        """Name the group by its profile, which is the only part worth printing."""
        return f'<RuntimeGroup {self.profile.digest}>'


_groups: dict[Profile, RuntimeGroup] = {}
#: one lock over choosing a group, building its transport and closing it. Reentrant because
#: `close_groups` holds it while a group's `close` reaches back through it
_lock = threading.RLock()
#: armed with the first group rather than per group, so a process that builds five closes them
#: with one callback. `close_groups` is idempotent either way
_exit_hook_armed = False


def group_for(settings: 'Mapping[str, Any]') -> RuntimeGroup:
    """Return the group one bot belongs to, building it the first time it is asked for.

    Keyed by the profile rather than by the bot, which is the whole point: a second bot whose
    settings agree gets the group the first one built, and one that differs anywhere gets its
    own.
    """
    global _exit_hook_armed  # noqa: PLW0603 - one hook per process, like the groups it closes
    profile = profile_of(settings)
    with _lock:
        group = _groups.get(profile)
        # replaced rather than handed back when it has been closed: `close()` is public, so a
        # caller can retire a group the registry still holds -- and a retired group answers by
        # asking here, which without this would be the same group answering itself for ever
        if group is not None and group._gone:  # noqa: SLF001 - the registry owns this flag
            group = None
        if group is None:
            group = _groups[profile] = RuntimeGroup(profile, settings)
            if not _exit_hook_armed:
                # `Broker.close()` says it is called at shutdown, and until 4.1 nothing called it
                # there: a Kafka consumer that disappears without leaving its group holds its
                # partitions until the session times out, which is a restart that delivers
                # nothing for that long. Each transport's `close` already restricts what it
                # touches from a thread that does not own it -- this runs on the main one
                atexit.register(close_groups)
                _exit_hook_armed = True
        return group


def live_groups() -> tuple[RuntimeGroup, ...]:
    """Every group this process has built, for a listing or a shutdown."""
    with _lock:
        return tuple(_groups.values())


def close_groups() -> None:
    """Close every group and forget them. Safe to call when there are none."""
    with _lock:
        current = list(_groups.values())
        _groups.clear()
        # every one of them, whatever the first says: they are already out of the registry, so
        # a close that raised part-way through would leave the rest open with nothing able to
        # reach them again. The first failure is the one raised, once the others are shut
        refused: Exception | None = None
        for group in current:
            try:
                group.close()
            except Exception as error:  # noqa: BLE001, PERF203 - a transport may refuse in any way it likes
                refused = refused or error
    if refused is not None:
        raise refused


@receiver(setting_changed, dispatch_uid='django_aiogram.runtime.groups')
def _forget_the_groups(**kwargs: 'Any') -> None:
    """Rebuild on the next ask when the settings a group was built from change.

    Only for this package's own two, like every other cache here: an unrelated
    ``override_settings`` in a project's suite must not close a connection.
    """
    if kwargs.get('setting') in {SETTINGS_NAME, BOTS_SETTINGS_NAME}:
        close_groups()
