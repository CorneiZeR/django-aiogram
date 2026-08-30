"""Rules about the queue: which transport, what it needs, and what the consumer may ask of it.

`BROKER` and the settings each transport declares, the consumer named in `DELIVERY`, the worker
name the in-flight bookkeeping is keyed on, and the pop deadline that has to sit inside the
transport's own.

Nothing here imports a driver, and the two helpers that resolve the configured transport say why
at the import: the registry reaches a transport module, whose driver has been an extra since 4.0,
and `manage.py check` runs on every `migrate`.
"""

import os
import re
import socket
from typing import TYPE_CHECKING

from django.core.exceptions import ImproperlyConfigured

from django_aiogram.config.checks.conditions import _bot_is_enabled, _identity_matters
from django_aiogram.config.checks.problems import Problem
from django_aiogram.config.checks.shapes import _reads_as_a_dotted_path, _setting
from django_aiogram.config.defaults import DEFAULTS
from django_aiogram.config.settings import SETTINGS_NAME, coerce_bool, conf, take_ceiling

if TYPE_CHECKING:  # the seam is a type here and nothing more: importing it at run time would
    # pull the broker package into every process that only runs the checks
    from django_aiogram.broker.base import Broker


#: what a project reading `DELIVERY` from a 3.x settings file has in it, against the path that
#: does the same thing. Imported from the consumer would cost aiogram at check time -- see
#: `_a_usable_delivery` -- so the two copies are pinned against each other by the suite instead
THREE_X_DELIVERIES = {'blpop': 'django_aiogram.consumer.delivery.BlpopDelivery', 'keyspace': ''}


_DELIVERY_HINT = (
    'DELIVERY takes a dotted path to a Delivery subclass -- the shipped one is '
    "'django_aiogram.consumer.delivery.BlpopDelivery'. Whether the path imports, and imports a "
    'Delivery, is settled when start_tgbot builds it; see the Delivery page.'
)


#: what Docker generates when a container is started without `hostname:`
_EPHEMERAL_HOSTNAME = re.compile(r'[0-9a-f]{12}')


def _the_deadline_the_broker_declares(resolved: 'type[Broker]', key: str) -> list[Problem]:
    """Report a transport that cannot answer how long one of its calls may take.

    Its own function so `E047` keeps one return per finding without growing past what a reader can
    follow, and because the two findings are one subject: the name, and the number behind it.

    ``key`` is the setting the calling rule is about, and it is here only to be excluded from the
    rules asked below -- a broker naming it as its own deadline option would otherwise re-enter this
    rule for ever.
    """
    named = resolved.CALL_TIMEOUT_OPTION
    if not named or named not in resolved.OPTIONS:
        return [
            Problem(
                f'is {resolved.__name__}, which declares no call deadline: CALL_TIMEOUT_OPTION is {named!r}.',
                hint=(
                    'A Broker names the one of its own options that bounds a single call, so '
                    "W004 can quote it and the consumer can cap its reads by it -- 'REDIS_TIMEOUT' "
                    'on the Redis transports, and each of the others its own. See the Delivery '
                    'page for what a transport has to declare.'
                ),
            )
        ]
    try:
        resolved.call_timeout()
    except (ImproperlyConfigured, TypeError, ValueError) as refused:
        # one rule per setting, which is the convention `W004` states from the other side: a
        # deadline sitting in the package-wide table has a rule of its own -- `REDIS_TIMEOUT` has
        # `E030` -- and two errors about one value make the reader look for two problems. But the
        # rules are *asked*, not assumed: a broker of somebody's own can name `REDIS_TIMEOUT` and
        # refuse a 3 that `E030` accepts, and suppressing on the name alone left that silent until
        # the transport first used it. The registry rather than a list of names here, so this picks
        # a setting up on the day #23 moves it out of that table and its own rule goes with it
        from django_aiogram.config.checks import CHECKS  # noqa: PLC0415 - the registry is assembled from this module

        if any(check.run() for check in CHECKS if check.key == named and check.key != key):
            return []
        return [
            Problem(
                f'is {resolved.__name__}, whose call deadline is unusable: {refused}',
                hint=(
                    'It bounds one call to the transport, W004 quotes it, and the consumer caps '
                    'each take by it — see the page for your transport for the range it accepts.'
                ),
            )
        ]
    return []


def _a_usable_broker(key: str) -> list[Problem]:
    """Refuse a transport that cannot be reached before anything tries to send through it.

    `BROKER` is a dotted path and nothing is inferred from what happens to be installed, which
    is only safe if the name is judged here: `redis` left the base dependencies in 4.0, so a
    project that names the Redis list without `django-aiogram[redis]` installed is a working
    configuration one `pip install` short — and the difference between hearing that at startup
    and hearing `ModuleNotFoundError: redis` from inside a producer is the whole point of this
    rule.

    Each finding says what to do rather than what happened: the setting is empty; it names
    something that is not a broker; the driver behind it is absent, in which case the hint carries
    the install line for that extra; or the transport's own required settings are unset.

    The driver and the required settings are gated on the bot being enabled, for the same reason
    `W002` is: a process with `ENABLED` off sends nothing, so asking it to install a driver it will
    never call is an error nobody can act on except by installing it anyway. Which is also why the
    driver is verified *after* everything this rule can judge without one: a disabled process
    returns early on a missing driver, and a broken deadline in that process went unreported by
    anything -- as it did in a process that simply had not installed the extra yet. The gate is a trade,
    not a proof -- a disabled process that reads `queue_depth` *does* reach the transport, and
    hears the `ModuleNotFoundError` this rule exists to prevent. Documented rather than checked,
    because firing here would warn every image build and migration container that never reads a
    depth. The name itself is judged either way — nothing legitimately names a non-broker, and a
    typo in the web tier is the same typo in the worker, where it would fail.

    Nothing here imports the driver — the registry checks its own table of shipped brokers
    before importing anything, so an absent one is named rather than discovered by traceback.
    Nothing here *needs* the driver either, which is why it is verified last: see below.

    Then the call deadline the seam is built on, in two more findings. `CALL_TIMEOUT_OPTION` unset,
    or naming an option the broker does not declare -- the only finding here about a broker somebody
    wrote, and without it the failure lands as a `KeyError` out of `option('')`, in whichever rule
    asks first. And a deadline that cannot be one: `RABBITMQ_TIMEOUT` was neither a number nor
    positive on a configuration that passed every rule, and the transport then refused to build a
    channel at the first send. `W004` quotes that name and the consumer caps its reads by that
    number, so both belong to whichever rule owns `BROKER`.

    The second of those stands aside where the option it names has a rule of its own that is already
    reporting the value -- `REDIS_TIMEOUT` has `E030`, because it sits in the package-wide table.
    Already *reporting*, and not merely present: a broker of somebody's own can name that setting
    and refuse a value `E030` accepts.

    Neither is gated on `ENABLED`: both are arithmetic over settings that needs no driver, and a
    deadline the transport refuses is refused in every process that reaches it.
    """
    # deferred like every other import of the broker package here: `manage.py check` runs on every
    # `migrate`, and this module reaches a transport whose driver is an extra since 4.0
    from django_aiogram.broker.exceptions import (  # noqa: PLC0415 - as above
        BrokerDependencyError,
        BrokerNotConfiguredError,
    )

    enabled = _bot_is_enabled()
    # the driver is verified separately below, so everything this rule can judge without one is
    # judged in every environment. Resolving *with* the check first meant the deadline findings
    # were reachable only where the extra happened to be installed -- and never at all in a
    # disabled process, which returns early on a missing driver by design
    try:
        resolved = _configured_broker()
    except BrokerNotConfiguredError as wrong:
        return [Problem(f'is unusable: {wrong}', hint='Name a Broker subclass by dotted path.')]
    deadline = _the_deadline_the_broker_declares(resolved, key)
    if deadline:
        return deadline
    try:
        _configured_broker(verify_driver=True)
    except BrokerDependencyError as missing:
        if not enabled:
            return []
        return [
            Problem(
                f'names {_setting(key)!r}, whose driver is not installed.',
                hint=f'pip install "django-aiogram[{missing.extra}]"',
            )
        ]
    required = [option for option in resolved.required() if not str(conf.get(option) or '').strip()]
    if required and enabled:
        return [
            Problem(
                f'is {resolved.__name__}, which needs {", ".join(required)} set.',
                hint='Each transport declares the settings it cannot work without — see Settings.',
            )
        ]
    return []


def _a_url_pickle_can_survive(key: str) -> list[Problem]:
    """Refuse a decoding URL where pickle may be read: the pair cannot work at all.

    Decoding is otherwise supported — one REDIS_URL is often shared with a cache
    backend that wants it, and :func:`~django_aiogram.redis.as_bytes` is there
    for it. Pickle is the exception, and it fails in the one place nothing can
    recover from: redis-py decodes inside its own parser, so a blocking pop raises
    `UnicodeDecodeError` *after* the server has moved the message to the in-flight
    list, and each later start trips over it once before carrying on — measured on
    Redis 8: one error per restart, then the queue drains around it. Not a wedged
    consumer, which is what the wording used to imply, and the difference matters:
    an operator who believes the queue is dead drains it by hand.
    """
    try:
        allowed = coerce_bool(conf.get('ALLOW_PICKLE'), f"{SETTINGS_NAME}['ALLOW_PICKLE']")
    except ImproperlyConfigured:
        # unreadable is E017's finding; this check cannot say anything about it
        return []
    if not allowed:
        return []
    # deferred: this module is imported at every enabled boot, and redis-py is not
    from django_aiogram.redis import url_decodes_responses  # noqa: PLC0415 - as above

    if not url_decodes_responses(str(_setting(key) or '')):
        return []
    return [
        Problem(
            'sets decode_responses while ALLOW_PICKLE is True. A pickled payload is not '
            'valid text, so the consumer raises inside redis-py after the message has '
            'already left the queue: that message is stranded in the in-flight list, and '
            'each restart trips over it once more before carrying on.',
            hint=(
                'Drop decode_responses from the URL, or turn ALLOW_PICKLE off and use the '
                "'json' serializer. Give the cache its own URL if it needs decoding."
            ),
        )
    ]


def _a_worker_that_keeps_its_name(key: str) -> list[Problem]:
    """Say when the name a worker's in-flight list is keyed on cannot survive a restart.

    Crash safety rests on a restarted worker recognizing its own list. With
    ``WORKER_NAME`` unset the name is the hostname — which is fine on a host, and
    is not fine in a container started without ``hostname:``, where Docker invents
    a fresh twelve-character hex name for each container it creates. Restarting
    one in place keeps it; replacing it does not, and a redeploy replaces it. What
    the old container was sending is then stranded where nothing will look again.

    Narrow on purpose. An unset ``WORKER_NAME`` is the documented default and
    correct almost everywhere, so warning about it as such would fire on every
    untouched installation and teach people to stop reading warnings. This fires
    only on the shape that is actually broken.

    And only on the transports it is broken *for*. A broker answering
    ``needs_identity`` false does not key anything on the worker's name — a Redis
    Streams group's pending list belongs to the group, so any consumer can recover a
    dead one's work whatever it is called. Telling such a deployment to pin its
    hostname would be advice with nothing behind it.
    """
    if not _identity_matters():
        return []
    # the same test `worker_identity()` makes. Stripping here would warn about a
    # hostname the worker does not use: a padded name is a poor one, but it is
    # stable, and stability is the only thing this check is about
    if _setting(key):
        return []
    hostname = os.environ.get('HOSTNAME') or socket.gethostname()
    if not _EPHEMERAL_HOSTNAME.fullmatch(hostname):
        return []
    return [
        Problem(
            f"is empty and this container's hostname ({hostname}) is one Docker generated, so a "
            'replacement container gets a different one. The in-flight list is keyed on that name, '
            'so a worker killed mid-send would never find its own message again.',
            hint=(
                'Set WORKER_NAME, or give the container a fixed `hostname:`. This matters only '
                'where `start_tgbot` runs, which is why it is information rather than a warning: '
                'nothing here can tell that from a web process. '
                '`manage.py tgbot_reclaim --worker <name>` is the way back from a list already '
                'stranded, run from a process whose own name differs.'
            ),
        )
    ]


def _known_keys(_key: str) -> list[Problem]:
    """Warn about keys nothing reads: settings keeps them, so a typo is silent.

    The package-wide table is not the whole answer since 4.0. A transport declares settings
    of its own — a stream has a key and a group, and neither belongs in every other
    transport's namespace — so what counts as known is the table *plus* whatever the
    configured broker declares. Without that, naming the Streams broker and setting the key
    it requires would be reported as a typo.

    Only the configured one, not every shipped one: a key belonging to a transport this
    project is not using is read by nothing, which is exactly what this warns about.

    Two settings are in both tables and mean it: `REDIS_URL` and `REDIS_TIMEOUT` are read by the
    package under every transport, because the FSM storage builds a Redis client whatever the queue
    is. They are known whichever broker is configured, and that is not the gap `REDIS_MESSAGES_KEY`
    was — a list key is read by nothing at all under Kafka.
    """
    known = set(DEFAULTS) | _broker_options()
    # a non-string key would raise out of join and sorting mixed types raises
    # too, so everything unknown is rendered through repr's eyes first
    unknown = sorted(repr(key) for key in set(conf) - known)
    if not unknown:
        return []
    return [
        Problem(
            f'contains unknown keys: {", ".join(unknown)}.',
            hint=f'Known keys are: {", ".join(sorted(known))}.',
        )
    ]


def _configured_broker(*, verify_driver: bool = False) -> 'type[Broker]':
    """Resolve `BROKER` for a rule, importing the registry only when one runs.

    Every caller that needed it imported the registry itself -- three rules and two helpers -- so
    the sentence about *why* the import is down here was copied once per call site. The reason is
    the same for all of them and belongs in one place: `manage.py check` runs on every `migrate`,
    and the registry reaches a transport module, which since 4.0 may not have its driver installed.

    `verify_driver` defaults to **False** here, unlike in the registry: a rule that needs the
    driver is `E047` and says so, while every other rule is arithmetic over settings and answering
    "no such broker" on a machine one `pip install` short is how a whole page of findings went
    missing.
    """
    # deferred: `manage.py check` runs on every `migrate`, and the registry reaches a transport
    # module whose driver is an extra since 4.0
    from django_aiogram.broker.registry import broker_class  # noqa: PLC0415 - as above

    return broker_class(verify_driver=verify_driver)


def _broker_error() -> type[Exception]:
    """`BrokerError`, for the `except` clauses that catch it.

    A function because an `except` clause needs the class where it stands, and the module it lives
    in is one a check must not import at module scope. The same idiom as `_response_error()` in the
    Redis list transport, and for the same reason.
    """
    # deferred for the same reason: the broker package is not imported until a rule needs it
    from django_aiogram.broker.exceptions import BrokerError  # noqa: PLC0415 - as above

    return BrokerError


def _broker_options() -> set[str]:
    """Collect what the configured transport declares, or nothing if it cannot be resolved.

    Nothing rather than a guess: a `BROKER` that names something unusable is `E047`'s
    finding, and this rule reporting a pile of unknown keys on top of it would bury the one
    message that says what to do.

    Without verifying the driver, because what a transport *declares* is class state and needs
    none. Verified, this returned nothing on a machine that had not installed the extra yet — so
    `W003` called that transport's own required settings unknown keys and invited an operator to
    delete them, and every rule that guards one of those settings stopped running.
    """
    try:
        return set(_configured_broker().OPTIONS)
    except (_broker_error(), ImproperlyConfigured):
        return set()


def _a_pop_inside_the_deadline(key: str) -> list[Problem]:
    """Warn when BLPOP is asked to wait longer than the consumer will let it.

    The consumer caps the pop rather than letting it raise, so a setting above the cap
    is quietly ignored — and the operator who raised it goes on believing it took.

    Compared against the whole cap, not the read deadline alone, which is what this
    rule used to do. ``HEARTBEAT_INTERVAL`` binds it just as hard, so
    ``BLPOP_TIMEOUT=30, HEARTBEAT_INTERVAL=10, REDIS_TIMEOUT=60`` was silent while the
    pop ran at ten — and when the rule *did* fire, its hint named ``REDIS_TIMEOUT``
    whether or not that was the term doing the binding. It now reports the cap the
    consumer actually computes, from the same helper the consumer uses, and names
    whichever setting produced it.

    And the deadline term comes from the **configured transport**, which is #41: it was
    ``REDIS_TIMEOUT`` whichever broker was running, so this hint told a Kafka deployment to raise
    a setting it does not have. The broker is asked for the name and the number without being
    built — ``call_timeout()`` is a classmethod for that reason, since building one would import
    its driver, which is `E047`'s business rather than this rule's.
    """
    try:
        asked = int(_setting(key))
        # without the driver check: the cap is arithmetic over settings, and staying silent
        # because an extra is not installed would drop a settings warning on every machine that
        # has not installed it. `E047` owns the driver, with the install line
        broker = _configured_broker()
        ceiling = take_ceiling(broker.CALL_TIMEOUT_OPTION, broker.call_timeout())
    except (ImproperlyConfigured, TypeError, ValueError, OverflowError):
        # `OverflowError` because `int(float('inf'))` raises that and neither of the other two: a
        # rule about `BLPOP_TIMEOUT` must not be what ends the run, and `E014` owns the value
        return []  # E014, E023 and E030 own the type complaints
    except (_broker_error(), KeyError):
        # E047 owns every complaint about which transport is configured, including a broker that
        # declares no deadline option -- `option('')` raises `KeyError`, and a rule about
        # `BLPOP_TIMEOUT` must not be the thing that takes `manage.py check` down
        return []
    if asked <= ceiling.seconds:
        return []
    named = ' and '.join(f"{SETTINGS_NAME}['{key}']" for key in ceiling.bound_by)
    binds = 'which is what binds it' if len(ceiling.bound_by) == 1 else 'which both bind it, so both have to move'
    return [
        Problem(
            f'is {asked}, which the consumer caps at {ceiling.seconds}.',
            hint=f'Raise {named}, {binds}, or lower this.',
        )
    ]


def worker_name_problems() -> list[Problem]:
    """Return the worker-name problems, for a caller that knows it *is* the consumer.

    `I001` reports this as information because a system check cannot tell which process it
    is running in. `start_tgbot` can, and warns. One rule, two audiences.
    """
    return _a_worker_that_keeps_its_name('WORKER_NAME')


def _a_usable_delivery(key: str) -> list[Problem]:
    """Refuse a consumer that cannot be built, before the thread that would build it starts.

    ``DELIVERY`` became a dotted path in 4.0, so it can be wrong in the ways a path can be wrong
    rather than in the one way a choice list allowed. What this reports, each saying what to
    write: the setting is empty; it holds a 3.x word, named against the path that replaced it; or
    it is not a dotted path at all. Whether the path *imports*, and imports a `Delivery`, is the
    paragraph below -- and no count is given here on purpose, since the list is what a reader
    needs and a number beside it is a second thing to keep true.

    Judged whether or not the bot is enabled, unlike the driver half of `E047`: nothing
    legitimately names a non-delivery, and a typo in the web tier is the same typo in the worker
    -- where it would take the consumer down at startup with the queue filling behind it.

    **This rule does not resolve the path**, and that is a measured decision rather than a
    weaker one taken for convenience. Importing the consumer module costs aiogram and pydantic --
    not through anything the consumer chose, but through `wire.serializers`, which encodes
    aiogram models and so imports their types at module level. Measured on a bare settings
    module: 883ms and 135 MiB, on every `migrate`, `runserver` and `shell` that runs the checks.
    `E018` exists because that cost was once paid here; putting it back to catch a typo would
    trade the same seconds for a smaller class of typo.

    So this rule answers what a string can answer -- empty, a 3.x word, or something that is not
    a dotted path at all -- and `start_tgbot` answers the rest before it starts a thread:
    `get_delivery` resolves the path in `handle()`, so a path that does not import, or imports
    something that is not a `Delivery`, ends the command loudly with the queue untouched. The
    hint says so, because a reader who typed a plausible path deserves to know where it *will*
    be checked.
    """
    value = _setting(key)
    path = str(value or '').strip()
    if not path:
        # E005 owns "required and empty" for the keys that have no default; this one has one,
        # so an empty value is a project having cleared it
        return [Problem('is empty, so no consumer is chosen.', hint=_DELIVERY_HINT)]
    if path in THREE_X_DELIVERIES:
        # two different things happened to these two names, and one sentence for both said each of
        # them about the wrong one: `blpop` is the consumer that is still here under a dotted path,
        # `keyspace` is the one 3.0 removed. Telling a reader their consumer was replaced when it
        # was deleted sends them looking for a new name that does not exist
        replacement = THREE_X_DELIVERIES[path]
        if replacement:
            said = f'is {path!r}, which 4.0 replaced with a dotted path. Write {replacement!r}.'
            return [Problem(said, hint=_DELIVERY_HINT)]
        return [Problem(f'is {path!r}, a consumer removed in 3.0.', hint=_DELIVERY_HINT)]
    if not _reads_as_a_dotted_path(path):
        return [Problem(f'is {path!r}, which is not a dotted path.', hint=_DELIVERY_HINT)]
    return []
