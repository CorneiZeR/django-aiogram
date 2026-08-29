"""Django system checks for the package settings.

Every check is a row in :data:`CHECKS`: an id, the setting it guards and a rule.
The id is spelled out in the row, so grepping ``E019`` finds both the check and
the ``docs/wiki/Settings.md`` entry that explains it.

Check ids are ``django_aiogram.EXXX``, and an id is never reused once its
setting is gone: a project silencing ``E013`` must not silently start silencing
whatever came after it.
"""

import math
import os
import re
import socket
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass, fields
from functools import partial
from typing import TYPE_CHECKING, Any

from django.conf import settings as django_settings
from django.core.checks import CheckMessage, Error, Info
from django.core.checks import Warning as CheckWarning
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string

from django_aiogram.config.defaults import DEFAULTS
from django_aiogram.config.enums import (
    KNOWN_RATE_LIMIT_KEYS,
    PayloadDetail,
    SerializerKind,
    StorageKind,
    UpdateMode,
    choices,
)
from django_aiogram.config.settings import SETTINGS_NAME, coerce_bool, conf, take_ceiling
from django_aiogram.eventlog.events import known_kinds

if TYPE_CHECKING:  # the seam is a type here and nothing more: importing it at run time would
    # pull the broker package into every process that only runs the checks
    from django_aiogram.broker.base import Broker

MODE_CHOICES = choices(UpdateMode)
SERIALIZER_CHOICES = choices(SerializerKind)
PAYLOAD_CHOICES = choices(PayloadDetail)


def _reads_as_a_dotted_path(path: str) -> bool:
    r"""Whether the string could name something importable at all.

    `str.isidentifier` rather than a `\w`-based pattern, which was the first version and was
    wrong in a way worth recording: with a `str` pattern, Python's `\\w` matches any Unicode word
    character, so `pkg.mod\u00b2.Consumer` satisfied it while `'mod\u00b2'.isidentifier()` is
    False. The rule accepted a path no import could ever resolve.

    Keywords are **not** refused, though `'class'.isidentifier()` being True makes them look like
    the same case. Measured: a file called `class.py` imports perfectly well --
    `importlib.import_module('pkg.class')` returns the module and `import_string('pkg.class.C')`
    returns the class -- because only the `import` *statement* goes through Python's grammar, and
    nothing here does. Refusing that would refuse a path the project can use.
    """
    segments = path.split('.')
    return len(segments) > 1 and all(segment.isidentifier() for segment in segments)


#: what a project reading `DELIVERY` from a 3.x settings file has in it, against the path that
#: does the same thing. Imported from the consumer would cost aiogram at check time -- see
#: `_a_usable_delivery` -- so the two copies are pinned against each other by the suite instead
THREE_X_DELIVERIES = {'blpop': 'django_aiogram.consumer.delivery.BlpopDelivery', 'keyspace': ''}

_DELIVERY_HINT = (
    'DELIVERY takes a dotted path to a Delivery subclass -- the shipped one is '
    "'django_aiogram.consumer.delivery.BlpopDelivery'. Whether the path imports, and imports a "
    'Delivery, is settled when start_tgbot builds it; see the Delivery page.'
)

_STORAGE_CHOICES = choices(StorageKind)
#: what Docker generates when a container is started without `hostname:`
_EPHEMERAL_HOSTNAME = re.compile(r'[0-9a-f]{12}')
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


def _a_readable_boolean(key: str) -> list[Problem]:
    """Accept whatever ``coerce_bool`` accepts, and report what it would refuse.

    This rule used to demand a real ``bool``, which had it backwards in both
    directions. ``{'ENABLED': 'true'}`` is documented, boots, sends — and failed
    ``manage.py check``; while the values ``coerce_bool`` genuinely refuses raise
    ``ImproperlyConfigured`` out of ``apps.ready()`` before any check runs, so the
    error could never fire on the case it was written for. The package's own fixtures
    tripped it on working settings.

    Asked by trying the coercion rather than by reimplementing its rules, so the check
    and the runtime cannot disagree — and the message is the one the runtime would
    have raised, which is the sentence a reader needs.
    """
    try:
        coerce_bool(_setting(key), f"{SETTINGS_NAME}['{key}']")
    except ImproperlyConfigured as error:
        # the message already names the setting, and `Check._message` prefixes it
        # again — so hand back only the tail
        return [Problem(str(error).replace(f"{SETTINGS_NAME}['{key}'] ", '', 1))]
    return []


def _an_integer(key: str, *, minimum: int | None = None) -> list[Problem]:
    """Require an integer, at or above ``minimum`` when one is given."""
    value = _setting(key)
    # bool is a subclass of int, so it has to be rejected explicitly
    if isinstance(value, bool) or not isinstance(value, int):
        return [Problem(f'must be an integer, got {type(value).__name__}.')]
    if minimum is not None and value < minimum:
        return [Problem(f'must be >= {minimum}, got {value}.')]
    return []


def _a_number(key: str, *, minimum: float | None = None) -> list[Problem]:
    """Require a finite number, at or above ``minimum`` when one is given.

    Wider than :func:`_an_integer` because seconds are a place a fraction is a
    reasonable thing to write. `nan` is refused with the rest: comparisons against
    it are all false, so it would slip past the bound and then make every deadline
    built from it expire immediately.
    """
    value = _setting(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return [Problem(f'must be a number, got {type(value).__name__}.')]
    if not math.isfinite(value):
        return [Problem(f'must be a finite number, got {value}.')]
    if minimum is not None and value < minimum:
        return [Problem(f'must be >= {minimum}, got {value}.')]
    return []


def _a_string(key: str, *, allowed: Collection[str] | None = None) -> list[Problem]:
    """Require a string, one of ``allowed`` when the setting is an enumeration."""
    value = _setting(key)
    if not isinstance(value, str):
        return [Problem(f'must be a string, got {type(value).__name__}.')]
    if allowed is not None and value not in allowed:
        return [Problem(f'must be one of {sorted(allowed)}, got {value!r}.')]
    return []


def _a_callable(key: str) -> list[Problem]:
    """Require something callable."""
    value = _setting(key)
    if callable(value):
        return []
    return [Problem(f'must be callable, got {type(value).__name__}.')]


def _a_mapping(key: str) -> list[Problem]:
    """Require a mapping."""
    value = _setting(key)
    if isinstance(value, Mapping):
        return []
    return [Problem(f'must be a mapping, got {type(value).__name__}.')]


def _known_bot_properties(key: str) -> list[Problem]:
    """Reject names ``DefaultBotProperties`` does not have, which it would drop."""
    value = _setting(key)
    if not isinstance(value, Mapping):
        return []
    # before the import, not after: the default is {}, which is a Mapping, so without
    # this every `manage.py check` in every project would pay for aiogram
    if not value:
        return []
    # deferred: aiogram costs most of a second, and checks only run on demand
    from aiogram.client.default import DefaultBotProperties  # noqa: PLC0415 - as above

    known = {field.name for field in fields(DefaultBotProperties)}
    # keys may be anything a project typed into settings, so stringify before joining
    unknown = sorted(str(name) for name in value if name not in known)
    if not unknown:
        return []
    return [Problem(f'has unknown properties: {", ".join(unknown)}. Known: {", ".join(sorted(known))}.')]


def _importable_storage(key: str) -> list[Problem]:
    """Resolve a dotted path here, so a typo fails before the first message."""
    value = _setting(key)
    if not isinstance(value, str):
        return []
    if value in _STORAGE_CHOICES:
        return []
    if '.' not in value:
        return [Problem(f"must be 'redis', 'memory', or a dotted path, got {value!r}.")]
    # deferred like the other aiogram imports: a disabled boot must not pay for it
    from aiogram.fsm.storage.base import BaseStorage  # noqa: PLC0415 - as above

    try:
        storage = import_string(value)
    except ImportError as error:
        return [Problem(f'cannot be imported: {error}')]
    if not (isinstance(storage, type) and issubclass(storage, BaseStorage)):
        return [Problem(f'must point to a BaseStorage subclass, got {value!r}.')]
    return []


def _sane_rate_limits(key: str) -> list[Problem]:
    """Require known budget names holding non-negative numbers."""
    value = _setting(key)
    if value is None:
        return []
    if not isinstance(value, Mapping):
        return [Problem(f'must be a mapping or None, got {type(value).__name__}.')]
    unknown = sorted(str(name) for name in value if name not in KNOWN_RATE_LIMIT_KEYS)
    if unknown:
        known = ', '.join(sorted(KNOWN_RATE_LIMIT_KEYS))
        return [Problem(f'has unknown keys: {", ".join(unknown)}. Known: {known}.')]
    for name, rate in value.items():
        if isinstance(rate, bool) or not isinstance(rate, (int, float)) or rate < 0:
            return [Problem(f'{name} must be a non-negative number, got {rate!r}.')]
    return []


def _readable_serializer(key: str) -> list[Problem]:
    """Refuse to write pickle the reader would throw away: sends would vanish."""
    # coerced like the reader coerces it: from the environment this is a string
    if _setting(key) != SerializerKind.PICKLE:
        return []
    try:
        # coerced like the reader coerces it: from the environment this is a string
        allowed = coerce_bool(conf.get('ALLOW_PICKLE'), f"{SETTINGS_NAME}['ALLOW_PICKLE']")
    except ImproperlyConfigured:
        # unreadable is E017's finding; this check cannot say anything about it
        return []
    if allowed:
        return []
    return [
        Problem(
            "is 'pickle' while ALLOW_PICKLE is False, so queued messages would be "
            'written and then refused on read. Set ALLOW_PICKLE to True, or use '
            "the 'json' serializer.",
        )
    ]


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


def _serviceable_webhook(key: str) -> list[Problem]:
    """Reject a webhook Telegram cannot reach, or one anybody could post to."""
    url = str(_setting(key) or '').strip()
    webhook_mode = str(conf.get('MODE') or '').strip().lower() == UpdateMode.WEBHOOK
    if not url:
        if webhook_mode:
            return [
                Problem(
                    "is required when MODE is 'webhook': Telegram has to be told where to "
                    "post updates. Switch MODE back to 'polling' if you cannot serve one.",
                )
            ]
        return []

    problems: list[Problem] = []
    if not str(conf.get('WEBHOOK_SECRET') or '').strip():
        problems.append(
            Problem(
                'is required when WEBHOOK_URL is set: the view compares it with the header '
                'Telegram echoes back, and without it anyone who finds the URL can feed '
                'your bot updates.',
                key='WEBHOOK_SECRET',
            )
        )
    if not url.startswith('https://'):
        problems.append(Problem(f'must be https, got {url!r} — Telegram refuses anything else.'))
    return problems


def _known_update_types(key: str) -> list[Problem]:
    """Require a real collection: a string would reach Telegram as single characters."""
    allowed = _setting(key)
    if not allowed:
        return []
    if isinstance(allowed, (str, bytes)) or not isinstance(allowed, Collection):
        return [Problem(f'must be a list or tuple of update types, got {type(allowed).__name__}.')]

    # deferred for the same reason as DefaultBotProperties above
    from aiogram.enums import UpdateType  # noqa: PLC0415 - as above

    known = {member.value for member in UpdateType}
    # anything unhashable would raise out of the membership test below, so the
    # type is settled first and reported by repr rather than by value
    invalid = [repr(name) for name in allowed if not isinstance(name, str)]
    invalid += [repr(name) for name in allowed if isinstance(name, str) and name not in known]
    if invalid:
        return [
            Problem(f'contains update types Telegram does not have: {sorted(invalid)}. Valid ones are {sorted(known)}.')
        ]
    return []


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


def _identity_matters() -> bool:
    """Whether the configured transport keys anything on the worker's name.

    True when it cannot be answered, which is the safe direction: an unresolvable ``BROKER``
    is `E047`'s finding, and suppressing an unrelated rule on the back of it would hide advice
    that is right for the default transport.

    A throwaway instance rather than :func:`~django_aiogram.broker.registry.get_broker`,
    which builds the one this process will *use* and caches it. A system check runs in
    `migrate`, `shell` and every other management command, and leaving a live broker behind
    in each of them is a side effect nobody asked this rule for. Constructing one costs
    nothing and connects to nothing — both shipped transports only set flags in `__init__`.
    """
    try:
        return bool(_configured_broker(verify_driver=True)().needs_identity)
    except (_broker_error(), ImproperlyConfigured):
        return True


def _setting(key: str) -> Any:  # noqa: ANN401 - a setting holds whatever the project put there
    """Resolve one setting the way its own table does, package-wide or transport-owned.

    `conf` folds in the package-wide table and answers `None` for anything outside it, so a rule
    about a transport's own setting read `None` where the transport reads its declared default --
    `E007` reported `REDIS_MESSAGES_KEY` as "must be a string, got NoneType" on the very
    configuration that supplies it. A transport setting is resolved the way the transport resolves
    it, through `option`, which is the one place that knows the default.
    """
    if key in DEFAULTS or key not in _broker_options():
        return conf.get(key)
    return _configured_broker().option(key)


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
    except (ImproperlyConfigured, TypeError, ValueError):
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


def _a_collection_of_strings(key: str) -> list[Problem]:
    """Require a real collection: a string would be read one character per item."""
    value = _setting(key)
    if not value:
        return []
    if isinstance(value, (str, bytes)) or not isinstance(value, Collection):
        return [Problem(f'must be a list or tuple, got {type(value).__name__}.')]
    # anything unhashable would raise out of a membership test later, so the
    # element type is settled here and reported through repr's eyes
    invalid = sorted(repr(name) for name in value if not isinstance(name, str))
    if invalid:
        return [Problem(f'contains names that are not strings: {", ".join(invalid)}.')]
    return []


def _kinds_this_version_records(key: str) -> list[Problem]:
    """Warn about a kind nothing writes: a typo here silently records nothing."""
    value = _setting(key)
    if not value or isinstance(value, (str, bytes)) or not isinstance(value, Collection):
        return []  # E032 owns the shape complaint
    known = known_kinds()
    unknown = sorted(repr(name) for name in value if isinstance(name, str) and name not in known)
    if not unknown:
        return []
    return [
        Problem(
            f'names kinds nothing records: {", ".join(unknown)}.',
            hint=f'Known kinds are: {", ".join(sorted(known))}.',
        )
    ]


def _the_log_is_on() -> bool:
    """Whether events are recorded, coerced the way the recorder coerces it."""
    try:
        return coerce_bool(conf['EVENT_LOG'], f"{SETTINGS_NAME}['EVENT_LOG']")
    except ImproperlyConfigured:
        # unreadable is E031's finding; assume off, so the rest stays quiet
        return False


def _a_configured_log_database(key: str) -> list[Problem]:
    """Resolve the alias here: the writer runs on a thread nobody is watching.

    An alias missing from DATABASES raises ConnectionDoesNotExist inside the
    writer thread, where the only trace is a log line in a container nobody
    reads and a queue that quietly fills and drops.
    """
    value = _setting(key)
    if not isinstance(value, str):
        return []  # E040 owns the type complaint
    alias = value.strip()
    if not alias:
        return []
    # deferred like the aiogram imports: a boot that records nothing must not
    # pay for the connection handler
    from django.db import connections  # noqa: PLC0415 - as above

    if alias in connections:
        return []
    return [
        Problem(
            f'names {alias!r}, which is not in DATABASES.',
            hint=f'Configured aliases are: {", ".join(sorted(connections))}.',
        )
    ]


def _somewhere_to_write_the_log(key: str) -> list[Problem]:
    """Warn, never error, when the log is on with no database behind it.

    A project may legitimately boot without one — this package's own suite does
    — so this must not be able to fail ``manage.py check``.

    The engine is what gets asked, not whether DATABASES is empty: Django fills
    an empty setting in with the dummy backend the first time anything touches
    connections, so by the time checks run the dict is never empty.
    """
    if not _the_log_is_on():
        return []
    # deferred for the same reason as the alias check above
    from django.db import DEFAULT_DB_ALIAS, connections  # noqa: PLC0415 - as above

    alias = str(conf.get('EVENT_LOG_DATABASE') or '').strip() or DEFAULT_DB_ALIAS
    if alias not in connections:
        return []  # E041 owns the missing alias
    engine = str(connections[alias].settings_dict.get('ENGINE') or '')
    if engine and engine != 'django.db.backends.dummy':
        return []
    return [
        Problem(
            f'is on while {alias!r} has no database engine, so every event is dropped.',
            hint=f'Configure a database, or leave {key} off in processes that have none.',
        )
    ]


def worker_name_problems() -> list[Problem]:
    """Return the worker-name problems, for a caller that knows it *is* the consumer.

    `I001` reports this as information because a system check cannot tell which process it
    is running in. `start_tgbot` can, and warns. One rule, two audiences.
    """
    return _a_worker_that_keeps_its_name('WORKER_NAME')


def _a_routed_log_database(key: str) -> list[Problem]:
    """Say when the log is pointed at its own alias with nothing routing it there.

    ``EVENT_LOG_DATABASE`` names where the rows belong; ``TelegramEventLogRouter`` is
    what puts them there. Set the first and forget the second and every existing check
    passes — E040 sees a string, E041 sees a configured alias with a real engine, W005
    sees a database — while a plain ``migrate`` does not create the table on it and the
    writer logs ``no such table`` once per batch for ever. ``migrate --database=<alias>``
    still would, which is why this is information: someone may be doing exactly that.

    I002 rather than a warning, because a project may route this app by hand: a router
    of its own that returns the same alias is a legitimate way to do it, and this rule
    cannot see inside one. The hint says so too, and `Settings.md` lists it under the
    information ids.

    Compared through ``import_string`` so both spellings count. ``DATABASE_ROUTERS``
    accepts dotted paths and instances alike, and a project mixing the two — a path
    for ours, an instance for its own — is exactly the case a string comparison gets
    wrong.
    """
    if not _the_log_is_on():
        return []
    alias = str(_setting(key) or '').strip()
    if not alias:
        return []  # nothing was pointed anywhere, so nothing needs routing
    from django_aiogram.eventlog.dbrouter import TelegramEventLogRouter  # noqa: PLC0415 - no django.db at import

    for entry in getattr(django_settings, 'DATABASE_ROUTERS', ()) or ():
        candidate = entry
        if isinstance(entry, str):
            try:
                candidate = import_string(entry)
            except ImportError:
                continue  # a router Django itself will complain about
        if candidate is TelegramEventLogRouter or isinstance(candidate, TelegramEventLogRouter):
            return []
    return [
        Problem(
            f'is {alias!r}, and this check cannot see a router that sends this app there.',
            hint=(
                "Add 'django_aiogram.eventlog.dbrouter.TelegramEventLogRouter' to DATABASE_ROUTERS, "
                f'or leave {key} unset so the log uses the default database. A router of your own '
                'returning that alias is equally correct and is what this cannot read, which is why '
                'this is information rather than a warning.'
            ),
        )
    ]


#: paths a 3.x project wrote into its *own* settings, against what to write in 4.0. The router is
#: the only one a check can reach: nothing here can read a project's `urls.py`, so the webhook view
#: is documentation and **Upgrading** carries it
THREE_X_PATHS = {
    'django_redis_aiogram.dbrouter.TelegramEventLogRouter': ('django_aiogram.eventlog.dbrouter.TelegramEventLogRouter'),
}


def _a_router_this_release_still_has(_key: str) -> list[Problem]:
    """Report a `DATABASE_ROUTERS` entry that names 3.x, with the 4.0 path.

    The one moved path a check can reach. `DATABASE_ROUTERS` is Django's, and a project wrote our
    dotted path into it by hand, so the rename in 4.0 leaves a string nothing resolves -- and
    nothing here imports it, so nothing fails until the first query, which then fails with
    Django's own `ImportError` naming a module rather than the fix.

    Reported for **any** `django_redis_aiogram.` entry rather than only the router, and both halves
    of that matter: the known path gets its replacement named, and an unknown one still gets told
    that the distribution is gone, since guessing a replacement it never had would be worse than
    saying so.

    An error rather than a warning: this cannot work. The string names a package that a 4.0
    install does not have, and a router Django cannot import takes down the first query that needs
    routing -- there is no configuration in which this is deliberate.
    """
    problems = []
    for entry in getattr(django_settings, 'DATABASE_ROUTERS', ()) or ():
        if not isinstance(entry, str) or not entry.startswith('django_redis_aiogram.'):
            continue
        replacement = THREE_X_PATHS.get(entry)
        instead = (
            f'Write {replacement!r}.'
            if replacement
            else 'That distribution is gone in 4.0; the package is `django_aiogram` now.'
        )
        problems.append(
            Problem(
                f'names {entry!r}, which 4.0 renamed. {instead}',
                label='DATABASE_ROUTERS',
                hint=(
                    'The rename is mechanical for the router, and the webhook view in your '
                    "`urls.py` moved with it -- to 'django_aiogram.consumer.webhook."
                    "telegram_webhook'. Nothing here can read `urls.py`, so that one is only in "
                    'the Upgrading page.'
                ),
            )
        )
    return problems


def _a_log_that_is_pruned(key: str) -> list[Problem]:
    """Warn when nothing will ever delete a row, so the table only grows."""
    if not _the_log_is_on():
        return []
    try:
        days = int(_setting(key))
    except (TypeError, ValueError):
        return []  # E039 owns the type complaint
    if days > 0:
        return []
    return [
        Problem(
            'is 0 while the log is on, so nothing ever deletes a row.',
            hint='Set it and schedule `manage.py tgbot_prune_events`, or accept unbounded growth.',
        )
    ]


def _a_batch_the_buffer_can_hold(key: str) -> list[Problem]:
    """Warn when the batch can never fill, so the interval paces every write.

    The writer stops collecting at the buffer's size, which makes a larger batch
    not a bigger write but a partial one every flush interval.
    """
    try:
        batch = int(_setting(key))
        buffer = int(conf['EVENT_LOG_BUFFER_SIZE'])
    except (TypeError, ValueError):
        return []  # E036 and E037 own the type complaints
    if batch <= buffer:
        return []
    return [
        Problem(
            f'is {batch}, which the buffer caps at {buffer}.',
            hint=f"Raise {SETTINGS_NAME}['EVENT_LOG_BUFFER_SIZE'] above it, or lower this.",
        )
    ]


def _a_writer_that_does_not_block(key: str) -> list[Problem]:
    """Warn that synchronous recording puts a database round trip in the send.

    The whole design rests on recording never making a caller wait. This
    setting deliberately breaks that for tests, so the trade is stated rather
    than left to be discovered under load.

    Silent while the log is off, because `record()` returns before it ever
    reads this one: warning there would describe a cost nobody is paying.
    """
    if not _the_log_is_on():
        return []
    try:
        if not coerce_bool(_setting(key), f"{SETTINGS_NAME}['{key}']"):
            return []
    except ImproperlyConfigured:
        return []  # E042 owns the type complaint
    return [
        Problem(
            'is on, so every recorded event is written on the calling thread and a send waits for the database.',
            hint='Leave it off outside tests.',
        )
    ]


def _bot_is_enabled() -> bool:
    """Whether the bot is on, coerced the way startup and sending coerce it."""
    try:
        return coerce_bool(conf['ENABLED'], f"{SETTINGS_NAME}['ENABLED']")
    except ImproperlyConfigured:
        # unreadable is E001's finding; assume on, so the credential warnings show
        return True


def _redis_is_in_use() -> bool:
    """Whether anything this configuration selects actually connects to Redis.

    ``REDIS_URL`` was a hard requirement while Redis was the only transport, and `W002` asked
    every enabled bot for it. It is one setting among several now, so on a RabbitMQ or Kafka
    deployment with memory storage that warning is about a key nothing reads — and a warning
    about an unused setting is how an operator learns to stop reading warnings.

    Two consumers, and either is enough: a Redis broker, or the Redis FSM storage. ``SHIPPED``
    answers for the broker rather than the class, so this stays true where the driver is not
    installed and imports nothing to find out.

    A ``BROKER`` outside that table is somebody's own, and nothing here can know what it
    connects with. The answer is then no: a missing warning costs a moment's confusion at the
    first send, and a wrong one costs the credibility of every other warning in the list.

    ``FSM_STORAGE`` is compared as a member first, because ``str()`` on one does not give its
    value: ``StorageKind`` mixes in ``str``, and since 3.11 ``str(StorageKind.REDIS)`` is
    ``'StorageKind.REDIS'``. Normalising that reads ``'storagekind.redis'`` and matches nothing,
    so a project passing the enum this package publishes — which is what `API.md` documents it
    for — had its warning suppressed and then needed ``REDIS_URL`` at runtime anyway.
    """
    from django_aiogram.broker.registry import SHIPPED  # noqa: PLC0415 - only when the checks run

    broker = str(conf.get('BROKER') or '').strip()
    driver, _extra = SHIPPED.get(broker, ('', ''))
    return driver == 'redis' or _redis_fsm_storage()


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
        replacement = THREE_X_DELIVERIES[path]
        instead = f'Write {replacement!r}.' if replacement else 'That consumer was removed in 3.0.'
        return [Problem(f'is {path!r}, which 4.0 replaced with a dotted path. {instead}', hint=_DELIVERY_HINT)]
    if not _reads_as_a_dotted_path(path):
        return [Problem(f'is {path!r}, which is not a dotted path.', hint=_DELIVERY_HINT)]
    return []


def _redis_fsm_storage() -> bool:
    """Whether ``FSM_STORAGE`` names the Redis store.

    Compared as a member first, because ``str()`` on one does not give its value:
    ``StorageKind`` mixes in ``str``, and since 3.11 ``str(StorageKind.REDIS)`` is
    ``'StorageKind.REDIS'``. Normalising that reads ``'storagekind.redis'`` and matches
    nothing, so a project passing the enum this package publishes — which `API.md` documents
    it for — had its warning suppressed and then needed ``REDIS_URL`` at runtime anyway.
    """
    storage = conf.get('FSM_STORAGE')
    if isinstance(storage, StorageKind):
        return storage is StorageKind.REDIS
    return str(storage or '').strip().lower() == StorageKind.REDIS.value


def _filled_in_when_enabled(key: str, *, hint: str, only_if: Callable[[], bool] | None = None) -> list[Problem]:
    """Warn, never error, when an enabled bot has nothing to connect with.

    A project may legitimately boot without credentials — during migrations or
    image builds — so this must not be able to fail ``manage.py check``.

    ``only_if`` narrows the rule to configurations that need the setting at all. `W001` needs
    none: every deployment talks to Telegram. `W002` does, because two of the four transports
    never open a Redis connection — the list and Streams are both Redis, RabbitMQ and Kafka
    are neither.

    **The gate on ``ENABLED`` leaves one hole, measured and kept.** ``Dispatcher`` is built with
    :func:`build_storage` the first time anything touches it, and neither ``start_polling`` nor
    ``feed_update`` reads ``ENABLED`` first — both are public, and `API.md` says 1.x code drives
    them by hand. So a disabled process driving the dispatcher itself, with the Redis store named
    and no URL, raises ``ImproperlyConfigured`` after a clean ``manage.py check``.

    Warning regardless of the flag was tried and is worse: ``FSM_STORAGE`` defaults to ``redis``,
    so — measured — settings of ``{'ENABLED': False}`` alone produce `W002`. That is every image
    build, migration container and CI job, warned about a URL they exist not to need, which is
    how an operator learns to stop reading the list. The hole needs a caller who disabled the bot
    and then drove its dispatcher anyway; the noise needs nothing.
    """
    if only_if is not None and not only_if():
        return []
    if not _bot_is_enabled() or str(_setting(key) or '').strip():
        return []
    return [Problem('is empty while the bot is enabled.', hint=hint)]


CHECKS: tuple[Check, ...] = (
    Check('E001', 'ENABLED', _a_readable_boolean),
    Check('E002', 'AUTODISCOVER', _a_readable_boolean),
    Check('E003', 'RAISE_EXCEPTION', _a_readable_boolean),
    Check('E017', 'ALLOW_PICKLE', _a_readable_boolean),
    Check('E004', 'TOKEN', _a_string),
    Check('E005', 'REDIS_URL', _a_string),
    Check('E006', 'MODULE_NAME', _a_string),
    Check('E007', 'REDIS_MESSAGES_KEY', _a_string),
    Check('E021', 'WORKER_NAME', _a_string),
    Check('E009', 'DELIVERY', _a_usable_delivery),
    Check('E010', 'SERIALIZER', partial(_a_string, allowed=SERIALIZER_CHOICES)),
    Check('E011', 'FSM_STORAGE', _a_string),
    Check('E012', 'MAX_RETRIES', partial(_an_integer, minimum=1)),
    Check('E014', 'BLPOP_TIMEOUT', partial(_an_integer, minimum=1)),
    # 2, not 1: the consumer's blocking pop is capped one second inside this, and at 1
    # the subtraction clamps back to 1 — so the pop's own timeout equals the read
    # deadline and the deadline always wins. Every idle second then costs a
    # `TimeoutError`, a traceback and a reconnect, on a healthy server, for ever
    Check('E030', 'REDIS_TIMEOUT', partial(_an_integer, minimum=2)),
    Check('W004', 'BLPOP_TIMEOUT', _a_pop_inside_the_deadline),
    Check('E023', 'HEARTBEAT_INTERVAL', partial(_an_integer, minimum=1)),
    Check('E024', 'HEALTHCHECK_MAX_QUEUE', partial(_an_integer, minimum=0)),
    Check('E028', 'MODE', partial(_a_string, allowed=MODE_CHOICES)),
    Check('E025', 'WEBHOOK_URL', _a_string),
    Check('E026', 'WEBHOOK_SECRET', _a_string),
    Check('E027', 'WEBHOOK_URL', _serviceable_webhook),
    Check('E029', 'WEBHOOK_ALLOWED_UPDATES', _known_update_types),
    Check('E015', 'DEFAULT_KWARGS', _a_callable),
    Check('E016', 'DEFAULT_BOT_PROPERTIES', _a_mapping),
    Check('E018', 'DEFAULT_BOT_PROPERTIES', _known_bot_properties),
    Check('E020', 'RATE_LIMIT', _sane_rate_limits),
    Check('E022', 'SERIALIZER', _readable_serializer),
    Check('E019', 'FSM_STORAGE', _importable_storage),
    Check('E031', 'EVENT_LOG', _a_readable_boolean),
    Check('E032', 'EVENT_LOG_KINDS', _a_collection_of_strings),
    Check('E033', 'EVENT_LOG_PAYLOAD', partial(_a_string, allowed=PAYLOAD_CHOICES)),
    Check('E034', 'EVENT_LOG_MAX_PAYLOAD_BYTES', partial(_an_integer, minimum=0)),
    Check('E035', 'EVENT_LOG_REDACT_KEYS', _a_collection_of_strings),
    Check('E036', 'EVENT_LOG_BUFFER_SIZE', partial(_an_integer, minimum=1)),
    Check('E037', 'EVENT_LOG_BATCH_SIZE', partial(_an_integer, minimum=1)),
    Check('E038', 'EVENT_LOG_FLUSH_INTERVAL', partial(_an_integer, minimum=1)),
    Check('E039', 'EVENT_LOG_RETENTION_DAYS', partial(_an_integer, minimum=0)),
    Check('E040', 'EVENT_LOG_DATABASE', _a_string),
    Check('E041', 'EVENT_LOG_DATABASE', _a_configured_log_database),
    # I, not W: this cannot see inside a router, so a project whose own router returns
    # the alias is correctly configured and would still be reported. Information the
    # reader can act on, not a condition worth failing `check --fail-level WARNING`
    Check('I002', 'EVENT_LOG_DATABASE', _a_routed_log_database),
    Check('E047', 'BROKER', _a_usable_broker),
    Check('E042', 'EVENT_LOG_SYNC', _a_readable_boolean),
    Check('E043', 'REDIS_URL', _a_url_pickle_can_survive),
    Check('E044', 'DRAIN_TIMEOUT', partial(_a_number, minimum=0)),
    Check('E045', 'MAX_IN_FLIGHT', partial(_an_integer, minimum=0)),
    Check('E046', 'REQUIRE_CRASH_SAFE', _a_readable_boolean),
    Check('W005', 'EVENT_LOG', _somewhere_to_write_the_log),
    Check('W006', 'EVENT_LOG_RETENTION_DAYS', _a_log_that_is_pruned),
    Check('W007', 'EVENT_LOG_BATCH_SIZE', _a_batch_the_buffer_can_hold),
    Check('W008', 'EVENT_LOG_KINDS', _kinds_this_version_records),
    Check('W009', 'EVENT_LOG_SYNC', _a_writer_that_does_not_block),
    # I, not W: a check cannot tell a consumer from the web tier, and every container
    # without `hostname:` matches — so as a warning it failed `check --fail-level WARNING`
    # in processes that own no in-flight list. `start_tgbot` warns for itself, where being
    # the consumer is known
    Check('I001', 'WORKER_NAME', _a_worker_that_keeps_its_name),
    Check('W003', '', _known_keys),
    Check('E048', '', _a_router_this_release_still_has),
    Check(
        'W001',
        'TOKEN',
        partial(
            _filled_in_when_enabled,
            hint='Set it, or set ENABLED to False in processes that never send to Telegram.',
        ),
    ),
    Check(
        'W002',
        'REDIS_URL',
        partial(
            _filled_in_when_enabled,
            hint=(
                'Set it: BROKER or FSM_STORAGE names Redis, so something here needs the URL. '
                'Or set ENABLED to False in processes that never touch either.'
            ),
            only_if=_redis_is_in_use,
        ),
    ),
)


def check_settings(**kwargs: Any) -> list[CheckMessage]:
    """Run every registered check and return everything it reported."""
    return [message for check in CHECKS for message in check.run()]
