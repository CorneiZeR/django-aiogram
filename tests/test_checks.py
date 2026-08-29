"""Before 2.0 these checks silently passed on every input: the validation flag
was only ever set inside an `isinstance` branch that a wrong type never entered.
"""

import builtins
import importlib
import pathlib
import re

import pytest
from django.core.checks import WARNING, Error
from django.core.checks import Warning as CheckWarning
from django.core.checks.registry import registry
from django.core.exceptions import ImproperlyConfigured
from django.core.management import call_command
from django.core.management.base import SystemCheckError
from django.test import override_settings
from django.utils.module_loading import import_string

from django_aiogram.broker.redis_list import RedisListBroker
from django_aiogram.broker.registry import SHIPPED
from django_aiogram.config.checks import CHECKS, check_settings, worker_name_problems
from django_aiogram.config.defaults import DEFAULTS
from django_aiogram.config.enums import StorageKind
from django_aiogram.config.settings import take_ceiling
from django_aiogram.eventlog.dbrouter import TelegramEventLogRouter
from django_aiogram.eventlog.events import worker_identity
from django_aiogram.redis import read_timeout


@pytest.fixture(autouse=True)
def _stable_hostname(monkeypatch):
    """Pin the hostname, because I001 reads it.

    Without this the whole module's results depend on where it runs: a container
    started without `hostname:` gets a twelve-character hex name, which is exactly
    what I001 exists to report, so `test_the_defaults_report_nothing` would pass
    on a laptop and fail in Docker. The tests that are *about* I001 patch it back.
    """
    monkeypatch.setenv('HOSTNAME', 'bot-worker-1')


def ids(messages):
    return {message.id for message in messages}


def errors(messages):
    return [message for message in messages if isinstance(message, Error)]


@override_settings(TELEGRAM_BOT={'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost'})
def test_valid_settings_produce_no_errors():
    assert errors(check_settings()) == []


@override_settings(TELEGRAM_BOT={'MAX_RETRIES': 'ten', 'TOKEN': '42:x', 'REDIS_URL': 'r://x'})
def test_wrong_integer_type_is_caught():
    assert 'django_aiogram.E012' in ids(errors(check_settings()))


@override_settings(TELEGRAM_BOT={'MAX_RETRIES': True, 'TOKEN': '42:x', 'REDIS_URL': 'r://x'})
def test_bool_is_not_accepted_as_integer():
    assert 'django_aiogram.E012' in ids(errors(check_settings()))


@override_settings(TELEGRAM_BOT={'MAX_RETRIES': 0, 'TOKEN': '42:x', 'REDIS_URL': 'r://x'})
def test_integer_below_minimum_is_caught():
    assert 'django_aiogram.E012' in ids(errors(check_settings()))


@override_settings(
    TELEGRAM_BOT={
        'ENABLED': 'true',
        'AUTODISCOVER': '1',
        'ALLOW_PICKLE': 'no',
        'EVENT_LOG': 'on',
        'EVENT_LOG_SYNC': 'off',
        'REQUIRE_CRASH_SAFE': 0,
        'TOKEN': '42:x',
        'REDIS_URL': 'r://x',
    }
)
def test_a_configuration_that_boots_and_sends_does_not_fail_the_checks():
    """Every boolean here comes from the environment, which has only strings.

    This exact settings dict is documented, loads, and sends — and used to fail
    `manage.py check` on five separate ids, because the rule demanded a real `bool`
    while every one of these is coerced at the point of use. It was inverted twice
    over: the values `coerce_bool` genuinely refuses raise `ImproperlyConfigured`
    out of `apps.ready()` before a check ever runs, so the errors could not fire on
    the case they were written for.
    """
    reported = {str(message.id) for message in check_settings()}
    boolean_ids = {f'django_aiogram.{code}' for code in ('E001', 'E002', 'E017', 'E031', 'E042', 'E046')}

    assert reported & boolean_ids == set(), f'a working configuration was refused: {sorted(reported & boolean_ids)}'


@override_settings(TELEGRAM_BOT={'ENABLED': 'maybe', 'TOKEN': '42:x', 'REDIS_URL': 'r://x'})
def test_a_boolean_nothing_can_read_is_still_caught():
    """The other direction, which is what keeps the rule a rule.

    `'maybe'` is what `coerce_bool` refuses, so this is the value that would have
    taken `apps.ready()` down — and the check has to name it rather than staying
    quiet because it stopped demanding a `bool`.
    """
    reported = errors(check_settings())

    assert 'django_aiogram.E001' in ids(reported)
    # the message is the one the runtime would have raised, not a paraphrase
    assert any("must be one of ['0', '1', 'false'" in str(message) for message in reported), reported


@override_settings(TELEGRAM_BOT={'RAISE_EXCEPTION': 'false', 'TOKEN': '42:x', 'REDIS_URL': 'r://x'})
def test_raise_exception_accepts_what_the_environment_can_express():
    """It was the last setting demanding a real bool, and only because of a defect.

    `client.py` read it with a bare `if`, so `'false'` — truthy, and what
    `DJANGO_AIOGRAM_RAISE_EXCEPTION=false` arrives as — re-raised the exception the
    project had asked to have swallowed. The check was strict to say so at boot rather
    than at the first failed send. The read goes through `coerce_bool` now, so the
    documented spelling is a working value here too.
    """
    assert 'django_aiogram.E003' not in ids(errors(check_settings()))


@override_settings(TELEGRAM_BOT={'TOKEN': 42, 'REDIS_URL': 'r://x'})
def test_wrong_string_type_is_caught():
    assert 'django_aiogram.E004' in ids(errors(check_settings()))


@override_settings(TELEGRAM_BOT={'DELIVERY': 'carrier-pigeon', 'TOKEN': '42:x', 'REDIS_URL': 'r://x'})
def test_unknown_delivery_is_rejected():
    assert 'django_aiogram.E009' in ids(errors(check_settings()))


@override_settings(TELEGRAM_BOT={'SERIALIZER': 'yaml', 'TOKEN': '42:x', 'REDIS_URL': 'r://x'})
def test_unknown_serializer_is_rejected():
    assert 'django_aiogram.E010' in ids(errors(check_settings()))


@override_settings(TELEGRAM_BOT={'DEFAULT_KWARGS': {}, 'TOKEN': '42:x', 'REDIS_URL': 'r://x'})
def test_non_callable_default_kwargs_is_caught():
    assert 'django_aiogram.E015' in ids(errors(check_settings()))


@override_settings(TELEGRAM_BOT={'DEFAULT_BOT_PROPERTIES': 'HTML', 'TOKEN': '42:x', 'REDIS_URL': 'r://x'})
def test_non_mapping_bot_properties_is_caught():
    assert 'django_aiogram.E016' in ids(errors(check_settings()))


@override_settings(TELEGRAM_BOT={'TOEKN': 'typo', 'TOKEN': '42:x', 'REDIS_URL': 'r://x'})
def test_typo_in_a_key_is_reported_as_warning():
    messages = check_settings()
    assert errors(messages) == []
    assert 'django_aiogram.W003' in ids(messages)


@override_settings(TELEGRAM_BOT={})
def test_missing_credentials_warn_but_do_not_fail():
    messages = check_settings()
    assert errors(messages) == []
    assert {'django_aiogram.W001', 'django_aiogram.W002'} <= ids(messages)


@override_settings(TELEGRAM_BOT={'ENABLED': False})
def test_disabled_bot_does_not_warn_about_credentials():
    messages = check_settings()
    assert isinstance(messages, list)
    assert not [m for m in messages if isinstance(m, CheckWarning) and m.id.endswith('W001')]


SETTINGS_PAGE = pathlib.Path(__file__).resolve().parent.parent / 'docs' / 'wiki' / 'Settings.md'
# the table separates a range with an en dash
DOCUMENTED = re.compile('`([EWI]\\d{3})`(?:\\s*[\u2013-]\\s*`([EWI]\\d{3})`)?')

# Every id the checks can emit. Three settings dicts are needed: a wrong type
# stops a check before it can reach its value-level complaint, and an alias that
# is not in DATABASES stops the one asking whether that alias has an engine.
# E008 and E013 guarded the keyspace settings 3.0 removed. Their ids are gone
# rather than reused: a project silencing one must not start silencing a new rule
RETIRED_IDS = {'E008', 'E013'}
# I001 is left out of the emitted set on purpose: it fires only when this machine's
# hostname looks Docker-generated, so whether the fixtures below produce it differs
# between a laptop and CI. It has its own tests, and
# `test_every_check_id_is_documented` reads the registry rather than this set, so it
# is still held to the documentation
HOSTNAME_DEPENDENT_IDS = {'I001'}
EXPECTED_IDS = (
    ({f'E{code:03d}' for code in range(1, 47)} - RETIRED_IDS)
    # W011 and W010 became I002 and I001: a check cannot tell which process it runs in, so
    # neither condition is one to fail `check --fail-level WARNING` over
    | {f'W{code:03d}' for code in range(1, 10)}
    | ({'I001', 'I002'} - HOSTNAME_DEPENDENT_IDS)
)

WRONG_TYPES = {
    # non-coercible on purpose: 'yes' and 'no' are documented, working values, and
    # E001/E002 asking for a real bool was the defect. `[]` and 4.2 take the other
    # refusal path in `coerce_bool` — wrong type rather than unrecognized word
    'ENABLED': 'maybe',
    'AUTODISCOVER': [],
    'RAISE_EXCEPTION': 'maybe',
    'ALLOW_PICKLE': 'maybe',
    'TOKEN': 42,
    'REDIS_URL': 42,
    'MODULE_NAME': 42,
    'REDIS_MESSAGES_KEY': 42,
    'WORKER_NAME': 42,
    'DELIVERY': 42,
    'SERIALIZER': 42,
    'FSM_STORAGE': 42,
    'MAX_RETRIES': 'ten',
    'BLPOP_TIMEOUT': 'five',
    'REDIS_TIMEOUT': 'five',
    'HEARTBEAT_INTERVAL': 'ten',
    'HEALTHCHECK_MAX_QUEUE': 'lots',
    'WEBHOOK_URL': 42,
    'WEBHOOK_SECRET': 42,
    'MODE': 42,
    'DEFAULT_KWARGS': 42,
    'DEFAULT_BOT_PROPERTIES': 42,
    'RATE_LIMIT': 42,
    'EVENT_LOG': 4.2,
    'EVENT_LOG_KINDS': 'outbound.sent',
    'EVENT_LOG_PAYLOAD': 42,
    'EVENT_LOG_MAX_PAYLOAD_BYTES': 'lots',
    'EVENT_LOG_REDACT_KEYS': 'token',
    'EVENT_LOG_BUFFER_SIZE': 'many',
    'EVENT_LOG_BATCH_SIZE': 'some',
    'EVENT_LOG_FLUSH_INTERVAL': 'often',
    'EVENT_LOG_RETENTION_DAYS': 'thirty',
    'EVENT_LOG_DATABASE': 42,
    'EVENT_LOG_SYNC': 'maybe',
    'DRAIN_TIMEOUT': 'soon',
    'MAX_IN_FLIGHT': 'two',
    'REQUIRE_CRASH_SAFE': 'maybe',
    'NOT_A_SETTING': 1,
}

WRONG_VALUES = {
    'TOKEN': '',
    'REDIS_URL': '',
    'DELIVERY': 'carrier-pigeon',
    'SERIALIZER': 'pickle',
    'ALLOW_PICKLE': False,
    'FSM_STORAGE': 'no.such.Storage',
    'DEFAULT_BOT_PROPERTIES': {'not_a_property': 1},
    'RATE_LIMIT': {'overall_per_second': 'fast'},
    # a URL with no secret, and not https either
    'WEBHOOK_URL': 'http://example.test/tg/',
    'WEBHOOK_SECRET': '',
    'MODE': 'sideways',
    # a pop asked to wait longer than a read may take, which the consumer caps
    'BLPOP_TIMEOUT': 30,
    'REDIS_TIMEOUT': 5,
    'WEBHOOK_ALLOWED_UPDATES': 'message',
    # the log on with nowhere to write it, nothing to prune it, a batch the
    # buffer can never fill, an alias that is not in DATABASES, and a typo
    'EVENT_LOG': True,
    'EVENT_LOG_RETENTION_DAYS': 0,
    'EVENT_LOG_BATCH_SIZE': 5000,
    'EVENT_LOG_DATABASE': 'nope',
    'EVENT_LOG_KINDS': ('outbound.snet',),
    # readable numbers, but not ones a bound can be made of: a drain that
    # finishes before it starts and a limit of minus one message
    'DRAIN_TIMEOUT': -1,
    'MAX_IN_FLIGHT': -1,
}

# the log on and pointed at a real alias, which under tests.settings is the
# dummy backend Django fills an empty DATABASES in with
LOG_WITHOUT_A_DATABASE = {'EVENT_LOG': True, 'EVENT_LOG_SYNC': True}

# E043 is about a pair, not a value: neither half is wrong alone, so it needs
# its own settings rather than a slot in the maps above
PICKLE_ON_A_DECODING_URL = {
    'ALLOW_PICKLE': True,
    'REDIS_URL': 'redis://localhost:6379/0?decode_responses=1',
}


def documented_ids():
    """The table lists ranges, so a documented E004-E011 covers each id between."""
    found = set()
    for first, last in DOCUMENTED.findall(SETTINGS_PAGE.read_text(encoding='utf-8')):
        if not last:
            found.add(first)
            continue
        found.update(f'{first[0]}{number:03d}' for number in range(int(first[1:]), int(last[1:]) + 1))
    return found


def emitted_ids():
    """What the checks actually emit — running them, not reading their source.

    Scraping the registrations was worse: reformatting one dropped it from the
    scan, and the documentation check below stayed green without it.
    """
    found = set()
    for settings in (WRONG_TYPES, WRONG_VALUES, LOG_WITHOUT_A_DATABASE, PICKLE_ON_A_DECODING_URL):
        with override_settings(TELEGRAM_BOT=settings):
            found |= {str(message.id).removeprefix('django_aiogram.') for message in check_settings()}
    return found


def test_the_expected_ids_are_the_ones_the_checks_emit():
    """A new check has to be added here, and therefore to the docs, to pass."""
    emitted = emitted_ids()
    assert emitted - EXPECTED_IDS == set(), f'undeclared check ids: {sorted(emitted - EXPECTED_IDS)}'
    assert EXPECTED_IDS - emitted == set(), f'ids nothing emitted: {sorted(EXPECTED_IDS - emitted)}'


def test_every_check_id_is_documented():
    """An operator meeting E021 has to be able to look it up.

    Read from the registry rather than from `EXPECTED_IDS`, which is the set of ids
    the fixtures below *emit*. Some rows cannot be emitted by a `TELEGRAM_BOT` dict at
    all — I001 needs an ephemeral hostname, I002 needs `DATABASE_ROUTERS` — so an id
    added without touching that set was documented only by whoever remembered to.
    """
    missing = sorted({check.code for check in CHECKS} - documented_ids())
    assert not missing, f'check ids missing from docs/wiki/Settings.md: {missing}'


def test_the_documented_floor_is_the_floor_the_check_enforces():
    """`E030` refuses a number, and the table has to name the same number.

    The row said `below 1` while the registry had moved to 2, which is worse than
    saying nothing: an operator reading it concludes `REDIS_TIMEOUT = 1` passes.
    Read from the registry so the two cannot drift again — and pin the floor from
    both sides, because a table naming a number no check enforces is the same
    defect facing the other way.
    """
    floor = next(check for check in CHECKS if check.code == 'E030').validate.keywords['minimum']
    row = next(line for line in SETTINGS_PAGE.read_text(encoding='utf-8').splitlines() if line.startswith('| `E030`'))
    assert f'below {floor}' in row, f'the table does not name the floor the check enforces ({floor}): {row}'

    for value, refused in ((floor - 1, True), (floor, False)):
        with override_settings(TELEGRAM_BOT={'TOKEN': '42:x', 'REDIS_TIMEOUT': value}):
            reported = 'django_aiogram.E030' in ids(errors(check_settings()))
        assert reported is refused, f'REDIS_TIMEOUT={value} is {"accepted" if refused else "refused"}'


def test_no_test_asserts_on_a_check_id_the_registry_no_longer_has():
    """A half-done rename leaves assertions that are true of every configuration.

    W010 became I001 and three assertions here kept the old spelling. Once an id is gone,
    asking that the prefixed form of it be absent from `ids(...)` holds whatever the
    settings are — so the fixed hostname, the padded name and the named worker were each
    pinned by nothing. The grep that found the documentation found those lines too, which
    is why the guard has to be executable rather than a habit.

    Spelled without the package prefix on purpose: this scan reads every file under
    `tests/`, its own docstring included, and an example written in full would be the
    only thing it ever found.

    Retired ids are exempt by name, not by pattern: their absence is the point, and
    `RETIRED_IDS` is where that decision is written down.
    """
    live = {check.code for check in CHECKS} | RETIRED_IDS
    stale = {}
    for path in sorted(pathlib.Path(__file__).resolve().parent.rglob('*.py')):
        named = set(re.findall(r'django_aiogram\.([EWI]\d{3})', path.read_text(encoding='utf-8')))
        if named - live:
            stale[path.name] = sorted(named - live)

    assert not stale, f'assertions naming ids the registry does not have: {stale}'


def test_the_page_explains_every_severity_the_registry_uses():
    """The table gained `I001` and `I002` while its legend still said errors and warnings.

    Documenting an id is not documenting what it does to a build, and the level is the
    part an operator acts on: `--fail-level WARNING` is what a CI step runs, and whether
    a row can fail it decides whether they can deploy. Read from the registry, so a
    fourth prefix cannot arrive unexplained the way the third one did.
    """
    page = SETTINGS_PAGE.read_text(encoding='utf-8')
    unexplained = sorted(
        f'django_aiogram.{check.code[0]}XXX' for check in CHECKS if f'django_aiogram.{check.code[0]}XXX' not in page
    )

    assert not unexplained, f'the Check ids legend does not explain: {unexplained}'


def test_every_registry_row_reports_under_its_own_id():
    """Two rows sharing an id would make the docs entry ambiguous."""
    codes = [check.code for check in CHECKS]
    assert sorted(codes) == sorted(set(codes))


STREAMS = {
    'TOKEN': '42:x',
    'REDIS_URL': 'redis://localhost',
    'BROKER': 'django_aiogram.broker.redis_streams.RedisStreamsBroker',
    'REDIS_STREAM_KEY': 'tg',
}


def test_a_key_belonging_to_another_transport_is_reported_as_stranded():
    """The symptom #23 is about: a project that moved to Streams keeps its list key.

    `REDIS_MESSAGES_KEY` was in the package-wide table, so it stayed known whichever transport was
    configured — and a line nothing reads is exactly what `W003` exists to name. It is the
    transport's now, so the rule answers.
    """
    with override_settings(TELEGRAM_BOT={**STREAMS, 'REDIS_MESSAGES_KEY': 'TELEGRAM_BOT_MESSAGE'}):
        found = [message for message in check_settings() if message.id == 'django_aiogram.W003']

    assert len(found) == 1, f'W003 reported {[message.msg for message in found]}'
    assert 'REDIS_MESSAGES_KEY' in found[0].msg, found[0].msg


def test_a_transport_setting_is_validated_only_where_that_transport_is_configured():
    """The other half: `E007` validated a list key as a string on a deployment that has no list.

    Both directions, because either alone is a rule that could be doing nothing: under Streams the
    value is not the package's business, and under the list the same value is still reported.
    """
    with override_settings(TELEGRAM_BOT={**STREAMS, 'REDIS_MESSAGES_KEY': 42}):
        under_streams = ids(check_settings())

    listed = {'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost', 'REDIS_MESSAGES_KEY': 42}
    with override_settings(TELEGRAM_BOT=listed):
        under_the_list = ids(check_settings())

    assert 'django_aiogram.E007' not in under_streams, 'a list key was validated on a stream'
    assert 'django_aiogram.E007' in under_the_list, 'and the rule no longer reports where it applies'


def test_a_transports_own_settings_are_known_without_its_driver(monkeypatch):
    """What a transport declares is class state, so nothing about it needs the extra installed.

    Resolved with the driver verified, this collapsed to "the package-wide table" on a machine that
    had not run the `pip install` yet — so `W003` called that transport's own required settings
    unknown keys and invited an operator to delete them, and every rule guarding one of those
    settings quietly stopped running. Reproduced with `find_spec` patched, because the suite has
    every driver installed and a case that only answers without one is a case nobody runs.
    """
    monkeypatch.setattr('importlib.util.find_spec', lambda name, *args: None if name == 'pika' else True)
    settings = {
        'TOKEN': '42:x',
        'REDIS_URL': 'redis://localhost',
        'BROKER': 'django_aiogram.broker.rabbitmq.RabbitMQBroker',
        'RABBITMQ_URL': 'amqp://localhost',
        'RABBITMQ_QUEUE': 'tg',
        'RABBITMQ_PREFETCH': 0,
    }
    with override_settings(TELEGRAM_BOT=settings):
        found = [message for message in check_settings() if message.id == 'django_aiogram.W003']

    assert found == [], f'a transport was told to delete its own settings: {[m.msg for m in found]}'


def test_the_settings_the_package_reads_are_known_whichever_transport_runs():
    """`REDIS_URL` and `REDIS_TIMEOUT` are in both tables and mean it.

    The FSM storage builds a Redis client under every transport, so a Kafka deployment with
    `FSM_STORAGE = 'redis'` sets both — and neither may be reported as a key nothing reads. This is
    what stops the split from being "every REDIS_ name belongs to the Redis transports".
    """
    settings = {
        'TOKEN': '42:x',
        'BROKER': 'django_aiogram.broker.kafka.KafkaBroker',
        'KAFKA_BOOTSTRAP': 'localhost:9092',
        'KAFKA_TOPIC': 'tg',
        'FSM_STORAGE': 'redis',
        'REDIS_URL': 'redis://localhost',
        'REDIS_TIMEOUT': 10,
        'BLPOP_TIMEOUT': 5,
    }
    with override_settings(TELEGRAM_BOT=settings):
        found = [message for message in check_settings() if message.id == 'django_aiogram.W003']

    assert found == [], f'a package-wide setting was called unknown: {[m.msg for m in found]}'


def test_every_registry_row_guards_a_real_setting():
    """A typo in the key would validate a setting nothing ever reads.

    Two tables answer "a real setting" since #23, and a row may guard either kind: the package-wide
    defaults, or an option some transport declares. Both, and not their union loosely — a row whose
    key is in neither is a typo, and one whose key belongs to a transport runs only where that
    transport is configured, which is what `Check.run` decides from exactly this distinction.
    """
    # the unknown-keys row is about the settings dict as a whole, so it has no key
    guarded = {check.key for check in CHECKS if check.key}
    declared = {option for path in SHIPPED for option in import_string(path).OPTIONS}

    assert sorted(guarded - set(DEFAULTS) - declared) == []


@override_settings(TELEGRAM_BOT={'TOKEN': '42:x', 'REDIS_URL': 'redis://x', 'WORKER_NAME': 7})
def test_a_non_string_worker_name_is_reported():
    """It names the in-flight list, so a wrong type breaks reclaim at startup."""
    assert 'django_aiogram.E021' in ids(check_settings())


@override_settings(TELEGRAM_BOT={'TOKEN': '42:x', 'REDIS_URL': 'redis://x', 42: 'numeric'})
def test_a_non_string_settings_key_is_reported_not_raised():
    """`", ".join` over mixed key types used to raise out of manage.py check."""
    reported = {message.id for message in check_settings()}

    assert 'django_aiogram.W003' in reported


@override_settings(TELEGRAM_BOT={'ENABLED': 'false', 'TOKEN': '', 'REDIS_URL': ''})
def test_a_textually_disabled_bot_does_not_warn_about_credentials():
    """'false' from the environment disables startup and sending, so the
    credential warnings have to agree rather than nag a disabled process."""
    reported = {message.id for message in check_settings()}

    assert 'django_aiogram.W001' not in reported
    assert 'django_aiogram.W002' not in reported


@override_settings(TELEGRAM_BOT={'ENABLED': 'maybe', 'TOKEN': '', 'REDIS_URL': ''})
def test_an_unreadable_enabled_still_warns_and_reports_its_own_problem():
    """E001 owns the type complaint; the warnings assume the bot is on."""
    reported = {message.id for message in check_settings()}

    assert 'django_aiogram.E001' in reported
    assert 'django_aiogram.W001' in reported


@override_settings(
    TELEGRAM_BOT={
        'BLPOP_TIMEOUT': 30,
        'HEARTBEAT_INTERVAL': 10,
        'REDIS_TIMEOUT': 60,
        'TOKEN': '1:x',
        'REDIS_URL': 'redis://x',
    }
)
def test_a_pop_capped_by_the_heartbeat_is_reported_and_names_it():
    """The configuration W004 used to be silent on.

    The consumer caps the pop at `min(BLPOP_TIMEOUT, HEARTBEAT_INTERVAL, REDIS_TIMEOUT
    - 1)`, so this waits ten seconds while the setting says thirty. Comparing against
    the read deadline alone — 60 here — said nothing, and the operator went on
    believing the thirty took.
    """
    reported = [message for message in check_settings() if str(message.id).endswith('W004')]

    assert reported, 'a pop capped at a third of its setting was not reported'
    assert 'caps at 10' in reported[0].msg, reported[0].msg
    assert 'HEARTBEAT_INTERVAL' in (reported[0].hint or ''), reported[0].hint


@override_settings(
    TELEGRAM_BOT={
        'BLPOP_TIMEOUT': 30,
        'HEARTBEAT_INTERVAL': 60,
        'REDIS_TIMEOUT': 10,
        'TOKEN': '1:x',
        'REDIS_URL': 'redis://x',
    }
)
def test_a_pop_capped_by_the_read_deadline_names_that_instead():
    """The other binding term, because a hint that always says the same thing is a
    hint that sends half its readers to the wrong setting."""
    reported = [message for message in check_settings() if str(message.id).endswith('W004')]

    assert reported, 'a pop outside the read deadline was not reported'
    assert 'caps at 9' in reported[0].msg, reported[0].msg
    assert 'REDIS_TIMEOUT' in (reported[0].hint or ''), reported[0].hint


@override_settings(
    TELEGRAM_BOT={
        'BLPOP_TIMEOUT': 30,
        'HEARTBEAT_INTERVAL': 9,
        'REDIS_TIMEOUT': 10,
        'TOKEN': '1:x',
        'REDIS_URL': 'redis://x',
    }
)
def test_a_tie_between_the_two_limits_names_both():
    """A hint that names one of two tied limits sends the operator on a round trip.

    `HEARTBEAT_INTERVAL` at 9 and `REDIS_TIMEOUT` at 10 both produce a ceiling of 9.
    Raise the heartbeat alone and `REDIS_TIMEOUT - 1` still caps at 9, so the warning
    comes back unchanged — which is the same defect this whole rule was fixed for, one
    level down.
    """
    reported = [message for message in check_settings() if str(message.id).endswith('W004')]

    assert reported, 'a tied cap was not reported at all'
    assert 'caps at 9' in reported[0].msg, reported[0].msg
    hint = reported[0].hint or ''
    assert 'HEARTBEAT_INTERVAL' in hint, hint
    assert 'REDIS_TIMEOUT' in hint, hint
    assert 'both have to move' in hint, hint


@override_settings(
    TELEGRAM_BOT={
        'BLPOP_TIMEOUT': 10,
        'HEARTBEAT_INTERVAL': 10,
        'REDIS_TIMEOUT': 60,
        'TOKEN': '1:x',
        'REDIS_URL': 'redis://x',
    }
)
def test_a_pop_exactly_at_the_cap_is_not_reported():
    """Equal is not over. The consumer runs it at ten, which is what was asked for, so
    warning here would be the "fires on a working install" defect in miniature."""
    assert [message for message in check_settings() if str(message.id).endswith('W004')] == []


ROUTED_LOG = {'EVENT_LOG': True, 'EVENT_LOG_DATABASE': 'events', 'TOKEN': '1:x', 'REDIS_URL': 'redis://x'}


def routing_warnings():
    """The I002 messages the current settings produce."""
    return [message for message in check_settings() if str(message.id).endswith('I002')]


@override_settings(TELEGRAM_BOT=ROUTED_LOG, DATABASE_ROUTERS=[])
def test_a_log_database_nothing_routes_to_is_reported():
    """E040, E041 and W005 all pass on this, and `migrate` still never creates the table.

    The alias names where the rows belong; the router is what puts them there. Set the
    first and forget the second and every existing check is satisfied while the writer
    logs `no such table` once per batch for ever.
    """
    reported = routing_warnings()

    assert reported, 'a log pointed at an unrouted alias was not reported'
    # the level, not only the id: a router of your own returning this alias is equally
    # correct, so this cannot be allowed to fail `check --fail-level WARNING`. Reported as
    # information is the whole reason it stopped being W011
    assert reported[0].level < WARNING, 'a check that cannot see inside a router warned'
    assert 'cannot see a router that sends this app there' in reported[0].msg
    assert 'TelegramEventLogRouter' in (reported[0].hint or '')


@override_settings(
    TELEGRAM_BOT=ROUTED_LOG,
    DATABASE_ROUTERS=['django_aiogram.eventlog.dbrouter.TelegramEventLogRouter'],
)
def test_a_dotted_path_router_satisfies_it():
    """The spelling Django's own documentation uses."""
    assert routing_warnings() == []


@override_settings(TELEGRAM_BOT=ROUTED_LOG, DATABASE_ROUTERS=[TelegramEventLogRouter()])
def test_an_instance_router_satisfies_it_too():
    """`DATABASE_ROUTERS` takes instances as well, and a project mixing the two — a path
    for ours, an instance for its own — is exactly what a string comparison gets wrong."""
    assert routing_warnings() == []


@override_settings(TELEGRAM_BOT={'EVENT_LOG': True, 'TOKEN': '1:x', 'REDIS_URL': 'redis://x'}, DATABASE_ROUTERS=[])
def test_a_log_on_the_default_database_needs_no_router():
    """Nothing was pointed anywhere, so nothing needs routing — and warning here would
    be the "fires on a working install" defect this whole issue is about."""
    assert routing_warnings() == []


@override_settings(TELEGRAM_BOT={'TOKEN': '1:x', 'REDIS_URL': 'redis://localhost:6379/0'})
def test_the_defaults_report_nothing():
    """A warning on an untouched install teaches people to ignore the checks.

    2.1.0 shipped `REDIS_TIMEOUT` at 5 next to `BLPOP_TIMEOUT` at 5, so W004
    fired on every default configuration.
    """
    reported = [f'{message.id}: {message.msg}' for message in check_settings()]

    assert reported == [], reported


@override_settings(TELEGRAM_BOT={'EVENT_LOG': False, 'EVENT_LOG_SYNC': True})
def test_the_synchronous_writer_warning_is_silent_while_the_log_is_off():
    """`record()` returns before it ever reads EVENT_LOG_SYNC, so warning here
    would describe a cost nobody is paying — and a warning that is wrong is one
    people learn to scroll past."""
    emitted = {str(message.id).removeprefix('django_aiogram.') for message in check_settings()}

    assert 'W009' not in emitted, emitted


@override_settings(
    TELEGRAM_BOT={
        'TOKEN': '42:x',
        'REDIS_URL': 'redis://localhost:6379/0?decode_responses=true',
        'ALLOW_PICKLE': True,
        'SERIALIZER': 'pickle',
    }
)
def test_a_decoding_url_with_pickle_is_refused():
    """The one pairing nothing can recover from at runtime.

    redis-py decodes inside its own parser, so a pickled payload raises after the
    server has already moved the message to the in-flight list, and every later
    reclaim trips over the same message for ever.
    """
    assert 'django_aiogram.E043' in ids(check_settings())


@override_settings(
    TELEGRAM_BOT={
        'TOKEN': '42:x',
        'REDIS_URL': 'redis://localhost:6379/0?decode_responses=true',
        'ALLOW_PICKLE': False,
    }
)
def test_a_decoding_url_without_pickle_is_fine():
    """Decoding is supported: one REDIS_URL is often shared with a cache backend."""
    assert 'django_aiogram.E043' not in ids(check_settings())


@override_settings(TELEGRAM_BOT={'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost', 'ALLOW_PICKLE': True})
def test_a_plain_url_with_pickle_is_fine():
    assert 'django_aiogram.E043' not in ids(check_settings())


@override_settings(
    TELEGRAM_BOT={
        'TOKEN': '42:x',
        # reads as off and is not: redis-py has no boolean parser for this key, so
        # the string 'false' reaches the connection and enables decoding
        'REDIS_URL': 'redis://localhost:6379/0?decode_responses=false',
        'ALLOW_PICKLE': True,
    }
)
def test_a_url_that_only_looks_like_it_disables_decoding_is_refused():
    assert 'django_aiogram.E043' in ids(check_settings())


@override_settings(TELEGRAM_BOT={'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost', 'DRAIN_TIMEOUT': 'soon'})
def test_an_unreadable_drain_timeout_is_reported():
    """`close()` reads this while shutting down, which is the worst place to raise."""
    assert 'django_aiogram.E044' in ids(check_settings())


@override_settings(TELEGRAM_BOT={'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost', 'DRAIN_TIMEOUT': -1})
def test_a_negative_drain_timeout_is_reported():
    """A negative budget makes the drain expire before it starts."""
    assert 'django_aiogram.E044' in ids(check_settings())


@override_settings(TELEGRAM_BOT={'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost', 'DRAIN_TIMEOUT': float('nan')})
def test_a_drain_timeout_that_is_not_a_number_is_reported():
    """Every comparison against nan is false, so it slips past a plain bound."""
    assert 'django_aiogram.E044' in ids(check_settings())


@override_settings(TELEGRAM_BOT={'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost', 'DRAIN_TIMEOUT': 2.5})
def test_a_fractional_drain_timeout_is_fine():
    assert 'django_aiogram.E044' not in ids(check_settings())


@override_settings(TELEGRAM_BOT={'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost'})
def test_a_container_that_forgot_its_hostname_is_warned_about(monkeypatch):
    """The in-flight list is keyed on the worker's name.

    Docker invents a twelve-character hex hostname when a container is started
    without `hostname:`, so replacing one strands whatever the last one was
    sending, somewhere nothing will look for it again.
    """
    monkeypatch.setenv('HOSTNAME', 'ba333cb79e00')

    assert 'django_aiogram.I001' in ids(check_settings())


@override_settings(
    TELEGRAM_BOT={
        'TOKEN': '42:x',
        'REDIS_URL': 'redis://localhost',
        'BROKER': 'django_aiogram.broker.redis_streams.RedisStreamsBroker',
        'REDIS_STREAM_KEY': 'TELEGRAM_BOT_STREAM',
    }
)
def test_a_transport_that_needs_no_worker_name_is_not_asked_for_one(monkeypatch):
    """The same container, on a transport where the advice would be empty.

    A Streams group's pending list belongs to the group, so any consumer can recover a dead
    one's work whatever it is called — `needs_identity` says so, and this is the check reading
    it. Without the gate, a Streams deployment with a Docker-generated hostname is told to pin
    it in order to protect an in-flight list that does not exist.

    The hostname is the same one the case above is warned about, which is what makes this
    about the transport rather than about the environment.
    """
    monkeypatch.setenv('HOSTNAME', 'ba333cb79e00')

    assert 'django_aiogram.I001' not in ids(check_settings())


@override_settings(TELEGRAM_BOT={'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost'})
def test_a_fixed_hostname_is_not_warned_about(monkeypatch):
    """An unset WORKER_NAME is the documented default and correct almost
    everywhere; warning about it as such would fire on every install."""
    monkeypatch.setenv('HOSTNAME', 'bot-worker-1')

    assert 'django_aiogram.I001' not in ids(check_settings())


@override_settings(TELEGRAM_BOT={'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost', 'WORKER_NAME': '   '})
def test_a_padded_name_is_judged_the_way_the_worker_judges_it(monkeypatch):
    """`worker_identity()` takes any truthy value, so a padded name *is* the name.

    A check that stripped first would call this empty, look at the hostname, and
    warn about a name the worker never uses. Poor as that name is, it is stable,
    and stability is the only thing I001 is about.
    """
    monkeypatch.setenv('HOSTNAME', 'ba333cb79e00')

    assert worker_identity() == '   ', 'the runtime stopped taking a padded name'
    assert 'django_aiogram.I001' not in ids(check_settings())


@override_settings(TELEGRAM_BOT={'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost', 'WORKER_NAME': 'bot-1'})
def test_a_named_worker_is_not_warned_about(monkeypatch):
    monkeypatch.setenv('HOSTNAME', 'ba333cb79e00')

    assert 'django_aiogram.I001' not in ids(check_settings())


@override_settings(TELEGRAM_BOT={'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost:6379/0'})
def test_a_documented_configuration_survives_fail_level_warning():
    """`check --fail-level WARNING` is what projects run in CI and in entrypoints.

    Both of the ids added in this release were warnings about conditions a check cannot
    decide from where it stands: an ephemeral hostname, which every container without
    `hostname:` has whether or not it consumes anything, and a log alias that a router
    this check cannot read may well serve. So a web container that upgraded went from
    exit 0 to exit 1 on a configuration that works. They report as information now.
    """
    reported = [message for message in check_settings() if message.level >= WARNING]

    assert reported == [], f'a working configuration would fail --fail-level WARNING: {ids(reported)}'


@override_settings(TELEGRAM_BOT={'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost:6379/0'})
def test_the_worker_name_rule_is_information_and_the_consumer_warns_for_itself(monkeypatch):
    """One rule, two audiences: the check informs, `start_tgbot` warns.

    Being the consumer is knowable in the command and not in a check, and this is the
    one place the same rule is asked twice — so it is asked of one function.
    """
    monkeypatch.setattr('django_aiogram.config.checks.transport.socket.gethostname', lambda: 'ba333cb79e00')
    monkeypatch.delenv('HOSTNAME', raising=False)

    reported = [message for message in check_settings() if str(message.id).endswith('I001')]

    assert len(reported) == 1, 'the rule stopped reporting at all'
    assert reported[0].level < WARNING, 'a check that cannot tell which process it is in warned'
    assert worker_name_problems(), 'the command would be told nothing'


@override_settings(TELEGRAM_BOT={'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost:6379/0', 'REDIS_TIMEOUT': 1})
def test_a_read_deadline_of_one_second_is_refused():
    """At 1 the consumer's blocking pop cannot fit inside the deadline it is capped by.

    `take_ceiling()` promises one second inside the transport's deadline; at 1 the subtraction
    clamps back to 1, so the pop's own timeout *equals* the read deadline and the deadline
    always wins. Every idle second then costs a `TimeoutError`, a traceback and a
    reconnect, against a healthy server, for ever — and `W004` invited exactly that by
    suggesting `BLPOP_TIMEOUT` be lowered to match.
    """
    assert 'django_aiogram.E030' in ids(errors(check_settings()))


@pytest.mark.parametrize('timeout', [2, 3, 5, 10, 60])
def test_every_read_deadline_the_check_admits_leaves_room_for_the_pop(timeout):
    """The floor and the ceiling are one statement, so they are asserted together."""
    with override_settings(
        TELEGRAM_BOT={'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost:6379/0', 'REDIS_TIMEOUT': timeout}
    ):
        assert errors(check_settings()) == [], 'the check refuses a value the consumer can work with'
        ceiling = take_ceiling(RedisListBroker.CALL_TIMEOUT_OPTION, RedisListBroker.call_timeout())
        assert ceiling.seconds < read_timeout(), 'the take cannot outlast the socket it reads through'


@override_settings(TELEGRAM_BOT={'TOKEN': 42, 'REDIS_URL': 'r://x'})
def test_manage_py_check_surfaces_these_ids():
    """Every test above calls `check_settings()` directly, and none goes through Django.

    So the registration itself was unpinned: replacing `register(check_settings)` with
    `_ = check_settings` left the whole suite green, 1068 tests and 129 database tests,
    while `manage.py check` reported nothing at all. Two headline claims of this release
    rest on that path — that `check` no longer imports aiogram, and that the boolean rules
    stopped refusing configuration that works — and neither could be verified end to end.

    A wrong-typed `TOKEN` is `E004`, which needs no Redis, no token and no database.
    """
    with pytest.raises(SystemCheckError) as raised:
        call_command('check')

    assert 'django_aiogram.E004' in str(raised.value)


def test_the_checks_are_registered_with_django():
    """The same invariant asked directly, so a failure says which half broke.

    `manage.py check` failing to report our ids has two possible causes — the rule stopped
    finding the problem, or nothing registered the rule. The test above cannot tell them
    apart; this one can.
    """
    registered = [check for check in registry.get_checks() if check is check_settings]

    assert registered, 'apps.ready() no longer registers check_settings with Django'


@override_settings(
    TELEGRAM_BOT={
        'ENABLED': True,
        'TOKEN': '42:x',
        'REDIS_URL': 'redis://localhost/0?decode_responses=1',
        'ALLOW_PICKLE': True,
        'BROKER': 'django_aiogram.broker.redis_list.RedisListBroker',
    }
)
def test_a_check_behind_e047_does_not_crash_looking_for_the_driver(monkeypatch):
    """A rule reported after `E047` must not replace it with the traceback it prevents.

    Checks report; they do not stop the ones behind them. `E043` asks redis-py whether a URL
    enables `decode_responses`, and since the driver became an extra that question needs an
    import — so on a base install with `ALLOW_PICKLE` on, `manage.py check` died with
    `ModuleNotFoundError: No module named 'redis'` *from inside a check*, which is the exact
    failure the extras work exists to replace. Measured on a real driverless install before
    this was fixed.

    `ALLOW_PICKLE` is what makes it reachable: without it `E043` returns before asking, which
    is why the first driverless measurement of this branch came back clean.

    The import is made to fail rather than the package uninstalled — the suite needs redis for
    everything else — and `find_spec` is patched alongside it so `E047` reaches the same
    conclusion and this reads as the one configuration it describes.
    """
    real_import = builtins.__import__

    def refuse(name, *args, **kwargs):
        if name == 'redis' or name.startswith('redis.'):
            raise ModuleNotFoundError(f"No module named '{name}'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, '__import__', refuse)
    monkeypatch.setattr('importlib.util.find_spec', lambda name, *args: None if name == 'redis' else True)

    reported = ids(check_settings())

    assert 'django_aiogram.E047' in reported, 'the missing driver was not reported'
    assert 'django_aiogram.E043' not in reported, 'a rule that cannot ask the driver still answered'


@override_settings(
    TELEGRAM_BOT={
        'ENABLED': True,
        'TOKEN': '42:x',
        'REDIS_URL': 'redis://localhost',
        # named rather than defaulted, in all four of these: they are about one transport's
        # behaviour, and a change of default would re-aim them at another one while they
        # stayed green. This one lost its explicit name to a stacked `override_settings`,
        # where the inner mapping wins, and went on passing through the default
        'BROKER': 'django_aiogram.broker.redis_list.RedisListBroker',
    }
)
def test_a_named_broker_whose_driver_is_missing_is_reported_with_the_install_line(monkeypatch):
    """`redis` left the base dependencies, so this is now a reachable configuration.

    A project that names the Redis list without `django-aiogram[redis]` installed is one
    `pip install` short of working — and the difference between hearing that from
    `manage.py check` and hearing `ModuleNotFoundError: redis` from inside a producer is
    the whole reason `BROKER` is judged rather than trusted.

    The driver is made to look absent through `find_spec`, not uninstalled: the suite runs
    with redis present because every other test needs it, and an import that really failed
    would take this process down rather than produce a finding.
    """
    monkeypatch.setattr('importlib.util.find_spec', lambda name, *args: None if name == 'redis' else True)

    problems = [problem for problem in check_settings() if str(problem.id) == 'django_aiogram.E047']

    assert problems, 'a broker whose driver is absent produced no finding'
    assert 'pip install "django-aiogram[redis]"' in problems[0].hint, problems[0].hint


@override_settings(
    TELEGRAM_BOT={
        'ENABLED': True,
        'TOKEN': '42:x',
        'REDIS_URL': 'redis://localhost',
        'BROKER': 'django_aiogram.broker.redis_list.RedisListBroker',
    }
)
def test_running_the_checks_leaves_no_broker_behind():
    """A rule may ask the transport a question without building the one this process uses.

    `I001` reads `needs_identity`, which needs an instance — and reaching for the process-wide
    accessor to get one would leave a live broker cached in `migrate`, `shell`, `collectstatic`
    and every other command that runs checks. A throwaway costs nothing and connects to
    nothing; the cache is for the process that actually sends.
    """
    from django_aiogram.broker import registry

    registry.close_broker()

    check_settings()

    assert registry._broker is None, 'running the checks cached a broker for the whole process'


@override_settings(TELEGRAM_BOT={'ENABLED': False, 'BROKER': 'django_aiogram.broker.redis_list.RedisListBroker'})
def test_a_disabled_process_is_not_asked_to_install_a_driver_it_never_calls(monkeypatch):
    """The rule above, gated the way `W002` is gated, and for the same reason.

    A web container with `ENABLED` off registers every check — that is what makes this
    worth pinning — and it reaches no transport, so the driver it does not have is not a
    problem it has. Without the gate this is an `Error`, which fails `manage.py check`
    outright and can only be answered by installing a driver nothing in that process calls.

    The name is still judged: the sibling case below names a non-broker with the bot
    disabled and is reported, because a typo is a typo in every process.
    """
    monkeypatch.setattr('importlib.util.find_spec', lambda name, *args: None if name == 'redis' else True)

    problems = [problem for problem in check_settings() if str(problem.id) == 'django_aiogram.E047']

    assert not problems, f'a disabled process was asked for a driver: {problems}'


@override_settings(
    TELEGRAM_BOT={
        'ENABLED': False,
        'BROKER': 'django_aiogram.producer.client.TelegramBot',
    }
)
def test_a_broker_setting_naming_something_else_is_refused():
    """Importable and callable is not the same as being a transport.

    A dotted path that resolves is the easy half. This one resolves to a real class that is
    not a `Broker`, which is what a copied line from the wrong page produces — and it would
    otherwise fail on the first `publish` with an `AttributeError` naming a method rather
    than the setting.

    Asserted with the bot **disabled**, which is the half of this rule that is not gated:
    the process above is excused its missing driver, and this one is not excused its typo,
    because a name that is wrong here is wrong in the worker that does send.
    """
    problems = [problem for problem in check_settings() if str(problem.id) == 'django_aiogram.E047']

    assert problems, 'a BROKER naming a non-broker produced no finding'
    assert 'not a Broker subclass' in problems[0].msg, problems[0].msg


@override_settings(
    TELEGRAM_BOT={
        'ENABLED': True,
        'TOKEN': '42:x',
        'BROKER': 'django_aiogram.broker.kafka.KafkaBroker',
        'KAFKA_BOOTSTRAP': 'localhost:9092',
        'FSM_STORAGE': 'memory',
    }
)
def test_a_kafka_deployment_is_not_asked_for_a_redis_url():
    """`W002` asked every enabled bot for `REDIS_URL` while Redis was the only transport.

    Three of the four never open a Redis connection, so on this configuration the warning is
    about a key nothing reads — and a warning about an unused setting is how an operator
    learns to stop reading the list it is in.
    """
    reported = [problem for problem in check_settings() if str(problem.id) == 'django_aiogram.W002']

    assert reported == [], f'a Kafka deployment was asked for a Redis URL: {reported}'


@override_settings(
    TELEGRAM_BOT={
        'ENABLED': True,
        'TOKEN': '42:x',
        'BROKER': 'django_aiogram.broker.redis_list.RedisListBroker',
        'FSM_STORAGE': 'memory',
    }
)
def test_a_redis_broker_with_no_url_is_still_asked_for_one():
    """The other direction, which is what makes the rule above a narrowing and not a removal."""
    reported = [problem for problem in check_settings() if str(problem.id) == 'django_aiogram.W002']

    assert reported, 'a Redis broker with no URL produced no warning'
    assert 'BROKER or FSM_STORAGE names Redis' in (reported[0].hint or ''), reported[0].hint


@override_settings(
    TELEGRAM_BOT={
        'ENABLED': True,
        'TOKEN': '42:x',
        'BROKER': 'django_aiogram.broker.kafka.KafkaBroker',
        'KAFKA_BOOTSTRAP': 'localhost:9092',
        'FSM_STORAGE': 'redis',
    }
)
def test_the_fsm_storage_alone_is_enough_to_need_the_url():
    """Two consumers, and either is sufficient — the broker is not the only one.

    A Kafka queue with Redis-backed FSM state is a reasonable deployment, and it does need
    `REDIS_URL`. A gate that looked only at `BROKER` would have taken this warning away.
    """
    reported = [problem for problem in check_settings() if str(problem.id) == 'django_aiogram.W002']

    assert reported, 'Redis FSM storage with no URL produced no warning'


@override_settings(TELEGRAM_BOT={'ENABLED': True, 'FSM_STORAGE': 'memory', 'REDIS_URL': 'redis://x'})
def test_the_missing_token_hint_says_sending_rather_than_reaching():
    """The hint is text the code emits, so it is API to whoever reads a failing build.

    It said "processes that never reach Telegram", which claimed `ENABLED` gates every
    connection — and the depth reads answer regardless of it. Pinned because the wording is
    the whole value of a hint: it tells an operator which setting to touch.
    """
    reported = [problem for problem in check_settings() if str(problem.id) == 'django_aiogram.W001']

    assert reported, 'an enabled bot with no token produced no warning'
    hint = reported[0].hint or ''
    assert 'never send to Telegram' in hint, hint
    assert 'at all' not in hint, f'the hint claims ENABLED gates more than sending: {hint}'


@override_settings(
    TELEGRAM_BOT={
        'ENABLED': True,
        'TOKEN': '42:x',
        'BROKER': 'django_aiogram.broker.kafka.KafkaBroker',
        'KAFKA_BOOTSTRAP': 'localhost:9092',
        'FSM_STORAGE': StorageKind.REDIS,
    }
)
def test_the_enum_this_package_publishes_counts_as_redis_storage():
    """`FSM_STORAGE` accepts the enum, and `API.md` documents it as a way to write settings.

    `str()` on it does not give its value: `StorageKind` mixes in `str`, and since 3.11
    `str(StorageKind.REDIS)` is `'StorageKind.REDIS'`. So a gate that normalised the string saw
    `'storagekind.redis'`, matched nothing, and suppressed `W002` for a deployment that goes on
    to need `REDIS_URL` the moment it builds its storage.
    """
    reported = [problem for problem in check_settings() if str(problem.id) == 'django_aiogram.W002']

    assert reported, 'the enum form of Redis FSM storage did not ask for a URL'


@pytest.mark.parametrize(
    ('value', 'says'),
    [
        ('', 'is empty'),
        ('blpop', "Write 'django_aiogram.consumer.delivery.BlpopDelivery'"),
        ('keyspace', 'removed in 3.0'),
        ('smoke-signals', 'not a dotted path'),
        ('BlpopDelivery', 'not a dotted path'),
        # `\w` with a `str` pattern matches any Unicode word character, so the first version of
        # this rule accepted a segment `str.isidentifier()` refuses -- a path no import resolves
        ('pkg.mod\u00b2.Consumer', 'not a dotted path'),
    ],
    ids=['empty', '3.x blpop', '3.x keyspace', 'not a path', 'bare name', 'not an identifier'],
)
def test_e009_reports_what_a_string_can_be_wrong_about(value, says):
    """`DELIVERY` is a dotted path since 4.0, so it can be wrong in the ways a path can be.

    The 3.x words are reported by name rather than as "not a path": a project upgrading has one
    of them in its settings file, and being told what to write is the difference between a
    minute and an afternoon.
    """
    with override_settings(TELEGRAM_BOT={'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost', 'DELIVERY': value}):
        found = [message for message in check_settings() if message.id == 'django_aiogram.E009']

    assert len(found) == 1, f'E009 reported {len(found)} problems for {value!r}'
    assert says in found[0].msg, found[0].msg
    # every finding carries the same hint, and it has to name where the rest is settled:
    # a reader told only about the shape goes hunting for a rule that cannot exist
    assert 'start_tgbot' in (found[0].hint or ''), found[0].hint


@override_settings(
    TELEGRAM_BOT={
        'TOKEN': '42:x',
        'REDIS_URL': 'redis://localhost',
        'DELIVERY': 'myproject.consumers.NotHereYet',
    }
)
def test_e009_accepts_a_path_it_cannot_resolve_and_says_where_it_will_be(monkeypatch):
    """The rule stops at the shape, and the hint says so, because resolving costs aiogram.

    Importing the consumer module pulls `wire.serializers`, which encodes aiogram models and so
    imports their types -- measured at 883ms and 135 MiB on a bare settings module, on every
    `migrate`, `runserver` and `shell`. `E018` exists because that cost was once paid here.

    So a plausible path that does not exist passes the checks and fails at `start_tgbot`, which
    resolves it before starting a thread. What this case pins is that the reader is *told* that:
    a hint naming only the shape would send somebody hunting for a rule that cannot exist.
    """

    # an empty report is not evidence on its own: a rule that resolved the path and swallowed the
    # `ImportError` would satisfy it too, and that is exactly the regression this case exists to
    # prevent -- the aiogram import coming back into `manage.py check`. So both routes to a
    # resolution are *recorded*, and the assertion below means "did not try".
    #
    # Recorded rather than raised, and that distinction was measured: the first version of this
    # guard raised `AssertionError`, and a rule that wrapped its resolution in `except Exception`
    # -- which is exactly how a swallowing regression looks -- ate the guard and the case passed.
    # A flag cannot be swallowed
    attempts = []

    def record(*args: object, **kwargs: object) -> None:
        attempts.append(args)
        msg = 'mined'
        raise ImportError(msg)

    # Each rules module binds `import_string` at import, so patching it where it is *defined*
    # leaves every name they call pointing at the real one -- the mine would sit beside the road.
    # Measured: with the wrong target, a rule restored to resolving passed this case.
    #
    # Planted in every module of the package that holds the name rather than in a list of two:
    # what this asserts is that *no* rule resolves the path, and a module added later that binds
    # it would otherwise be the one place the mine is missing
    package = importlib.import_module('django_aiogram.config.checks')
    for name in sorted(pathlib.Path(str(package.__path__[0])).glob('*.py')):
        module = importlib.import_module(f'django_aiogram.config.checks.{name.stem}')
        if hasattr(module, 'import_string'):
            monkeypatch.setattr(f'{module.__name__}.import_string', record)
    monkeypatch.setattr('django_aiogram.consumer.delivery.delivery_class', record)

    reported = [message for message in check_settings() if message.id == 'django_aiogram.E009']

    assert attempts == [], 'the check resolved the path, which is what costs aiogram at check time'
    assert reported == [], f'the shape is fine, so nothing should be reported: {reported}'
    registered = [check for check in CHECKS if check.key == 'DELIVERY']
    # the id as well as the key: a rule renamed to E0xx while still watching DELIVERY would
    # satisfy "some check looks at it", and every message this suite matches on says E009
    assert [check.code for check in registered] == ['E009'], f'DELIVERY is watched by {registered}'


def test_the_two_lists_of_3x_delivery_names_agree():
    """The consumer and the checks each carry the table, and they must not drift.

    Two copies on purpose: the checks cannot import the consumer without paying aiogram, which
    is the whole reason `E009` stops at the shape. So the copies are pinned against each other
    here -- the cheapest place to notice, and the only one that fails when a name is added to one.
    """
    from django_aiogram.config import checks as rules
    from django_aiogram.consumer import delivery as consumer

    assert rules.THREE_X_DELIVERIES == consumer.THREE_X_DELIVERIES, (
        f'checks: {rules.THREE_X_DELIVERIES}; consumer: {consumer.THREE_X_DELIVERIES}'
    )


@override_settings(
    TELEGRAM_BOT={
        'TOKEN': '42:x',
        'REDIS_URL': 'redis://localhost',
        'DELIVERY': 'myapp.class.Consumer',
    }
)
def test_e009_accepts_a_path_whose_segment_is_a_python_keyword():
    """`'class'.isidentifier()` is True, and that is the whole answer.

    It looks like the superscript case above and is its opposite: measured, a file called
    `class.py` imports perfectly well — `importlib.import_module('pkg.class')` returns the module
    and `import_string('pkg.class.Consumer')` returns the class — because only the `import`
    *statement* goes through Python's grammar, and neither of those does.

    So a rule refusing it would refuse a path the project can use, which is worse than the gap it
    would close. It was refused for one round of this pull request, and this case is what stops
    that returning.
    """
    reported = [message for message in check_settings() if message.id == 'django_aiogram.E009']

    assert reported == [], f'a keyword segment is importable, and was refused: {reported}'


class MyOwnRouter:
    """A router a project wrote, for the case that has to stay silent.

    Real rather than a dotted path that does not resolve, because `override_settings` cannot be
    used with one -- see the note in the case below.
    """

    def db_for_read(self, model, **hints):
        return None


@pytest.mark.parametrize(
    ('routers', 'says'),
    [
        (
            ['django_redis_aiogram.dbrouter.TelegramEventLogRouter'],
            "Write 'django_aiogram.eventlog.dbrouter.TelegramEventLogRouter'",
        ),
        # a path we never had: naming a replacement it never had would be worse than saying the
        # distribution is gone
        (['django_redis_aiogram.something.Else'], 'That distribution is gone in 4.0'),
    ],
    ids=['the router', 'some other 3.x path'],
)
@override_settings(TELEGRAM_BOT={'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost'})
def test_e048_names_the_4_0_path_for_a_3_x_router(routers, says, monkeypatch):
    """A project wrote our dotted path into Django's settings, and 4.0 moved it.

    In a deployment nothing imports `DATABASE_ROUTERS` until the first query that needs routing,
    and then it fails with Django's own `ImportError` -- a module name, not the fix. Which is why
    a check is worth having.

    Set with `monkeypatch` rather than `override_settings`, and the reason is worth writing down:
    Django's own `clear_routers_cache` receiver rebuilds `ConnectionRouter().routers` on
    `setting_changed`, which **imports every router eagerly** -- so `override_settings` with a path
    that does not resolve raises inside itself, before the code under test runs. The rule reads
    `getattr(django_settings, 'DATABASE_ROUTERS', ())`, so this patches exactly what it reads.
    """
    from django.conf import settings as django_settings

    monkeypatch.setattr(django_settings, 'DATABASE_ROUTERS', routers, raising=False)
    found = [message for message in check_settings() if message.id == 'django_aiogram.E048']

    assert len(found) == 1, f'E048 reported {len(found)} problems for {routers}'
    assert says in found[0].msg, found[0].msg
    # the label is Django's setting, not ours: a message introducing itself as
    # TELEGRAM_BOT['...'] would send the reader to the wrong file
    assert found[0].msg.startswith('DATABASE_ROUTERS '), found[0].msg
    assert 'urls.py' in (found[0].hint or ''), 'the hint does not mention the other moved path'


@pytest.mark.parametrize(
    'routers',
    [
        [],
        ['django_aiogram.eventlog.dbrouter.TelegramEventLogRouter'],
        ['tests.test_checks.MyOwnRouter'],
        ['django_aiogram.eventlog.dbrouter.TelegramEventLogRouter', 'tests.test_checks.MyOwnRouter'],
    ],
    ids=['none', 'the 4.0 path', "a project's own", 'both'],
)
def test_e048_is_silent_on_anything_that_is_not_a_3_x_path(routers):
    """The rule reports a rename, so a correct configuration and an unrelated one are the same.

    A router of the project's own is the case worth having: it neither resolves to ours nor starts
    with the old distribution, and reporting it would make the rule noise on a working setup. This
    one is a real class, so `override_settings` can carry it the way a project's settings would.
    """
    with override_settings(TELEGRAM_BOT={'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost'}, DATABASE_ROUTERS=routers):
        found = [message for message in check_settings() if message.id == 'django_aiogram.E048']

    assert found == [], f'E048 reported {[message.msg for message in found]} for {routers}'


@pytest.mark.parametrize(
    ('broker', 'extra', 'names'),
    [
        ('django_aiogram.broker.redis_list.RedisListBroker', {}, 'REDIS_TIMEOUT'),
        (
            'django_aiogram.broker.kafka.KafkaBroker',
            {'KAFKA_BOOTSTRAP': 'localhost:9092', 'KAFKA_TOPIC': 'tg'},
            'KAFKA_TIMEOUT',
        ),
        (
            'django_aiogram.broker.rabbitmq.RabbitMQBroker',
            {'RABBITMQ_URL': 'amqp://guest:guest@localhost:5672/', 'RABBITMQ_QUEUE': 'tg'},
            'RABBITMQ_TIMEOUT',
        ),
    ],
    ids=['redis list', 'kafka', 'rabbitmq'],
)
def test_w004_names_the_deadline_of_the_configured_transport(broker, extra, names):
    """The hint has to name a setting the deployment actually has.

    Until #41 the cap weighed `REDIS_TIMEOUT` whichever transport was configured, so a Kafka
    deployment was told to raise a setting it does not read — and, worse, had its poll shortened
    by it: measured, `REDIS_TIMEOUT: 2` capped a 30-second `KAFKA_TIMEOUT` at one second.

    Both directions here. The named setting has to be the configured transport's, and the Redis
    one must not appear on a transport that never reads it.
    """
    settings = {
        'TOKEN': '42:x',
        'REDIS_URL': 'redis://localhost',
        'BROKER': broker,
        'BLPOP_TIMEOUT': 30,
        'HEARTBEAT_INTERVAL': 60,
        names: 5,
        **extra,
    }
    with override_settings(TELEGRAM_BOT=settings):
        found = [message for message in check_settings() if message.id == 'django_aiogram.W004']

    assert len(found) == 1, f'W004 reported {len(found)} problems on {broker}'
    assert names in (found[0].hint or ''), found[0].hint
    assert 'which the consumer caps at 4' in found[0].msg, found[0].msg
    if names != 'REDIS_TIMEOUT':
        assert 'REDIS_TIMEOUT' not in (found[0].hint or ''), f'the hint names a Redis setting: {found[0].hint}'


def test_w004_is_silent_when_the_transport_deadline_leaves_room():
    """The rule reports a cap the consumer applies, so a setting under the cap is not a finding.

    On Kafka with its own timeout at 30 the cap is 29, and `BLPOP_TIMEOUT` at 5 is under it — where
    the same configuration used to be reported, and capped, against `REDIS_TIMEOUT`.

    Which is why `REDIS_TIMEOUT` is **2** here rather than left out: at its default of 10 the old
    cap was 9, above the 5 being asked for, so the rule was silent before the fix as well and the
    case proved nothing. At 2 the old cap is 1, and a `BLPOP_TIMEOUT` of 5 was reported.
    """
    settings = {
        'TOKEN': '42:x',
        'REDIS_URL': 'redis://localhost',
        'REDIS_TIMEOUT': 2,
        'BROKER': 'django_aiogram.broker.kafka.KafkaBroker',
        'KAFKA_BOOTSTRAP': 'localhost:9092',
        'KAFKA_TOPIC': 'tg',
        'KAFKA_TIMEOUT': 30,
        'BLPOP_TIMEOUT': 5,
        'HEARTBEAT_INTERVAL': 60,
    }
    with override_settings(TELEGRAM_BOT=settings):
        found = [message for message in check_settings() if message.id == 'django_aiogram.W004']

    assert found == [], f'W004 reported {[message.msg for message in found]} with room to spare'


class BrokerWithNoDeadline(RedisListBroker):
    """A broker somebody wrote that never says what bounds one of its calls."""

    CALL_TIMEOUT_OPTION = ''


class BrokerNamingAnOptionItDoesNotHave(RedisListBroker):
    """And one that names an option it does not declare, which is the same gap one step later."""

    CALL_TIMEOUT_OPTION = 'SOME_OTHER_TIMEOUT'


class BrokerNeedingMoreThanTheRuleAsks(RedisListBroker):
    """A broker of somebody's own whose transport needs more of `REDIS_TIMEOUT` than `E030` does.

    `E030` accepts any integer from 2 up, so a value this refuses is one that rule reports nothing
    about — which is the whole point of the case that uses this.
    """

    _FLOOR = 5

    @classmethod
    def call_timeout(cls) -> float:
        timeout = super().call_timeout()
        if timeout < cls._FLOOR:
            msg = f"TELEGRAM_BOT['REDIS_TIMEOUT'] is {timeout}, and this transport needs {cls._FLOOR} or more."
            raise ImproperlyConfigured(msg)
        return timeout


@pytest.mark.parametrize(
    'broker',
    ['tests.test_checks.BrokerWithNoDeadline', 'tests.test_checks.BrokerNamingAnOptionItDoesNotHave'],
    ids=['declares nothing', 'names what it does not declare'],
)
def test_e047_reports_a_broker_that_cannot_name_its_call_deadline(broker):
    """The seam needs the name, and without it the failure lands as a `KeyError` somewhere else.

    `W004` quotes that name and the consumer caps its reads by the number behind it, so a broker
    that does not declare it is incomplete rather than merely unusual — and `option('')` raises
    `KeyError`, which would surface out of whichever rule asked first.
    """
    with override_settings(TELEGRAM_BOT={'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost', 'BROKER': broker}):
        found = [message for message in check_settings() if message.id == 'django_aiogram.E047']

    assert len(found) == 1, f'E047 reported {len(found)} problems for {broker}'
    assert 'declares no call deadline' in found[0].msg, found[0].msg
    assert 'CALL_TIMEOUT_OPTION' in found[0].msg, found[0].msg


@pytest.mark.parametrize(
    'broker',
    ['tests.test_checks.BrokerWithNoDeadline', 'tests.test_checks.BrokerNamingAnOptionItDoesNotHave'],
    ids=['declares nothing', 'names what it does not declare'],
)
def test_the_checks_survive_a_broker_that_cannot_name_its_call_deadline(broker):
    """`manage.py check` has to answer, and `W004` must not be what stops it.

    A rule about `BLPOP_TIMEOUT` reaching `option('')` would raise `KeyError` out of the whole
    run — every other finding lost with it, on a configuration one of those findings is about.
    """
    with override_settings(
        TELEGRAM_BOT={'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost', 'BROKER': broker, 'BLPOP_TIMEOUT': 300}
    ):
        reported = check_settings()

    assert [message for message in reported if message.id == 'django_aiogram.E047'], 'E047 said nothing'
    assert [message for message in reported if message.id == 'django_aiogram.W004'] == [], (
        'W004 reported a cap it cannot compute'
    )


@pytest.mark.parametrize(
    ('broker', 'option'),
    [
        ('django_aiogram.broker.rabbitmq.RabbitMQBroker', 'RABBITMQ_TIMEOUT'),
        ('django_aiogram.broker.kafka.KafkaBroker', 'KAFKA_TIMEOUT'),
    ],
)
@pytest.mark.parametrize('value', ['', 'abc', 0, -1], ids=repr)
def test_e047_reports_a_deadline_the_transport_refuses(broker, option, value):
    """A deadline that cannot be one passed every rule, and the transport refused it at first send.

    Nothing owned the type of a transport's own deadline: `REDIS_TIMEOUT` has `E030` because it
    sits in the package-wide table, and the other three sit with their transports where no rule
    reached them. So `RABBITMQ_TIMEOUT='abc'` was a clean `manage.py check` and a `ValueError` out
    of the first publish, naming `float` rather than the setting.

    Reported by whichever rule owns `BROKER`, because the deadline is part of what a transport has
    to supply -- and ungated by `ENABLED`, since it is arithmetic over settings that needs no
    driver, and refused in the web tier exactly as in the worker.
    """
    settings = {
        'TOKEN': '42:x',
        'REDIS_URL': 'redis://localhost',
        'BROKER': broker,
        'RABBITMQ_URL': 'amqp://localhost',
        'RABBITMQ_QUEUE': 'tg',
        'KAFKA_BOOTSTRAP': 'localhost:9092',
        'KAFKA_TOPIC': 'tg',
        option: value,
    }
    with override_settings(TELEGRAM_BOT=settings):
        found = [message for message in check_settings() if message.id == 'django_aiogram.E047']

    assert len(found) == 1, f'E047 reported {len(found)} problems for {option}={value!r}'
    assert option in found[0].msg, found[0].msg
    assert 'call deadline is unusable' in found[0].msg, found[0].msg


def test_one_rule_reports_a_deadline_that_has_a_rule_of_its_own():
    """`REDIS_TIMEOUT` is guarded by `E030`, so `E047` says nothing about the same value.

    Two errors about one setting sends the reader looking for two problems, and this is the
    convention `W004` already states from the other side -- it stays silent because "E014, E023 and
    E030 own the type complaints". The rule asks the registry rather than carrying a list of names,
    so the day `REDIS_TIMEOUT` leaves the package-wide table with its own rule (#23), this one
    picks it up.
    """
    settings = {'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost', 'REDIS_TIMEOUT': 'five'}
    with override_settings(TELEGRAM_BOT=settings):
        reported = {str(message.id) for message in check_settings()}

    assert 'django_aiogram.E030' in reported, 'the rule that owns REDIS_TIMEOUT said nothing'
    assert 'django_aiogram.E047' not in reported, 'E047 reported a setting another rule owns'


def test_the_ceiling_keeps_both_bounds_when_one_name_answers_for_both():
    """A broker may name `HEARTBEAT_INTERVAL` as its own deadline option, and then the two are one.

    Legal, because `Broker.option` refuses only a *differing* default, and reachable as soon as the
    broker overrides `call_timeout()` to return something other than the setting it names. Keyed by
    option name, the deadline entry then replaced the heartbeat entry and the larger of the two won:
    a heartbeat of 2 against a deadline of 100 capped the read at 99, so the consumer waits its own
    heartbeat out and is reaped while healthy — which is the failure this ceiling exists to prevent.

    The name is asserted once, too: the hint has to read as one setting, not the same one twice.
    """
    with override_settings(TELEGRAM_BOT={'TOKEN': '42:x', 'HEARTBEAT_INTERVAL': 2}):
        ceiling = take_ceiling('HEARTBEAT_INTERVAL', 100.0)

    assert ceiling.seconds == 2, f'the heartbeat bound was lost: {ceiling}'
    assert ceiling.bound_by == ('HEARTBEAT_INTERVAL',), f'one setting named twice: {ceiling.bound_by}'


@pytest.mark.parametrize('enabled', [True, False], ids=['enabled', 'disabled'])
def test_a_deadline_is_judged_without_the_transport_driver(monkeypatch, enabled):
    """Nothing about the deadline needs the driver, so nothing about it may depend on having one.

    The rule used to resolve the broker *with* its driver verified, and a missing one returned
    first: the install line where the bot is enabled, and nothing at all where it is not. So a
    deadline that cannot be one was reported only on a machine that happened to have the extra
    installed — and never in a disabled web tier, which is where a settings typo is most likely to
    sit unnoticed. It is also why the cases below this one passed here and failed on a CI leg that
    installs one driver, which is the shape of green that means nothing.

    `find_spec` is patched rather than pika uninstalled — the suite needs the driver for everything
    else, and a case that only answers on a machine without it is the defect this one is about. On
    `importlib.util` itself, since the registry imports it inside the function that asks.
    """
    monkeypatch.setattr('importlib.util.find_spec', lambda name, *args: None if name == 'pika' else True)
    settings = {
        'TOKEN': '42:x',
        'REDIS_URL': 'redis://localhost',
        'ENABLED': enabled,
        'BROKER': 'django_aiogram.broker.rabbitmq.RabbitMQBroker',
        'RABBITMQ_URL': 'amqp://localhost',
        'RABBITMQ_QUEUE': 'tg',
        'RABBITMQ_TIMEOUT': 'abc',
    }
    with override_settings(TELEGRAM_BOT=settings):
        found = [message for message in check_settings() if message.id == 'django_aiogram.E047']

    assert len(found) == 1, f'E047 reported {[message.msg for message in found]}'
    assert 'call deadline is unusable' in found[0].msg, found[0].msg
    assert 'RABBITMQ_TIMEOUT' in found[0].msg, found[0].msg


def test_e047_reports_a_deadline_only_its_own_transport_refuses():
    """Standing aside for `E030` must mean "it is reporting this", not "it exists".

    A broker somebody wrote can name `REDIS_TIMEOUT` — the two Redis transports do — and need more
    of it than `E030` asks for, which is any integer from 2 up. Suppressing on the name alone left
    a 3 that this broker refuses unreported by every rule, and refused by the transport the first
    time it read the setting.
    """
    settings = {
        'TOKEN': '42:x',
        'REDIS_URL': 'redis://localhost',
        'REDIS_TIMEOUT': 3,
        'BROKER': 'tests.test_checks.BrokerNeedingMoreThanTheRuleAsks',
    }
    with override_settings(TELEGRAM_BOT=settings):
        reported = check_settings()

    found = [message for message in reported if message.id == 'django_aiogram.E047']
    assert len(found) == 1, f'E047 reported {[message.msg for message in found]}'
    assert 'needs 5 or more' in found[0].msg, found[0].msg
    assert [message for message in reported if message.id == 'django_aiogram.E030'] == [], (
        'E030 reported a value it accepts, so this case proves nothing about the suppression'
    )


@pytest.mark.parametrize(
    ('broker', 'option'),
    [
        ('django_aiogram.broker.rabbitmq.RabbitMQBroker', 'RABBITMQ_TIMEOUT'),
        ('django_aiogram.broker.kafka.KafkaBroker', 'KAFKA_TIMEOUT'),
    ],
)
def test_the_checks_survive_a_deadline_the_transport_refuses(broker, option):
    """And the run still answers: `W004` reads the same number, and it must not be what raises."""
    settings = {
        'TOKEN': '42:x',
        'REDIS_URL': 'redis://localhost',
        'BROKER': broker,
        'RABBITMQ_URL': 'amqp://localhost',
        'RABBITMQ_QUEUE': 'tg',
        'KAFKA_BOOTSTRAP': 'localhost:9092',
        'KAFKA_TOPIC': 'tg',
        'BLPOP_TIMEOUT': 300,
        option: 'abc',
    }
    with override_settings(TELEGRAM_BOT=settings):
        reported = check_settings()

    found = [message for message in reported if message.id == 'django_aiogram.E047']
    assert len(found) == 1, f'E047 reported {[message.msg for message in found]}'
    # the message and not merely the id: on a machine without the driver this rule has another
    # finding to make, and asserting the id alone passed on exactly that machine while proving
    # nothing about the deadline
    assert 'call deadline is unusable' in found[0].msg, found[0].msg
    assert [message for message in reported if message.id == 'django_aiogram.W004'] == [], (
        'W004 reported a cap it cannot compute'
    )


@pytest.mark.parametrize(
    ('timeout', 'cap'),
    # 2.6 rather than 2.5 alone: `round(2.5)` is 2 in Python, so rounding and flooring agree
    # there and the case could not tell them apart. At 2.6 they differ -- flooring allows one
    # whole second inside the deadline, rounding would allow two and leave 0.6 of a second
    [('0.5', 1), (2.5, 1), (2.6, 1), (30, 29)],
    ids=['a fraction below one', 'a fraction at the tie', 'a fraction above the tie', 'a whole number'],
)
def test_w004_reads_a_fractional_transport_deadline(timeout, cap):
    """`KAFKA_TIMEOUT` accepts fractions, and reading it as an integer got both ends wrong.

    `0.5` raised out of `int()`, so the rule fell silent on a deployment whose poll is capped at a
    second; `2.5` became `2`, so the rule reported a ceiling one second away from the one the
    consumer applies. The floor is deliberate in the other direction: 2.5 allows one whole second
    inside the deadline, not two.
    """
    settings = {
        'TOKEN': '42:x',
        'REDIS_URL': 'redis://localhost',
        'BROKER': 'django_aiogram.broker.kafka.KafkaBroker',
        'KAFKA_BOOTSTRAP': 'localhost:9092',
        'KAFKA_TOPIC': 'tg',
        'KAFKA_TIMEOUT': timeout,
        'BLPOP_TIMEOUT': 300,
        'HEARTBEAT_INTERVAL': 600,
    }
    with override_settings(TELEGRAM_BOT=settings):
        found = [message for message in check_settings() if message.id == 'django_aiogram.W004']

    assert len(found) == 1, f'W004 said nothing about KAFKA_TIMEOUT={timeout!r}'
    assert f'caps at {cap}.' in found[0].msg, found[0].msg


@override_settings(
    TELEGRAM_BOT={
        'TOKEN': '42:x',
        'REDIS_URL': 'redis://localhost',
        'EVENT_LOG': True,
        'EVENT_LOG_RETENTION_DAYS': -7,
    }
)
def test_the_retention_warning_quotes_the_value_that_is_set():
    """A negative retention reaches the same branch as zero, and is not zero.

    The rule said "is 0" whatever the value was, so an operator who had written `-7` was told
    about a setting that says something else and went looking for the one that says `0`. The
    branch is right — nothing deletes a row either way — and the sentence has to name what it read.
    """
    found = [message for message in check_settings() if message.id == 'django_aiogram.W006']

    assert len(found) == 1, [message.msg for message in found]
    assert 'is -7' in found[0].msg, found[0].msg


def test_a_broken_database_backend_does_not_take_the_run_down(monkeypatch):
    """W005 is a warning, and a warning may not be the thing that stops `manage.py check`.

    Reading the engine through `connections[alias]` builds the wrapper, which imports the backend
    module — and an alias naming one that cannot be imported raises `ImproperlyConfigured` from
    inside a rule whose finding is advice. `connections.settings` is the same values without the
    machinery.

    The backend is made unimportable rather than a real broken one configured, because what is on
    trial is the rule's reach: any `ENGINE` Django cannot load produces this.
    """
    from django.db import connections

    def refuse(_alias):
        msg = 'the backend could not be imported'
        raise ImproperlyConfigured(msg)

    monkeypatch.setattr(type(connections), '__getitem__', lambda _self, alias: refuse(alias))
    settings = {
        'TOKEN': '42:x',
        'REDIS_URL': 'redis://localhost',
        'EVENT_LOG': True,
        'EVENT_LOG_DATABASE': 'default',
    }

    with override_settings(TELEGRAM_BOT=settings):
        reported = [message.id for message in check_settings()]

    # that the run answered at all is asserted by reaching this line; that the *rule* answered is
    # asserted here, since a rule which caught the error and returned nothing would also not raise
    assert 'django_aiogram.W005' in reported, reported


@pytest.mark.parametrize(
    ('key', 'identifier', 'value'),
    [
        ('WEBHOOK_ALLOWED_UPDATES', 'django_aiogram.E029', {'message': True}),
        ('EVENT_LOG_KINDS', 'django_aiogram.E032', {'outbound.sent': True}),
        ('EVENT_LOG_REDACT_KEYS', 'django_aiogram.E035', {'token': True}),
    ],
    ids=['allowed updates', 'kinds', 'redact keys'],
)
def test_a_mapping_is_refused_where_a_list_is_meant(key, identifier, value):
    """A dict passes every other test these rules make, and means something else downstream.

    It *is* a collection — of its keys — so `list()` on it produces the names and drops the values
    without a word. `webhook_settings` registers those keys as the allowed updates; the log's two
    settings become a frozenset of them. Somebody who wrote `{'message': True}` meant something,
    and the package would do a different thing quietly.

    A set is not refused: iterating one gives back what was written.
    """
    settings = {'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost', 'EVENT_LOG': True, key: value}
    with override_settings(TELEGRAM_BOT=settings):
        found = [message for message in check_settings() if message.id == identifier]

    assert len(found) == 1, f'{identifier} reported {[message.msg for message in found]}'
    assert 'dict' in found[0].msg, found[0].msg


@pytest.mark.parametrize(
    ('key', 'identifier'),
    [
        ('WEBHOOK_ALLOWED_UPDATES', 'django_aiogram.E029'),
        ('EVENT_LOG_KINDS', 'django_aiogram.E032'),
        ('EVENT_LOG_REDACT_KEYS', 'django_aiogram.E035'),
    ],
    ids=['allowed updates', 'kinds', 'redact keys'],
)
def test_a_set_is_still_a_collection(key, identifier):
    """The other half, so the refusal above cannot quietly become "anything but a list or tuple"."""
    values = {
        'WEBHOOK_ALLOWED_UPDATES': {'message'},
        'EVENT_LOG_KINDS': {'outbound.sent'},
        'EVENT_LOG_REDACT_KEYS': {'token'},
    }
    settings = {'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost', 'EVENT_LOG': True, key: values[key]}

    with override_settings(TELEGRAM_BOT=settings):
        found = [message for message in check_settings() if message.id == identifier]

    assert found == [], [message.msg for message in found]


@pytest.mark.parametrize('rate', [float('nan'), float('inf'), float('-inf')], ids=repr)
def test_a_rate_limit_that_is_not_a_number_is_refused(rate):
    """`nan` is a float and beats every comparison, which is how it passed a bound check.

    And it does not stop there: the limiter compares against the budget too, and every comparison
    against `nan` is false — so a budget of `nan` admits every message rather than none, which is
    the opposite of what somebody configuring a rate limit wanted. The infinities are refused with
    it: a bound nothing can exceed is a limiter that is not one.
    """
    settings = {'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost', 'RATE_LIMIT': {'overall_per_second': rate}}
    with override_settings(TELEGRAM_BOT=settings):
        found = [message for message in check_settings() if message.id == 'django_aiogram.E020']

    assert len(found) == 1, f'E020 reported {[message.msg for message in found]}'
    assert 'non-negative number' in found[0].msg, found[0].msg


@override_settings(
    TELEGRAM_BOT={
        'TOKEN': '42:x',
        'REDIS_URL': 'redis://localhost',
        'EVENT_LOG': True,
        'EVENT_LOG_RETENTION_DAYS': float('inf'),
    }
)
def test_an_infinite_retention_does_not_take_the_run_down():
    """`int(float('inf'))` raises `OverflowError`, which is neither of the two that were caught.

    Found by sweeping the same predicate as the `nan` rate limit rather than by meeting it: a
    numeric guard that compares without asking whether the number is finite. Measured before the
    fix — `manage.py check` ended with `OverflowError: cannot convert float infinity to integer`,
    out of a rule that only warns, taking every other finding with it.
    """
    reported = [message.id for message in check_settings()]

    assert 'django_aiogram.E039' in reported, reported


def test_a_router_class_in_the_setting_is_not_a_router_in_use():
    """Django uses a non-string entry as it stands, so a bare class there is not a router.

    Its `db_for_read` would be called without an instance, which is a `TypeError` at the first
    query rather than this app's log being routed. Reading it as installed silences the one warning
    that would have said so.
    """
    settings = {'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost', 'EVENT_LOG': True, 'EVENT_LOG_DATABASE': 'logs'}
    with override_settings(TELEGRAM_BOT=settings, DATABASE_ROUTERS=[TelegramEventLogRouter]):
        found = [message for message in check_settings() if message.id == 'django_aiogram.I002']

    assert len(found) == 1, f'I002 reported {[message.msg for message in found]}'


def test_a_router_instance_in_the_setting_is_one():
    """The other half: an instance is what Django calls, and this must not start refusing it."""
    settings = {'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost', 'EVENT_LOG': True, 'EVENT_LOG_DATABASE': 'logs'}
    with override_settings(TELEGRAM_BOT=settings, DATABASE_ROUTERS=[TelegramEventLogRouter()]):
        found = [message for message in check_settings() if message.id == 'django_aiogram.I002']

    assert found == [], [message.msg for message in found]
