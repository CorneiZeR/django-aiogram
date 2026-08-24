"""The scripts that produce the recorded numbers have to measure what the package does.

`scripts/measurements` is not run in CI and cannot be: each script needs a broker and answers
with a number rather than a pass. What *can* be checked without one is the part that has gone
wrong three times — whether the publish being timed is the publish this package makes. Each time
the script measured a cheaper promise, and each time the number reached the changelog, the
settings page and several docstrings before anybody asked.

So these read the files as source and compare them, rather than trusting either side. Nothing
here imports a driver: the scripts import `aio_pika` and `aiokafka` at module scope and those
live in the `measure` group, which a test environment has no reason to install.
"""

import ast
import pathlib
import re

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent


def parsed(relative):
    """Read one file as a syntax tree, so a driver never has to be importable."""
    return ast.parse((ROOT / relative).read_text())


def calls_to(tree, name):
    """Every call in ``tree`` to something spelled ``name``, however it was imported."""
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        called = node.func.attr if isinstance(node.func, ast.Attribute) else getattr(node.func, 'id', None)
        if called == name:
            found.append(node)
    return found


def keywords(call):
    """The call's keywords as ``{name: source}``, which is enough to compare two spellings."""
    return {keyword.arg: ast.unparse(keyword.value) for keyword in call.keywords}


def assigned(tree):
    """Every ``name = value`` in ``tree`` as ``{name: source}``, at any depth.

    Both files build their properties into a local and pass the local, so resolving the name is
    what lets an assertion follow the value that actually reaches ``basic_publish`` instead of
    settling for a persistent one existing somewhere in the file.
    """
    found = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    found[target.id] = ast.unparse(node.value)
    return found


def producer_config(call):
    """The ``{key: source}`` of a producer built from a dict literal, or ``{}``."""
    if not call.args or not isinstance(call.args[0], ast.Dict):
        return {}
    return {
        ast.literal_eval(key): ast.unparse(value)
        for key, value in zip(call.args[0].keys, call.args[0].values, strict=True)
        if isinstance(key, ast.Constant)
    }


def keyword_args(call):
    """The call's keywords as ``{name: source}``, for a client configured by keyword."""
    return {keyword.arg: ast.unparse(keyword.value) for keyword in call.keywords}


MEASUREMENT = 'scripts/measurements/amqp_driver_choice.py'
TRANSPORT = 'src/django_aiogram/broker/rabbitmq/broker.py'


@pytest.mark.parametrize('source', [MEASUREMENT, TRANSPORT])
def test_every_amqp_publish_is_mandatory_and_persistent(source):
    """Both files publish the same way, or the measurement is timing a promise nobody makes.

    Asserted of the transport as well as the script, because the invariant is that they agree:
    a transport that stopped asking for persistence would leave the script measuring something
    the package no longer does, which is the same defect from the other end.
    """
    tree = parsed(source)
    publishes = calls_to(tree, 'basic_publish')
    assert publishes, f'{source} makes no basic_publish call — has it been renamed?'
    for publish in publishes:
        passed = keywords(publish)
        assert passed.get('mandatory') == 'True', (
            f'{source}: a publish without mandatory=True lets an unroutable message vanish: {passed}'
        )
        assert 'properties' in passed, f'{source}: a publish with no properties cannot be persistent'

        # resolved to what it names rather than taken as read: asserting that *a* persistent
        # BasicProperties exists in the file would pass on a publish handed a different,
        # transient one — which is the shape of every defect this test exists to catch
        names = assigned(tree)
        properties = passed['properties']
        built = names.get(properties, properties)
        assert 'BasicProperties' in built, (
            f'{source}: the properties reaching basic_publish are {built!r}, not a BasicProperties'
        )
        mode = keywords(ast.parse(built).body[0].value).get('delivery_mode', '')
        assert 'PERSISTENT' in mode.upper(), (
            f'{source}: delivery_mode {mode!r} reaches basic_publish, so the broker answers before it writes'
        )


@pytest.mark.parametrize(
    ('command', 'transport'),
    [
        ('rpush', 'src/django_aiogram/broker/redis_list/broker.py'),
        ('xadd', 'src/django_aiogram/broker/redis_streams/broker.py'),
    ],
)
def test_the_baseline_times_the_command_its_transport_publishes_with(command, transport):
    """The divisor has to be the same publish as the thing being divided.

    Five ratios elsewhere are quoted against these two rows, so a baseline that timed a
    cheaper command would understate every one of them — and quietly, because a ratio reads as
    a measurement whichever command produced it.
    """
    # inside what `measure` is handed, not anywhere in the file: a baseline that timed `LLEN`
    # while an `RPUSH` sat two lines above it would otherwise pass, and its row would then be
    # the divisor for five ratios
    timed = [
        call
        for measured in calls_to(parsed('scripts/measurements/redis_baseline.py'), 'measure')
        for argument in measured.args
        for call in calls_to(argument, command)
    ]
    assert timed, f'nothing handed to measure() calls {command}, which one of its own rows names'
    assert len(timed) == 1, f'{len(timed)} timed {command} calls: a row times one publish or it is not that row'

    # exactly two arguments, the key and one payload: `rpush(KEY, BODY, BODY)` would still be an
    # `rpush` handed to `measure`, and it would publish two messages per sample -- halving the
    # divisor that five ratios are quoted against, with nothing here noticing
    passed = [ast.unparse(argument) for argument in timed[0].args]
    assert len(passed) == 2, f'{command} was timed with {passed}, which is not one key and one payload'

    published = calls_to(parsed(transport), command)
    assert published, f'{transport} no longer publishes with {command}, so the baseline is measuring the wrong call'


#: a measured range, in either of the two ways these files write one. `\u2013` is an escape
#: rather than the character because the literal reads as a hyphen at a glance.
#:
#: The unit is optional after `to` and required after a dash, which is not fussiness: prose
#: writes "323 to 393 microseconds against 18 to 20" and means microseconds both times, and that
#: second span is where a drift hid from an earlier version of this pattern. A dash without a
#: unit, meanwhile, is usually a character class -- `[0-9]`, `0-0` -- and never a measurement.
#: The lookahead after the `to` form is what keeps other units out: `0 to 90 000 ms` would
#: otherwise read as the pair (0, 90), and a millisecond measurement is not one of these.
_OTHER_UNIT = r'(?!\s*(?:\d|ms\b|milliseconds|seconds|second\b|s\b|minutes|MiB|MB|KB))'
SPAN = re.compile(
    rf'(\d+(?:\.\d+)?)\s+to\s+(\d+(?:\.\d+)?)(?:\s*(?:\u00b5s|us\b|microseconds))?{_OTHER_UNIT}'
    r'|(\d+(?:\.\d+)?)\s*[\u2013-]\s*(\d+(?:\.\d+)?)\s*(?:\u00b5s|us\b|microseconds)'
)

#: everywhere a measured span is quoted: the code, the packaging comment that explains an extra,
#: and the prose. The changelog is read only as far as its first released heading -- a released
#: section is frozen history and must not be dragged to match a number re-taken since
QUOTING = [
    *sorted(str(path) for path in pathlib.Path('src').rglob('*.py')),
    'pyproject.toml',
    'CHANGELOG.md',
    'docs/wiki/Settings.md',
    'docs/wiki/Delivery.md',
]


def spans(relative):
    """Every microsecond range in a file, with the wrapping flattened first.

    Flattened because these are prose: a span written across a line break matches nothing
    otherwise, which is how `354 to 492` survived three sweeps for it while five other places
    said 351.

    The changelog is read only down to its second `## ` heading. Everything below that is
    released, and a released entry records what was measured then -- holding it to a number
    re-taken since would make the test demand that history be rewritten.
    """
    text = (ROOT / relative).read_text()
    if relative == 'CHANGELOG.md':
        headings = [match.start() for match in re.finditer(r'^## ', text, re.MULTILINE)]
        text = text[headings[0] : headings[1]]
    # two alternatives, so each match carries one pair and two empty strings
    found = SPAN.findall(re.sub(r'\s+', ' ', text))
    return {(low, high) for match in found for low, high in (match[:2], match[2:]) if low}


@pytest.mark.parametrize('source', QUOTING)
def test_every_quoted_span_is_one_the_measurements_recorded(source):
    """A measured range may be quoted anywhere, but only the README decides what it is.

    Docstrings across four transports quote these numbers, and each re-run moves one of them.
    Updating the table and missing a docstring is the failure this catches -- it happened four
    times in this release, twice because the phrase was wrapped across two lines and a search
    for it found nothing.

    Both directions are covered by the one assertion: a span in the source that the README does
    not list is either a docstring nobody updated or a number nobody measured, and there is no
    third case.
    """
    recorded = spans('scripts/measurements/README.md')
    assert recorded, 'the README lists no spans at all -- has its table moved?'

    unrecorded = {f'{low}-{high}' for low, high in spans(source) - recorded}
    assert not unrecorded, f'{source} quotes {sorted(unrecorded)}, which the README does not record'


def test_the_aio_pika_row_publishes_the_same_promise():
    """The other driver's row has to make the same promise, or the comparison is of two things.

    `basic_publish` is pika's spelling; aio-pika publishes through `default_exchange.publish`
    with the durability on the message rather than on the call. Checking only the first leaves
    the row this one is compared against free to drop persistence -- which is the defect the
    whole guarantee-constant rule exists to catch, and it would show up as aio-pika looking
    faster.
    """
    tree = parsed('scripts/measurements/amqp_driver_choice.py')
    # the exchange's method, not any call spelled `publish`: the script also has an inner
    # coroutine of that name, and matching it would assert about the wrong thing
    published = [call for call in calls_to(tree, 'publish') if isinstance(call.func, ast.Attribute)]
    assert published, 'the script no longer publishes through an exchange'

    for call in published:
        passed = keywords(call)
        assert passed.get('mandatory') == 'True', (
            f'an aio-pika publish without mandatory=True lets an unroutable message vanish: {passed}'
        )

    messages = calls_to(tree, 'Message')
    assert messages, 'the script builds no aio_pika.Message'
    for built in messages:
        mode = keywords(built).get('delivery_mode', '')
        assert 'PERSISTENT' in mode.upper(), (
            f'delivery_mode {mode!r} on the message means the broker answers before it writes'
        )


def test_both_drivers_are_measured_at_the_same_batching_setting():
    """Holding the guarantee constant includes holding the batching constant.

    The script's whole claim is that one driver waits less than the other for the same
    acknowledgement. `confluent-kafka` is configured through a dict and `aiokafka` through
    keywords, so it is easy to set one and forget the other — and a run with 0 on one side and
    the driver's default on the other would report a difference that is mostly `linger`.
    """
    tree = parsed('scripts/measurements/kafka_driver_choice.py')
    confluent = producer_config(calls_to(tree, 'Producer')[0])
    aiokafka = keyword_args(calls_to(tree, 'AIOKafkaProducer')[0])

    assert confluent.get('linger.ms') == '0', f'confluent-kafka is not measured at 0: {confluent}'
    assert aiokafka.get('linger_ms') == '0', f'aiokafka is not measured at 0: {aiokafka}'


def test_the_kafka_measurement_lingers_as_little_as_the_transport():
    """One send is a batch of one on both sides, or the script reports a latency nobody gets.

    This is the defect that was found by asking the question: the transport took librdkafka's
    default of 5 milliseconds while the script set 0, so the measured 166-295 microseconds was
    a number no `bot.send()` could reach — the real cost was 6.4 milliseconds.
    """
    script = producer_config(calls_to(parsed('scripts/measurements/kafka_driver_choice.py'), 'Producer')[0])
    transport = producer_config(calls_to(parsed('src/django_aiogram/broker/kafka/client.py'), 'KafkaProducer')[0])
    assert script.get('linger.ms') == '0', f'the script no longer measures at linger.ms 0: {script}'
    assert transport.get('linger.ms') == script.get('linger.ms'), (
        f'the transport lingers differently from the measurement: {transport} against {script}'
    )
