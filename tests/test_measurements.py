"""The scripts that produce the recorded numbers have to measure what the package does.

`scripts/measurements` is not run in CI and cannot be: each script needs a broker and answers
with a number rather than a pass. What *can* be checked without one is the part that has gone
wrong twice — whether the publish being timed is the publish this package makes. Both times the
script was measuring a cheaper promise, and both times the number reached the changelog, the
settings page and three docstrings before anybody noticed.

So these read the two files as source and compare them, rather than trusting either. Nothing
here imports a driver: the scripts import `aio_pika` and `aiokafka` at module scope and those
live in the `measure` group, which a test environment has no reason to install.
"""

import ast
import pathlib

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


def producer_config(call):
    """The ``{key: source}`` of a producer built from a dict literal, or ``{}``."""
    if not call.args or not isinstance(call.args[0], ast.Dict):
        return {}
    return {
        ast.literal_eval(key): ast.unparse(value)
        for key, value in zip(call.args[0].keys, call.args[0].values, strict=True)
        if isinstance(key, ast.Constant)
    }


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

    properties = calls_to(tree, 'BasicProperties')
    assert properties, f'{source} builds no BasicProperties'
    for built in properties:
        mode = keywords(built).get('delivery_mode', '')
        assert 'PERSISTENT' in mode.upper(), (
            f'{source}: delivery_mode {mode!r} is not persistent, so the broker answers before it writes'
        )


def test_the_kafka_measurement_lingers_as_little_as_the_transport():
    """One send is a batch of one on both sides, or the script reports a latency nobody gets.

    This is the defect that was found by asking the question: the transport took librdkafka's
    default of 5 milliseconds while the script set 0, so the measured 166-232 microseconds was
    a number no `bot.send()` could reach — the real cost was 6.4 milliseconds.
    """
    script = producer_config(calls_to(parsed('scripts/measurements/kafka_driver_choice.py'), 'Producer')[0])
    transport = producer_config(calls_to(parsed('src/django_aiogram/broker/kafka/client.py'), 'KafkaProducer')[0])
    assert script.get('linger.ms') == '0', f'the script no longer measures at linger.ms 0: {script}'
    assert transport.get('linger.ms') == script.get('linger.ms'), (
        f'the transport lingers differently from the measurement: {transport} against {script}'
    )
