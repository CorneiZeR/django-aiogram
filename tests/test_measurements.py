"""The script that produces the recorded numbers has to measure what the transport does.

`scripts/measurements` is not run in CI and cannot be: it needs a broker and answers with a
number rather than a pass. What *can* be checked without one is the part that went wrong — the
script measured a single confirmed publish at `linger.ms` 0 while the transport took the
driver's default of 5ms, so the figure it reported was one no `bot.send()` could reach, and it
had reached the changelog, the settings page and three docstrings before anybody asked.

So this reads both files as source and compares them, rather than trusting either. Nothing here
imports a driver: the script imports `aiokafka` at module scope and that lives in the `measure`
group, which a test environment has no reason to install.
"""

import ast
import pathlib

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
