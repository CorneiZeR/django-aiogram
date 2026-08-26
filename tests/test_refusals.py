"""Every refusal keeps what it was told, or says here why it does not.

`ProduceRefusedError` keeps the topic and what librdkafka said, as attributes and in the
message, so a caller deciding whether to retry does not have to match on a sentence.
`QueueRefusedError` -- the same shape, the other transport -- formatted the reason away and kept
only the queue, so the same decision on RabbitMQ meant parsing English. That was one instance of
something wider: an exception composes a message out of what it was handed and then drops it, and
nothing anywhere says whether that was a decision.

So the rule is written down instead of remembered: a parameter of a refusal's `__init__` is kept
as an attribute of the same name, or it appears in `EXEMPT` with a reason. Neither list is
derived from the classes -- a mapping generated from signatures would agree with any signature --
so a new refusal fails this module until somebody decides which of the two it is.

Private classes are out of scope. `_UnhealthyError` is the healthcheck's own control flow and
never leaves the process that raised it, and a caller that cannot see a class cannot branch on
its attributes.
"""

import importlib
import inspect
import pathlib
import pkgutil
import re

import pytest

import django_aiogram

#: what to hand each refusal, per class. Written out because the point is to construct a real
#: one: `LoopThreadNotStartedError` formats a float into a sentence, `UnsupportedInputFileError`
#: reads the type of what it is given, and a synthesised argument of the wrong kind would make
#: this module fail for a reason that is not the one it is about
ARGUMENTS = {
    'BrokerDependencyError': {'broker': 'pkg.mod.Broker', 'module': 'pika', 'extra': 'rabbitmq'},
    'WorkerDepthUnavailableError': {'broker': 'KafkaBroker', 'worker': 'bot-7'},
    'ProduceRefusedError': {'topic': 'telegram-outbound', 'reason': 'Unknown topic or partition'},
    'QueueRefusedError': {'queue': 'telegram-outbound', 'reason': 'NO_ROUTE'},
    'StreamServerTooOldError': {},
    'StreamLagUnknownError': {'key': 'telegram-stream', 'group': 'workers'},
    'ShuttingDownError': {},
    'LoopThreadNotStartedError': {'timeout': 2.5},
    'UnknownApiMethodError': {'function': 'download_file', 'method_count': 331},
    'MalformedEnvelopeError': {'found': 'a decoded str, not a mapping'},
    'UnknownEnvelopeVersionError': {'version': 9},
    'UnsupportedInputFileError': {'value': object()},
    'UnknownInputFileKindError': {'kind': 'tarball'},
    'UnknownModelError': {'name': 'Sticker'},
    'NonMappingPayloadError': {},
    'UnknownSerializerError': {'name': 'yaml'},
    'PickleReadRefusedError': {},
    'PickleWriteRefusedError': {'settings_name': 'TELEGRAM_BOT'},
    'EventLogRefusedError': {'count': 3},
}

#: parameters deliberately not kept, and why. Each of these is a value the holder of the
#: exception can already answer for, which is the whole test: an attribute is for what only the
#: raiser knew
EXEMPT = {
    ('UnknownApiMethodError', 'method_count'): (
        'the length of the installed aiogram method table, which anyone holding this can read'
    ),
    ('MalformedEnvelopeError', 'found'): (
        'what it is handed is already a sentence -- "a decoded str, not a mapping" -- composed at '
        'the raise site precisely so the untrusted value itself never travels'
    ),
    ('UnsupportedInputFileError', 'value'): 'the caller passed the object; only its type is named',
    ('UnknownSerializerError', 'name'): "the project's own SERIALIZER setting, which the caller can read",
    ('PickleWriteRefusedError', 'settings_name'): "this package's own settings key, and there is one",
    ('EventLogRefusedError', 'count'): (
        'it never escapes the flush that raises it -- so there is no third-party caller to branch '
        'on it, and the one place that catches it holds the batch it was counted from'
    ),
}


def refusals():
    """Every public exception the package defines that builds its own message.

    Walked rather than listed: a module added later would not be in a list, and this whole
    module exists because a class arrived without anybody deciding this question about it.
    """
    for info in pkgutil.walk_packages(django_aiogram.__path__, 'django_aiogram.'):
        importlib.import_module(info.name)
    found = {}
    stack = [Exception]
    while stack:
        for cls in stack.pop().__subclasses__():
            if not cls.__module__.startswith('django_aiogram.') or cls.__name__.startswith('_'):
                stack.append(cls)
                continue
            stack.append(cls)
            if '__init__' in cls.__dict__:
                found[cls.__name__] = cls
    return found


REFUSALS = refusals()
PARAMETERS = [
    (name, parameter)
    for name, cls in sorted(REFUSALS.items())
    for parameter in inspect.signature(cls.__init__).parameters
    if parameter != 'self'
]


def test_every_refusal_that_builds_a_message_is_accounted_for():
    """A new one fails here, which is the point: the decision is not optional.

    Both directions. A class that leaves the package has to leave `ARGUMENTS` with it, or the
    next reader is looking at a rule about something that no longer exists.
    """
    assert set(REFUSALS) == set(ARGUMENTS), (
        f'undeclared: {sorted(set(REFUSALS) - set(ARGUMENTS))}; gone: {sorted(set(ARGUMENTS) - set(REFUSALS))}'
    )
    for name, cls in REFUSALS.items():
        expected = {p for p in inspect.signature(cls.__init__).parameters if p != 'self'}
        assert set(ARGUMENTS[name]) == expected, (
            f'{name} takes {sorted(expected)}, this module hands it {sorted(ARGUMENTS[name])}'
        )


@pytest.mark.parametrize(('name', 'parameter'), PARAMETERS, ids=[f'{n}.{p}' for n, p in PARAMETERS])
def test_a_refusal_keeps_what_it_was_told(name, parameter):
    """Kept as an attribute of the same name, or exempt with a reason -- never silently dropped."""
    given = ARGUMENTS[name]
    refusal = REFUSALS[name](**given)
    reason = EXEMPT.get((name, parameter))

    if reason:
        assert not hasattr(refusal, parameter), (
            f'{name}.{parameter} is exempt here but the class keeps it; the exemption is stale'
        )
        assert len(reason) > 20, f'{name}.{parameter} is exempt without a reason worth reading'
        return
    assert getattr(refusal, parameter, None) == given[parameter], (
        f'{name} formats {parameter} into its message and keeps nothing, so a caller has to read English'
    )


def kept(cls):
    """The parameters this refusal keeps, which is what a caller may act on."""
    return {
        parameter
        for parameter in inspect.signature(cls.__init__).parameters
        if parameter != 'self' and (cls.__name__, parameter) not in EXEMPT
    }


def errors_section():
    """The `## Errors` section of the API page, which is where the surface is published."""
    page = (pathlib.Path(__file__).resolve().parent.parent / 'docs' / 'wiki' / 'API.md').read_text(encoding='utf-8')
    assert '\n## Errors\n' in page, 'the API page no longer has the section this reads'
    return page.split('\n## Errors\n', 1)[1].split('\n## ', 1)[0]


def published(name, section):
    """What the page says this refusal carries, read off the one row or sentence about it.

    Per row rather than per section on purpose: `name` is carried by one refusal and exempt on
    another, so a section-wide search would find the first one's attribute while checking the
    second and call the page wrong.
    """
    for line in section.splitlines():
        if line.startswith('|') and f'`{name}`' in line:
            # the last cell, not the whole row: the middle one says when the refusal is raised
            # and is entitled to name the method that raises it, which read as an attribute and
            # made this case fail on a row that was perfectly correct
            cells = [cell.strip() for cell in line.strip().strip('|').split('|')]
            return set(re.findall(r'`([a-z_]+)`', cells[-1]))
    prose = re.sub(r'\s+', ' ', '\n'.join(line for line in section.splitlines() if not line.startswith('|')))
    for sentence in prose.split('. '):
        if f'`{name}`' in sentence:
            return set(re.findall(r'`([a-z_]+)`', sentence))
    return None


@pytest.mark.parametrize('name', sorted(REFUSALS))
def test_the_api_page_names_what_a_refusal_it_documents_carries(name):
    """An attribute is public the moment a caller is told to read it, so the page has to say so.

    Scoped to the refusals the page names rather than to all of them: a transport's internal
    refusal is not published there and a rule demanding it would be argued away every time.
    Both directions for the ones it does name, so the page can neither miss an attribute nor
    promise one that has gone.
    """
    section = errors_section()
    listed = published(name, section)
    if listed is None:
        pytest.skip(f'{name} is not published on the API page')

    assert listed == kept(REFUSALS[name]), (
        f'the page says {name} carries {sorted(listed)}, the class keeps {sorted(kept(REFUSALS[name]))}'
    )


def test_both_transports_refuse_a_publish_with_the_same_pair():
    """The finding this module started from, asserted rather than described.

    Named separately from the sweep above because it is the *symmetry* that matters: the two
    transports answer the same question for the same caller, and one of them made it harder.
    """
    from django_aiogram.broker.kafka.exceptions import ProduceRefusedError
    from django_aiogram.broker.rabbitmq.exceptions import QueueRefusedError

    kafka = ProduceRefusedError('telegram-outbound', 'Unknown topic or partition')
    rabbit = QueueRefusedError('telegram-outbound', 'NO_ROUTE')

    assert (kafka.topic, kafka.reason) == ('telegram-outbound', 'Unknown topic or partition')
    assert (rabbit.queue, rabbit.reason) == ('telegram-outbound', 'NO_ROUTE')
    assert 'NO_ROUTE' in str(rabbit), 'the reason left the message when it became an attribute'
