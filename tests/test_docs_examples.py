"""Configuration examples in the docs have to be copy-pasteable.

The README's LOGGING snippet once referenced a `console` handler it never
defined, so anyone pasting it into settings.py got
`ValueError: Unable to configure logger` at startup.
"""

import ast
import pathlib
import re

import pytest
from django.test import override_settings

from django_aiogram.config.enums import PayloadDetail, SerializerKind, StorageKind, UpdateMode

ROOT = pathlib.Path(__file__).resolve().parent.parent
DOCS = [ROOT / 'README.md', *sorted((ROOT / 'docs' / 'wiki').glob('*.md'))]
LOGGING_BLOCK = re.compile(r'^LOGGING = (\{.*?^\})', re.DOTALL | re.MULTILINE)


def logging_examples():
    for path in DOCS:
        if not path.is_file():
            continue
        for match in LOGGING_BLOCK.finditer(path.read_text(encoding='utf-8')):
            yield path.name, match.group(1)


EXAMPLES = list(logging_examples())


def test_there_is_a_logging_example_to_check():
    assert EXAMPLES, 'no LOGGING example found in the docs'


@pytest.mark.parametrize(('name', 'source'), EXAMPLES, ids=[name for name, _ in EXAMPLES])
def test_every_referenced_handler_is_defined(name, source):
    config = ast.literal_eval(source)
    defined = set(config.get('handlers', {}))
    named = dict(config.get('loggers', {}))
    if 'root' in config:  # dictConfig takes the root logger outside 'loggers'
        named['root'] = config['root']
    for logger, options in named.items():
        missing = set(options.get('handlers', [])) - defined
        assert not missing, f'{name}: logger {logger!r} references undefined handlers {missing}'


SETTINGS_BLOCK = re.compile(r'^TELEGRAM_BOT = (\{.*?^\})', re.DOTALL | re.MULTILINE)

#: the enums the pages tell a project to import, and the readers that turn each setting into
#: behaviour. A documented spelling that one of these cannot read is a page that does not work
ENUMS = {
    'PayloadDetail': PayloadDetail,
    'SerializerKind': SerializerKind,
    'StorageKind': StorageKind,
    'UpdateMode': UpdateMode,
}


def resolve(node):
    """Turn one node of a documented settings literal into the value it names.

    A small evaluator rather than `eval`: the point is partly that the *names* resolve, so an
    `UpdateMode.POLLNIG` in a page is a failure here rather than something a reader discovers.
    """
    if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id in ENUMS:
        return getattr(ENUMS[node.value.id], node.attr)
    return ast.literal_eval(node)


def documented_settings():
    """Every `TELEGRAM_BOT = {...}` in the docs that names one of the published enums."""
    for path in DOCS:
        if not path.is_file():
            continue
        for match in SETTINGS_BLOCK.finditer(path.read_text(encoding='utf-8')):
            source = match.group(1)
            if not any(name in source for name in ENUMS):
                continue
            try:
                tree = ast.parse(source, mode='eval').body
            except SyntaxError:
                continue  # a fragment written for a reader, with prose inside it
            settings = {resolve(key): resolve(value) for key, value in zip(tree.keys, tree.values, strict=True)}
            yield path.name, settings


@pytest.mark.parametrize(
    ('name', 'settings'),
    list(documented_settings()),
    ids=lambda value: value if isinstance(value, str) else '',
)
def test_a_documented_settings_block_configures_what_it_says(name, settings):
    """The page tells a project to write the enum member; the package has to read it as one.

    It did not. `str()` on a member gives its *name* since 3.11, so `'MODE': UpdateMode.POLLING` --
    copied from **API.md** -- raised `ImproperlyConfigured` at startup naming `'updatemode.polling'`,
    a value nobody typed. `'EVENT_LOG_PAYLOAD': PayloadDetail.FULL` was quieter and worse: summaries
    instead of full payloads, with nothing said.

    Driven through the readers rather than compared as text, because what a page promises is
    behaviour. Nothing here knows which settings the block sets, so a page documenting another one
    is covered on the day it is written.
    """
    from django_aiogram.config.checks.conditions import _redis_fsm_storage
    from django_aiogram.consumer.webhook import current_mode
    from django_aiogram.wire.payloads import detail_level
    from django_aiogram.wire.serializers import get_serializer

    readers = {
        'MODE': lambda value: current_mode() == UpdateMode(value).value,
        'EVENT_LOG_PAYLOAD': lambda value: detail_level() is PayloadDetail(value),
        'FSM_STORAGE': lambda value: _redis_fsm_storage() is (StorageKind(value) is StorageKind.REDIS),
        # pickle is excluded below rather than here: reading it needs `ALLOW_PICKLE`, which is a
        # different rule's subject and not what this case is about
        'SERIALIZER': lambda value: get_serializer().name == SerializerKind(value).value,
    }
    driven = [key for key in settings if key in readers and settings[key] != SerializerKind.PICKLE]

    # a block naming an enum on a key nothing here drives would otherwise skip every assertion and
    # pass -- a case that cannot fail, about a page that promises behaviour
    assert driven, f'{name} names an enum but sets nothing this drives; add the setting to `readers`'
    with override_settings(TELEGRAM_BOT={'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost', **settings}):
        for key in driven:
            assert readers[key](settings[key]), f'{name}: {key}'
