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


SETTINGS_BLOCK = re.compile(r'^TELEGRAM_BOT_DEFAULTS = (\{.*?^\})', re.DOTALL | re.MULTILINE)

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
    """Every `TELEGRAM_BOT_DEFAULTS = {...}` in the docs that names one of the published enums."""
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
                if not prose(source):
                    UNREADABLE.append((path.name, source.splitlines()[0]))
                continue
            settings = {resolve(key): resolve(value) for key, value in zip(tree.keys, tree.values, strict=True)}
            yield path.name, settings


SETTINGS_EXAMPLES = list(documented_settings())


def test_there_is_a_documented_settings_block_to_check():
    """An empty parametrize is a skipped test, and a skipped test is a guard that is not there.

    The same reason the LOGGING examples have this above them: the block below asserts that a
    documented spelling works, and a regex that stopped matching would take that assertion with it
    without failing anything.
    """
    assert SETTINGS_EXAMPLES, 'no TELEGRAM_BOT_DEFAULTS example naming a published enum was found in the docs'


@pytest.mark.parametrize(
    ('name', 'settings'),
    SETTINGS_EXAMPLES,
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
    from django_aiogram.config.bots import defaults_record
    from django_aiogram.config.checks.conditions import _redis_fsm_storage
    from django_aiogram.consumer.webhook import current_mode
    from django_aiogram.wire.payloads import detail_level
    from django_aiogram.wire.serializers import get_serializer

    readers = {
        'MODE': lambda value: current_mode() == UpdateMode(value).value,
        'EVENT_LOG_PAYLOAD': lambda value: detail_level() is PayloadDetail(value),
        'FSM_STORAGE': lambda value: _redis_fsm_storage(defaults_record()) is (StorageKind(value) is StorageKind.REDIS),
        # pickle is excluded below rather than here: reading it needs `ALLOW_PICKLE`, which is a
        # different rule's subject and not what this case is about
        'SERIALIZER': lambda value: get_serializer().name == SerializerKind(value).value,
    }
    driven = [key for key in settings if key in readers and settings[key] != SerializerKind.PICKLE]

    # a block naming an enum on a key nothing here drives would otherwise skip every assertion and
    # pass -- a case that cannot fail, about a page that promises behaviour
    assert driven, f'{name} names an enum but sets nothing this drives; add the setting to `readers`'
    with override_settings(TELEGRAM_BOT_DEFAULTS={'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost', **settings}):
        for key in driven:
            assert readers[key](settings[key]), f'{name}: {key}'


#: the multi-bot blocks, which are the ones a reader copies to serve a second client
BOTS_BLOCK = re.compile(r'^TELEGRAM_BOTS = (\{.*?^\})', re.DOTALL | re.MULTILINE)
#: what a token looks like, so a page writing `'...'` where one goes is read as the placeholder
#: it is rather than as a credential this could not parse
TOKEN_SHAPE = re.compile(r'\d+:\S+')

#: what a fragment written for a reader carries where the rest of a dict would be. A block
#: holding one is prose and is skipped; a block that is *meant* to be copied and cannot be
#: parsed is a published configuration that does not work, and `test_every_documented_block_is_
#: readable` fails naming the page rather than letting it disappear from the parametrize
PROSE = ('...', '<', '# …', '…')

#: blocks this file could not read, with the page they are on. Filled while harvesting and
#: asserted on below: a block that quietly left the list took its assertions with it
UNREADABLE: list[tuple[str, str]] = []


def prose(source: str) -> bool:
    """Whether this block is a fragment written for a reader rather than one to copy."""
    return any(mark in source for mark in PROSE)


def a_token(alias: str) -> str:
    """A token shaped like Telegram's, for a documented section that reads one from the env.

    The pages write `os.environ['SUPPORT_TOKEN']`, which is what a project should write and
    what nothing here can evaluate. Substituting a token of our own keeps the example on the
    page honest *and* lets this drive it: what is under test is that the section resolves --
    the alias, the identity, the override winning over the default -- and none of that is
    about which characters the credential has.
    """
    return f'{abs(hash(alias)) % 900000 + 100000}:AA{alias}'


def documented_bots(source: str, alias: str):
    """Read one documented section into the values it names, env lookups included."""
    tree = ast.parse(source, mode='eval').body

    def value(node):
        """Resolve one node: a literal, an enum member, or a token read from the environment."""
        if isinstance(node, ast.Subscript | ast.Call):
            # `os.environ['X']` and `os.environ.get('X')`: a credential, whichever way it is read
            return a_token(alias)
        return resolve(node)

    written = {resolve(key): value(item) for key, item in zip(tree.keys, tree.values, strict=True)}
    if not TOKEN_SHAPE.fullmatch(str(written.get('TOKEN', ''))):
        # a page that writes `'...'` where the credential goes, which is the right thing for a
        # page to write and not something this can resolve. The shape is what is under test
        written['TOKEN'] = a_token(alias)
    return written


def multi_bot_examples():
    """Every `TELEGRAM_BOTS = {...}` block the documentation tells a project to write."""
    for path in DOCS:
        if not path.is_file():
            continue
        for match in BOTS_BLOCK.finditer(path.read_text(encoding='utf-8')):
            source = match.group(1)
            try:
                sections = ast.parse(source, mode='eval').body
                written = {
                    resolve(alias): documented_bots(ast.unparse(section), resolve(alias))
                    for alias, section in zip(sections.keys, sections.values, strict=True)
                }
            except (SyntaxError, ValueError):
                # prose is skipped and everything else is reported: a block a reader is meant
                # to copy, which this cannot read, is a published configuration that does not
                # work -- and dropping it here would take its assertions with it, silently
                if not prose(source):
                    UNREADABLE.append((path.name, source.splitlines()[0]))
                continue
            yield path.name, written


BOT_EXAMPLES = list(multi_bot_examples())


def test_every_documented_block_is_readable():
    """A block nothing here could parse is one nothing here checks, which is the worse failure.

    The harvesters above skip what they cannot read, and a skip is invisible: the page keeps a
    configuration that does not work and this file keeps passing, because another block in
    another page still gives the parametrize something to run. So what is skipped is recorded,
    and a fragment written for a reader says so with an ellipsis or an angle bracket.
    """
    assert UNREADABLE == [], f'documented blocks this suite could not read: {UNREADABLE}'


def test_there_is_a_documented_multi_bot_block_to_check():
    """An empty parametrize is a guard that is not there -- see the two cases above it."""
    assert BOT_EXAMPLES, 'no TELEGRAM_BOTS example was found in the docs'


@pytest.mark.parametrize(
    ('name', 'written'),
    BOT_EXAMPLES,
    ids=lambda value: value if isinstance(value, str) else '',
)
def test_a_documented_multi_bot_block_resolves_to_the_bots_it_names(name, written):
    """The page tells a project to write these sections; the package has to read them as bots.

    Driven through the resolver rather than compared as text, because what a page promises is
    behaviour: every alias resolves to a bot, every bot has the identity its token carries, and
    every setting a section names wins over the shared default -- which is the whole of what
    `TELEGRAM_BOTS` is for, and the one thing a reader cannot check by looking.
    """
    from django_aiogram.config.bots import aliases, record

    # a value each section overrides, so "the override wins" is asserted against something
    defaults = {'REDIS_URL': 'redis://localhost:6379/0', 'RATE_LIMIT': {'overall_per_second': 1}}
    with override_settings(TELEGRAM_BOT_DEFAULTS=defaults, TELEGRAM_BOTS=written):
        assert sorted(aliases()) == sorted(written), f'{name}: the aliases did not resolve'
        for alias, section in written.items():
            found = record(alias)
            assert found.bot_id == int(section['TOKEN'].split(':')[0]), f'{name}: {alias} has the wrong identity'
            for key, value in section.items():
                assert found[key] == value, f'{name}: {alias} did not keep its own {key}'
            for key, value in defaults.items():
                if key not in section:
                    assert found[key] == value, f'{name}: {alias} did not inherit {key}'
