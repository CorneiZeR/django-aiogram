"""Sentences the documentation says about this package, held to what the package does.

A universal claim -- "every X", "only Y", "never Z" -- is cheap to write, expensive to be
wrong about, and verifiable in minutes. The three here were each written down and then not
checked by anything, which is how a page keeps promising something the code stopped doing.

Each is quoted from the page that makes it, and each fails if the behaviour moves.

---

**Every boolean setting is parsed**, which is a sentence the documentation says out loud.

`Settings.md` promises that `'false'`, `'no'`, `'off'` and `0` all mean false wherever a
boolean is accepted, and that anything unparseable raises rather than reading as true. That
promise is the difference between a container started with `DJANGO_AIOGRAM_ENABLED=false` and
one that sends messages anyway, because the environment can only hand a process a string and
every non-empty string is truthy.

It was also, until this file, a claim nothing checked. `RAISE_EXCEPTION` really was read with a
bare `if` once — the page says so — and the only thing that would have caught the next one was
somebody remembering. Verified when this was written: fifteen reads across nine modules, all
of them through `coerce_bool`, and none reaching the settings any other way.

Read from the syntax tree rather than by grepping lines, because the call wraps: a `coerce_bool`
whose argument sits on the next line is the shape this package writes most often, and a
line-oriented check either misses it or has to guess.
"""

import ast
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
SOURCE = ROOT / 'src' / 'django_aiogram'
MODULES = sorted(SOURCE.rglob('*.py'))


def boolean_settings() -> set[str]:
    """Every setting whose default is a boolean: the package's own and each transport's."""
    from django.utils.module_loading import import_string

    from django_aiogram.broker.registry import SHIPPED
    from django_aiogram.config.defaults import DEFAULTS

    names = {name for name, value in DEFAULTS.items() if isinstance(value, bool)}
    for path in SHIPPED:
        options = getattr(import_string(path), 'OPTIONS', {})
        names |= {name for name, value in options.items() if isinstance(value, bool)}
    return names


def reads_of(tree: ast.AST, names: set[str]) -> list[tuple[int, bool]]:
    """Every read of one of those settings, with whether a `coerce_bool` encloses it.

    Two shapes, one walker: `conf['X']` and `conf.get('X')`. The first draft of this used a
    separate pass for the second shape and marked every one of them unparsed -- so it reported
    the two `conf.get('ALLOW_PICKLE')` reads in the checks, which are wrapped, and the failure
    was in the test rather than in the package.
    """
    found: list[tuple[int, bool]] = []
    depth: list[bool] = [False]

    class Walk(ast.NodeVisitor):
        def visit_Call(self, node: ast.Call) -> None:
            parsing = isinstance(node.func, ast.Name) and node.func.id == 'coerce_bool'
            reader = isinstance(node.func, ast.Attribute) and node.func.attr == 'get'
            named = any(isinstance(argument, ast.Constant) and argument.value in names for argument in node.args)
            if reader and named:
                found.append((node.lineno, depth[-1]))
            depth.append(parsing or depth[-1])
            self.generic_visit(node)
            depth.pop()

        def visit_Subscript(self, node: ast.Subscript) -> None:
            named = isinstance(node.slice, ast.Constant) and node.slice.value in names
            reader = isinstance(node.value, ast.Name) and node.value.id == 'conf'
            if named and reader:
                found.append((node.lineno, depth[-1]))
            self.generic_visit(node)

    Walk().visit(tree)
    return found


def test_there_are_boolean_settings_to_check():
    """The control: a rule that finds nothing to judge is a rule that always passes."""
    assert len(boolean_settings()) >= 5, sorted(boolean_settings())


@pytest.mark.parametrize('path', MODULES, ids=lambda path: str(path.relative_to(SOURCE)))
def test_every_boolean_setting_is_read_through_the_parser(path):
    """A bare `if conf['ENABLED']` reads the string `'false'` as true.

    Which is exactly what `DJANGO_AIOGRAM_ENABLED=false` puts there, and the failure is silent:
    the process sends, the setting says it should not, and nothing in the logs disagrees.
    """
    names = boolean_settings()
    tree = ast.parse(path.read_text(encoding='utf-8'))

    raw = [f'{path.name}:{line}' for line, parsed in reads_of(tree, names) if not parsed]

    assert not raw, f'these read a boolean setting without coerce_bool: {raw}'


#: claims the pages make about *this package's* behaviour, in the form "every X", "only Y",
#: "never Z". Each is one sentence somebody will rely on and nothing else checks. They are
#: here rather than each beside its own subject because what they have in common is the
#: shape: a universal quantifier, which is exactly the kind of sentence that is cheap to
#: write, expensive to be wrong about, and verifiable in minutes
def test_the_event_log_is_only_ever_inserted():
    """Event-log says its rows are "inserted and never updated", and readers plan around it.

    A row that could be updated is a row whose history is not a history: the page tells people
    to join on `correlation_id` and read the sequence as what happened, and an exporter reading
    the table twice would have to reconcile. The writer is the only thing that writes.
    """
    writer = (SOURCE / 'eventlog' / 'writer.py').read_text(encoding='utf-8')
    tree = ast.parse(writer)

    saves = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == 'save'
    ]
    updates = [
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in {'update', 'bulk_update', 'update_or_create'}
    ]

    assert saves, 'the writer no longer saves anything the way this test reads it'
    for save in saves:
        forced = [keyword for keyword in save.keywords if keyword.arg == 'force_insert']
        assert forced, f'a save without force_insert at writer.py:{save.lineno} could update a row'
        assert forced[0].value.value is True, f'force_insert is not True at writer.py:{save.lineno}'
    assert not updates, f'the event log updates rows: {updates}'


def test_every_aiogram_model_is_tagged_with_its_class_name():
    """Serialization says so, and decoding is what depends on it.

    The tag is how the reader finds the class again: a payload carrying the fields of an
    `InlineKeyboardMarkup` and no name is a dict, and the send fails somewhere else entirely.
    """
    from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, LinkPreviewOptions

    from django_aiogram.wire.serializers import ModelCodec, SerializationTag

    codec = ModelCodec()
    for model in (
        LinkPreviewOptions(is_disabled=True),
        InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='ok', callback_data='ok')]]),
    ):
        encoded = codec.encode(model)

        assert encoded[SerializationTag.MODEL] == type(model).__name__, encoded
