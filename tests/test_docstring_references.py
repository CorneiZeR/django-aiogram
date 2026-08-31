"""Every `:mod:`, `:meth:` and `:attr:` reference into this package names something that is there.

Prose is the one part of the package no gate reads. `ruff` and `mypy` see code, the suite sees
behaviour, and a docstring that sends a reader to a function which moved two releases ago passes
all three — while being the only thing that reader has to go on.

This release moved a great deal: two files were split into nine, and the four modules under each
of them took names with them. What that left behind was found by hand, twice, which is once more
than it should take:

* `eventlog/signals.py` pointed at `EventRecorder._publish`, a method the split deleted — the
  fan-out is `eventlog.publishing.publish` now.
* `broker/redis_list/broker.py` pointed at `django_aiogram.broker.Broker`, a path the package
  deliberately does not provide: `broker/__init__.py` is empty on purpose so that there is one
  path to `Broker` and not two.

So the rule is checked rather than remembered. Only references *into this package* are resolved —
`aiogram.types.Message` is somebody else's to keep — and resolution is static, by reading the
syntax tree of the module named, so the test needs no import and no settings.

**A name that resolves is not the same as a name at its home.** `recorder.Event` still resolves,
because the recorder imports `Event` from `records`, and this test says nothing about that: it
catches the reference that leads nowhere, which is the one a reader cannot recover from.
"""

import ast
import re
from pathlib import Path

import pytest

SOURCE = Path(__file__).resolve().parent.parent / 'src'
MODULES = sorted(SOURCE.rglob('*.py'))
PACKAGE = 'django_aiogram'
#: the roles that name something inside a module. `:doc:` and `:ref:` name pages, not code
REFERENCE = re.compile(r':(mod|class|meth|func|attr|data|exc):`~?([A-Za-z_][\w.]*)`')


def module_file(dotted: str) -> Path | None:
    """The file a dotted module path names, or None when this package has no such module."""
    if not dotted.startswith(PACKAGE):
        return None
    relative = dotted.replace('.', '/')
    for candidate in (SOURCE / f'{relative}.py', SOURCE / relative / '__init__.py'):
        if candidate.is_file():
            return candidate
    return None


def bound_names(path: Path) -> set[str]:
    """Every name a module binds at the top level, plus one level inside its classes.

    Imports count: a module that re-exports a name does provide it, and a reference through the
    re-export leads somewhere. `if TYPE_CHECKING:` blocks count for the same reason — the name is
    reachable to a reader following the reference, which is what a docstring is for.
    """
    found: set[str] = set()
    body = list(ast.parse(path.read_text(encoding='utf-8')).body)
    # one level into a top-level `if`, which is where TYPE_CHECKING imports live
    for node in list(body):
        if isinstance(node, ast.If):
            body.extend(node.body)
    for node in body:
        if isinstance(node, ast.ClassDef):
            found.add(node.name)
            found.update(f'{node.name}.{name}' for name in _members(node))
        elif isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            found.add(node.name)
        elif isinstance(node, ast.Assign):
            found.update(target.id for target in node.targets if isinstance(target, ast.Name))
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            found.add(node.target.id)
        elif isinstance(node, ast.Import | ast.ImportFrom):
            found.update(alias.asname or alias.name.split('.')[0] for alias in node.names)
    return found


def _members(node: ast.ClassDef) -> set[str]:
    """What a class body binds: methods, class attributes, annotated declarations."""
    members: set[str] = set()
    for inner in node.body:
        if isinstance(inner, ast.FunctionDef | ast.AsyncFunctionDef):
            members.add(inner.name)
        elif isinstance(inner, ast.AnnAssign) and isinstance(inner.target, ast.Name):
            members.add(inner.target.id)
        elif isinstance(inner, ast.Assign):
            members.update(target.id for target in inner.targets if isinstance(target, ast.Name))
    return members


def unresolved(path: Path) -> list[str]:
    """Every reference in one module that names nothing this package has."""
    missing = []
    for role, target in REFERENCE.findall(path.read_text(encoding='utf-8')):
        if not target.startswith(PACKAGE):
            continue
        if role == 'mod':
            if module_file(target) is None:
                missing.append(f'{target} (no such module)')
            continue
        parts = target.split('.')
        for cut in range(len(parts) - 1, 0, -1):
            holder = module_file('.'.join(parts[:cut]))
            if holder is None:
                continue
            tail = '.'.join(parts[cut:])
            if tail not in bound_names(holder):
                missing.append(f'{target} ({tail} is not in {holder.name})')
            break
        else:
            missing.append(f'{target} (nothing on that path is a module)')
    return missing


def test_there_are_references_to_resolve():
    """The control: a regex that stopped matching would report a clean package for ever."""
    total = sum(len(REFERENCE.findall(path.read_text(encoding='utf-8'))) for path in MODULES)

    assert total > 100, f'only {total} references found, so the pattern has stopped reading them'


@pytest.mark.parametrize('path', MODULES, ids=lambda path: str(path.relative_to(SOURCE)))
def test_every_reference_into_this_package_resolves(path):
    """A docstring that names a moved function is the only thing a reader has, and it is wrong."""
    missing = unresolved(path)

    assert not missing, 'references that lead nowhere:\n  ' + '\n  '.join(missing)


@pytest.mark.parametrize(
    ('source', 'expected'),
    [
        (':meth:`~django_aiogram.eventlog.recorder.EventRecorder.flush`', []),
        (':meth:`~django_aiogram.eventlog.recorder.EventRecorder.gone`', ['gone is not in recorder.py']),
        (':mod:`django_aiogram.eventlog.publishing`', []),
        (':mod:`django_aiogram.eventlog.nowhere`', ['no such module']),
        # somebody else's package, and none of this test's business
        (':class:`aiogram.types.Message`', []),
        # a name reached through a re-export resolves, which is the point of counting imports
        (':class:`~django_aiogram.eventlog.recorder.Event`', []),
    ],
    ids=['a real method', 'a missing method', 'a real module', 'a missing module', 'not ours', 'a re-export'],
)
def test_the_resolver_answers_each_shape(source, expected, tmp_path):
    """The resolver is the test, so its own answers are pinned rather than assumed."""
    scratch = tmp_path / 'sample.py'
    scratch.write_text(f'"""{source}"""\n', encoding='utf-8')

    missing = unresolved(scratch)

    assert len(missing) == len(expected), f'{source} -> {missing}'
    for found, wanted in zip(missing, expected, strict=True):
        assert wanted in found, f'{found} does not mention {wanted}'
