"""The layout is an API decision, so it is asserted rather than described.

`AGENTS.md` says where a module lives and why. These tests are the half of that a reader
cannot forget: a package with no declared exports, and a published path that quietly moved.
"""

import ast
import pathlib

import pytest

SRC = pathlib.Path(__file__).resolve().parent.parent / 'src' / 'django_aiogram'

#: Django finds these by path and imports nothing *from* them, so there is nothing to
#: export and an `__all__` would be a claim about an empty room
DJANGO_OWNED = frozenset({'management', 'management/commands', 'migrations'})

#: every package inside the app, found rather than listed — a new one is covered the day it
#: appears, which is the point
PACKAGES = sorted(
    name
    for name in (p.parent.relative_to(SRC).as_posix() for p in SRC.rglob('__init__.py') if p.parent != SRC)
    if name not in DJANGO_OWNED
)

#: paths the outside world names: a settings string, a urls entry, a compose command. Moving
#: one of these is a breaking change for every project that wrote it down
PUBLISHED = (
    'django_aiogram/apps.py',
    'django_aiogram/models.py',
    'django_aiogram/admin.py',
    'django_aiogram/healthcheck.py',
    'django_aiogram/exceptions.py',
    'django_aiogram/consumer/webhook.py',
    'django_aiogram/consumer/delivery.py',
    'django_aiogram/wire/serializers.py',
    'django_aiogram/eventlog/dbrouter.py',
    'django_aiogram/eventlog/signals.py',
    'django_aiogram/config/settings.py',
    'django_aiogram/config/enums.py',
)


@pytest.mark.parametrize('package', PACKAGES)
def test_every_package_declares_what_it_exports(package):
    """A package with no `__all__` exports whatever it happened to import.

    That is how a name becomes public without anyone deciding it should be, and once a
    project imports it the name cannot be moved. The cluster packages declare it empty on
    purpose: callers import from the modules, so there is one path to each name.
    """
    source = (SRC / package / '__init__.py').read_text(encoding='utf-8')

    assert any(_assigns_all(node) for node in ast.parse(source).body), f'{package}/__init__.py does not declare __all__'


def _assigns_all(node: ast.stmt) -> bool:
    """Whether this statement assigns `__all__`, annotated or not.

    Both forms appear on purpose: the cluster packages annotate an empty tuple, and a
    package with real exports writes the plain `__all__ = (...)`. Reaching for `.target`
    as a `getattr` default would evaluate it on every node — including the plain
    assignments, which have `.targets` and no `.target` — and turn a clean failure into an
    `AttributeError` from inside the test.
    """
    if isinstance(node, ast.AnnAssign):
        return getattr(node.target, 'id', None) == '__all__'
    if isinstance(node, ast.Assign):
        return any(getattr(target, 'id', None) == '__all__' for target in node.targets)
    return False


@pytest.mark.parametrize('path', PUBLISHED)
def test_a_published_path_is_where_it_says_it_is(path):
    """These are named in strings a project wrote, so a move is a break rather than a tidy.

    `DELIVERY`, `SERIALIZER` and `DATABASE_ROUTERS` hold dotted paths; `urls.py` holds the
    view; a compose healthcheck runs `python -m django_aiogram.healthcheck`. None of that is
    found by an import a test would notice — it fails in the project, on startup, after a
    release. So the location is pinned here, and moving one is a deliberate act with an
    entry in `Upgrading.md`.
    """
    assert (SRC.parent / path).is_file(), f'{path} moved, and something out there names it'
