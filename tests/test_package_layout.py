"""The layout is an API decision, so it is asserted rather than described.

`AGENTS.md` says where a module lives and why. These tests are the half of that a reader
cannot forget: a package with no declared exports, and a published path that quietly moved.
"""

import ast
import pathlib

import pytest

from django_aiogram.broker.registry import SHIPPED
from tests.support import run_python

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
#: what `BROKER` may hold, spelled out. A project typed one of these into its settings, so
#: these strings are as public as any function this package exports — and the point of writing
#: them here is that renaming the class and its registry entry together cannot pass unnoticed
PUBLISHED_BROKERS = (
    'django_aiogram.broker.redis_list.RedisListBroker',
    'django_aiogram.broker.redis_streams.RedisStreamsBroker',
    'django_aiogram.broker.rabbitmq.RabbitMQBroker',
    'django_aiogram.broker.kafka.KafkaBroker',
)

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
        # a value, not just an annotation: `__all__: tuple[str, ...]` with nothing after it
        # is a promise to a type checker and binds no name at runtime, so a package
        # carrying only that exports whatever it happened to import
        return node.value is not None and getattr(node.target, 'id', None) == '__all__'
    if isinstance(node, ast.Assign):
        return any(getattr(target, 'id', None) == '__all__' for target in node.targets)
    return False


def test_the_root_package_declares_its_exports_too():
    """Excluded from the sweep above, and the one package where it matters most.

    `django_aiogram/__init__.py` is what `from django_aiogram import ...` reads, so a name
    that appears there by accident is public immediately. The sweep skips the root because
    its `__init__` is a lazy surface rather than a cluster facade, which is exactly why it
    is asserted here instead of quietly not at all.
    """
    source = (SRC / '__init__.py').read_text(encoding='utf-8')

    assert any(_assigns_all(node) for node in ast.parse(source).body), 'the root declares no __all__'


@pytest.mark.parametrize('package', sorted(DJANGO_OWNED))
def test_the_django_owned_packages_stay_empty_of_exports(package):
    """The exemption is asserted, so it cannot quietly become a hiding place.

    These exist because Django looks for them by path. Nothing imports a name *from* them,
    so an `__all__` would be a claim about an empty room — but a package that grew real
    contents should stop being exempt, and this is what notices.
    """
    body = ast.parse((SRC / package / '__init__.py').read_text(encoding='utf-8')).body
    meaningful = [node for node in body if not (isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant))]

    assert not meaningful, f'{package}/__init__.py has contents now, so it needs __all__ like the rest'


def test_configuration_does_not_import_what_it_configures():
    """`config/__init__.py` says it may not reach into `producer`, `consumer` or `broker`.

    It said so while the checks imported `KNOWN_RATE_LIMIT_KEYS` from
    `producer.throttling` at module scope — and `checks` is imported from `apps.ready()`,
    so every boot that registers checks paid for whatever the limiter pulls. The names now
    live in `config.enums`, beside the enum they come from.

    A subprocess, because `sys.modules` in this one is already full of everything.
    """
    # all three the docstring names, not just the one that was being violated: a test that
    # covers a third of a rule reports a clean bill for the other two thirds
    forbidden = ('producer', 'consumer', 'broker')
    code = (
        'import sys\n'
        'import django_aiogram.config.checks\n'
        f'forbidden = {forbidden!r}\n'
        "print(sorted(m for m in sys.modules if m.split('.')[:2][1:] and m.split('.')[1] in forbidden"
        " and m.startswith('django_aiogram.')))\n"
        "print('aiogram' in sys.modules)\n"
    )
    finished = run_python(code, check=True)
    pulled, aiogram = finished.stdout.strip().splitlines()

    assert pulled == '[]', f'importing the checks pulled {pulled}'
    assert aiogram == 'False', 'importing the checks pulled aiogram'


def test_neither_the_transport_nor_the_producer_imports_the_driver():
    """`AGENTS.md`: a transport imports its driver lazily, never at module scope.

    Since 4.0 `redis` is an extra, so this is what makes the rest of the design work
    rather than a tidiness rule. A module-scope driver import puts an `ImportError` in
    front of every message that would have named the extra: the `E047` check cannot run,
    `from django_aiogram import bot` fails in a process that named another transport, and
    the reader gets `No module named 'redis'` where `pip install "django-aiogram[redis]"`
    belongs.

    The producer is asserted beside the transports because it is the import a project
    actually writes, and it was the one that failed: it pulled the driver twice over — a
    `Redis` annotation, and aiogram's Redis FSM storage, which imports the driver itself.

    Every shipped transport, taken from the registry rather than named here, so the rule
    covers one added later instead of covering whichever ones somebody remembered.

    A subprocess, and a fresh interpreter each time, because `sys.modules` in this one has
    everything in it. Blocking the driver instead would prove less: the point is not that
    an absent driver is survivable but that a present one is left alone until a connection
    is built.
    """
    transports = [path.rsplit('.', 1)[0] for path in sorted(SHIPPED)]
    for module in [*transports, 'django_aiogram.producer.client']:
        code = f'import sys, {module}; print("redis" in sys.modules)'
        finished = run_python(code, check=True)

        assert finished.stdout.strip() == 'False', f'importing {module} pulled the driver'


@pytest.mark.parametrize('path', PUBLISHED_BROKERS, ids=lambda path: path.rsplit('.', 1)[-1])
def test_the_dotted_path_a_project_writes_for_a_transport_still_resolves(path):
    """`BROKER` holds one of these, written by hand into a project's settings.

    The strongest case in this file for a name that cannot move quietly: `DELIVERY` and
    `SERIALIZER` hold short names the package maps itself, while this one is a dotted path
    straight into these modules. A rename here is a break in every project that named it, and
    the failure lands at startup with `E047` saying the path cannot be imported — accurate,
    and no help to somebody who did nothing wrong.

    A class path rather than a file path, because that is what a project writes. Written out
    above rather than read from the registry, which is the difference between a test and a
    tautology: renaming the class *and* its registry entry together leaves a registry-driven
    version green while every project that named the old path is broken. Spelled here, the
    rename fails and somebody decides on purpose.
    """
    from django.utils.module_loading import import_string

    from django_aiogram.broker.base import Broker

    resolved = import_string(path)

    assert isinstance(resolved, type), f'{path} does not name a class'
    assert issubclass(resolved, Broker), f'{path} names something that is not a Broker'


def test_the_registry_ships_exactly_the_transports_written_down_here():
    """The other half: a name added or dropped has to be a decision, not a side effect.

    Without this the list above could fall behind the registry — a transport shipped and
    published with nothing pinning its path — and the case above would keep passing for the
    two it happens to name.
    """
    assert set(SHIPPED) == set(PUBLISHED_BROKERS), (
        'the registry and the published paths disagree: '
        f'registry only {sorted(set(SHIPPED) - set(PUBLISHED_BROKERS))}, '
        f'list only {sorted(set(PUBLISHED_BROKERS) - set(SHIPPED))}'
    )


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
