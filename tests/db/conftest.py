"""Guards for the database suite.

Run it with its own settings module:

    python -m pytest --ds=tests.db_settings tests/db
"""

import threading

import pytest
from django.conf import settings
from django.db import connection, connections

from django_aiogram.eventlog.recorder import WRITER_THREAD, recorder


@pytest.fixture(autouse=True)
def _no_writer_outlives_its_test():
    """End the process-wide writer between tests.

    `recorder` is a singleton and its thread is deliberately not tied to any
    one test: it lives until the process ends. Left running, it drains the next
    test's queue before that test can look at it, and it is still enumerable
    when a test asserts no writer exists — two failures that depend on file
    ordering and reproduce nowhere else.
    """
    yield
    recorder.stop(timeout=5)


@pytest.fixture
def paused_writer(monkeypatch):
    """Let the recorder build its queue without anything draining it.

    `record()` starts the writer on the first event, and a live writer makes
    every assertion about the buffer a race: it may have taken the event before
    the test looks. Neutering start() leaves the production path intact —
    queue, bounds, drops — with the test in charge of when it is drained.

    join() goes with it, because joining a thread that never started raises.

    Keyed on the writer's name: patching every Thread would silently neuter any
    other thread a test starts, which is a trap rather than a fixture.
    """
    real_start, real_join = threading.Thread.start, threading.Thread.join

    def start(self):
        if self.name != WRITER_THREAD:
            real_start(self)

    def join(self, timeout=None):
        if self.name != WRITER_THREAD:
            real_join(self, timeout)

    monkeypatch.setattr(threading.Thread, 'start', start)
    monkeypatch.setattr(threading.Thread, 'join', join)


def pytest_configure(config):
    """Refuse to run under the no-database settings, rather than erroring later.

    The default invocation ignores `tests/db`, so getting here with the wrong
    module means someone pointed pytest at it by hand; a dozen confusing
    ImproperlyConfigured failures is a worse answer than one sentence.
    """
    if not settings.DATABASES:
        pytest.exit('tests/db needs a database: run it with --ds=tests.db_settings', returncode=4)


@pytest.fixture
def a_backend_that_can_lose_a_connection(monkeypatch):
    """Let `close_if_unusable_or_obsolete` act, on a backend that cannot let it.

    On PostgreSQL this does nothing, and the case using it is then the real thing: a closed DBAPI
    connection makes `is_usable()` false by itself and `close()` genuinely closes.

    SQLite in memory can do neither. `is_usable()` is unconditionally true, and `close()` is a
    documented no-op — deliberately, since closing would destroy the database the test runs in — so
    both answers are supplied there. Patched on the *class*: the wrapper that matters lives on the
    executor thread and is a different instance from the one this thread's proxy resolves to.
    """
    if connections['default'].vendor != 'sqlite':
        return
    wrapper = type(connections['default'])
    monkeypatch.setattr(wrapper, 'is_usable', lambda self: False)
    monkeypatch.setattr(wrapper, 'is_in_memory_db', lambda self: False)


@pytest.fixture
def observed_closes(monkeypatch):
    """Watch what closes a connection, without letting the close reach the database.

    The cases that use this ask whether the code *called* close, which is the behaviour they are
    about — so the call is recorded and goes no further, and both backends answer the same. On
    SQLite the substitution is also a necessity rather than a convenience: an in-memory database
    that is really closed is gone, and Django knows it, which is why `is_in_memory_db` has to be
    answered as well before the recycle will consider acting at all.

    Returns a factory: give it the connection to watch, keep the list it hands back.
    """

    def watch(wrapper=None):
        watched = wrapper if wrapper is not None else connections['default']
        closed: list[str] = []
        if watched.vendor == 'sqlite':
            monkeypatch.setattr(watched, 'is_in_memory_db', lambda: False)
        monkeypatch.setattr(watched, '_close', lambda: closed.append('closed'))
        return closed

    return watch


@pytest.fixture
def query_plan():
    """Return one query's plan as a single upper-case string, in whichever dialect this backend speaks.

    SQLite answers ``EXPLAIN QUERY PLAN`` with a row per step and PostgreSQL answers ``EXPLAIN``
    with its own text; the cases below ask the same two questions of either -- does it sort, and
    does it use the index this app added.

    **PostgreSQL is asked with `enable_seqscan` off**, and that is the difference between a case
    that means something here and one that measures the size of a test fixture. The question is
    whether the index *can* serve the query; a table holding a handful of rows is always cheaper to
    read whole, so the planner would answer about the fixture every time. Turning the sequential
    scan off does not force the *order*: if the index cannot provide it, a sort still appears, which
    is exactly the regression these cases exist to catch.
    """

    def explain(sql, params=None):
        with connection.cursor() as cursor:
            if connection.vendor == 'postgresql':
                cursor.execute('SET LOCAL enable_seqscan = off')
                cursor.execute(f'EXPLAIN {sql}', params)
            else:
                cursor.execute(f'EXPLAIN QUERY PLAN {sql}', params)
            return ' | '.join(str(row[-1]) for row in cursor.fetchall()).upper()

    return explain


def sorts(plan: str) -> bool:
    """Whether the plan orders rows itself instead of reading them in order.

    Two spellings of one answer: SQLite builds a temporary B-tree, PostgreSQL adds a `Sort` node.
    """
    return 'TEMP B-TREE' in plan or 'SORT' in plan


def sorts_beyond_a_tie(plan: str) -> bool:
    """Whether the plan sorts the *whole* result rather than breaking a tie inside it.

    An index that provides the order still leaves the rows that share a value in no particular
    one, and a query ordering by a second column to settle that asks the database to sort within
    each group. Both backends say which they are doing, in their own words: SQLite writes "for last
    term of order by" for the tie and nothing about a term for the whole thing, PostgreSQL calls
    the first an `Incremental Sort` and the second a `Sort`.

    The distinction is the point of the case that uses it: sorting a tie is what the index leaves
    to be done, and sorting everything is the index not being used for the order at all.
    """
    return 'USE TEMP B-TREE FOR ORDER BY' in plan or ('SORT' in plan and 'INCREMENTAL SORT' not in plan)


def uses(plan: str, index: str) -> bool:
    """Whether the plan reads through the named index.

    Asserted alongside :func:`sorts` and not instead of it: a plan with no sort and no index is
    what a full scan of a small table looks like, and it reads exactly like the fixed one at the
    cost the index was added to remove.
    """
    return index.upper() in plan
