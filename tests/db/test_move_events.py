"""Moving the rows 3.x wrote into the table 4.0 reads.

The old table has no model any more, so every case here creates it the way a real upgrade leaves
it: same columns, same primary key, rows already in it. Built from the model's own column list
rather than from a literal, because a copy that names columns is only safe while the two agree —
a case that hard-coded them would keep passing on the release that makes them differ.

`sqlite_sequence` behaves differently from a PostgreSQL sequence, so the case about the *sequence*
lives with the integration suite and this file says what it can: what moves, what does not move
twice, and what a stopped run does.
"""

from io import StringIO

import pytest
from django.core.management import CommandError, call_command
from django.db import connection
from django.db.utils import IntegrityError, OperationalError

from django_aiogram.config.enums import EventKind
from django_aiogram.eventlog.events import new_correlation_id
from django_aiogram.eventlog.moving import OLD_TABLE, shared_columns
from django_aiogram.models import TelegramEvent


@pytest.fixture
def old_table():
    """The 3.x table, created as the rename left it and dropped afterwards."""
    columns = ', '.join(f'"{name}"' for name in shared_columns() if name != 'id')
    with connection.cursor() as cursor:
        cursor.execute(
            f'CREATE TABLE {OLD_TABLE} AS SELECT * FROM {TelegramEvent._meta.db_table} WHERE 1 = 0'  # noqa: S608
        )
    yield columns
    with connection.cursor() as cursor:
        cursor.execute(f'DROP TABLE {OLD_TABLE}')


def write_old_rows(count):
    """Put `count` rows into the 3.x table, shaped exactly like the rows this app writes.

    Written through the model and then moved across, rather than assembled column by column: a
    literal row would need updating on the release that adds a column, and until somebody noticed
    it would be wrong in the shape this command exists to prevent. Deleting them from the new table
    afterwards leaves what an upgrade leaves — ids in the old table, none in the new one.

    Returns the ids it wrote, because they are not `1..count`: a sequence does not roll back with
    the transaction a test runs in, so on PostgreSQL each case continues where the last one
    stopped. A case that spelled the ids out passed alone and failed in the file, which is the
    same defect as asserting on a number nobody derived.
    """
    names = ', '.join(f'"{name}"' for name in shared_columns())
    for _ in range(count):
        TelegramEvent.objects.create(
            kind=EventKind.OUTBOUND_SENT.value,
            correlation_id=new_correlation_id(),
            function='send_message',
        )
    # both table names are this package's own and no value is interpolated, so the rule has
    # nothing to catch here — the values went in through the model above
    statement = f'INSERT INTO {OLD_TABLE} ({names}) SELECT {names} FROM {TelegramEvent._meta.db_table}'  # noqa: S608
    with connection.cursor() as cursor:
        cursor.execute(statement)
    written = sorted(TelegramEvent.objects.values_list('id', flat=True))
    TelegramEvent.objects.all().delete()
    return written


def move(**options):
    """Run the command and return what it printed."""
    out = StringIO()
    call_command('tgbot_move_events', stdout=out, sleep=0, **options)
    return out.getvalue()


@pytest.mark.django_db
def test_nothing_to_move_when_the_old_table_is_gone():
    """The common case, and the one that must not raise: a project that never ran 3.x."""
    output = move()

    assert 'nothing to move' in output, output
    assert TelegramEvent.objects.count() == 0


@pytest.mark.django_db
def test_every_row_moves_once(old_table):
    """The whole point, and the ids come across unchanged: they are what a rerun reads to resume."""
    written = write_old_rows(5)

    output = move(chunk=2)

    assert TelegramEvent.objects.count() == 5, output
    assert sorted(TelegramEvent.objects.values_list('id', flat=True)) == written, 'the ids changed on the way'
    assert 'moved 5 events' in output, output


@pytest.mark.django_db
def test_rows_below_what_this_release_already_wrote_still_move(old_table):
    """The destination is *not* empty when the move runs, and that is the normal case.

    A project upgrades, `migrate` creates the table, the bot writes to it from the first message,
    and the operator schedules the copy for a quiet night. Resuming above the destination's highest
    id would then skip every old row beneath it — the whole history — and report a completed move.

    Written with the native row's id deliberately above the old ones, which is what a fresh
    sequence produces on the new table, and the assertion is on the ids: every old id has to arrive
    even though the destination's maximum already exceeds all of them.
    """
    written = write_old_rows(3)
    native = TelegramEvent.objects.create(
        kind=EventKind.OUTBOUND_SENT.value,
        correlation_id=new_correlation_id(),
        function='send_message',
    )
    assert native.pk > max(written), 'the case needs a destination row above the old ids'

    output = move()

    assert sorted(TelegramEvent.objects.exclude(pk=native.pk).values_list('id', flat=True)) == written, output
    assert 'moved 3 events' in output, output


@pytest.mark.django_db
def test_an_id_this_release_already_holds_is_reported_not_overwritten(old_table):
    """Two rows cannot share a primary key, so one of them stays where it is and is named.

    Renumbering somebody's history to make a total tidy is not this command's decision, and a total
    that quietly counted the collision as moved would be the same defect as skipping it silently.
    """
    written = write_old_rows(2)
    taken = TelegramEvent.objects.create(
        kind=EventKind.OUTBOUND_SENT.value,
        correlation_id=new_correlation_id(),
        function='send_message',
    )
    # the collision sits *above* a row that can still move, which is the order a first pass meets
    # them in: the run that walks a range is the one that reports what it could not take
    statement = f'UPDATE {OLD_TABLE} SET id = %s WHERE id = %s'  # noqa: S608 - the table name is a constant of this package
    with connection.cursor() as cursor:
        cursor.execute(statement, [taken.pk, written[1]])

    output = move()

    assert 'moved 1 events' in output, output
    assert '1 rows were left because the id is already taken' in output, output
    assert TelegramEvent.objects.count() == 2, 'a row was overwritten or duplicated'


@pytest.mark.django_db
def test_a_second_run_moves_nothing(old_table):
    """Idempotent, because the destination's highest id is what the next run starts above."""
    write_old_rows(4)
    move()

    output = move()

    assert TelegramEvent.objects.count() == 4, output
    assert 'nothing is left to copy' in output, output


@pytest.mark.django_db
def test_a_stopped_run_resumes_where_it_left_off(old_table):
    """`--max-chunks` is the bound a nightly job runs under, so stopping is the normal case.

    Asserted as two halves and a total: the first run copies what its bound allows, the second
    picks up the rest, and no id arrives twice — which resuming by `created_at` would not give,
    since rows share timestamps.
    """
    written = write_old_rows(6)

    first = move(chunk=2, max_chunks=1)
    assert TelegramEvent.objects.count() == 2, first
    assert 'Stopped after 1 chunks' in first, first

    second = move(chunk=2)

    assert TelegramEvent.objects.count() == 6, second
    assert sorted(TelegramEvent.objects.values_list('id', flat=True)) == written, 'a row moved twice or not at all'


@pytest.mark.django_db
def test_a_dry_run_reports_and_moves_nothing(old_table):
    """The rehearsal an operator runs first, on a table they cannot afford to guess about."""
    write_old_rows(3)

    output = move(dry_run=True)

    assert 'would move 3 events' in output, output
    assert TelegramEvent.objects.count() == 0, 'a dry run wrote rows'


@pytest.mark.django_db
def test_an_alias_that_is_not_configured_is_refused_by_name(old_table):
    """This runs from cron, where a Django traceback is the least useful thing to wake up to."""
    with pytest.raises(CommandError, match='no database is configured'):
        move(database='nowhere')


@pytest.mark.django_db
def test_the_check_reports_the_table_and_names_the_command(old_table):
    """`I003` is what makes the command discoverable, so it has to fire on the upgrade's shape."""
    from django_aiogram.config.checks import check_settings

    found = [message for message in check_settings() if message.id == 'django_aiogram.I003']

    assert len(found) == 1, [message.msg for message in found]
    assert OLD_TABLE in found[0].msg, found[0].msg
    assert 'tgbot_move_events' in (found[0].hint or ''), found[0].hint


@pytest.mark.django_db
def test_the_check_is_silent_without_the_old_table():
    """And says nothing on a project that never ran 3.x, which is most of them."""
    from django_aiogram.config.checks import check_settings

    assert [message for message in check_settings() if message.id == 'django_aiogram.I003'] == []


@pytest.mark.django_db
@pytest.mark.skipif(connection.vendor != 'postgresql', reason='a sequence only behaves this way on a real one')
def test_a_rerun_after_the_last_chunk_still_fixes_the_sequence(old_table):
    """Killed between the last commit and the reset, a rerun has nothing to copy -- and must still reset.

    That gap is one statement wide and the consequence outlives it: the ids are copied, the sequence
    is behind them, and every rerun takes the "nothing is left to copy" path. Skipping the reset
    there leaves the deployment one duplicate-key error away from its next write, with a command
    that reports success each time it is run.

    Reproduced by copying and then putting the sequence back, which is the state that gap produces.
    """
    write_old_rows(3)
    move()
    with connection.cursor() as cursor:
        cursor.execute("SELECT setval(pg_get_serial_sequence(%s, 'id'), 1, false)", [TelegramEvent._meta.db_table])

    output = move()

    assert 'nothing is left to copy' in output, output
    fresh = TelegramEvent.objects.create(
        kind=EventKind.OUTBOUND_SENT.value,
        correlation_id=new_correlation_id(),
        function='send_message',
    )
    assert fresh.pk > max(TelegramEvent.objects.exclude(pk=fresh.pk).values_list('id', flat=True))


@pytest.mark.django_db
@pytest.mark.skipif(connection.vendor != 'postgresql', reason='a sequence only behaves this way on a real one')
def test_the_next_insert_after_a_move_succeeds(old_table):
    """The step a hand-written `INSERT ... SELECT` leaves out, and the one SQLite cannot show.

    Explicit ids do not advance a PostgreSQL sequence. So a table copied into with the sequence
    still where `migrate` left it accepts every row of the copy and then refuses the *bot's* next
    write with a duplicate key — at whatever hour the first message after the migration arrives,
    with a traceback naming the primary key rather than the copy that caused it.

    Asserted by writing a row the way the recorder does, which is the thing that would have
    failed. `sqlite_sequence` follows an explicit id on its own, so this case is skipped there and
    the suite says so rather than passing for a reason that does not hold anywhere else.
    """
    write_old_rows(3)
    # where `migrate` leaves it on a table it has just created, and where the fixture above does
    # *not* leave it: those rows went through the model first, so the sequence is already past
    # them and the copy would look harmless. Set by hand rather than through the helper the
    # command uses, so a broken helper cannot quietly establish the precondition it is judged on
    with connection.cursor() as cursor:
        cursor.execute("SELECT setval(pg_get_serial_sequence(%s, 'id'), 1, false)", [TelegramEvent._meta.db_table])

    move()

    fresh = TelegramEvent.objects.create(
        kind=EventKind.OUTBOUND_SENT.value,
        correlation_id=new_correlation_id(),
        function='send_message',
    )

    assert fresh.pk > max(TelegramEvent.objects.exclude(pk=fresh.pk).values_list('id', flat=True))


@pytest.mark.django_db
def test_a_database_that_cannot_be_read_stops_the_command(monkeypatch):
    """ "Nothing to move" and "I could not look" are the same sentence to a cron job.

    Only one of them means the history has been moved, and a command that exits zero on the other
    tells an operator the migration is done. The check has the opposite duty -- a rule that raises
    takes `manage.py check` down -- so the two callers handle this differently on purpose, and the
    case below holds the other half.
    """

    def refuse(_alias):
        msg = 'connection refused'
        raise OperationalError(msg)

    monkeypatch.setattr('django_aiogram.management.commands.tgbot_move_events.old_table_is_present', refuse)

    with pytest.raises(CommandError, match='could not look for'):
        move()


@pytest.mark.django_db
def test_a_database_that_cannot_be_read_leaves_the_check_quiet(old_table, monkeypatch):
    """And the rule says nothing rather than taking every other finding down with it.

    The old table is present here on purpose: without it `I003` is silent whatever happens, and a
    case that passes for that reason proves nothing about the handling it is named after.
    """
    from django_aiogram.config.checks import check_settings

    def refuse(_alias):
        msg = 'connection refused'
        raise OperationalError(msg)

    monkeypatch.setattr('django_aiogram.eventlog.moving.old_table_is_present', refuse)

    # that the run *answers* is asserted by reaching this line at all: a rule which let the error
    # through would come out of `check_settings` itself, and there is nothing to add after that
    assert [message for message in check_settings() if message.id == 'django_aiogram.I003'] == []


@pytest.mark.django_db
def test_a_chunk_that_loses_a_race_is_retried(old_table, monkeypatch):
    """`NOT EXISTS` chooses the ids; it does not hold them until the insert commits.

    A row this release writes in that gap takes one, and the insert ends as a unique violation --
    likeliest of all before this command has ever run, when the destination's sequence is still
    where `migrate` left it and the bot is drawing the very ids the old table used.

    The race is produced rather than waited for: the first insert raises the error a lost race
    raises, and the retry runs against a table where the row has landed. What the case pins is that
    the run continues and the rest of the chunk arrives, not that the failure was rare.
    """
    written = write_old_rows(3)
    real_execute = connection.cursor().__class__.execute
    calls = []

    def flaky(self, sql, params=None):
        if sql.lstrip().startswith('INSERT') and not calls:
            calls.append(sql)
            msg = 'duplicate key value violates unique constraint'
            raise IntegrityError(msg)
        return real_execute(self, sql, params)

    monkeypatch.setattr(connection.cursor().__class__, 'execute', flaky)

    output = move()

    assert calls, 'the insert never ran, so nothing was retried'
    assert sorted(TelegramEvent.objects.values_list('id', flat=True)) == written, output


@pytest.mark.django_db
def test_a_chunk_that_keeps_losing_says_so(old_table, monkeypatch):
    """And it stops with a sentence rather than a traceback about a primary key.

    A collision that repeats is a bot still writing the ids being copied, which no amount of
    retrying fixes -- so the command names the range and says what to do, and what already landed
    stays landed.
    """
    write_old_rows(2)
    real_execute = connection.cursor().__class__.execute

    def always(self, sql, params=None):
        if sql.lstrip().startswith('INSERT'):
            msg = 'duplicate key value violates unique constraint'
            raise IntegrityError(msg)
        return real_execute(self, sql, params)

    monkeypatch.setattr(connection.cursor().__class__, 'execute', always)

    with pytest.raises(CommandError, match='kept colliding'):
        move()
