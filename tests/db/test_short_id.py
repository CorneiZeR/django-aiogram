"""The short id: what it is, where it comes from, and what fills the rows that predate it.

The correlation id is a UUIDv7, whose first 48 bits are a clock — so the eight characters the admin
used to show named the minute rather than the message. These cases are about the answer: twelve
characters of the *random* bits, in an alphabet a person can read aloud.
"""

import time
import uuid
from io import StringIO

import pytest
from django.core.management import CommandError, call_command
from django.test import override_settings

from django_aiogram.config.enums import EventKind
from django_aiogram.eventlog.events import SHORT_ID_LENGTH, new_correlation_id, normalise_short_id, short_id
from django_aiogram.eventlog.recorder import Event
from django_aiogram.eventlog.writer import write_batch
from django_aiogram.models import TelegramEvent

ON = {'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost', 'EVENT_LOG': True}


def an_event(**kwargs):
    kwargs.setdefault('correlation_id', new_correlation_id())
    return Event(kind=EventKind.OUTBOUND_SENT.value, **kwargs)


def test_a_prefix_names_the_clock_and_the_short_id_does_not():
    """The measurement the whole change rests on, kept as a case.

    Version 7 opens with a 48-bit millisecond, and the eight characters the column used to show are
    its top 32 bits -- measured, that prefix is exactly `ms >> 16`. So the label changed once every
    2**16 ms, a little over a minute, and every message in between wore the same one. The short id
    reads the random half instead, and tells all hundred apart.

    The bound is arithmetic rather than a guess about how fast the loop runs: however long the run
    takes, it can cross at most one step of that clock per 65.536 seconds. A slow machine widens the
    bound instead of failing the case.
    """
    started = time.monotonic()
    ids = [new_correlation_id() for _ in range(100)]
    spanned = time.monotonic() - started

    prefixes = {str(identifier).replace('-', '')[:8] for identifier in ids}
    codes = {short_id(identifier) for identifier in ids}

    allowed = int(spanned / 65.536) + 1
    assert len(prefixes) <= allowed, (
        f'a prefix distinguished {len(prefixes)} of 100 over {spanned:.3f}s, so it is not the clock '
        f'this case says it is'
    )
    assert len(codes) == 100, f'the short id distinguished only {len(codes)} of 100'


def test_the_short_id_is_the_same_for_the_same_id():
    """A pure function, which is what lets it be stored in one place and computed anywhere else.

    The literal is the point of the case. The encoding is data on disk now: a release that changed
    which bits it reads, the alphabet's order or the width would leave every stored code disagreeing
    with the one this function computes for the same id, and nothing else in the suite would notice —
    both sides would move together.
    """
    identifier = uuid.UUID('a615799d-dce6-42bc-af47-22c6ccf2c525')

    assert short_id(identifier) == 'YHS2RV6F5H95'
    assert short_id(identifier) == short_id(identifier)
    assert len(short_id(identifier)) == SHORT_ID_LENGTH
    assert set(short_id(identifier)) <= set('0123456789ABCDEFGHJKMNPQRSTVWXYZ')


@pytest.mark.parametrize(
    'written',
    ['{code}', '{lower}', '{spaced}', '{confused}'],
    ids=['as shown', 'lower case', 'with a space', 'read aloud'],
)
def test_a_short_id_is_read_the_way_a_person_writes_it_down(written):
    """Copied from a screen, typed from a ticket, or read over a call and typed back.

    The last is why the alphabet drops `I`, `L`, `O` and `U`: somebody saying `0` says "oh", and
    what comes back has an `O` in it. Folding those on input is the half that makes the alphabet
    worth having on output.
    """
    # built backwards from a code carrying both characters people mispronounce, because an id whose
    # code happens to hold neither leaves the read-aloud case asserting on unchanged text — which is
    # what it did until the fold was removed and every case stayed green
    number = 0
    for character in '01ABCDEFGH0J1K'[:SHORT_ID_LENGTH]:
        number = number * 32 + '0123456789ABCDEFGHJKMNPQRSTVWXYZ'.index(character)
    identifier = uuid.UUID(int=number)

    code = short_id(identifier)
    for confusable in '01':
        assert confusable in code, f'nothing here is confusable for {confusable!r}, so the last case reads as the first'
    text = written.format(
        code=code,
        lower=code.lower(),
        spaced=f'{code[:4]} {code[4:8]} {code[8:]}',
        confused=code.replace('0', 'O').replace('1', 'I'),
    )

    assert normalise_short_id(text) == code


@pytest.mark.parametrize(
    'text',
    ['', 'abc', 'a615799d-dce6-42bc-af47-22c6ccf2c525', '42', 'ABCDEFGHJKM@', 'ABCDEFGHJKMNP'],
    ids=repr,
)
def test_what_is_not_a_short_id_reads_as_nothing(text):
    """`''` is how the admin tells a code from a chat id or a chat id from a UUID without guessing.

    The last two matter most: the right length with a character the alphabet does not have, and the
    right alphabet at the wrong length. Either check alone would take one of them and go searching
    for something that cannot exist.
    """
    assert normalise_short_id(text) == ''


@pytest.mark.django_db
@override_settings(TELEGRAM_BOT=ON)
def test_a_row_is_written_with_its_short_id():
    """One place computes it, and it is the place the row is built."""
    event = an_event(function='send_message')

    write_batch([event])

    row = TelegramEvent.objects.get()
    assert row.short_id == short_id(event.correlation_id)


@pytest.mark.django_db
@override_settings(TELEGRAM_BOT=ON)
def test_two_messages_that_share_a_code_are_both_shown():
    """The column is not unique, and this is what that buys.

    A unique index would fail the backfill on data already in the table and make the writer loop to
    find a free code. Instead a code that matches two messages returns both rows, which is honest:
    the reader can see there are two and tell them apart by the correlation id the cell carries.
    """
    shared_bits = 0x0123456789ABCDE
    first = uuid.UUID(int=(1 << 64) | shared_bits)
    second = uuid.UUID(int=(2 << 64) | shared_bits)
    assert first != second
    assert short_id(first) == short_id(second), 'these two ids do not collide, so this case proves nothing'

    write_batch([an_event(function='send_message', correlation_id=first)])
    write_batch([an_event(function='send_message', correlation_id=second)])

    found = TelegramEvent.objects.filter(short_id=short_id(first))

    assert {row.correlation_id for row in found} == {first, second}


@pytest.mark.django_db
@override_settings(TELEGRAM_BOT=ON)
def test_the_backfill_fills_only_what_is_empty_and_can_be_stopped():
    """A row with no short id is a row still to do, which is the whole of the resume rule.

    No watermark to keep: the absence is the watermark. So a run stopped by `--max-chunks` leaves
    the rest for the next one, and a second run over a finished table does nothing.
    """
    for _ in range(5):
        write_batch([an_event(function='send_message')])
    TelegramEvent.objects.update(short_id='')

    first = StringIO()
    call_command('tgbot_backfill_short_ids', chunk=2, max_chunks=1, sleep=0, stdout=first)
    assert TelegramEvent.objects.exclude(short_id='').count() == 2, first.getvalue()

    second = StringIO()
    call_command('tgbot_backfill_short_ids', chunk=10, sleep=0, stdout=second)
    assert TelegramEvent.objects.filter(short_id='').count() == 0, second.getvalue()

    for row in TelegramEvent.objects.all():
        assert row.short_id == short_id(row.correlation_id)

    third = StringIO()
    call_command('tgbot_backfill_short_ids', sleep=0, stdout=third)
    assert 'already has a short id' in third.getvalue()


@pytest.mark.django_db
@override_settings(TELEGRAM_BOT=ON)
def test_a_dry_run_reports_and_fills_nothing():
    """The rehearsal, on a table an operator cannot afford to guess about."""
    write_batch([an_event(function='send_message')])
    TelegramEvent.objects.update(short_id='')

    out = StringIO()
    call_command('tgbot_backfill_short_ids', dry_run=True, sleep=0, stdout=out)

    assert 'would fill 1 rows' in out.getvalue()
    assert TelegramEvent.objects.filter(short_id='').count() == 1, 'a dry run wrote something'


@pytest.mark.django_db
@override_settings(TELEGRAM_BOT=ON)
def test_an_alias_that_is_not_configured_is_refused_by_name():
    """This runs from cron, where a Django traceback is the least useful thing to wake up to."""
    with pytest.raises(CommandError, match='no database is configured'):
        call_command('tgbot_backfill_short_ids', database='nowhere', sleep=0)
