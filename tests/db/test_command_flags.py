"""Every command acts on the bots and queues it was told about, and on nothing else.

`tgbot_reclaim` requeueing another client's in-flight messages, or the mover publishing one
client's row to another's queue, is the kind of failure only production finds — and it finds
it as messages arriving where nobody expected them.
"""

from io import StringIO

import pytest
from django.core.management import CommandError, call_command
from django.test import override_settings
from django.utils import timezone

from django_aiogram.models import TelegramEvent, TelegramScheduledSend
from django_aiogram.runtime import providers

pytestmark = pytest.mark.django_db

# the feed is *off* here on purpose: these cases write their own rows and read them back, and
# a recorder left on records the mover's own publish from a test connection the writer thread
# cannot see -- which counts as a drop and shows up as a `log.dropped` row in whichever case
# runs next. Measured: it broke `tests/db/test_inbound.py` two files away
SETTINGS = {
    'BROKER': 'django_aiogram.testing.InMemoryBroker',
    'FSM_STORAGE': 'memory',
}


@pytest.fixture(autouse=True)
def _no_read_kept():
    """The providers cache by watermark, and these cases write rows under it."""
    providers.forget()
    yield
    providers.forget()


def a_row(**kwargs):
    """One feed row, written around the recorder the way the other feed cases do."""
    from django_aiogram.eventlog.events import new_correlation_id, short_id

    identifier = new_correlation_id()
    fields = {
        'kind': 'outbound.failed',
        'correlation_id': identifier,
        'short_id': short_id(identifier),
        'function': 'send_message',
        'chat_id': 1,
        'detail': {'kwargs': {'chat_id': 1, 'text': 'hi'}},
    }
    fields.update(kwargs)
    return TelegramEvent.objects.create(**fields)


def a_scheduled(**kwargs):
    """One row waiting for its moment, due now unless a case says otherwise."""
    from django_aiogram.eventlog.events import new_correlation_id

    fields = {
        'correlation_id': new_correlation_id(),
        'function': 'send_message',
        'chat_id': 1,
        'payload': b'{}',
        'due_at': timezone.now() - timezone.timedelta(seconds=1),
    }
    fields.update(kwargs)
    return TelegramScheduledSend.objects.create(**fields)


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_pruning_one_bots_history_leaves_the_others():
    """A client who left takes their history with them, and nobody else's."""
    old = timezone.now() - timezone.timedelta(days=90)
    a_row(bot_id=111111, created_at=old)
    a_row(bot_id=222222, created_at=old)

    call_command('tgbot_prune_events', days=30, bot=[111111], stdout=StringIO())

    assert {row.bot_id for row in TelegramEvent.objects.all()} == {222222}


# the replay refuses outright with the feed off -- it has no other source -- so its own case
# turns it on, in a dry run that writes nothing and leaves the recorder with nothing to drop
@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'EVENT_LOG': True})
def test_a_replay_sends_only_the_bot_it_was_told_about():
    """An incident is one client's, and a replay is the one command that *sends*."""
    mine = a_row(bot_id=111111)
    theirs = a_row(bot_id=222222)
    out = StringIO()

    call_command('tgbot_replay', since='2000-01-01', bot=[111111], dry_run=True, stdout=out)

    said = out.getvalue()
    # the identity, not the count: a regression that selected the *other* bot also reports one
    assert str(mine.short_id) in said, said
    assert str(theirs.short_id) not in said, said
    assert TelegramEvent.objects.filter(kind='outbound.replayed').count() == 0


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_the_mover_claims_only_the_rows_it_was_told_about():
    """Claiming another client's row publishes it to their queue from a container nobody asked."""
    mine = a_scheduled(bot_id=111111)
    theirs = a_scheduled(bot_id=222222)

    call_command('tgbot_dispatch_scheduled', bot=[111111], stdout=StringIO(), stderr=StringIO())

    assert not TelegramScheduledSend.objects.filter(pk=mine.pk).exists(), 'the named bot was not moved'
    assert TelegramScheduledSend.objects.filter(pk=theirs.pk).exists(), "another bot's row was taken"


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_dry_run_of_the_mover_counts_only_the_rows_it_was_told_about():
    """A rehearsal that counted every bot promises a pass this one will not make."""
    a_scheduled(bot_id=111111)
    a_scheduled(bot_id=222222)
    out = StringIO()

    call_command('tgbot_dispatch_scheduled', bot=[111111], dry_run=True, stdout=out)

    assert '1 due now' in out.getvalue(), out.getvalue()


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'BROKER': 'django_aiogram.broker.redis_list.RedisListBroker'})
def test_reclaiming_from_an_undeclared_queue_is_refused():
    """A typo would otherwise read an empty list and report that nothing is in flight.

    Which is indistinguishable from the queue having been drained already — the one answer
    that stops somebody looking further.
    """
    from django_aiogram.models import TelegramQueue

    TelegramQueue.objects.create(name='client-a', pool='default')

    with pytest.raises(CommandError, match='not a declared queue'):
        call_command('tgbot_reclaim', worker='dead-worker', queue=['client-z'], stdout=StringIO())


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_reclaiming_refuses_two_queues_rather_than_taking_the_last():
    """A person who typed two names is asking for two runs.

    argparse's default action keeps the last one silently, and taking it without a word is how
    the first client's messages stay where they are while the output claims a reclaim happened.
    """
    with pytest.raises(CommandError, match='takes one queue'):
        call_command('tgbot_reclaim', worker='dead-worker', queue=['client-a', 'client-b'], stdout=StringIO())


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
@pytest.mark.parametrize('given', [[''], '', ['   ']])
def test_reclaiming_refuses_an_empty_queue_rather_than_using_this_process_one(given):
    """An empty string would fall through to the process's own list, which is another client's.

    Both shapes, because they arrive by different routes: a list from argparse's `append`,
    and a bare string from `call_command`. The scalar is the one that used to pass through
    the falsy check *before* normalisation and reclaim the wrong queue.
    """
    with pytest.raises(CommandError, match='cannot be empty'):
        call_command('tgbot_reclaim', worker='dead-worker', queue=given, stdout=StringIO())


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'BROKER': 'django_aiogram.broker.redis_list.RedisListBroker'})
def test_a_readable_declaration_with_nothing_in_it_still_refuses_a_name():
    """*Unknown* and *none* are different answers, and only one of them may accept a name.

    A table that could not be read declares unknown, and refusing there would block a reclaim
    during the outage it is most needed in. A table that was read and holds nothing declares
    none — and a name against that is a typo.
    """
    with pytest.raises(CommandError, match='none are declared'):
        call_command('tgbot_reclaim', worker='dead-worker', queue=['client-z'], stdout=StringIO())


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'BROKER': 'django_aiogram.broker.redis_list.RedisListBroker'})
def test_a_queue_named_programmatically_as_a_string_is_one_queue():
    """`call_command('tgbot_reclaim', queue='vip')` is a supported way to run this.

    It forwards the value as written rather than through argparse's `append`, so a command
    that counted the argument's length would refuse a perfectly good name by counting its
    characters.
    """
    from django_aiogram.models import TelegramQueue

    TelegramQueue.objects.create(name='client-a', pool='default')

    with pytest.raises(CommandError, match='not a declared queue'):
        call_command('tgbot_reclaim', worker='dead-worker', queue='client-z', stdout=StringIO())


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'EVENT_LOG': True})
def test_a_replay_takes_its_arguments_from_its_own_bots_rows():
    """A correlation id names a message, not a row in the feed.

    Two bots can carry one — a caller reusing an id, a broadcast joined under one thread — and
    matching on it alone would replay one client's failure with another client's arguments.
    """
    from django_aiogram.eventlog.events import new_correlation_id, short_id

    shared = new_correlation_id()
    for identity, text in ((111111, 'mine'), (222222, 'theirs')):
        TelegramEvent.objects.create(
            kind='outbound.queued',
            correlation_id=shared,
            short_id=short_id(shared),
            bot_id=identity,
            function='send_message',
            chat_id=1,
            detail={'chat_id': 1, 'text': text},
        )
    failed = a_row(bot_id=111111, correlation_id=shared, short_id=short_id(shared))
    out = StringIO()

    call_command('tgbot_replay', correlation_id=[str(failed.correlation_id)], bot=[111111], dry_run=True, stdout=out)

    said = out.getvalue()
    assert 'mine' in said, said
    assert 'theirs' not in said, said


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'EVENT_LOG': True})
def test_a_failure_that_names_no_bot_still_finds_its_arguments():
    """Every row written before 5.0 names none, and so does anything that did not know.

    Narrowing those to `bot_id=None` would find nothing for an upgraded deployment's whole
    history — the arguments are there, on a row that knew more than the failure did.
    """
    from django_aiogram.eventlog.events import new_correlation_id, short_id

    shared = new_correlation_id()
    TelegramEvent.objects.create(
        kind='outbound.queued',
        correlation_id=shared,
        short_id=short_id(shared),
        bot_id=111111,
        function='send_message',
        chat_id=1,
        detail={'chat_id': 1, 'text': 'from before'},
    )
    failed = a_row(bot_id=None, correlation_id=shared, short_id=short_id(shared))
    out = StringIO()

    call_command('tgbot_replay', correlation_id=[str(failed.correlation_id)], dry_run=True, stdout=out)

    assert 'from before' in out.getvalue(), out.getvalue()
