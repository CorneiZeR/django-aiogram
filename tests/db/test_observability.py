"""Every line and every row about a bot says which bot it was.

With one bot, that was the whole log. With twenty clients it is the difference between a feed
somebody can answer a question from and a column of nulls — and the rule the package already
had says the value goes in `extra`, never interpolated into the message.
"""

import logging

import pytest
from django.test import override_settings

from django_aiogram.config.enums import EventKind
from django_aiogram.eventlog.records import Event
from django_aiogram.models import TelegramBot, TelegramEvent
from django_aiogram.runtime import providers

pytestmark = pytest.mark.django_db

FROM_DB = ('django_aiogram.runtime.providers.from_database',)
SETTINGS = {
    'BROKER': 'django_aiogram.testing.InMemoryBroker',
    'FSM_STORAGE': 'memory',
    'EVENT_LOG': True,
    'BOT_PROVIDERS': FROM_DB,
}
FEED = '/admin/django_aiogram/telegramevent/'


@pytest.fixture(autouse=True)
def _registered():
    """The feed's admin is registered in `ready` behind the flag, and a client needs it here."""
    from django_aiogram.admin import register_event_log_admin

    register_event_log_admin()


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
        'kind': 'outbound.sent',
        'correlation_id': identifier,
        'short_id': short_id(identifier),
        'function': 'send_message',
    }
    fields.update(kwargs)
    return TelegramEvent.objects.create(**fields)


def a_bot(**kwargs):
    """One bot from the table, as a `TelegramBot` object built from its row."""
    from django_aiogram.runtime.registry import bots
    from django_aiogram.tokens import store_token

    fields = {'bot_id': 123456, 'token': store_token('123456:AAaa'), 'label': 'a client'}
    fields.update(kwargs)
    TelegramBot.objects.create(**fields)
    providers.forget()
    (record,) = [found for found in providers.desired() if found.bot_id == fields['bot_id']]
    return bots.for_record(record)


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'ENABLED': False})
def test_a_send_that_was_refused_says_which_bot_it_was_for(caplog):
    """#123's named case: the value is in `extra`, and not in the message text.

    A value interpolated into the message is still there to `grep`, and that is not the
    point: it stops being a *field*, so nothing can filter, group or alert on it, and every
    line becomes its own message string. That is the whole reason the rule exists.
    """
    served = a_bot()

    with caplog.at_level(logging.DEBUG, logger='django_aiogram'):
        served.send_raw(chat_id=1, text='hi')

    (said,) = [record for record in caplog.records if 'send skipped' in record.getMessage()]
    assert said.tg_bot_id == 123456
    assert '123456' not in said.getMessage(), 'the identity was interpolated into the message'


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_queued_row_says_which_bot_it_is_for():
    """A feed with twenty clients answers "whose messages were queued" or nothing at all.

    `transaction=True` because the writer is a thread with its own connection, which is how
    every other case about a written row runs.
    """
    from django_aiogram.eventlog.recorder import recorder

    served = a_bot()
    try:
        served.send('send_message', chat_id=1, text='hi')
        recorder.flush(timeout=5)
    finally:
        served.close()

    (row,) = TelegramEvent.objects.filter(kind=EventKind.OUTBOUND_QUEUED.value)
    assert row.bot_id == 123456


def test_the_writer_keeps_the_identity_it_was_given():
    """The column has an index leading with it, and nothing filled it before 5.0."""
    from django_aiogram.eventlog.writer import to_row

    assert to_row(Event(kind='inbound.received', bot_id=654321)).bot_id == 654321


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_the_feed_can_be_narrowed_to_one_bot(client):
    """Which is the question a shared deployment asks first: what happened to *this* client."""
    from django.contrib.auth.models import Permission, User

    a_bot()
    a_row(bot_id=123456)
    a_row(bot_id=654321)
    user = User.objects.create_user(username='reader', password='x', is_staff=True)
    user.user_permissions.add(Permission.objects.get(codename='view_telegramevent'))
    client.force_login(user)

    page = client.get(f'{FEED}?bot=123456')

    assert page.status_code == 200
    shown = {row.bot_id for row in page.context['cl'].queryset}
    assert shown == {123456}, shown
    assert 'a client (123456)' in page.content.decode(), 'the filter did not offer the bot by name'


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_the_feed_can_still_be_narrowed_to_a_client_who_has_gone(client):
    """Which is one of the questions a feed is kept for: what happened to the bot that left.

    Django drops a filter value that is not among the lookups, so a bot nothing configures
    any more would silently show every row instead — and its rows outlive it by the retention
    period on purpose.
    """
    from django.contrib.auth.models import Permission, User

    a_row(bot_id=123456)
    a_row(bot_id=654321)
    user = User.objects.create_user(username='reader', password='x', is_staff=True)
    user.user_permissions.add(Permission.objects.get(codename='view_telegramevent'))
    client.force_login(user)

    page = client.get(f'{FEED}?bot=654321')

    assert page.status_code == 200
    assert {row.bot_id for row in page.context['cl'].queryset} == {654321}
    assert '654321 (no longer configured)' in page.content.decode()


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_bot_filter_outside_the_column_answers_nothing(client):
    """The column is a BIGINT, and a bigger number raises while the query is built.

    So it is refused here rather than handed to the database — the same bound the search box
    keeps, and for the same reason.
    """
    from django.contrib.auth.models import Permission, User

    a_row(bot_id=123456)
    user = User.objects.create_user(username='reader', password='x', is_staff=True)
    user.user_permissions.add(Permission.objects.get(codename='view_telegramevent'))
    client.force_login(user)

    page = client.get(f'{FEED}?bot={2**63}')

    assert page.status_code == 200
    assert not list(page.context['cl'].queryset)


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_the_detail_page_names_the_bot(client):
    """A row opened from a search has to say whose bot it is about without going back."""
    from django.contrib.auth.models import Permission, User

    row = a_row(bot_id=123456)
    user = User.objects.create_user(username='reader', password='x', is_staff=True)
    user.user_permissions.add(Permission.objects.get(codename='view_telegramevent'))
    client.force_login(user)

    page = client.get(f'{FEED}{row.pk}/change/')

    assert page.status_code == 200
    assert '123456' in page.content.decode()
    assert 'bot id' in page.content.decode().lower()


@pytest.mark.django_db(transaction=True)
@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_failure_line_names_the_bot_the_send_went_out_under(caplog, monkeypatch):
    """A token rotated while a send is in flight must not rename the send that is failing.

    `bot_id` resolves this bot's settings on every ask, so a callback reading it when the
    task finishes reports whichever bot the row names *then* — which is a line attributing
    one client's failure to another.
    """
    served = a_bot()
    try:
        task = served.loop.create_task(_raising())
        served._register(task, _an_outbound())
        # the row moves under the bot while the send is in flight, the way a rotation does
        monkeypatch.setattr(type(served), 'bot_id', property(lambda self: 999999))
        with caplog.at_level(logging.ERROR, logger='django_aiogram'):
            served.loop.run_until_complete(_settled(task))
    finally:
        served.close()

    (said,) = [record for record in caplog.records if 'scheduled send failed' in record.getMessage()]
    assert said.tg_bot_id == 123456, 'the failure was attributed to whichever bot the row named later'


async def _raising():
    """A send that fails, the way one Telegram refused does."""
    msg = 'refused'
    raise RuntimeError(msg)


async def _settled(task):
    """Wait for the task and swallow what it raised: the log line is the subject."""
    import contextlib

    with contextlib.suppress(RuntimeError):
        await task


def _an_outbound():
    """The call description `_register` keeps, with nothing interesting in it."""
    import uuid as _uuid

    from django_aiogram.producer.outbound import Outbound

    return Outbound(function='send_message', call_kwargs={'chat_id': 1}, correlation_id=_uuid.uuid4())
