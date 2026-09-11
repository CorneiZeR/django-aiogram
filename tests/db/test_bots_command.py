"""`tgbot_bots`: what an operator reads when `manage.py check` cannot see the bots.

The checks read settings, and a client connected through the project's own interface is a
row — so for a deployment whose bots arrive at run time this is the only thing that can say
what will actually happen, and whether twenty bots really did collapse into one group.
"""

import json

import pytest
from django.core.management import call_command
from django.test import override_settings
from django.utils import timezone

from django_aiogram.models import TelegramBot, TelegramBotLease, TelegramBotProfile
from django_aiogram.runtime import providers

pytestmark = pytest.mark.django_db

FROM_DB = ('django_aiogram.runtime.providers.from_database',)
SETTINGS = {
    'BROKER': 'django_aiogram.testing.InMemoryBroker',
    'FSM_STORAGE': 'memory',
    'BOT_PROVIDERS': FROM_DB,
}


@pytest.fixture(autouse=True)
def _no_read_kept():
    """The providers cache by watermark, and these cases write rows under it."""
    providers.forget()
    yield
    providers.forget()


def a_bot(identity, **kwargs):
    """One bot in the table, with a token that carries its identity."""
    from django_aiogram.tokens import store_token

    fields = {'bot_id': identity, 'token': store_token(f'{identity}:AAaa'), 'label': f'client {identity}'}
    fields.update(kwargs)
    return TelegramBot.objects.create(**fields)


def listed(**options):
    """Run the command in JSON and return what it said, row by row."""
    from io import StringIO

    out = StringIO()
    call_command('tgbot_bots', json=True, stdout=out, **options)
    return [json.loads(line) for line in out.getvalue().splitlines() if line.strip()]


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_bots_configured_alike_share_one_group():
    """#124's acceptance: the listing explains a grouping decision without reading code.

    Twenty bots on one configuration collapse into one transport, one dispatcher and one
    consumer thread — and the only way to know it worked is to see one profile digest against
    all of them.
    """
    for identity in (111111, 222222, 333333):
        a_bot(identity)

    rows = listed()

    assert len({row['profile'] for row in rows}) == 1, [row['profile'] for row in rows]
    assert {row['bot_id'] for row in rows} == {111111, 222222, 333333}


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_bot_on_another_queue_is_in_another_group():
    """Which is the other half of the same question: what *stops* two bots sharing.

    Two bots addressed at different queues must not share a transport — each would read the
    other's messages off the queue it is addressed to.
    """
    a_bot(111111)
    a_bot(222222, overrides={'QUEUE': 'vip'})

    rows = {row['bot_id']: row for row in listed()}

    assert rows[111111]['profile'] != rows[222222]['profile']
    assert rows[222222]['queue'] == 'vip'


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_the_listing_says_who_is_serving_a_bot():
    """A lease is which container is polling that bot, which is the question during an incident."""
    a_bot(111111)
    TelegramBotLease.objects.create(
        bot_id=111111,
        holder='worker-a',
        expires_at=timezone.now() + timezone.timedelta(seconds=30),
    )

    (row,) = listed()

    assert row['lease'] == 'worker-a'


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_lease_that_has_expired_is_not_somebody_serving_the_bot():
    """A held lease is a live one: an expired row says the container stopped renewing."""
    a_bot(111111)
    TelegramBotLease.objects.create(
        bot_id=111111,
        holder='worker-a',
        expires_at=timezone.now() - timezone.timedelta(seconds=1),
    )

    (row,) = listed()

    assert row['lease'] == '—', row


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_switched_off_bot_is_left_out_unless_it_is_asked_for():
    """`desired` is what a supervisor serves, and a listing that mixed the two would mislead.

    But a bot somebody has just switched off is exactly what they then go looking for, so
    `--all` includes them.
    """
    a_bot(111111)
    a_bot(222222, enabled=False)

    assert {row['bot_id'] for row in listed()} == {111111}
    assert {row['bot_id'] for row in listed(all=True)} == {111111, 222222}


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_the_listing_can_be_narrowed_to_one_bot():
    """A deployment with five hundred clients is not read a page at a time."""
    a_bot(111111)
    a_bot(222222)

    assert {row['bot_id'] for row in listed(bots=[222222])} == {222222}


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_the_profile_digest_is_not_the_settings_it_was_built_from():
    """Those hold `REDIS_URL`, which carries the password to the broker."""
    profile = TelegramBotProfile.objects.create(name='vip', overrides={'REDIS_URL': 'redis://user:swordfish@host/0'})
    a_bot(111111, profile=profile)

    from io import StringIO

    out = StringIO()
    call_command('tgbot_bots', stdout=out)

    assert 'swordfish' not in out.getvalue()


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_deployment_with_no_bots_says_so():
    """Rather than printing a header with nothing under it, which reads as a broken command."""
    from io import StringIO

    out = StringIO()
    call_command('tgbot_bots', stdout=out)

    assert 'no bots are configured' in out.getvalue()
