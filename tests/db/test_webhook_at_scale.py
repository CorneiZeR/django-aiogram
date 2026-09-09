"""A webhook for each of many bots: the URL that names one, and the pass that registers it.

Polling costs a connection and a task per bot; a webhook costs nothing per bot, which is why
this is the shape a deployment past a few dozen bots uses. Every case here is about one of the
three things that makes it work at that size: the identity in the path, the secret that is not
shared, and a reconciliation that asks Telegram rather than assuming.
"""

import json

import pytest
from django.core.management import CommandError, call_command
from django.test import override_settings
from django.test.client import RequestFactory

from django_aiogram.consumer.webhook import SECRET_HEADER, registered, telegram_webhook, webhook_settings
from django_aiogram.models import TelegramBot

pytestmark = pytest.mark.django_db

FROM_DB = ('django_aiogram.runtime.providers.from_database',)
SETTINGS = {
    'BROKER': 'django_aiogram.testing.InMemoryBroker',
    'FSM_STORAGE': 'memory',
    'MODE': 'webhook',
    'WEBHOOK_URL': 'https://example.test/tg',
    'WEBHOOK_SECRET': 'the-process-secret',
    'BOT_PROVIDERS': FROM_DB,
}
UPDATE = {'update_id': 1, 'message': {'message_id': 1, 'date': 0, 'chat': {'id': 1, 'type': 'private'}, 'text': 'hi'}}


def posted(bot_id=None, secret='the-process-secret'):  # noqa: S107 - a test secret, and the subject
    """One update posted the way Telegram posts it, through the view."""
    request = RequestFactory().post(
        '/tg/', data=json.dumps(UPDATE), content_type='application/json', **{SECRET_HEADER: secret}
    )
    return telegram_webhook(request, bot_id) if bot_id is not None else telegram_webhook(request)


@pytest.fixture(autouse=True)
def _cold_cache():
    """Forget the identities between cases: the cache is a module's, and cases share it."""
    from django_aiogram.consumer import webhook

    webhook._served = frozenset()
    webhook._read_at = None
    yield
    webhook._served = frozenset()
    webhook._read_at = None


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_an_update_for_a_served_bot_costs_no_query(django_assert_num_queries, monkeypatch):
    """A webhook is asked per request, so per request it must not be asked of the database.

    With the miss grace at zero, so the case says which path is being asserted: a *hit* is
    answered from the cache for the whole interval and never consults the grace, while a miss
    is what may re-read. Reversed -- the refresh asked for first -- this fails.
    """
    monkeypatch.setattr('django_aiogram.consumer.webhook.MISS_GRACE', 0.0)
    TelegramBot.objects.create(bot_id=123456, token='123456:AAaa', overrides={'WEBHOOK_SECRET': 'mine'})
    assert posted(123456, 'mine').status_code == 200  # the first update fills the cache

    with django_assert_num_queries(0):
        assert posted(123456, 'mine').status_code == 200


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_stranger_posting_nonsense_cannot_keep_the_database_busy(django_assert_num_queries):
    """Otherwise the webhook is a way to load the database by posting identities at it.

    A miss re-reads so a bot registered a moment ago is served, and that read is bounded to
    one a second -- `MISS_GRACE` -- because the alternative is a stranger posting unknown
    identities as fast as the database can answer them. The flood is the second post onward,
    and those are answered from the cache.
    """
    TelegramBot.objects.create(bot_id=123456, token='123456:AAaa', overrides={'WEBHOOK_SECRET': 'mine'})
    assert posted(999999, 'mine').status_code == 404

    with django_assert_num_queries(0):
        assert posted(999999, 'mine').status_code == 404
        assert posted(888888, 'mine').status_code == 404


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_bot_registered_a_moment_ago_is_not_refused_for_the_interval(monkeypatch):
    """The reason the miss re-reads at all: a new client's bot posts as soon as it is set up.

    The grace is stepped over rather than waited out: what the case is about is that a miss
    re-reads, not how long a second is.
    """
    assert posted(123456, 'mine').status_code == 404
    monkeypatch.setattr('django_aiogram.consumer.webhook.MISS_GRACE', 0.0)

    TelegramBot.objects.create(bot_id=123456, token='123456:AAaa', overrides={'WEBHOOK_SECRET': 'mine'})

    assert posted(123456, 'mine').status_code == 200, 'a bot added after the last read was refused'


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_bots_own_secret_is_the_one_that_has_to_match():
    """One secret for every bot makes a leak from one client's bot a way to post as all."""
    TelegramBot.objects.create(bot_id=123456, token='123456:AAaa', overrides={'WEBHOOK_SECRET': 'mine'})
    TelegramBot.objects.create(bot_id=654321, token='654321:BBbb', overrides={'WEBHOOK_SECRET': 'theirs'})

    assert posted(123456, 'mine').status_code == 200
    assert posted(123456, 'theirs').status_code == 403, "another bot's secret was accepted"
    assert posted(123456, 'the-process-secret').status_code == 403, 'the shared secret was accepted'


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_bot_with_no_secret_of_its_own_is_not_served_under_the_process_one():
    """Served that way, every bot would accept the same secret again."""
    TelegramBot.objects.create(bot_id=123456, token='123456:AAaa', overrides={'WEBHOOK_SECRET': ''})

    assert posted(123456, 'the-process-secret').status_code == 503


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_the_url_a_bot_registers_carries_its_identity():
    """Which is what the view reads back out of the request, and what tells the bots apart."""
    TelegramBot.objects.create(bot_id=123456, token='123456:AAaa', overrides={'WEBHOOK_SECRET': 'mine'})
    from django_aiogram.runtime.providers import desired

    (record,) = desired()

    arguments = webhook_settings(record, record.bot_id)

    assert arguments['url'] == 'https://example.test/tg/123456/'
    assert arguments['secret_token'] == 'mine'


def test_telegram_having_the_wrong_url_is_what_a_pass_repairs():
    """`getWebhookInfo` is the only authority on what Telegram will post to."""
    desired_state = {'url': 'https://example.test/tg/123456/', 'allowed_updates': None}

    class Has:
        """What `getWebhookInfo` answers with."""

        def __init__(self, url, allowed=None):
            """Take what Telegram says it has."""
            self.url = url
            self.allowed_updates = allowed

    assert registered(desired_state, Has('https://example.test/tg/123456/'))
    assert not registered(desired_state, Has(''))
    assert not registered(desired_state, Has('https://example.test/tg/654321/'))
    assert not registered({**desired_state, 'allowed_updates': ['message']}, Has(desired_state['url']))
    assert registered({**desired_state, 'allowed_updates': ['message']}, Has(desired_state['url'], ['message']))


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_bot_the_deployment_does_not_configure_is_refused_by_name():
    """A typo in `--bot` would otherwise be a run that reported nothing and changed nothing."""
    with pytest.raises(CommandError, match='No bot with the identity'):
        call_command('tgbot_webhook', 'info', '--bot', '999999')
