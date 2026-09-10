"""The admin a deployment configures its bots from: what it shows, and what it refuses to.

The credential is the subject of most of it. A page that renders a token has put it in a
browser history, a proxy log and a screenshot, and none of those are the database whose access
control was the protection — so what is asserted here is mostly absence.
"""

import pytest
from django.contrib.auth.models import Permission, User
from django.db import connection
from django.test import override_settings
from django.test.utils import CaptureQueriesContext

from django_aiogram.admin_bots import HIDDEN, register_bot_admin
from django_aiogram.models import TelegramBot, TelegramBotProfile, TelegramQueue
from django_aiogram.tokens import TokenStorage, read_token

pytestmark = pytest.mark.django_db

BOTS = '/admin/django_aiogram/telegrambot/'
TOKEN = '123456:AAFakeTokenThatLooksExactlyLikeARealOne'
ROTATED = '123456:BBAnotherOneEntirelyButTheSameBotId'


def a_user(username, *codenames):
    """A staff user holding exactly the permissions named."""
    user = User.objects.create_user(username=username, password='x', is_staff=True)
    for codename in codenames:
        user.user_permissions.add(Permission.objects.get(codename=codename))
    return user


@pytest.fixture(autouse=True)
def _registered():
    """Registration happens in `ready`, and a test client needs it done here too."""
    register_bot_admin()


def a_bot(**kwargs):
    """One configured bot, stored the way the admin stores one."""
    from django_aiogram.tokens import store_token

    fields = {'bot_id': 123456, 'label': 'a client', 'token': store_token(TOKEN)}
    fields.update(kwargs)
    return TelegramBot.objects.create(**fields)


def test_a_user_without_the_permission_sees_no_token_anywhere(client):
    """#121's acceptance, and the reason the permission exists at all.

    Not masked: the credentials section is not rendered, because a section that renders is a
    section that posts back, and a token field left there would let a user who may not read
    the credential replace it — which is the same thing as taking the bot.
    """
    bot = a_bot()
    client.force_login(a_user('support', 'view_telegrambot', 'change_telegrambot'))
    page = client.get(f'{BOTS}{bot.pk}/change/').content.decode()

    assert 'Credentials' not in page
    assert TOKEN not in page
    assert HIDDEN not in page
    assert 'New token' not in page


def test_a_trusted_user_sees_the_mask_and_never_the_token(client):
    """The mask says a credential is there and names the identity, which is not a secret."""
    bot = a_bot()

    client.force_login(a_user('trusted', 'view_telegrambot', 'change_telegrambot', 'view_telegrambot_token'))
    page = client.get(f'{BOTS}{bot.pk}/change/').content.decode()

    assert 'Credentials' in page
    assert HIDDEN in page
    assert TOKEN not in page, 'the token was rendered into the page'


def test_saving_an_empty_token_field_keeps_the_token(client):
    """Empty means keep, which is what lets a rotation happen without a read.

    A form that wrote the unbound field back would delete the credential of every bot whose
    label somebody corrected.
    """
    bot = a_bot()

    client.force_login(a_user('editor', 'view_telegrambot', 'change_telegrambot', 'view_telegrambot_token'))
    response = client.post(
        f'{BOTS}{bot.pk}/change/',
        {'label': 'renamed', 'token': '', 'overrides': '{}', 'enabled': 'on'},
    )

    assert response.status_code == 302, response.content
    bot.refresh_from_db()
    assert bot.label == 'renamed'
    assert read_token(bot.token) == TOKEN, 'leaving the box alone deleted the credential'


def test_a_rotated_token_is_stored_through_the_seam(client):
    """With an encrypting storage configured, the admin must write no plaintext either."""
    bot = a_bot()

    client.force_login(a_user('rotator', 'view_telegrambot', 'change_telegrambot', 'view_telegrambot_token'))
    with override_settings(
        TELEGRAM_BOT_DEFAULTS={
            'TOKEN_STORAGE': 'tests.db.test_bot_admin.Reversing',
        }
    ):
        response = client.post(
            f'{BOTS}{bot.pk}/change/',
            {'label': 'a client', 'token': ROTATED, 'overrides': '{}', 'enabled': 'on'},
        )

        assert response.status_code == 302, response.content
        bot.refresh_from_db()
        assert bot.token == ROTATED[::-1], 'the admin wrote the column directly'
        assert read_token(bot.token) == ROTATED


def test_a_new_bot_takes_its_identity_from_the_token(client):
    """Typed by hand it is a mis-keyed digit away from a row that serves somebody else's bot."""
    client.force_login(a_user('adder', 'add_telegrambot', 'view_telegrambot', 'view_telegrambot_token'))
    response = client.post(
        f'{BOTS}add/',
        {'label': 'a new client', 'token': TOKEN, 'overrides': '{}', 'enabled': 'on'},
    )

    assert response.status_code == 302, response.content
    (bot,) = TelegramBot.objects.all()
    assert bot.bot_id == 123456
    assert read_token(bot.token) == TOKEN


@pytest.mark.parametrize(('token', 'says'), [('not-a-token', 'not a bot token'), ('', 'needs a token')])
def test_a_string_that_is_not_a_token_is_refused_with_a_reason(client, token, says):
    """Refused at the form rather than at the first send: nothing could address such a bot."""
    client.force_login(a_user('adder', 'add_telegrambot', 'view_telegrambot', 'view_telegrambot_token'))
    response = client.post(
        f'{BOTS}add/',
        {'label': 'a new client', 'token': token, 'overrides': '{}', 'enabled': 'on'},
    )

    assert response.status_code == 200
    assert says in response.content.decode()
    assert not TelegramBot.objects.exists()


def test_the_changelist_asks_the_same_number_of_queries_however_many_bots_there_are(client):
    """A page over five hundred clients cannot afford a query per row for the two relations."""
    from django.test import Client

    profile = TelegramBotProfile.objects.create(name='vip')
    queue = TelegramQueue.objects.create(name='vip', pool='vip')
    a_bot(bot_id=111111, profile=profile, queue=queue)
    client = Client()
    client.force_login(a_user('viewer', 'view_telegrambot'))

    with CaptureQueriesContext(connection) as first:
        assert client.get(BOTS).status_code == 200
    for identity in range(222222, 222232):
        a_bot(bot_id=identity, label=f'client {identity}', profile=profile, queue=queue)
    with CaptureQueriesContext(connection) as second:
        assert client.get(BOTS).status_code == 200

    assert len(second) == len(first), f'{len(first)} queries for one bot, {len(second)} for eleven'


class Reversing(TokenStorage):
    """A storage that is obviously not the plain one, so a direct write is visible."""

    needs_keys = False

    def store(self, token):
        """Return the token backwards."""
        return token[::-1]

    def read(self, stored):
        """Return it the right way round again."""
        return stored[::-1]
