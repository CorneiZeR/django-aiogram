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


def post_bot(client, bot, **extra):
    """The change form as a person would submit it, with the pairs left unchecked."""
    body = {'label': bot.label, 'token': '', 'enabled': 'on'}
    body.update(extra)
    return client.post(f'{BOTS}{bot.pk}/change/', body)


def test_a_ticked_setting_becomes_this_bots_own_and_an_unticked_one_stays_inherited(client):
    """Sparse by key presence, which is why the checkbox exists at all.

    `RATE_LIMIT: None` is a value — it switches pacing off — so "inherit" cannot be spelled as
    an empty field, and a person needs a way to say it that is not a blank box.
    """
    bot = a_bot()

    client.force_login(a_user('editor', 'view_telegrambot', 'change_telegrambot'))
    response = post_bot(client, bot, set_MAX_RETRIES='on', val_MAX_RETRIES='3')

    assert response.status_code == 302, response.content
    bot.refresh_from_db()
    assert bot.overrides == {'MAX_RETRIES': 3}, bot.overrides


def test_unticking_a_setting_hands_it_back_to_the_profile(client):
    """Which is the whole point of the pair: a decision can be *undecided* again."""
    profile = TelegramBotProfile.objects.create(name='vip', overrides={'MAX_RETRIES': 5})
    bot = a_bot(profile=profile, overrides={'MAX_RETRIES': 3})

    client.force_login(a_user('editor', 'view_telegrambot', 'change_telegrambot'))
    response = post_bot(client, bot, profile=str(profile.pk))

    assert response.status_code == 302, response.content
    bot.refresh_from_db()
    assert bot.overrides == {}, bot.overrides

    from django_aiogram.runtime import providers

    providers.forget()
    with override_settings(
        TELEGRAM_BOT_DEFAULTS={'BOT_PROVIDERS': ('django_aiogram.runtime.providers.from_database',)}
    ):
        (found,) = providers.from_database()
    assert found['MAX_RETRIES'] == 5, 'the profile did not get the setting back'


def test_a_value_the_package_would_refuse_is_refused_on_the_page(client):
    """One law about what a valid value is: `E012` refuses a `MAX_RETRIES` under one.

    A second copy of the rules in a `clean_` method is how a page comes to accept a
    configuration the deployment then refuses to start with — in another person's terminal, at
    the next restart.
    """
    bot = a_bot()

    client.force_login(a_user('editor', 'view_telegrambot', 'change_telegrambot'))
    response = post_bot(client, bot, set_MAX_RETRIES='on', val_MAX_RETRIES='0')

    assert response.status_code == 200
    assert 'MAX_RETRIES' in response.content.decode()
    bot.refresh_from_db()
    assert bot.overrides == {}, 'a refused value was written anyway'


def test_the_form_says_what_leaving_a_setting_alone_would_mean(client):
    """A person can only choose to inherit when the page says what they are inheriting."""
    profile = TelegramBotProfile.objects.create(name='vip', overrides={'MAX_RETRIES': 5})
    bot = a_bot(profile=profile)

    client.force_login(a_user('reader', 'view_telegrambot', 'change_telegrambot'))
    page = client.get(f'{BOTS}{bot.pk}/change/').content.decode()

    assert "Inherited: 5 — from the profile 'vip'." in page, 'the inherited value and its layer were not shown'


def test_the_queue_a_bot_is_pointed_at_is_the_queue_it_publishes_to(client):
    """Otherwise the picker is decoration: routing read `QUEUE` and nothing wrote it.

    The foreign key is what an operator chooses and what `tgbot_prune_queues` reads as "a bot
    still publishes here", so it has to be the one the producer uses.
    """
    queue = TelegramQueue.objects.create(name='vip', pool='vip')
    a_bot(queue=queue)

    from django_aiogram.runtime import providers

    providers.forget()
    with override_settings(
        TELEGRAM_BOT_DEFAULTS={'BOT_PROVIDERS': ('django_aiogram.runtime.providers.from_database',)}
    ):
        (found,) = providers.from_database()

    assert found['QUEUE'] == 'vip'


def test_switching_bots_off_from_the_list_moves_the_watermark(client):
    """Otherwise the switch takes effect at the next restart rather than within a poll.

    `update()` moves no `auto_now` column, and `updated_at` is what every supervisor reads to
    notice a row changed at all — so an action that forgot it would look like it worked and
    change nothing anybody is serving.
    """
    bot = a_bot()
    before = bot.updated_at
    client.force_login(a_user('operator', 'view_telegrambot', 'change_telegrambot'))

    response = client.post(BOTS, {'action': 'switch_off', '_selected_action': [str(bot.pk)]}, follow=True)

    assert response.status_code == 200
    bot.refresh_from_db()
    assert bot.enabled is False
    assert bot.updated_at > before, 'the change would not reach a supervisor until it restarted'


def test_the_changelist_names_the_settings_a_bot_decides_for_itself(client):
    """A bot behaving unlike its neighbours is nearly always one carrying an old override."""
    a_bot(overrides={'MAX_RETRIES': 3})
    client.force_login(a_user('viewer', 'view_telegrambot'))

    page = client.get(BOTS).content.decode()

    assert 'MAX_RETRIES' in page, 'the list said nothing about what this bot overrides'


def test_a_profile_links_to_the_bots_on_it(client):
    """A count answers "is it in use"; the next question is always which bots."""
    profile = TelegramBotProfile.objects.create(name='vip')
    a_bot(profile=profile)
    client.force_login(a_user('viewer', 'view_telegrambotprofile', 'view_telegrambot'))

    page = client.get('/admin/django_aiogram/telegrambotprofile/').content.decode()

    assert f'{BOTS}?profile__id__exact={profile.pk}' in page


def test_a_user_without_the_permission_cannot_post_a_token_either(client):
    """The section is not rendered, and that alone is not a permission boundary.

    The admin builds its form from the form's own declaration, which declares `token` — so a
    POST carrying one would be accepted and stored, and the bot given away by somebody who may
    not read its credential. The field is dropped from the form that request gets.
    """
    bot = a_bot()
    client.force_login(a_user('support', 'view_telegrambot', 'change_telegrambot'))

    response = client.post(
        f'{BOTS}{bot.pk}/change/',
        {'label': 'renamed', 'token': '999999:CCstolen', 'enabled': 'on'},
    )

    assert response.status_code == 302, response.content
    bot.refresh_from_db()
    assert read_token(bot.token) == TOKEN, 'a user without the permission replaced the credential'
    assert bot.bot_id == 123456, 'the identity moved with a token nobody was allowed to set'


def test_adding_a_bot_needs_the_token_permission(client):
    """A new bot has no token to keep, so adding one *is* setting a credential."""
    client.force_login(a_user('support', 'add_telegrambot', 'view_telegrambot', 'change_telegrambot'))

    response = client.get(f'{BOTS}add/')

    assert response.status_code == 403
    assert not TelegramBot.objects.exists()


def test_renaming_a_queue_reaches_the_bots_pointed_at_it(client):
    """The providers answer from a watermark, and a bot's queue comes from the row it points at.

    A rename that moved no other table would leave every container publishing to the old name
    until it restarted — and renaming a queue is exactly the kind of edit an operator makes.
    """
    from django_aiogram.runtime import providers

    queue = TelegramQueue.objects.create(name='client-a', pool='default')
    a_bot(queue=queue)
    providers.forget()

    with override_settings(
        TELEGRAM_BOT_DEFAULTS={'BOT_PROVIDERS': ('django_aiogram.runtime.providers.from_database',)}
    ):
        assert providers.from_database()[0]['QUEUE'] == 'client-a'
        queue.name = 'client-a-renamed'
        queue.save()

        assert providers.from_database()[0]['QUEUE'] == 'client-a-renamed', 'the rename was invisible to the cache'
