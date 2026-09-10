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
    # the field's own error, not the word appearing on a page that renders every setting's
    # name anyway: any other refusal would leave `overrides` empty too, and the case would
    # pass without `_decided` having judged anything
    said = response.context['adminform'].form.errors
    assert 'val_MAX_RETRIES' in said, said
    assert said['val_MAX_RETRIES'] == ['MAX_RETRIES must be >= 1, got 0.'], said['val_MAX_RETRIES']
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


def test_an_inherited_secret_is_never_written_into_the_page(client):
    """`REDIS_URL` carries the password to the broker, and a webhook secret is a credential.

    Printing what a bot would inherit is what makes "leave it alone" a decision somebody can
    make — and for these two it would hand the value to every user who may edit a limit.
    """
    profile = TelegramBotProfile.objects.create(
        name='vip',
        overrides={'WEBHOOK_SECRET': 'hunter2', 'REDIS_URL': 'redis://user:swordfish@example:6379/0'},
    )
    bot = a_bot(profile=profile)
    client.force_login(a_user('trusted', 'view_telegrambot', 'change_telegrambot', 'view_telegrambot_token'))

    page = client.get(f'{BOTS}{bot.pk}/change/').content.decode()

    assert 'hunter2' not in page
    assert 'swordfish' not in page
    assert "Inherited: 'set' — from the profile 'vip'." in page, 'the page did not even say a secret was there'


def test_a_user_without_the_permission_cannot_set_a_secret_either(client):
    """Same boundary as the token: these settings *are* credentials."""
    bot = a_bot()
    client.force_login(a_user('support', 'view_telegrambot', 'change_telegrambot'))

    page = client.get(f'{BOTS}{bot.pk}/change/').content.decode()
    assert 'set_WEBHOOK_SECRET' not in page
    response = client.post(
        f'{BOTS}{bot.pk}/change/',
        {'label': bot.label, 'enabled': 'on', 'set_WEBHOOK_SECRET': 'on', 'val_WEBHOOK_SECRET': 'mine'},
    )

    assert response.status_code == 302, response.content
    bot.refresh_from_db()
    assert bot.overrides == {}, 'a user without the permission set a credential'


def test_overrides_are_judged_against_the_profile_the_form_chose(client):
    """A form that moves a bot to another profile must not judge it by the one it is leaving.

    `W004` reads `HEARTBEAT_INTERVAL` and the transport's deadline to decide whether a
    `BLPOP_TIMEOUT` is one the consumer will honour — so judging against the profile the row
    *held* refuses a value the profile it is *moving to* makes correct, and the operator is
    told to fix something that is already right.
    """
    cramped = TelegramBotProfile.objects.create(
        name='cramped',
        overrides={'HEARTBEAT_INTERVAL': 2, 'REDIS_TIMEOUT': 5},
    )
    roomy = TelegramBotProfile.objects.create(
        name='roomy',
        overrides={'HEARTBEAT_INTERVAL': 60, 'REDIS_TIMEOUT': 90},
    )
    bot = a_bot(profile=cramped)
    client.force_login(a_user('editor', 'view_telegrambot', 'change_telegrambot'))

    response = client.post(
        f'{BOTS}{bot.pk}/change/',
        {
            'label': bot.label,
            'enabled': 'on',
            'profile': str(roomy.pk),
            'set_BLPOP_TIMEOUT': 'on',
            'val_BLPOP_TIMEOUT': '30',
        },
    )

    assert response.status_code == 302, response.content
    bot.refresh_from_db()
    assert bot.overrides == {'BLPOP_TIMEOUT': 30}
    assert bot.profile_id == roomy.pk


def test_a_pair_of_settings_that_is_right_together_is_accepted(client):
    """The rules read each other, so they have to be judged against one another.

    `W004` decides whether a `BLPOP_TIMEOUT` will be honoured from `HEARTBEAT_INTERVAL` and the
    transport's deadline. Judged one at a time against the *inherited* neighbour, this pair is
    refused — and the operator is told to fix a configuration that is correct.
    """
    bot = a_bot()
    client.force_login(a_user('editor', 'view_telegrambot', 'change_telegrambot'))

    response = client.post(
        f'{BOTS}{bot.pk}/change/',
        {
            'label': bot.label,
            'enabled': 'on',
            'set_BLPOP_TIMEOUT': 'on',
            'val_BLPOP_TIMEOUT': '30',
            'set_HEARTBEAT_INTERVAL': 'on',
            'val_HEARTBEAT_INTERVAL': '60',
            'set_REDIS_TIMEOUT': 'on',
            'val_REDIS_TIMEOUT': '90',
        },
    )

    assert response.status_code == 302, response.content
    bot.refresh_from_db()
    assert bot.overrides == {'BLPOP_TIMEOUT': 30, 'HEARTBEAT_INTERVAL': 60, 'REDIS_TIMEOUT': 90}


def test_a_new_bots_own_environment_decides_what_it_inherits(client, monkeypatch):
    """A bot with no row yet still has an identity: it is in the token being submitted.

    Environment variables are keyed by it — `DJANGO_AIOGRAM_123456_HEARTBEAT_INTERVAL` — so
    resolving an unsaved bot as `default` judges it by another bot's variables. Asserted
    through a value that is only valid under *this* bot's, because a page merely rendering the
    right number passes either way once the row exists.
    """
    monkeypatch.setenv('DJANGO_AIOGRAM_HEARTBEAT_INTERVAL', '2')
    monkeypatch.setenv('DJANGO_AIOGRAM_REDIS_TIMEOUT', '5')
    monkeypatch.setenv('DJANGO_AIOGRAM_123456_HEARTBEAT_INTERVAL', '60')
    monkeypatch.setenv('DJANGO_AIOGRAM_123456_REDIS_TIMEOUT', '90')
    client.force_login(
        a_user('adder', 'add_telegrambot', 'change_telegrambot', 'view_telegrambot', 'view_telegrambot_token')
    )

    response = client.post(
        f'{BOTS}add/',
        {
            'label': 'a new client',
            'token': TOKEN,
            'enabled': 'on',
            'set_BLPOP_TIMEOUT': 'on',
            'val_BLPOP_TIMEOUT': '30',
        },
    )

    assert response.status_code == 302, response.context['adminform'].form.errors
    assert TelegramBot.objects.get().overrides == {'BLPOP_TIMEOUT': 30}


def test_a_reader_who_may_not_change_a_bot_still_gets_a_page(client):
    """Which sounds obvious, and was a 500: Django extends its exclude list with the field
    names on a change form the user may not edit, so a form built with `fields=None` reached
    `list.extend(None)`. A view-only page renders what the row *is*.
    """
    bot = a_bot()
    client.force_login(a_user('auditor', 'view_telegrambot'))

    response = client.get(f'{BOTS}{bot.pk}/change/')

    assert response.status_code == 200
    assert str(bot.bot_id) in response.content.decode()


REVEAL = '{pk}/token/'


def test_revealing_a_token_needs_the_permission(client):
    """A change permission is not a permission to read the credential."""
    bot = a_bot()
    client.force_login(a_user('support', 'view_telegrambot', 'change_telegrambot'))

    response = client.post(BOTS + REVEAL.format(pk=bot.pk))

    assert response.status_code == 403
    assert TOKEN not in response.content.decode()


def test_revealing_a_token_shows_it_once_and_records_who(client, monkeypatch):
    """ "Who saw this token" is a question an incident asks, and the feed is what answers it.

    Asserted on what reaches the recorder rather than on the row it writes: the writer is a
    thread with its own connection, and a case that waited for it would be asserting the
    recorder's own plumbing — which `tests/db/test_recorder.py` already covers.
    """
    from django_aiogram.config.enums import EventKind
    from django_aiogram.eventlog import recorder as recorder_module

    bot = a_bot()
    kept = []
    monkeypatch.setattr(type(recorder_module.recorder), 'active', property(lambda self: True))
    monkeypatch.setattr(recorder_module.recorder, 'record', kept.append)
    client.force_login(a_user('trusted', 'view_telegrambot', 'view_telegrambot_token'))

    page = client.get(BOTS + REVEAL.format(pk=bot.pk)).content.decode()
    assert TOKEN not in page, 'a GET showed the credential without being asked'
    assert not kept, 'a GET recorded a reveal that did not happen'

    shown = client.post(BOTS + REVEAL.format(pk=bot.pk)).content.decode()

    assert TOKEN in shown
    (event,) = kept
    assert event.kind == EventKind.BOT_TOKEN_REVEALED.value
    assert event.bot_id == bot.bot_id
    assert event.detail['by'] == 'trusted'


def test_the_feed_says_which_bot_a_row_is_about():
    """The column has been there since 5.0 and nothing filled it.

    A deployment with twenty clients reads this feed to answer "whose bot failed", and a
    column of nulls answers nothing.
    """
    from django_aiogram.eventlog.records import Event
    from django_aiogram.eventlog.writer import to_row

    row = to_row(Event(kind='outbound.sent', bot_id=123456))

    assert row.bot_id == 123456


def test_moving_a_bot_off_a_queue_that_still_holds_messages_is_refused(client, monkeypatch):
    """The messages already in the old queue would have nobody to take them.

    A consumer serves the queues it was told about, and after the move nothing points at that
    one — so the backlog is stranded rather than delivered late. The refusal names what to do
    first.
    """
    held = TelegramQueue.objects.create(name='client-a', pool='default')
    other = TelegramQueue.objects.create(name='client-b', pool='default')
    bot = a_bot(queue=held)
    monkeypatch.setattr('django_aiogram.admin_bots._waiting_in', lambda name: 4 if name == 'client-a' else 0)
    client.force_login(a_user('editor', 'view_telegrambot', 'change_telegrambot'))

    response = client.post(
        f'{BOTS}{bot.pk}/change/',
        {'label': bot.label, 'token': '', 'enabled': 'on', 'queue': str(other.pk)},
    )

    assert response.status_code == 200
    assert 'still holds 4 message' in response.content.decode()
    bot.refresh_from_db()
    assert bot.queue_id == held.pk, 'the bot was moved anyway'


def test_a_transport_that_cannot_be_built_refuses_the_edit(client):
    """A `BROKER` naming nothing importable means nobody can read that queue — ever.

    Not "the queue is empty": a misconfigured deployment would otherwise find every dangerous
    edit allowed, which is the shape where the refusal matters most.
    """
    held = TelegramQueue.objects.create(name='client-a', pool='default')
    other = TelegramQueue.objects.create(name='client-b', pool='default')
    bot = a_bot(queue=held)
    client.force_login(a_user('editor', 'view_telegrambot', 'change_telegrambot'))

    with override_settings(TELEGRAM_BOT_DEFAULTS={'BROKER': 'no.such.module.Broker'}):
        response = client.post(
            f'{BOTS}{bot.pk}/change/',
            {'label': bot.label, 'enabled': 'on', 'queue': str(other.pk)},
        )

    assert response.status_code == 200
    assert 'cannot be read' in response.content.decode()
    bot.refresh_from_db()
    assert bot.queue_id == held.pk, 'the bot was moved on a deployment nothing could read'


def test_a_queue_that_cannot_be_reached_does_not_block_the_edit(client, monkeypatch):
    """A page that refused every edit because Redis blinked would be worse than the mistake.

    The same trade the supervisor makes about a provider it could not read: an unreachable
    transport has not said the queue is full. Distinct from a transport that cannot be *built*,
    which is refused — the case above.
    """
    held = TelegramQueue.objects.create(name='client-a', pool='default')
    other = TelegramQueue.objects.create(name='client-b', pool='default')
    bot = a_bot(queue=held)

    class Unreachable:
        """A transport whose server is not answering, the way a blinking Redis looks."""

        def configured(self, _settings):
            """Answer as a configured broker does."""
            return self

        def depth(self):
            """Refuse the way a socket does."""
            msg = 'connection refused'
            raise ConnectionError(msg)

        def inflight_depth(self, worker=None):
            """Never reached; the depth read raises first."""
            return 0

    monkeypatch.setattr('django_aiogram.broker.registry.broker_class', lambda *_a, **_k: Unreachable())
    client.force_login(a_user('editor', 'view_telegrambot', 'change_telegrambot'))

    response = client.post(
        f'{BOTS}{bot.pk}/change/',
        {'label': bot.label, 'enabled': 'on', 'queue': str(other.pk)},
    )

    assert response.status_code == 302, response.content
    bot.refresh_from_db()
    assert bot.queue_id == other.pk
