"""The notice that says "read your bots again", and what it may not cost the thing that sends it.

Django's signals are in-process, so a row written in the web process reaches a bot container
through the transport or not at all. These cases are about that trip: what is published, what
a consumer does with it, and what happens when the queue is not there.
"""

import pytest
from django.test import override_settings

from django_aiogram.consumer.delivery import get_delivery
from django_aiogram.models import TelegramBot
from django_aiogram.runtime import control, groups
from django_aiogram.runtime.supervisor import Supervisor, serving
from django_aiogram.wire.serializers import get_serializer

pytestmark = pytest.mark.django_db(transaction=True)

MEMORY = {'BROKER': 'django_aiogram.testing.InMemoryBroker', 'FSM_STORAGE': 'memory'}


@pytest.fixture(autouse=True)
def _no_groups_left_behind():
    """A group holds a transport, and a case that leaves one decides the next case's answer."""
    groups.close_groups()
    yield
    groups.close_groups()


def test_a_notice_is_told_apart_from_a_call():
    """They share a queue, so a notice read as an envelope would be a message nobody delivers."""
    assert control.is_control(control.pack_reload())
    assert not control.is_control({'function': 'send_message', 'kwargs': {}})
    assert not control.is_control('a string off the wire')


@override_settings(TELEGRAM_BOT_DEFAULTS=MEMORY)
def test_saving_a_bot_puts_a_notice_on_the_queue():
    """Which is what makes a client connecting a bot reach a running container in a second."""
    TelegramBot.objects.create(bot_id=123456, token='123456:AAaa')

    (group,) = groups.live_groups()
    (published,) = group.broker.messages

    assert control.is_control(get_serializer().loads(published))


@override_settings(TELEGRAM_BOT_DEFAULTS=MEMORY)
def test_a_notice_reaches_the_supervisor_and_is_acknowledged():
    """Acted on rather than delivered, and not kept: nothing is waiting for it."""
    passes = []
    supervisor = Supervisor(start=lambda record: None, stop=lambda identity: None)
    supervisor.reconcile = lambda: passes.append(True)
    serving(supervisor)

    delivery = get_delivery(handler=lambda **call: pytest.fail(f'a notice was delivered as a call: {call}'))
    acknowledged = delivery.dispatch(get_serializer().dumps(control.pack_reload()))

    assert acknowledged is True, 'a notice was left in flight for a restart to read again'
    assert passes == [True]


@override_settings(TELEGRAM_BOT_DEFAULTS=MEMORY)
def test_a_notice_this_version_does_not_know_is_ignored(caplog):
    """A newer release may send another one, and a consumer that raised would end delivery."""
    serving(Supervisor(start=lambda record: None, stop=lambda identity: None))
    delivery = get_delivery(handler=lambda **call: None)

    with caplog.at_level('WARNING', logger='django_aiogram'):
        acknowledged = delivery.dispatch(get_serializer().dumps({control.CONTROL_KEY: 'defenestrate'}))

    assert acknowledged is True
    assert any('does not know' in record.getMessage() for record in caplog.records)


def test_listening_for_a_change_does_not_cost_the_fast_delete_path():
    """A `post_delete` receiver without a sender makes every delete in the project fetch rows.

    Django checks for receivers before it fast-deletes, so a listener registered for *any*
    model turns a bounded `DELETE ... WHERE id BETWEEN` into `DELETE ... WHERE id IN (...)`
    with a SELECT in front of it — measured on the event log's prune, whose whole design is
    the bounded range. The receivers are connected per model for that reason, and this is what
    holds them to it.
    """
    from django.db.models.signals import post_delete

    from django_aiogram.models import TelegramEvent

    assert not post_delete.has_listeners(TelegramEvent), 'the feed gained a delete listener'
    assert post_delete.has_listeners(TelegramBot), 'a bot change is not being listened for'


@override_settings(TELEGRAM_BOT_DEFAULTS={'BROKER': 'no.such.Broker'})
def test_a_queue_that_cannot_be_reached_does_not_refuse_the_save(caplog):
    """The poll is what makes a change arrive at all; the notice is what makes it arrive fast.

    An admin page that would not save a bot because a queue is unreachable is the wrong trade,
    and the save is durable either way — so the failure is logged where an operator can see it
    and nothing else.
    """
    with caplog.at_level('ERROR', logger='django_aiogram'):
        TelegramBot.objects.create(bot_id=123456, token='123456:AAaa')

    assert TelegramBot.objects.filter(bot_id=123456).exists(), 'the save was lost to an unreachable queue'
    assert any('could not announce a bot change' in record.getMessage() for record in caplog.records)
