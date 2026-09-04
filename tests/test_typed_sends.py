"""A send whose arguments are checked before it is made, by aiogram rather than by us.

`bot.send('send_mesage', ...)` passes every type checker and every linter, and `check_function`
catches it where the payload is built -- in a web request, as an exception about a name the
caller already wrote. The keyword arguments were `**kwargs: Any`, so nothing checked them at
all.

The fix is not 181 signatures of our own: aiogram declares every method as a pydantic model
with typed fields, so a caller who hands one over gets it checked by the library that owns the
API, in whatever type checker they run, and it moves when aiogram moves. What is left for this
package is the pair the wire carries, and one refusal aiogram does not make.
"""

import uuid

import pytest
from aiogram.methods import SendMessage, SetWebhook
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from django.test import override_settings

from django_aiogram import TelegramBot
from django_aiogram.api import API_METHODS, method_name, resolve_call
from django_aiogram.exceptions import UnknownApiMethodError
from django_aiogram.testing import capture_sends

SETTINGS = {'TOKEN': '42:x', 'FSM_STORAGE': 'memory', 'RATE_LIMIT': None}


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_method_object_queues_the_call_it_describes():
    """The whole point, through the real producer: names and arguments from aiogram."""
    with capture_sends() as sent:
        identifier = TelegramBot().send(SendMessage(chat_id=42, text='typed'))

    assert sent[0].function == 'send_message'
    assert sent[0].kwargs == {'chat_id': 42, 'text': 'typed'}
    assert sent[0].correlation_id == identifier


@override_settings(TELEGRAM_BOT=SETTINGS)
@pytest.mark.parametrize('form', ['send', 'enqueue'])
def test_every_queued_producer_takes_one(form):
    """`send`, `enqueue` and their awaiting twins, because a caller should not have to ask.

    These four cases are also the *pin*: a project writing `bot.send(SendMessage(...))` needs
    all four to keep accepting one, so this is where a widening that got reverted would be
    caught. `test_public_surface.py` pins names and this pins a call, which is the difference
    between a signature existing and a signature being used.
    """
    with capture_sends() as sent:
        getattr(TelegramBot(), form)(SendMessage(chat_id=1, text=form))

    assert [(one.function, one.kwargs['text']) for one in sent] == [('send_message', form)]


@override_settings(TELEGRAM_BOT=SETTINGS)
@pytest.mark.parametrize('form', ['asend', 'aenqueue'])
def test_the_awaiting_twins_take_one_too(form):
    """Separate implementations, so separate cases -- the sync ones would pass with these broken."""
    import asyncio

    with capture_sends() as sent:
        asyncio.run(getattr(TelegramBot(), form)(SendMessage(chat_id=1, text=form)))

    assert [(one.function, one.kwargs['text']) for one in sent] == [('send_message', form)]


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_nested_model_keeps_its_own_type_on_the_way_through():
    """The reason the fields are read rather than dumped.

    `model_dump(exclude_unset=True)` is the obvious conversion and loses discriminators:
    `wire.serializers` documents it from the other side, and an `InputMediaPhoto` comes back an
    `InputMediaAudio`. Read as attributes, nested models stay models and the serializer's own
    tags carry them.
    """
    markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='ok', callback_data='k')]])

    with capture_sends() as sent:
        TelegramBot().send(SendMessage(chat_id=1, text='x', reply_markup=markup))

    assert isinstance(sent[0].kwargs['reply_markup'], InlineKeyboardMarkup)
    assert sent[0].kwargs['reply_markup'].inline_keyboard[0][0].callback_data == 'k'


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_field_aiogram_does_not_declare_is_refused_at_the_call():
    """The half a type checker does not do, and the reason this refusal exists at all.

    Measured: aiogram's models are `extra='allow'`, so `parse_mod` is *accepted* by
    `SendMessage(...)` and kept in `model_fields_set` -- and neither mypy nor the pydantic mypy
    plugin reports it. On the wire it reaches `Bot.send_message(**kwargs)` in the worker, hours
    later, as a `TypeError` about a name nobody can see from there.
    """
    assert SendMessage.model_config.get('extra') == 'allow', 'aiogram now refuses this itself'

    with capture_sends(), pytest.raises(TypeError, match="no field 'parse_mod'"):
        TelegramBot().send(SendMessage(chat_id=1, text='x', parse_mod='HTML'))


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_denied_method_is_refused_by_the_same_allowlist():
    """The object form is not a way around `DENIED_METHODS`: it resolves to a name and is checked."""
    with capture_sends(), pytest.raises(UnknownApiMethodError):
        TelegramBot().send(SetWebhook(url='https://elsewhere.example/hook'))


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_arguments_beside_a_method_object_are_refused_rather_than_ignored():
    """Two sources for one call is a caller who means something this cannot answer."""
    with capture_sends(), pytest.raises(TypeError, match='cannot be passed beside it'):
        TelegramBot().send(SendMessage(chat_id=1, text='x'), text='and again')


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_string_form_is_untouched():
    """It is the 2.x call, the documented one, and the only form that takes a variable."""
    name = 'send_message'

    with capture_sends() as sent:
        TelegramBot().send(name, chat_id=1, text='by name')

    assert sent[0].function == 'send_message'
    assert sent[0].kwargs == {'chat_id': 1, 'text': 'by name'}


def test_the_string_form_still_accepts_whatever_it_is_given():
    """Stated as a case, because it is a difference between the two forms rather than an oversight.

    Checking keyword arguments on the string path would refuse calls that work today, on the
    call every 2.x project wrote. The object form is where a project moves to get that.
    """
    function, kwargs = resolve_call('send_message', {'parse_mod': 'HTML'})

    assert (function, kwargs) == ('send_message', {'parse_mod': 'HTML'})


def test_a_method_object_resolves_to_the_name_the_allowlist_holds():
    """Both halves derive the name the same way, or a method is allowed under one name and sent
    under another.

    Every aiogram method class, not a sample: `method_name` is a regular expression over
    `CamelCase`, and the one that breaks it will be a class name nobody predicted.
    """
    import aiogram.methods

    checked = 0
    for class_name in aiogram.methods.__all__:
        name = method_name(class_name)
        if name not in API_METHODS:
            continue  # administrative or not a Bot attribute, which the allowlist already decides
        kind = getattr(aiogram.methods, class_name)
        assert method_name(kind.__name__) == name, f'{class_name} resolves two ways'
        checked += 1

    assert checked == len(API_METHODS), f'{checked} of {len(API_METHODS)} allowed methods were reached'


def test_the_types_still_line_up_with_what_the_bot_takes():
    """A model field with no `Bot` parameter would reach the worker as a `TypeError`.

    True for all 181 today, measured. It is a property of aiogram rather than of this package,
    which is exactly why it is worth a case: an aiogram release that broke it would otherwise
    break sends in the worker, and the failure would arrive as somebody's lost message.
    """
    import inspect

    import aiogram.methods
    from aiogram import Bot

    stray = {}
    for class_name in aiogram.methods.__all__:
        name = method_name(class_name)
        if name not in API_METHODS:
            continue
        fields = set(getattr(aiogram.methods, class_name).model_fields)
        parameters = set(inspect.signature(getattr(Bot, name)).parameters) - {'self'}
        if fields - parameters:
            stray[name] = sorted(fields - parameters)

    assert stray == {}, f'model fields the Bot method cannot take: {stray}'


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_media_group_object_keeps_its_discriminated_items():
    """The case `wire.serializers` warns about, driven through the producer."""
    from aiogram.methods import SendMediaGroup

    with capture_sends() as sent:
        TelegramBot().send(SendMediaGroup(chat_id=1, media=[InputMediaPhoto(media='file-id')]))

    assert sent[0].function == 'send_media_group'
    assert isinstance(sent[0].kwargs['media'][0], InputMediaPhoto)


@override_settings(TELEGRAM_BOT={**SETTINGS, 'ENABLED': False})
def test_a_disabled_bot_answers_the_object_form_with_an_id_too():
    """Whatever the form, a send that does nothing still answers with the id it would have used."""
    with capture_sends() as sent:
        identifier = TelegramBot().send(SendMessage(chat_id=1, text='nothing doing'))

    assert isinstance(identifier, uuid.UUID)
    assert list(sent) == []
