"""Which method names a queued payload is allowed to name, and how a call becomes one.

A payload carries the name of the method to call, so without this list the queue
could reach anything public on ``Bot``: ``download_file`` writes to the
container's filesystem, ``token`` hands out the credential.

This lives apart from ``client`` so the delivery consumer can check a payload
before handing it anywhere, without importing the client.

It is also where an **aiogram method object** becomes the ``(name, kwargs)`` pair the wire
carries -- see :func:`resolve_call`. That is how a send gets checked before it is made
without this package declaring 181 signatures of its own: the names, the arguments and the
types all belong to aiogram, so they move when aiogram moves.
"""

import re
from typing import TYPE_CHECKING, Any

import aiogram.methods
from aiogram import Bot

from django_aiogram.exceptions import UnknownApiMethodError

if TYPE_CHECKING:
    from aiogram.methods.base import TelegramMethod

#: API methods a queued payload must never reach. They are administrative, not
#: sends: set_webhook would point updates at someone else's URL, and log_out or
#: close ends the session for the whole deployment.
DENIED_METHODS = frozenset({'set_webhook', 'delete_webhook', 'log_out', 'close'})


def method_name(camel_case: str) -> str:
    """Turn an aiogram method's name into the ``Bot`` attribute it corresponds to.

    ``SendMessage`` and ``sendMessage`` are both ``send_message``, which is what lets the two
    callers here use it on different inputs: the allowlist below is built from the class names
    in ``aiogram.methods.__all__``, and :func:`resolve_call` reads ``__api_method__`` off the
    object it was handed. Spelled twice, a ``CamelCase`` aiogram ever writes unusually would be
    allowed under one name and sent under another.
    """
    return re.sub(r'(?<!^)(?=[A-Z])', '_', camel_case).lower()


def _api_methods() -> frozenset[str]:
    """Return the Bot attributes that correspond to a Telegram API method."""
    api = {method_name(name) for name in aiogram.methods.__all__}
    public = {name for name in dir(Bot) if not name.startswith('_')}
    return frozenset(api & public) - DENIED_METHODS


API_METHODS = _api_methods()


def check_function(function: str) -> str:
    """Return ``function`` if it names a Telegram API method, else raise."""
    if function not in API_METHODS:
        raise UnknownApiMethodError(function, len(API_METHODS))
    return function


def is_method(value: object) -> bool:
    """Whether ``value`` is an aiogram method object rather than a method *name*.

    By the two attributes such an object has and nothing else does, rather than by
    ``isinstance(value, TelegramMethod)``. Not to avoid the import -- this module imports
    aiogram already -- but because the check then holds for a project's own subclass and for
    whatever aiogram reorganises its base classes into, and because there is nothing an
    ``isinstance`` would catch that this does not.
    """
    return hasattr(value, '__api_method__') and hasattr(value, 'model_fields_set')


def resolve_call(function: 'TelegramMethod[Any] | str', kwargs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Answer with the ``(name, kwargs)`` the wire carries, from either way of asking.

    A string passes straight through: that is the 2.x call and the only form that works when
    the method name is a variable. An aiogram method object is unpacked into the same pair,
    which is what makes ``bot.send(SendMessage(chat_id=..., text=...))`` a *typed* send --
    every name and every argument is checked by aiogram's own model, at the call site, by
    whatever type checker the project runs. Nothing here declares a signature, so nothing here
    can drift from the API aiogram supports.

    **The fields the caller set, as objects rather than as a dump.**
    ``model_dump(exclude_unset=True)`` looks like the way to do this and loses discriminators:
    ``wire.serializers`` says so from the other side, and an ``InputMediaPhoto`` comes back an
    ``InputMediaAudio``. Reading the attributes leaves nested models as models, which is the
    shape the serializer already tags -- and the shape a caller passing ``reply_markup=`` to
    ``send()`` has always used.

    Unset fields are left out for the same reason ``exclude_unset`` was reached for: aiogram
    fills them with a ``Default`` sentinel that means "whatever the bot is configured for",
    and the bot in the *worker* is the one that should answer that.

    **A field aiogram does not declare is refused here**, which is the half of this a type
    checker does not do for us. Measured: aiogram's method models are configured
    ``extra='allow'``, so ``SendMessage(chat_id=1, text='x', parse_mod='HTML')`` is *accepted*
    and keeps the misspelling in ``model_fields_set`` -- and neither mypy nor the pydantic mypy
    plugin says a word, checked with the plugin enabled. Put on the wire, it reaches
    ``Bot.send_message(**kwargs)`` in the worker as a ``TypeError`` about a name nobody can
    see from there. So the model's own field list is the allowlist, and the refusal happens on
    the line that wrote the typo. Every one of the 181 methods declares exactly the fields its
    ``Bot`` method takes as parameters -- measured, no exceptions -- so nothing legitimate is
    caught by this.

    The string form is left alone: it accepts whatever keyword arguments a caller passes, as
    it has since 2.x, and a project that wants them checked has the object form to move to.

    **The name comes from ``__api_method__`` rather than from the class name.** Measured: a
    project subclassing ``SendMessage`` -- to carry a default, or a field of its own -- has a
    class called whatever it called it, and the class name then resolves to ``my_send`` and is
    refused as no Telegram method. ``__api_method__`` is inherited, so the subclass resolves to
    ``send_message`` like its parent. Read off the *instance*, because on the base class it is
    a ``property`` and only the concrete ones declare a string.
    """
    if isinstance(function, str):
        return function, kwargs
    if not is_method(function):
        msg = (
            f'send takes a method name or an aiogram method object, not {type(function).__name__}. '
            f"Pass a string like 'send_message', or aiogram.methods.SendMessage(...)."
        )
        raise TypeError(msg)
    if kwargs:
        msg = (
            f'{type(function).__name__} carries its own arguments, so '
            f'{sorted(kwargs)} cannot be passed beside it. Put them in the method object.'
        )
        raise TypeError(msg)
    kind = type(function)
    unknown = sorted(set(function.model_fields_set) - set(kind.model_fields))
    if unknown:
        msg = (
            f'{kind.__name__} has no field {unknown[0]!r}'
            + (f' (and {len(unknown) - 1} more: {unknown[1:]})' if len(unknown) > 1 else '')
            + '. aiogram accepts unknown fields and this does not: the worker would refuse it '
            'where nobody is looking.'
        )
        raise TypeError(msg)
    name = check_function(method_name(str(function.__api_method__)))
    return name, {field: getattr(function, field) for field in function.model_fields_set}
