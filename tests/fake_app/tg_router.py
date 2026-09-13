"""Handlers picked up by autodiscover during django.setup()."""

from aiogram import F, types

from django_aiogram import bot

IMPORTED = True

#: what these handlers answer to, and nothing else. They exist to prove the module was
#: imported, not to handle anything — and since 5.0 the router is one per process, so a
#: filter that matched everything would sit in front of every handler any other case
#: registers and swallow its updates. aiogram stops at the first handler that matches.
MARKER = '__autodiscovered__'


@bot.message(F.text == MARKER)
async def autodiscovered_message(message: types.Message) -> None:  # pragma: no cover
    ...


@bot.callback_query(F.data == MARKER)
async def autodiscovered_callback(query: types.CallbackQuery) -> None:  # pragma: no cover
    ...
