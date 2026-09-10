"""Carrying out what an operator asked for in the admin, where there is a loop to do it on.

A request must not talk to Telegram. An admin action over five hundred selected bots that
called ``getMe`` would hold one request open for five hundred round trips, and a timeout half
way through would leave nobody able to say which of them happened. So the page writes what was
asked into the row -- ``TelegramBot.intent`` -- and this is the other half: a process that
already has an event loop, and already holds the bot, does it and writes back one line.

**Claimed before it is carried out.** Two containers may hold different bots and both read
this table, so the intent is taken with a compare-and-set against the value that was read --
the same claim the leases and the replay rows use, and for the same reason: it is the only one
that is atomic on every database this package supports.

**One bot's failure is its own.** A token Telegram refuses is one line in one row; the pass
carries on to the others, which is the rule the supervisor beside it already keeps.
"""

import logging
from typing import TYPE_CHECKING, Any

from django.utils import timezone

from django_aiogram.config.enums import BotIntent
from django_aiogram.wire.payloads import redact_text

if TYPE_CHECKING:
    from collections.abc import Iterable

    from django_aiogram.config.bots import BotRecord

__all__ = ('carry_out', 'outstanding')

logger = logging.getLogger('django_aiogram')

#: how much of an answer a row keeps. The column is 200 and the rest is a log line
RESULT = 200


def outstanding(identities: 'Iterable[int]') -> 'dict[int, str]':
    """Return the intent each of these bots is waiting on, for the ones that have one."""
    # deferred: the ORM, and this module is imported by a process that may have no database
    from django_aiogram.models import TelegramBot  # noqa: PLC0415 - as above

    rows = TelegramBot.objects.filter(bot_id__in=list(identities)).exclude(intent='')
    return dict(rows.values_list('bot_id', 'intent'))


def carry_out(records: 'Iterable[BotRecord]') -> int:
    """Do what has been asked of each of these bots, and say how many were answered.

    Reads once for the whole set: a pass over twenty bots that asked the table per bot would
    be twenty queries for a column that is empty almost always.
    """
    by_id = {record.bot_id: record for record in records if record.bot_id is not None}
    if not by_id:
        return 0
    try:
        asked = outstanding(by_id)
    except Exception:
        # the read-side rule this package keeps everywhere: a table that could not be read has
        # not said there is nothing to do
        logger.exception('could not read what the bots were asked to do')
        return 0
    done = 0
    for identity, intent in asked.items():
        if _one(identity, intent, by_id[identity]):
            done += 1
    return done


def _one(identity: int, intent: str, record: 'BotRecord') -> bool:
    """Claim one intent, carry it out, and write back what it answered."""
    if not _claimed(identity, intent):
        # another container took it, or the row moved under this pass. Either way it is not
        # this process's work any more, and doing it twice is a second call to Telegram
        return False
    try:
        answer = _performed(intent, record)
    except Exception as refused:  # noqa: BLE001 - one bot's failure is its own; see the module docstring
        answer = f'{type(refused).__name__}: {refused}'
        logger.warning(
            'a bot intent failed',
            extra={'tg_bot_id': identity, 'tg_intent': intent, 'tg_error': type(refused).__name__},
        )
    _wrote(identity, answer)
    return True


def _claimed(identity: int, intent: str) -> bool:
    """Take this intent, if it is still the one the row holds.

    Compare-and-set on the value that was read, so two containers reading one table cannot
    both call Telegram for the same bot. The row keeps ``intent_asked_at`` -- what it was asked
    and when it was asked are what a person reads -- and loses only the asking.
    """
    from django_aiogram.models import TelegramBot  # noqa: PLC0415 - as in `outstanding`

    # `updated_at` by hand: `update()` moves no `auto_now` column, and this row's watermark is
    # what every supervisor polls -- a claim that did not move it would be invisible until the
    # next unrelated edit
    return bool(
        TelegramBot.objects.filter(bot_id=identity, intent=intent).update(
            intent='',
            updated_at=timezone.now(),
        )
    )


def _wrote(identity: int, answer: str) -> None:
    """Write one line back, redacted, and let a database that refused not stop the pass."""
    from django_aiogram.models import TelegramBot  # noqa: PLC0415 - as above

    try:
        TelegramBot.objects.filter(bot_id=identity).update(
            # redacted for the reason the feed redacts: aiogram puts the API URL into its
            # messages, and the URL carries the token
            intent_result=redact_text(answer)[:RESULT],
            intent_done_at=timezone.now(),
            updated_at=timezone.now(),
        )
    except Exception:
        logger.exception('could not write back what a bot intent answered', extra={'tg_bot_id': identity})


def _performed(intent: str, record: 'BotRecord') -> str:
    """Do the one thing this intent names, and return the line a person will read."""
    # deferred: the registry reaches aiogram, and the webhook module reads the settings
    from django_aiogram.consumer.webhook import webhook_settings  # noqa: PLC0415 - as above
    from django_aiogram.runtime.registry import bots  # noqa: PLC0415 - as above

    serving = bots.for_record(record)
    loop = serving.loop
    if intent == BotIntent.CHECK.value:
        who = loop.run_until_complete(serving.bot.get_me())
        return f'ok: @{who.username} ({who.id})'
    if intent == BotIntent.SET_WEBHOOK.value:
        arguments: dict[str, Any] = webhook_settings(record, record.bot_id)
        loop.run_until_complete(serving.bot.set_webhook(**arguments))
        return f'webhook set to {arguments["url"]}'
    if intent == BotIntent.DELETE_WEBHOOK.value:
        loop.run_until_complete(serving.bot.delete_webhook())
        return 'webhook deleted'
    # an intent this version does not know: a row written by a newer admin against an older
    # container, which is a rolling deploy rather than a bug
    return f'unknown intent {intent!r}; this version does not know it'
