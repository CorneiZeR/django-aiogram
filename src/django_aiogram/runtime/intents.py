"""Carrying out what an operator asked for in the admin, where there is a loop to do it on.

A request must not talk to Telegram. An admin action over five hundred selected bots that
called ``getMe`` would hold one request open for five hundred round trips, and a timeout half
way through would leave nobody able to say which of them happened. So the page writes what was
asked into the row -- ``TelegramBot.intent`` -- and this is the other half: a process that
already has an event loop, and already holds the bot, does it and writes back one line.

**Claimed before it is carried out, and the claim is a lease.** Two containers may hold
different bots and both read this table, so the intent is taken with a compare-and-set --
the same claim the leases and the replay rows use, and for the same reason: it is the only one
that is atomic on every database this package supports. What is taken is a *claim beside* the
asking rather than the asking itself, because a worker that died between the two would
otherwise have carried the only record of the request away with it; the claim expires and the
next pass picks the intent up.

**And an answer is written only by the claim that is still held.** A slow worker finishing the
question before last must not replace the answer to the one asked after it -- the write is
conditional on the same claim, so a lapsed one writes nothing and says so in the log.

**One bot's failure is its own.** A token Telegram refuses is one line in one row; the pass
carries on to the others, which is the rule the supervisor beside it already keeps.
"""

import logging
import uuid
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from django.db.models import Q
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

#: how long a claim is believed. Longer than any single Telegram call and short enough that a
#: container killed mid-intent does not leave a person waiting: `getMe` and `setWebhook` answer
#: in well under a second, and the retry after this costs one duplicate call at worst
CLAIM_SECONDS = 60.0


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
    claim = uuid.uuid4().hex
    if not _claimed(identity, intent, claim):
        # another container holds it, or the row moved under this pass. Either way it is not
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
    return _wrote(identity, intent, claim, answer)


def _claimed(identity: int, intent: str, claim: str) -> bool:
    """Take this intent for this process, if nothing else holds a live claim on it.

    Compare-and-set against the asking *and* the claim, so two containers reading one table
    cannot both call Telegram for the same bot. The asking stays where it is: a worker that
    dies here must lose the intent rather than take the only record of it away, which is what
    the expiry below is for.
    """
    from django_aiogram.models import TelegramBot  # noqa: PLC0415 - as in `outstanding`

    now = timezone.now()
    lapsed = now - timedelta(seconds=CLAIM_SECONDS)
    rows = TelegramBot.objects.filter(bot_id=identity, intent=intent).filter(
        # unclaimed, or claimed by something that has stopped answering. The `NULL` is the
        # first claim on a row, which no comparison against a moment can match
        Q(intent_claim='') | Q(intent_claimed_at__isnull=True) | Q(intent_claimed_at__lt=lapsed)
    )
    # `updated_at` by hand: `update()` moves no `auto_now` column, and this row's watermark is
    # what every supervisor polls -- a claim that did not move it would be invisible until the
    # next unrelated edit
    return bool(rows.update(intent_claim=claim, intent_claimed_at=now, updated_at=now))


def _wrote(identity: int, intent: str, claim: str, answer: str) -> bool:
    """Write one line back under this claim, and say whether it was still ours to write.

    Conditional on the claim and on the asking: a worker slow enough to finish after its lease
    lapsed must not replace the answer to a question asked since, and an operator who asked for
    something else keeps their newer one.
    """
    from django_aiogram.models import TelegramBot  # noqa: PLC0415 - as above

    now = timezone.now()
    try:
        moved = TelegramBot.objects.filter(bot_id=identity, intent=intent, intent_claim=claim).update(
            intent='',
            intent_claim='',
            intent_claimed_at=None,
            # redacted for the reason the feed redacts: aiogram puts the API URL into its
            # messages, and the URL carries the token
            intent_result=redact_text(answer)[:RESULT],
            intent_done_at=now,
            updated_at=now,
        )
    except Exception:
        logger.exception('could not write back what a bot intent answered', extra={'tg_bot_id': identity})
        return False
    if not moved:
        logger.warning(
            'a bot intent was answered after its claim lapsed; the answer was dropped',
            extra={'tg_bot_id': identity, 'tg_intent': intent},
        )
    return bool(moved)


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
