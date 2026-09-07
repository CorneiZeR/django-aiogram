"""The tables a project configures its bots in, and the two claims that must not collide.

Everything here needs a database, which is why it is in `tests/db`: the point of these rows
is what the database enforces about them rather than what Python does.
"""

import uuid
from datetime import timedelta

import pytest
from django.db import IntegrityError, transaction
from django.utils import timezone

from django_aiogram.models import (
    TelegramBot,
    TelegramBotLease,
    TelegramBotProfile,
    TelegramEvent,
    TelegramQueue,
    TelegramReplayClaim,
    TelegramScheduledSend,
)

pytestmark = pytest.mark.django_db

TOKEN = '123456:AAone'


def test_two_bots_may_replay_the_same_failure():
    """One correlation id can name a message from each of them, and each gets its own claim.

    Unique on the id alone -- which is what 4.1 had, when there was one bot -- the second bot's
    replay would have been refused as already handled, and its message never re-sent.
    """
    correlation = uuid.uuid4()
    TelegramReplayClaim.objects.create(correlation_id=correlation, bot_id=123456)
    TelegramReplayClaim.objects.create(correlation_id=correlation, bot_id=654321)

    assert TelegramReplayClaim.objects.filter(correlation_id=correlation).count() == 2


def test_one_bot_may_not_claim_a_failure_twice():
    """Which is the whole point of the table: the database tells the second run, not a read."""
    correlation = uuid.uuid4()
    TelegramReplayClaim.objects.create(correlation_id=correlation, bot_id=123456)

    with pytest.raises(IntegrityError), transaction.atomic():
        TelegramReplayClaim.objects.create(correlation_id=correlation, bot_id=123456)


def test_a_claim_without_an_identity_still_collides():
    """`0` rather than `NULL`, and this is why.

    A unique index treats two NULLs as distinct on every database this package supports, so a
    nullable column would have let two runs claim one failure and send the message twice --
    for exactly the bots whose token has no identity to read, which is the case a nullable
    column looks like it is handling.
    """
    correlation = uuid.uuid4()
    TelegramReplayClaim.objects.create(correlation_id=correlation)

    assert TelegramReplayClaim.objects.get(correlation_id=correlation).bot_id == 0
    with pytest.raises(IntegrityError), transaction.atomic():
        TelegramReplayClaim.objects.create(correlation_id=correlation)


def test_a_feed_row_and_a_scheduled_send_say_which_bot_they_are_about():
    """Both carry the identity, and both accept none: a 4.x row has nothing to say."""
    TelegramEvent.objects.create(kind='outbound.queued', correlation_id=uuid.uuid4(), bot_id=123456)
    TelegramEvent.objects.create(kind='outbound.queued', correlation_id=uuid.uuid4())
    TelegramScheduledSend.objects.create(
        correlation_id=uuid.uuid4(),
        due_at=timezone.now(),
        function='send_message',
        payload=b'{}',
        bot_id=123456,
    )

    assert TelegramEvent.objects.filter(bot_id=123456).count() == 1
    assert TelegramEvent.objects.filter(bot_id__isnull=True).count() == 1
    assert TelegramScheduledSend.objects.filter(bot_id=123456).count() == 1


def test_a_bot_takes_its_settings_from_a_profile_and_its_own_overrides():
    """Three levels as rows: the shared defaults, the profile, and the bot."""
    profile = TelegramBotProfile.objects.create(name='vip', overrides={'MAX_IN_FLIGHT': 4})
    queue = TelegramQueue.objects.create(name='tg:vip', pool='vip')
    bot = TelegramBot.objects.create(
        bot_id=123456,
        label='a client',
        token=TOKEN,
        profile=profile,
        queue=queue,
        overrides={'RATE_LIMIT': None},
    )

    assert bot.profile.overrides == {'MAX_IN_FLIGHT': 4}
    assert bot.overrides == {'RATE_LIMIT': None}, 'a sparse override has to be able to say None'
    assert bot.queue.pool == 'vip'
    assert bot.enabled is True


def test_a_profile_in_use_cannot_be_deleted_from_under_its_bots():
    """`PROTECT`, because a bot pointing at nothing would resolve settings nobody chose."""
    profile = TelegramBotProfile.objects.create(name='vip')
    TelegramBot.objects.create(bot_id=123456, profile=profile)

    from django.db.models import ProtectedError

    with pytest.raises(ProtectedError):
        profile.delete()


def test_one_row_per_bot():
    """Two rows for one identity are two configurations for one Telegram account."""
    TelegramBot.objects.create(bot_id=123456)

    with pytest.raises(IntegrityError), transaction.atomic():
        TelegramBot.objects.create(bot_id=123456)


def test_one_process_holds_a_bot_at_a_time():
    """Polling is exclusive, and the unique row is the agreement."""
    expires = timezone.now() + timedelta(seconds=300)
    TelegramBotLease.objects.create(bot_id=123456, holder='worker-a', expires_at=expires)

    with pytest.raises(IntegrityError), transaction.atomic():
        TelegramBotLease.objects.create(bot_id=123456, holder='worker-b', expires_at=expires)


def test_a_bot_does_not_print_its_token():
    """It reaches an admin list, a log line and a traceback like any other `__str__`."""
    bot = TelegramBot.objects.create(bot_id=123456, label='a client', token=TOKEN)

    assert TOKEN not in str(bot)
    assert '123456' in str(bot)
