"""Each bot paced by its own numbers, and a change in a row taking effect without a restart.

Telegram meters the token, so per-bot budgets are the correct semantics rather than a
refinement: a noisy client throttled below the shared default, a client with paid
broadcasting above it. What this file is about is that the numbers come from that bot's own
resolved settings, and that a row moving is noticed.
"""

import pytest
from django.test import override_settings

from django_aiogram.models import TelegramBot
from django_aiogram.producer.throttling import get_rate_limiter, reset_rate_limiters

pytestmark = pytest.mark.django_db

FROM_DB = ('django_aiogram.runtime.providers.from_database',)
SETTINGS = {
    'BROKER': 'django_aiogram.testing.InMemoryBroker',
    'FSM_STORAGE': 'memory',
    'BOT_PROVIDERS': FROM_DB,
    # 23 rather than the packaged 30: a shared number equal to the default is a number the
    # case cannot tell from it, and "the bot inherited the shared budget" would then pass for
    # a limiter that read the packaged defaults or the process-wide dict instead
    'RATE_LIMIT': {'overall_per_second': 23},
}


@pytest.fixture(autouse=True)
def _no_limiters_left_behind():
    """The registry outlives a case, and a limiter left behind decides the next one's answer."""
    reset_rate_limiters()
    yield
    reset_rate_limiters()


def _desired():
    """The records the providers see, imported where the app registry is up."""
    from django_aiogram.runtime.providers import desired

    return desired()


def bot_for(bot_id):
    """The bot object one row describes."""
    from django_aiogram.runtime.providers import desired
    from django_aiogram.runtime.registry import bots

    found = next(record for record in desired() if record.bot_id == bot_id)
    return bots.for_record(found)


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_two_bots_with_different_budgets_pace_independently():
    """The claim #120 names: a client's own row decides their rate, not the shared default."""
    TelegramBot.objects.create(bot_id=123456, token='123456:AAaa', overrides={'RATE_LIMIT': {'overall_per_second': 1}})
    TelegramBot.objects.create(bot_id=654321, token='654321:BBbb', overrides={'RATE_LIMIT': {'overall_per_second': 5}})

    slow = bot_for(123456).rate_limiter
    quick = bot_for(654321).rate_limiter

    assert slow is not quick, 'two bots shared one budget'
    assert slow._overall.rate == 1, slow._overall.rate
    assert quick._overall.rate == 5, quick._overall.rate


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_bot_without_its_own_numbers_takes_the_shared_ones():
    """Which is most bots: a budget is a decision, and most clients need no decision."""
    TelegramBot.objects.create(bot_id=123456, token='123456:AAaa')

    assert bot_for(123456).rate_limiter._overall.rate == 23


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_budget_changed_in_a_row_takes_effect_without_a_restart():
    """The rates live in rows since 5.0, and a row moves without `setting_changed` firing.

    So a registry that only cleared on that signal would pace a client against the numbers
    their previous plan had — for as long as the process ran.
    """
    row = TelegramBot.objects.create(
        bot_id=123456,
        token='123456:AAaa',
        overrides={'RATE_LIMIT': {'overall_per_second': 1}},
    )
    assert bot_for(123456).rate_limiter._overall.rate == 1

    row.overrides = {'RATE_LIMIT': {'overall_per_second': 9}}
    row.save()

    assert bot_for(123456).rate_limiter._overall.rate == 9, 'the old plan was still pacing them'


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_the_same_token_asked_twice_is_one_limiter():
    """Telegram meters the token, so two objects holding one must not have a budget each."""
    TelegramBot.objects.create(bot_id=123456, token='123456:AAaa')
    found = bot_for(123456).settings

    assert get_rate_limiter('123456:AAaa', found) is get_rate_limiter('123456:AAaa', found)


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_a_bot_whose_limits_are_switched_off_keeps_no_limiter():
    """`RATE_LIMIT: {}` is a legitimate "do not pace this bot", and it has to be *now*.

    A limiter left in the registry would go on pacing a bot nothing asked to pace, which is
    the opposite of what the row now says.
    """
    row = TelegramBot.objects.create(
        bot_id=123456,
        token='123456:AAaa',
        overrides={'RATE_LIMIT': {'overall_per_second': 1}},
    )
    assert bot_for(123456).rate_limiter is not None

    row.overrides = {'RATE_LIMIT': {}}
    row.save()

    assert bot_for(123456).rate_limiter is None, 'a bot asked not to be paced was still paced'
    # and the entry is gone rather than left unread: a registry that kept one per bot whose
    # limits were ever switched off would grow by one per client for the life of the process
    from django_aiogram.producer.throttling import _registry

    assert '123456:AAaa' not in _registry._limiters, 'the limiter was kept, unread'


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_the_token_and_the_numbers_come_from_one_read(monkeypatch):
    """`settings` resolves each time it is asked, so two reads can straddle a rotation.

    The old token with the new numbers puts a limiter under a token nothing sends with, and
    the bot's pacing is then split across two budgets — which is the one thing keying on the
    token is there to prevent.
    """
    TelegramBot.objects.create(bot_id=123456, token='123456:AAaa', overrides={'RATE_LIMIT': {'overall_per_second': 1}})
    served = bot_for(123456)
    reads = []
    found = served.settings

    def rotating(self):
        """Answer with a different record every time, the way a rotation between reads looks."""
        reads.append(len(reads))
        if len(reads) > 1:
            return found.__class__(
                alias=found.alias,
                resolved={**found.resolved, 'TOKEN': '123456:AArotated'},
                origins=found.origins,
                declared=found.declared,
                provided=found.provided,
            )
        return found

    monkeypatch.setattr(type(served), 'settings', property(rotating))
    asked = []
    monkeypatch.setattr(
        'django_aiogram.producer.client.get_rate_limiter',
        lambda token, settings=None: asked.append((token, settings['TOKEN'])) or None,
    )

    assert served.rate_limiter is None  # the stand-in answers with nothing; the call is the point

    assert asked == [('123456:AAaa', '123456:AAaa')], 'the token and the numbers came from different reads'


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_two_records_holding_one_token_do_not_rebuild_each_other_s_limiter():
    """One token is one budget at Telegram, and a rebuild hands out a full burst.

    A row and a section may legally describe the same identity, and if
    each ask rebuilt the limiter the other built, alternating sends would start every call
    with an empty bucket queue, which is pacing switched off rather than shared.
    """
    row = TelegramBot.objects.create(
        bot_id=123456,
        token='123456:AAaa',
        overrides={'RATE_LIMIT': {'overall_per_second': 1}},
    )
    found = next(record for record in _desired() if record.bot_id == 123456)
    disagreeing = found.__class__(
        alias='rival',
        resolved={**found.resolved, 'RATE_LIMIT': {'overall_per_second': 7}},
        origins=found.origins,
        declared=True,
    )

    first = get_rate_limiter(row.token, found)
    assert get_rate_limiter(row.token, disagreeing) is first, 'the other configuration rebuilt it'
    assert first._overall.rate == 1, first._overall.rate
    assert get_rate_limiter(row.token, found) is first, 'the owner was displaced by the other one'


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_the_owner_of_a_shared_token_decides_even_when_it_decides_not_to_pace():
    """`RATE_LIMIT: {}` is a decision, and a shared token has to remember it as one.

    Kept by the owner rather than by the presence of a limiter: forgetting it would let the
    other configuration of the same identity build one on its next send, and the bot that
    asked not to be paced would be paced — by numbers nobody wrote for it.
    """
    row = TelegramBot.objects.create(bot_id=123456, token='123456:AAaa', overrides={'RATE_LIMIT': {}})
    found = next(record for record in _desired() if record.bot_id == 123456)
    disagreeing = found.__class__(
        alias='rival',
        resolved={**found.resolved, 'RATE_LIMIT': {'overall_per_second': 7}},
        origins=found.origins,
        declared=True,
    )

    assert get_rate_limiter(row.token, found) is None
    assert get_rate_limiter(row.token, disagreeing) is None, 'the other configuration started pacing the token'

    reset_rate_limiters()
    paced = get_rate_limiter(row.token, disagreeing)
    assert get_rate_limiter(row.token, found) is paced, 'the owner asked to pace and the token was not paced'
