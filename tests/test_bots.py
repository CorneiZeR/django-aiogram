"""The bots a project configures, and how each one resolves its own settings.

Three levels, sparse by key presence, plus the identity every other part of 5.0 keys on.
The cases here are what the rest of the multi-bot work is built on: if resolution picks the
wrong value or the wrong origin, every finding, every queue and every event-log row points at
the wrong bot.
"""

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.test import override_settings

from django_aiogram.config import bots
from django_aiogram.config.bots import env_prefix
from django_aiogram.config.checks import check_settings
from django_aiogram.config.settings import ENV_PREFIX

TOKEN = '123456:AAHfixture-token-for-tests-0000000'
OTHER = '654321:BBHfixture-token-for-tests-0000000'


def ids(messages):
    """The check ids in a run, without the package prefix."""
    return {message.id.removeprefix('django_aiogram.') for message in messages}


def test_a_project_with_no_sections_has_one_bot_called_default():
    """The single-bot project keeps writing what it wrote, and gets a bot out of it."""
    with override_settings(TELEGRAM_BOT_DEFAULTS={'TOKEN': TOKEN}, TELEGRAM_BOTS=None):
        assert bots.aliases() == ('default',)
        assert bots.record('default')['TOKEN'] == TOKEN


def test_a_section_overrides_the_defaults_and_says_so():
    """A finding has to name the section that holds the value, not the dict that could have."""
    with override_settings(
        TELEGRAM_BOT_DEFAULTS={'TOKEN': TOKEN, 'MAX_RETRIES': 3},
        TELEGRAM_BOTS={'support': {'MAX_RETRIES': 7}},
    ):
        support = bots.record('support')
        assert support['MAX_RETRIES'] == 7
        assert support['TOKEN'] == TOKEN, 'what a section leaves out comes from the defaults'
        assert support.label('MAX_RETRIES') == "TELEGRAM_BOTS['support']['MAX_RETRIES']"
        assert support.label('TOKEN') == "TELEGRAM_BOT_DEFAULTS['TOKEN']"


def test_a_section_may_switch_off_what_the_defaults_switched_on():
    """`RATE_LIMIT: None` is a value, and inheritance has to be able to carry it.

    This is why an override is recorded by key *presence* rather than by a value being non-null:
    a null-means-inherit rule cannot express "this bot has no limits" at all.
    """
    with override_settings(
        TELEGRAM_BOT_DEFAULTS={'TOKEN': TOKEN, 'RATE_LIMIT': {'overall_per_second': 30}},
        TELEGRAM_BOTS={'blaster': {'RATE_LIMIT': None}},
    ):
        assert bots.record('blaster')['RATE_LIMIT'] is None


def test_one_bot_reads_its_own_environment(monkeypatch):
    """A section left blank falls to the bot's own variable before the shared one."""
    monkeypatch.setenv('DJANGO_AIOGRAM_MAX_RETRIES', '2')
    monkeypatch.setenv('DJANGO_AIOGRAM_SUPPORT_MAX_RETRIES', '9')
    with override_settings(TELEGRAM_BOT_DEFAULTS={'TOKEN': TOKEN}, TELEGRAM_BOTS={'support': {}, 'other': {}}):
        assert bots.record('support')['MAX_RETRIES'] == 9
        assert bots.record('support').label('MAX_RETRIES') == 'DJANGO_AIOGRAM_SUPPORT_MAX_RETRIES'
        assert bots.record('other')['MAX_RETRIES'] == 2, 'another bot keeps the shared variable'


def test_a_section_cannot_decide_what_the_process_owns():
    """One writer thread and one in-flight list per process, so a per-bot value is refused.

    Honoured, it would mean "whichever bot resolved last wins" — silently, and differently
    depending on the order the sections happen to be written in.
    """
    with override_settings(
        TELEGRAM_BOT_DEFAULTS={'TOKEN': TOKEN, 'WORKER_NAME': 'shared'},
        TELEGRAM_BOTS={'support': {'WORKER_NAME': 'mine', 'EVENT_LOG': True}},
    ):
        assert bots.record('support')['WORKER_NAME'] == 'shared'
        assert bots.record('support')['EVENT_LOG'] is False
        assert 'E053' in ids(check_settings())


def test_the_identity_is_the_number_in_the_token():
    """It survives a rotation, which is what the wire and the feed need of it."""
    assert bots.parse_bot_id(TOKEN) == 123456
    assert bots.parse_bot_id('123456:a-new-secret-after-a-rotation') == 123456
    assert bots.parse_bot_id('not-a-token') is None
    assert bots.parse_bot_id('') is None
    assert bots.parse_bot_id(None) is None


def test_a_token_without_an_identity_is_reported():
    """Nothing could address the bot: `E052` says so where the token was written."""
    with override_settings(TELEGRAM_BOT_DEFAULTS={'TOKEN': 'no-colon-here'}):
        assert 'E052' in ids(check_settings())


def test_two_aliases_holding_one_token_are_reported():
    """One token is one bot: the pair would race for its updates and pace twice for it."""
    with override_settings(
        TELEGRAM_BOT_DEFAULTS={},
        TELEGRAM_BOTS={'a': {'TOKEN': TOKEN}, 'b': {'TOKEN': TOKEN}, 'c': {'TOKEN': OTHER}},
    ):
        reported = [message for message in check_settings() if message.id == 'django_aiogram.E051']
        assert len(reported) == 1, [message.msg for message in reported]
        # the dict that configures several bots, never the shared defaults: neither token is there
        assert reported[0].msg.startswith('TELEGRAM_BOTS configures bot 123456 more than once:')
        assert "a from TELEGRAM_BOTS['a']['TOKEN']" in reported[0].msg
        assert "b from TELEGRAM_BOTS['b']['TOKEN']" in reported[0].msg
        assert 'c' not in reported[0].msg.split(':', 1)[-1]


def test_two_sections_inheriting_one_token_are_reported_against_the_defaults():
    """The other way to configure one bot twice, and the reason the origins are per alias.

    Both sections hold nothing, so the duplicate comes from the shared dict — and a message
    that named the sections would send the reader to two places that say nothing about it.
    """
    with override_settings(TELEGRAM_BOT_DEFAULTS={'TOKEN': TOKEN}, TELEGRAM_BOTS={'a': {}, 'b': {}}):
        reported = [message for message in check_settings() if message.id == 'django_aiogram.E051']
        assert len(reported) == 1, [message.msg for message in reported]
        assert reported[0].msg.count("TELEGRAM_BOT_DEFAULTS['TOKEN']") == 2, reported[0].msg


def test_the_dict_4_x_used_is_reported_rather_than_ignored():
    """Every value left in the old dict is ignored, and nothing else would say so.

    Reported even here, where the new dict carries a working token: the finding is about a
    dict nothing reads, not about the configuration being unusable.
    """
    with override_settings(TELEGRAM_BOT={'TOKEN': TOKEN}, TELEGRAM_BOT_DEFAULTS={'TOKEN': TOKEN}):
        reported = [message for message in check_settings() if message.id == 'django_aiogram.E050']
        assert len(reported) == 1
        assert reported[0].msg.startswith('TELEGRAM_BOT is no longer read')


def test_unknown_keys_are_reported_once_for_the_dict_that_holds_them():
    """`W003` asks the shared dict and `W010` asks a section, so one typo is one finding.

    One rule over both would print the shared dict's typo once per bot, and a reader counting
    findings would go looking for as many problems as there are bots.
    """
    with override_settings(
        TELEGRAM_BOT_DEFAULTS={'TOKEN': TOKEN, 'NOT_A_SETTING': 1},
        TELEGRAM_BOTS={'a': {}, 'b': {'ALSO_NOT': 2}},
    ):
        reported = check_settings()
        shared = [message for message in reported if message.id == 'django_aiogram.W003']
        section = [message for message in reported if message.id == 'django_aiogram.W010']
        assert len(shared) == 1, [message.msg for message in shared]
        assert len(section) == 1, [message.msg for message in section]
        assert section[0].msg.startswith("TELEGRAM_BOTS['b']")


def test_an_unreadable_bots_dict_is_a_finding_and_not_a_traceback():
    """`manage.py check` is where a reader goes to be told what is wrong with their settings."""
    with override_settings(TELEGRAM_BOT_DEFAULTS={'TOKEN': TOKEN}, TELEGRAM_BOTS=['not', 'a', 'mapping']):
        reported = [message for message in check_settings() if message.id == 'django_aiogram.E054']
        assert len(reported) == 1, [message.msg for message in reported]
        assert reported[0].msg.startswith('TELEGRAM_BOTS must be a mapping')


@pytest.mark.parametrize(
    'configured',
    [
        {'not a name': {}},
        {'-leading-dash': {}},
        {'support': 'not a mapping'},
    ],
)
def test_a_section_that_could_not_be_one_refuses(configured):
    """A key that could not be an alias, and a section that could not be settings."""
    with override_settings(TELEGRAM_BOTS=configured), pytest.raises(ImproperlyConfigured):
        bots.records()


@pytest.mark.parametrize('pair', [('foo', 'FOO'), ('foo_bar', 'foo-bar')])
def test_two_aliases_cannot_share_one_environment_prefix(pair):
    """The prefix is the alias uppercased, and uppercasing merges names that differ.

    Left alone, `DJANGO_AIOGRAM_FOO_TOKEN` would configure both `foo` and `FOO` — one variable
    quietly deciding for two bots. The refusal is on the alias rather than on the collision,
    because a pair that collides today is one a project can write either half of tomorrow.
    """
    legal, refused = pair
    assert bots._ALIAS.match(legal), f'{legal!r} is meant to be a usable alias'
    assert not bots._ALIAS.match(refused), f'{refused!r} is meant to be refused'
    # what the refusal buys: under a wider pattern these two are one variable
    assert env_prefix(legal) == f'{ENV_PREFIX}{refused.upper().replace("-", "_")}_'
    with override_settings(TELEGRAM_BOTS={refused: {'TOKEN': TOKEN}}), pytest.raises(ImproperlyConfigured):
        bots.records()


def test_an_unknown_alias_names_the_ones_there_are():
    """A typo in `bots['supprt']` is answered with the list rather than with a KeyError."""
    with override_settings(TELEGRAM_BOTS={'support': {'TOKEN': TOKEN}}):
        with pytest.raises(ImproperlyConfigured) as refused:
            bots.record('supprt')
        assert 'support' in str(refused.value)


def test_changing_the_settings_resolves_the_bots_again():
    """`override_settings` in a project's own suite has to reach this cache too."""
    with override_settings(TELEGRAM_BOTS={'a': {'TOKEN': TOKEN}}):
        assert bots.aliases() == ('a',)
    with override_settings(TELEGRAM_BOTS={'b': {'TOKEN': TOKEN}}):
        assert bots.aliases() == ('b',)


def test_a_reset_is_taken_under_the_lock_records_are_built_under(monkeypatch):
    """A reset landing mid-resolution must wait, not be overwritten by what was already read.

    `all()` reads the settings and stores what it built from them under one lock. A `reset()`
    outside that lock can land between the two and be lost: the records built from the settings
    that have just changed are stored anyway, and every later read is served from them until
    something else resets.

    Asserted on the lock rather than by racing two threads. The race is real and its window is
    one assignment wide, so a threaded case passes whether or not the fix is there — measured:
    written that way first, it passed with the lock taken out.
    """
    taken = []
    real = bots._registry._lock

    class Watched:
        """Stand in for the lock, recording that it was entered."""

        def __enter__(self):
            taken.append(True)
            return real.__enter__()

        def __exit__(self, *unused):
            return real.__exit__(*unused)

    monkeypatch.setattr(bots._registry, '_lock', Watched())
    bots.reset()
    assert taken, 'reset() runs outside the lock the records are built under'


def test_a_record_does_not_print_its_token():
    """It reaches logs and tracebacks like any other repr, and the token is the credential."""
    with override_settings(TELEGRAM_BOT_DEFAULTS={'TOKEN': TOKEN}):
        printed = repr(bots.record('default'))
        assert TOKEN not in printed
        assert 'bot_id=123456' in printed
