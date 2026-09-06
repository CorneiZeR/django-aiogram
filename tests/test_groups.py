"""What bots share when their settings agree, and what stays theirs when they do not.

The profile decides, and it is computed rather than configured — so these cases are written
against *values*, never against a group somebody named. The last one is the invariant the
whole design rests on: grouping may change what is held open, and nothing else.
"""

from typing import Any

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.test import override_settings

import django_aiogram
from django_aiogram.broker.registry import use_broker
from django_aiogram.config import bots as configured
from django_aiogram.runtime import groups
from django_aiogram.runtime.profiles import profile_of
from django_aiogram.runtime.registry import bots
from django_aiogram.testing import InMemoryBroker
from django_aiogram.wire.envelope import unpack
from django_aiogram.wire.serializers import loads

TOKEN = '123456:AAone'
OTHER = '654321:BBtwo'
MEMORY = 'django_aiogram.testing.InMemoryBroker'


def queued(group) -> list[str]:
    """What a group holds, as the calls it would make.

    The bytes themselves cannot be compared across runs: every send carries its own
    correlation id and the moment it was queued, so two identical sends differ by
    construction. The call is the part a comparison is about.
    """
    read = [unpack(loads(message)) for message in group.broker.messages]
    # rendered rather than compared as dicts, which do not order against each other
    return sorted(f'{envelope.function} {sorted(envelope.kwargs.items())}' for envelope in read)


@pytest.fixture(autouse=True)
def _no_groups_left_behind():
    """A group holds a transport, and a case that leaves one decides the next case's answer."""
    groups.close_groups()
    yield
    groups.close_groups()


def two_bots(**apart: Any) -> dict[str, dict[str, Any]]:
    """Two bots that differ only in their tokens, plus whatever a case pulls apart."""
    return {'a': {'TOKEN': TOKEN}, 'b': {'TOKEN': OTHER, **apart}}


def test_bots_configured_alike_share_one_transport():
    """Twenty of them hold one connection, which is the whole point of a profile."""
    with override_settings(TELEGRAM_BOT_DEFAULTS={'BROKER': MEMORY}, TELEGRAM_BOTS=two_bots()):
        assert bots['a'].group is bots['b'].group
        assert bots['a'].broker is bots['b'].broker
        assert len(groups.live_groups()) == 1


def test_a_setting_the_transport_reads_pulls_them_apart():
    """A different deadline is a different broker, so it cannot be the same instance."""
    with override_settings(TELEGRAM_BOT_DEFAULTS={'BROKER': MEMORY}, TELEGRAM_BOTS=two_bots(MEMORY_TIMEOUT=9.0)):
        assert bots['a'].group is not bots['b'].group
        assert bots['a'].broker is not bots['b'].broker
        assert len(groups.live_groups()) == 2


def test_a_setting_only_the_bot_reads_does_not():
    """A token, a budget and a parse mode are one bot's; sharing a queue with another is free."""
    apart = {
        'RATE_LIMIT': {'overall_per_second': 5},
        'MAX_RETRIES': 2,
        'DEFAULT_BOT_PROPERTIES': {'parse_mode': 'HTML'},
    }
    with override_settings(TELEGRAM_BOT_DEFAULTS={'BROKER': MEMORY}, TELEGRAM_BOTS=two_bots(**apart)):
        assert bots['a'].group is bots['b'].group, 'a bot-only setting split the group'
        assert bots['a'].settings['MAX_RETRIES'] != bots['b'].settings['MAX_RETRIES']


def test_the_profile_is_the_resolved_value_and_not_the_writing_of_it():
    """One bot inherits what the other spells out, and they are the same configuration."""
    with override_settings(
        TELEGRAM_BOT_DEFAULTS={'BROKER': MEMORY, 'MAX_IN_FLIGHT': 4},
        TELEGRAM_BOTS={'a': {'TOKEN': TOKEN}, 'b': {'TOKEN': OTHER, 'MAX_IN_FLIGHT': 4}},
    ):
        assert profile_of(configured.record('a')) == profile_of(configured.record('b'))


def test_a_mapping_written_in_another_order_is_the_same_profile():
    """Two dicts with the same pairs are one configuration, whatever order they were typed in."""
    first = {'overall_per_second': 30, 'per_chat_per_second': 1}
    with override_settings(
        TELEGRAM_BOT_DEFAULTS={'BROKER': MEMORY, 'DEFAULT_BOT_PROPERTIES': first},
        TELEGRAM_BOTS={
            'a': {'TOKEN': TOKEN},
            'b': {'TOKEN': OTHER, 'DEFAULT_BOT_PROPERTIES': dict(reversed(list(first.items())))},
        },
    ):
        # not in the profile at all, so it cannot split them either way — asserted on the
        # normalisation itself, which is what a profile-deciding mapping would go through
        assert profile_of(configured.record('a')) == profile_of(configured.record('b'))


def test_grouping_changes_what_is_held_open_and_nothing_else():
    """The invariant the design rests on, and the reason grouping is safe to do at all.

    Two bots send the same two messages. Whether their settings put them in one group or two,
    the same payloads are queued — grouping decides how many connections are open, never what
    goes on the wire. A profile that changed the answer would be an optimisation that is also
    a behaviour change, which is the thing this must never be.
    """
    sent = {}
    for name, apart in (('shared', {}), ('split', {'MEMORY_TIMEOUT': 9.0})):
        with override_settings(TELEGRAM_BOT_DEFAULTS={'BROKER': MEMORY}, TELEGRAM_BOTS=two_bots(**apart)):
            groups.close_groups()
            bots['a'].enqueue('send_message', chat_id=1, text='one')
            bots['b'].enqueue('send_message', chat_id=2, text='two')
            sent[name] = sorted(call for group in groups.live_groups() for call in queued(group))
            expected = 1 if apart == {} else 2
            assert len(groups.live_groups()) == expected, f'{name} built {len(groups.live_groups())} groups'

    assert sent['shared'] == sent['split'], 'the same sends produced different calls once grouped'
    assert len(sent['shared']) == 2, 'both messages have to be there, or the case compares nothing'


def test_a_capture_reaches_a_bot_whichever_group_it_is_in():
    """`use_broker` is process-wide on purpose: a suite captures the sends, not a connection."""
    captured = InMemoryBroker()
    with (
        override_settings(TELEGRAM_BOT_DEFAULTS={'BROKER': MEMORY}, TELEGRAM_BOTS=two_bots(MEMORY_TIMEOUT=9.0)),
        use_broker(captured),
    ):
        assert bots['a'].broker is captured
        assert bots['b'].broker is captured, 'a second group escaped the capture'


def test_closing_a_bot_releases_its_group_and_the_next_send_rebuilds():
    """A group is shared, so closing is a release rather than a retirement."""
    with override_settings(TELEGRAM_BOT_DEFAULTS={'BROKER': MEMORY}, TELEGRAM_BOTS=two_bots()):
        first = bots['a'].broker
        bots['a'].close()

        assert bots['b'].broker is not first, 'the released transport came back'


def test_changing_the_settings_drops_the_groups():
    """A group is built from settings, and a suite that changes them must not read a stale one."""
    with override_settings(TELEGRAM_BOT_DEFAULTS={'BROKER': MEMORY}, TELEGRAM_BOTS=two_bots()):
        assert bots['a'].broker is not None
        assert groups.live_groups() != ()
    assert groups.live_groups() == (), 'leaving the block left a transport open'


def test_the_default_bot_is_the_one_the_package_exports():
    """`django_aiogram.bot` is where a project's handlers are registered and what shutdown closes."""
    with override_settings(TELEGRAM_BOTS={'default': {'TOKEN': TOKEN}, 'support': {'TOKEN': OTHER}}):
        assert bots['default'] is django_aiogram.bot
        assert bots['support'] is not django_aiogram.bot
        assert bots['support'] is bots['support'], 'a second lookup built a second bot'


def test_a_bot_is_reachable_by_the_identity_a_message_will_carry():
    """An alias is what a settings file writes; the identity is what the wire names."""
    with override_settings(TELEGRAM_BOTS={'default': {'TOKEN': TOKEN}, 'support': {'TOKEN': OTHER}}):
        assert bots.by_id(654321) is bots['support']
        with pytest.raises(ImproperlyConfigured, match='No bot is configured with the identity'):
            bots.by_id(1)


def test_an_unknown_alias_is_refused_with_the_ones_there_are():
    """The listing is the useful half: a typo is answered rather than merely rejected."""
    with (
        override_settings(TELEGRAM_BOTS={'support': {'TOKEN': TOKEN}}),
        pytest.raises(ImproperlyConfigured, match='support'),
    ):
        bots['supprt']
