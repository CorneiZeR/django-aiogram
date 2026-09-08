"""Django system checks for the package settings.

Every check is a row in :data:`CHECKS`: an id, the setting it guards and a rule. The id is spelled
out in the row, so grepping ``E019`` finds both the check and the ``docs/wiki/Settings.md`` entry
that explains it.

Check ids are ``django_aiogram.EXXX``, and an id is never reused once its setting is gone: a
project silencing ``E013`` must not silently start silencing whatever came after it.

**The registry is the map of this package.** The rules live beside the subject they judge --
`bot`, `transport`, `eventlog` -- with `shapes` for the ones that only care what a value looks
like, `conditions` for the questions several rules ask first, and `problems` for what a finding is.
A reader looking for the rule behind an id finds it by the setting in the row, which is what the
split is for.

Names are re-exported here deliberately and not because they happen to be importable: `CHECKS` and
`check_settings` are what `apps.ready` runs, `worker_name_problems` is what `start_tgbot` asks for
itself, and the rest of the surface is the tests' -- `Problem` and `Check` to build a row,
`THREE_X_DELIVERIES` and `THREE_X_PATHS` to hold two copies of a list against each other.
"""

from functools import partial
from typing import Any

from django.core.checks import CheckMessage
from django.core.exceptions import ImproperlyConfigured

from django_aiogram.config.bots import defaults_record, records
from django_aiogram.config.checks.bot import (
    MODE_CHOICES,
    PAYLOAD_CHOICES,
    SERIALIZER_CHOICES,
    _a_declared_queue,
    _a_lease_a_pass_can_renew,
    _a_readable_bots_dict,
    _a_token_with_an_identity,
    _importable_storage,
    _known_bot_properties,
    _known_update_types,
    _one_bot_per_token,
    _readable_serializer,
    _sane_rate_limits,
    _serviceable_webhook,
    _settings_one_process_decides,
    _the_dict_5_0_replaced,
)
from django_aiogram.config.checks.conditions import _redis_is_in_use
from django_aiogram.config.checks.eventlog import (
    THREE_X_PATHS,
    _a_batch_the_buffer_can_hold,
    _a_configured_log_database,
    _a_log_that_is_pruned,
    _a_log_the_rename_left_behind,
    _a_routed_log_database,
    _a_router_this_release_still_has,
    _a_writer_that_does_not_block,
    _kinds_this_version_records,
    _somewhere_to_write_the_log,
)
from django_aiogram.config.checks.problems import Check, Problem
from django_aiogram.config.checks.shapes import (
    _a_callable,
    _a_collection_of_strings,
    _a_mapping,
    _a_number,
    _a_readable_boolean,
    _a_string,
    _an_integer,
    _filled_in_when_enabled,
)
from django_aiogram.config.checks.transport import (
    THREE_X_DELIVERIES,
    _a_pop_inside_the_deadline,
    _a_url_pickle_can_survive,
    _a_usable_broker,
    _a_usable_delivery,
    _a_worker_that_keeps_its_name,
    _known_keys,
    _known_section_keys,
    worker_name_problems,
)

__all__ = (
    'CHECKS',
    'THREE_X_DELIVERIES',
    'THREE_X_PATHS',
    'Check',
    'Problem',
    'check_settings',
    'worker_name_problems',
)


CHECKS: tuple[Check, ...] = (
    Check('E001', 'ENABLED', _a_readable_boolean),
    Check('E002', 'AUTODISCOVER', _a_readable_boolean),
    Check('E003', 'RAISE_EXCEPTION', _a_readable_boolean),
    Check('E017', 'ALLOW_PICKLE', _a_readable_boolean),
    Check('E049', 'TRANSACTIONAL', _a_readable_boolean),
    Check('E004', 'TOKEN', _a_string),
    Check('E052', 'TOKEN', _a_token_with_an_identity),
    Check('E005', 'REDIS_URL', _a_string),
    Check('E006', 'MODULE_NAME', _a_string),
    Check('E007', 'REDIS_MESSAGES_KEY', _a_string),
    Check('E021', 'WORKER_NAME', _a_string),
    Check('E009', 'DELIVERY', _a_usable_delivery),
    Check('E010', 'SERIALIZER', partial(_a_string, allowed=SERIALIZER_CHOICES)),
    Check('E011', 'FSM_STORAGE', _a_string),
    Check('E012', 'MAX_RETRIES', partial(_an_integer, minimum=1)),
    Check('E014', 'BLPOP_TIMEOUT', partial(_an_integer, minimum=1)),
    # 2, not 1: the consumer's blocking pop is capped one second inside this, and at 1
    # the subtraction clamps back to 1 — so the pop's own timeout equals the read
    # deadline and the deadline always wins. Every idle second then costs a
    # `TimeoutError`, a traceback and a reconnect, on a healthy server, for ever
    Check('E030', 'REDIS_TIMEOUT', partial(_an_integer, minimum=2)),
    Check('W004', 'BLPOP_TIMEOUT', _a_pop_inside_the_deadline),
    Check('E023', 'HEARTBEAT_INTERVAL', partial(_an_integer, minimum=1)),
    Check('E024', 'HEALTHCHECK_MAX_QUEUE', partial(_an_integer, minimum=0)),
    Check('E028', 'MODE', partial(_a_string, allowed=MODE_CHOICES)),
    Check('E025', 'WEBHOOK_URL', _a_string),
    Check('E026', 'WEBHOOK_SECRET', _a_string),
    Check('E027', 'WEBHOOK_URL', _serviceable_webhook),
    Check('E029', 'WEBHOOK_ALLOWED_UPDATES', _known_update_types),
    Check('E015', 'DEFAULT_KWARGS', _a_callable),
    Check('E016', 'DEFAULT_BOT_PROPERTIES', _a_mapping),
    Check('E018', 'DEFAULT_BOT_PROPERTIES', _known_bot_properties),
    Check('E020', 'RATE_LIMIT', _sane_rate_limits),
    Check('E022', 'SERIALIZER', _readable_serializer),
    Check('E019', 'FSM_STORAGE', _importable_storage),
    Check('E031', 'EVENT_LOG', _a_readable_boolean),
    Check('E032', 'EVENT_LOG_KINDS', _a_collection_of_strings),
    Check('E033', 'EVENT_LOG_PAYLOAD', partial(_a_string, allowed=PAYLOAD_CHOICES)),
    Check('E034', 'EVENT_LOG_MAX_PAYLOAD_BYTES', partial(_an_integer, minimum=0)),
    Check('E035', 'EVENT_LOG_REDACT_KEYS', _a_collection_of_strings),
    Check('E036', 'EVENT_LOG_BUFFER_SIZE', partial(_an_integer, minimum=1)),
    Check('E037', 'EVENT_LOG_BATCH_SIZE', partial(_an_integer, minimum=1)),
    Check('E038', 'EVENT_LOG_FLUSH_INTERVAL', partial(_an_integer, minimum=1)),
    Check('E039', 'EVENT_LOG_RETENTION_DAYS', partial(_an_integer, minimum=0)),
    Check('E040', 'EVENT_LOG_DATABASE', _a_string),
    Check('E041', 'EVENT_LOG_DATABASE', _a_configured_log_database),
    # I, not W: this cannot see inside a router, so a project whose own router returns
    # the alias is correctly configured and would still be reported. Information the
    # reader can act on, not a condition worth failing `check --fail-level WARNING`
    Check('I002', 'EVENT_LOG_DATABASE', _a_routed_log_database),
    # keyed on nothing, because the condition is a table rather than a setting: it is true or
    # false whatever `TELEGRAM_BOT_DEFAULTS` says, and the message names the alias it asked
    Check('I003', '', _a_log_the_rename_left_behind, process=True),
    Check('E047', 'BROKER', _a_usable_broker),
    Check('E042', 'EVENT_LOG_SYNC', _a_readable_boolean),
    Check('E043', 'REDIS_URL', _a_url_pickle_can_survive),
    Check('E044', 'DRAIN_TIMEOUT', partial(_a_number, minimum=0)),
    Check('E045', 'MAX_IN_FLIGHT', partial(_an_integer, minimum=0)),
    Check('E060', 'MAX_IN_FLIGHT_PER_BOT', partial(_an_integer, minimum=0)),
    Check('E046', 'REQUIRE_CRASH_SAFE', _a_readable_boolean),
    Check('W005', 'EVENT_LOG', _somewhere_to_write_the_log),
    Check('W006', 'EVENT_LOG_RETENTION_DAYS', _a_log_that_is_pruned),
    Check('W007', 'EVENT_LOG_BATCH_SIZE', _a_batch_the_buffer_can_hold),
    Check('W008', 'EVENT_LOG_KINDS', _kinds_this_version_records),
    Check('W009', 'EVENT_LOG_SYNC', _a_writer_that_does_not_block),
    # I, not W: a check cannot tell a consumer from the web tier, and every container
    # without `hostname:` matches — so as a warning it failed `check --fail-level WARNING`
    # in processes that own no in-flight list. `start_tgbot` warns for itself, where being
    # the consumer is known
    Check('I001', 'WORKER_NAME', _a_worker_that_keeps_its_name),
    Check('W003', '', _known_keys, process=True),
    Check('W010', '', _known_section_keys),
    Check('E048', '', _a_router_this_release_still_has, process=True),
    # the three rows about the set of bots rather than about any one of them
    Check('E050', '', _the_dict_5_0_replaced, process=True),
    Check('E051', '', _one_bot_per_token, process=True),
    Check('E054', '', _a_readable_bots_dict, process=True),
    Check('E053', '', _settings_one_process_decides, process=True),
    # the leases a polling container takes: how many, and for how long
    # the queue a bot publishes to: a name, and one this deployment has declared
    Check('E057', 'QUEUE', _a_string),
    Check('E058', 'QUEUES', _a_collection_of_strings, process=True),
    Check('E059', 'QUEUE', _a_declared_queue),
    Check('E055', 'MAX_BOTS_PER_WORKER', partial(_an_integer, minimum=0), process=True),
    Check('E056', 'BOT_LEASE_SECONDS', partial(_a_number, minimum=1), process=True),
    Check('W011', 'BOT_LEASE_SECONDS', _a_lease_a_pass_can_renew, process=True),
    Check(
        'W001',
        'TOKEN',
        partial(
            _filled_in_when_enabled,
            hint='Set it, or set ENABLED to False in processes that never send to Telegram.',
        ),
    ),
    Check(
        'W002',
        'REDIS_URL',
        partial(
            _filled_in_when_enabled,
            hint=(
                'Set it: BROKER or FSM_STORAGE names Redis, so something here needs the URL. '
                'Or set ENABLED to False in processes that never touch either.'
            ),
            only_if=_redis_is_in_use,
        ),
    ),
)


def check_settings(**kwargs: Any) -> list[CheckMessage]:
    """Run every registered check: once for the process, then once for each configured bot.

    Two passes because a rule guards one of two kinds of setting. A process-scoped one is decided
    once whatever the deployment runs -- the event log's writer, router discovery -- and reporting
    it per bot would print the same finding five times. Everything else belongs to a bot, and its
    finding names the section that holds the value.
    """
    process = defaults_record()
    messages = [message for check in CHECKS if check.per_process for message in check.run(process)]
    try:
        configured = records()
    except ImproperlyConfigured:
        # `E054` has already reported it, and there are no bots to judge
        return messages
    for record in configured:
        messages += [message for check in CHECKS if not check.per_process for message in check.run(record)]
    return messages
