"""What a failure to serve a bot was, and what the answer to each kind costs.

The three that are named are named because their right answers differ: a revoked token is
waited on for ever by anything that retries it, a conflict clears itself, and a 429 is the
ordinary weather. Everything else is retried, which is the safe end of a guess.
"""

import pytest
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramConflictError,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramRetryAfter,
    TelegramServerError,
    TelegramUnauthorizedError,
)
from aiogram.methods import GetMe

from django_aiogram.runtime.lifecycle import Fate, classify, waits

METHOD = GetMe()


@pytest.mark.parametrize(
    ('raised', 'expected'),
    [
        (TelegramUnauthorizedError(method=METHOD, message='Unauthorized'), Fate.REVOKED),
        (TelegramConflictError(method=METHOD, message='terminated by other getUpdates'), Fate.CONFLICT),
        (TelegramRetryAfter(method=METHOD, message='Too Many Requests', retry_after=3), Fate.TRANSIENT),
        (TelegramNetworkError(method=METHOD, message='connection reset'), Fate.TRANSIENT),
        (TelegramServerError(method=METHOD, message='Bad Gateway'), Fate.TRANSIENT),
        (TelegramBadRequest(method=METHOD, message='chat not found'), Fate.UNKNOWN),
        (TelegramForbiddenError(method=METHOD, message='bot was blocked'), Fate.UNKNOWN),
        (RuntimeError('a broker that refused a connection'), Fate.UNKNOWN),
    ],
)
def test_a_failure_is_read_by_what_telegram_answered(raised, expected):
    """By exception class, because aiogram has already read the status code and chosen one."""
    assert classify(raised) is expected


def test_only_a_revoked_token_is_one_no_wait_ends():
    """The distinction the module exists for, asserted on its own rather than through a pass.

    A retried 401 is one request per bot every five minutes for as long as the container
    runs, and every one of them fails for the reason the first one did.
    """
    assert not waits(Fate.REVOKED)
    assert waits(Fate.CONFLICT)
    assert waits(Fate.TRANSIENT)
    assert waits(Fate.UNKNOWN)


def test_a_fate_survives_a_round_trip_through_a_column():
    """It is written into `TelegramBot.quarantine_reason` and read back out of it."""
    assert Fate.REVOKED == 'revoked'
    assert Fate('revoked') is Fate.REVOKED
