"""The pytest half: one fixture, activated by the project rather than by installation.

Not an entry point. A ``pytest11`` plugin loads itself into every suite that has this package
installed, including the ones that never test a bot, and a fixture that installs a broker is
not something to arrive unannounced. A project asks for it:

.. code-block:: python

    # conftest.py
    pytest_plugins = ('django_aiogram.testing.plugin',)
"""

import contextlib
from typing import TYPE_CHECKING

import pytest

from django_aiogram.testing.capture import Captured, capture_sends

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

__all__ = ('capture_telegram_sends', 'telegram_sends')


@pytest.fixture
def telegram_sends() -> 'Iterator[Captured]':
    """Capture the sends this test queues, for the whole of it.

    Named for what it holds rather than for what it does, because that is how it reads at the
    assertion: ``assert telegram_sends.kwargs == [...]``.

    Captures every bot in the process; ``captured.for_bot(...)`` sorts them out afterwards.
    Use :func:`capture_telegram_sends` instead to narrow the capture itself, so the bots
    outside it keep the transport they were configured with.
    """
    with capture_sends() as captured:
        yield captured


@pytest.fixture
def capture_telegram_sends() -> 'Iterator[Callable[..., Captured]]':
    """Start a capture narrowed to one bot or one queue, for the rest of the test.

    .. code-block:: python

        def test_only_the_client_is_told(capture_telegram_sends):
            sent = capture_telegram_sends(bot='support')

            notify_everyone()

            assert sent.for_bot('support') == []

    A factory rather than a parametrised fixture, because which bot a case is about is the
    case's own business and ``request.param`` would put it in a decorator. Each call installs
    one more capture, and every one of them is left on the way out -- in the order a stack
    unwinds, whatever order they were started in.
    """
    with contextlib.ExitStack() as captures:

        def start(bot: 'int | str | None' = None, *, queue: str | None = None) -> Captured:
            """Enter one capture and keep it standing until the test ends."""
            return captures.enter_context(capture_sends(bot, queue=queue))

        yield start
