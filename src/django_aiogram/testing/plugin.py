"""The pytest half: one fixture, activated by the project rather than by installation.

Not an entry point. A ``pytest11`` plugin loads itself into every suite that has this package
installed, including the ones that never test a bot, and a fixture that installs a broker is
not something to arrive unannounced. A project asks for it:

.. code-block:: python

    # conftest.py
    pytest_plugins = ('django_aiogram.testing.plugin',)
"""

from typing import TYPE_CHECKING

import pytest

from django_aiogram.testing.capture import Captured, capture_sends

if TYPE_CHECKING:
    from collections.abc import Iterator

__all__ = ('telegram_sends',)


@pytest.fixture
def telegram_sends() -> 'Iterator[Captured]':
    """Capture the sends this test queues, for the whole of it.

    Named for what it holds rather than for what it does, because that is how it reads at the
    assertion: ``assert telegram_sends.kwargs == [...]``.
    """
    with capture_sends() as captured:
        yield captured
