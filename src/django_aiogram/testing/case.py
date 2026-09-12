"""The ``TestCase`` half, for the projects that never adopted pytest.

Both halves of the Django world get this, and neither is assumed: the mixin below and
``django_aiogram.testing.plugin``'s fixture are the same context manager, entered by
``setUp`` in one case and by pytest in the other.
"""

from typing import TYPE_CHECKING

from django_aiogram.testing.capture import Captured, capture_sends

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ('SendCaptureMixin',)


class SendCaptureMixin:
    """Capture every send the test queues, and hand it to the case as ``self.sent``.

    .. code-block:: python

        class ApprovalTests(SendCaptureMixin, TestCase):
            def test_the_reviewer_is_told(self):
                approve(order)

                assert self.sent.kwargs == [{'chat_id': 42, 'text': 'Order approved'}]

    Mixed in **before** ``TestCase``, so its ``setUp`` runs first and the capture is already
    up when the case's own ``setUp`` queues anything.

    A multi-bot case narrows it by setting :attr:`capture_bot` or :attr:`capture_queue` on the
    class, which is the only place a ``TestCase`` has to say so -- ``setUp`` runs before the
    method and cannot be passed anything. Every other bot then keeps the transport it was
    configured with, and ``self.sent.for_bot(...)`` refuses a bot outside the capture instead
    of answering with an empty list.
    """

    #: what the block queued, replaced per test. Declared so a reader of the class knows it
    #: is there without running one
    sent: Captured

    #: which bot the capture is narrowed to, by identity or by the alias it is configured
    #: under. ``None`` -- the default, and what a single-bot suite keeps -- captures every bot
    capture_bot: 'int | str | None' = None

    #: which queue it is narrowed to. ``None`` captures whichever queue each bot names
    capture_queue: str | None = None

    #: `TestCase` supplies this; declared so the mixin type-checks on its own, since it is a
    #: plain object until something mixes it into a case
    addCleanup: 'Callable[..., None]'  # noqa: N815 - unittest's own name, not one this package chose

    def setUp(self) -> None:
        """Start the capture, then let the case's own setup run inside it."""
        self._capture = capture_sends(self.capture_bot, queue=self.capture_queue)
        self.sent = self._capture.__enter__()
        self.addCleanup(self._stop)
        super().setUp()  # type: ignore[misc]

    def _stop(self) -> None:
        """Leave the context, through `addCleanup` so a failing setup still restores settings.

        ``tearDown`` would not: it does not run when ``setUp`` raises, and this one has already
        replaced ``TELEGRAM_BOT_DEFAULTS`` by then -- leaving an override installed for the rest of the
        suite, which is the kind of failure that looks like an unrelated test being broken.
        """
        self._capture.__exit__(None, None, None)
