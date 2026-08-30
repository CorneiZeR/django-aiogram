"""The handler decorators, which are the only part of the bot that is only about the router.

Fifteen one-line methods and the builder they share. They read one attribute -- the
router -- and touch nothing else the client holds: not the loop, not the in-flight sends,
not the shutdown state. That is what makes them safe to lift out of a file whose subject is
those things, and why they are a mixin rather than a second object: ``bot.message(...)`` is
what every project writes, and a decorator that arrived through an attribute would be a
different API for the same thing.

**Fifteen is not all of them, and that is the honest shape.** A ``Router`` carries 27
observers on aiogram 3.x -- the business-account ones, message reactions, chat boosts and
more -- and these fifteen are what this package has published since 2.x and what
``tests/test_public_surface.py`` pins. Everything else is reached through ``bot.router``,
which is public for exactly that reason, so nothing is out of reach; adding a name here is
a decision about this package's surface rather than bookkeeping about aiogram's.
"""

from typing import Any

from aiogram import Router
from aiogram.dispatcher.event.handler import CallbackType


class RouterShortcuts:
    """``bot.message(...)`` and its fourteen siblings, on whatever holds a router."""

    #: provided by the class this is mixed into. Declared rather than assigned: this is a
    #: statement about what the methods below require, not a second place that builds one
    _router: Router

    def message(self, *args: Any, **kwargs: Any) -> CallbackType:
        """Return a decorator registering a handler for the 'message' observer."""
        return self._add_router(*args, event_name='message', **kwargs)

    def edited_message(self, *args: Any, **kwargs: Any) -> CallbackType:
        """Return a decorator registering a handler for the 'edited_message' observer."""
        return self._add_router(*args, event_name='edited_message', **kwargs)

    def channel_post(self, *args: Any, **kwargs: Any) -> CallbackType:
        """Return a decorator registering a handler for the 'channel_post' observer."""
        return self._add_router(*args, event_name='channel_post', **kwargs)

    def edited_channel_post(self, *args: Any, **kwargs: Any) -> CallbackType:
        """Return a decorator registering a handler for the 'edited_channel_post' observer."""
        return self._add_router(*args, event_name='edited_channel_post', **kwargs)

    def inline_query(self, *args: Any, **kwargs: Any) -> CallbackType:
        """Return a decorator registering a handler for the 'inline_query' observer."""
        return self._add_router(*args, event_name='inline_query', **kwargs)

    def chosen_inline_result(self, *args: Any, **kwargs: Any) -> CallbackType:
        """Return a decorator registering a handler for the 'chosen_inline_result' observer."""
        return self._add_router(*args, event_name='chosen_inline_result', **kwargs)

    def callback_query(self, *args: Any, **kwargs: Any) -> CallbackType:
        """Return a decorator registering a handler for the 'callback_query' observer."""
        return self._add_router(*args, event_name='callback_query', **kwargs)

    def shipping_query(self, *args: Any, **kwargs: Any) -> CallbackType:
        """Return a decorator registering a handler for the 'shipping_query' observer."""
        return self._add_router(*args, event_name='shipping_query', **kwargs)

    def pre_checkout_query(self, *args: Any, **kwargs: Any) -> CallbackType:
        """Return a decorator registering a handler for the 'pre_checkout_query' observer."""
        return self._add_router(*args, event_name='pre_checkout_query', **kwargs)

    def poll(self, *args: Any, **kwargs: Any) -> CallbackType:
        """Return a decorator registering a handler for the 'poll' observer."""
        return self._add_router(*args, event_name='poll', **kwargs)

    def poll_answer(self, *args: Any, **kwargs: Any) -> CallbackType:
        """Return a decorator registering a handler for the 'poll_answer' observer."""
        return self._add_router(*args, event_name='poll_answer', **kwargs)

    def my_chat_member(self, *args: Any, **kwargs: Any) -> CallbackType:
        """Return a decorator registering a handler for the 'my_chat_member' observer."""
        return self._add_router(*args, event_name='my_chat_member', **kwargs)

    def chat_member(self, *args: Any, **kwargs: Any) -> CallbackType:
        """Return a decorator registering a handler for the 'chat_member' observer."""
        return self._add_router(*args, event_name='chat_member', **kwargs)

    def chat_join_request(self, *args: Any, **kwargs: Any) -> CallbackType:
        """Return a decorator registering a handler for the 'chat_join_request' observer."""
        return self._add_router(*args, event_name='chat_join_request', **kwargs)

    def error(self, *args: Any, **kwargs: Any) -> CallbackType:
        """Return a decorator registering a handler for the 'error' observer."""
        return self._add_router(*args, event_name='error', **kwargs)

    def _add_router(self, *args: Any, event_name: str, **kwargs: Any) -> CallbackType:
        """Build the decorator every observer method above returns."""

        def wrapper(callback: CallbackType) -> CallbackType:
            """Register the handler and hand it back unchanged.

            Returning the callback rather than a wrapper is what lets these decorators
            stack, and what keeps the handler directly callable from a test.
            """
            observer = self._router.observers[event_name]
            observer.register(callback, *args, **kwargs)
            return callback

        return wrapper
