"""The send side: what a project calls to make a message happen.

`client` is the bot and the producer — routing a send to Telegram or to the queue, and
owning the loop a webhook process turns. `throttling` paces what reaches Telegram.

A caller outside this package names `client` and nothing else. `throttling` is reached
through the bot, and its rate is a setting rather than an argument.
"""

#: deliberately empty: callers import from the modules in this package, not from the
#: package itself. A re-export here would make a second path to every name, and the one
#: nobody chose is the one that cannot be moved later
__all__: tuple[str, ...] = ()
