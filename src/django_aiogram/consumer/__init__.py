"""The receive side: what turns a queued message or an update into a call.

`delivery` consumes the broker and hands each message to a handler; `webhook` is the view
an update arrives at; `routers` finds the handler modules a project wrote.

The consumer talks to a transport only through `broker`. That boundary is why 4.0 can add a
transport without touching this package.
"""

#: deliberately empty: callers import from the modules in this package, not from the
#: package itself. A re-export here would make a second path to every name, and the one
#: nobody chose is the one that cannot be moved later
__all__: tuple[str, ...] = ()
