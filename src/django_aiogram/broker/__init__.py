"""One transport per package, and the contract every one of them answers.

`base` is the contract, `models` the shapes it hands back, `exceptions` what goes wrong
choosing or reaching a transport, and `registry` how the one this process uses is resolved.
Each transport is a package of its own beside them.
"""

#: deliberately empty, like the other cluster packages: callers import from the modules, so
#: there is one path to `Broker` and one to `get_broker` rather than two of each
__all__: tuple[str, ...] = ()
