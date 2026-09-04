"""Integrations with things this package does not depend on.

Each one lives behind an extra and is imported by a *project* rather than by this package, so
a base install pulls nothing new and a missing library is an ``ImportError`` where somebody
asked for it rather than a broken install for everybody.

Nothing here is imported from anywhere else in the package, and nothing here is on any hot
path: a module in `contrib/` may only reach the seams a project could have reached itself.
"""

__all__: tuple[str, ...] = ()
