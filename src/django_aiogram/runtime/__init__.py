"""What a bot needs at run time: the profile it belongs to, and the objects that profile owns.

Callers import from the modules; this package re-exports nothing, for the reason
``AGENTS.md`` gives — a re-export is a second path to a name, and the one nobody chose is the
one that cannot be moved.
"""

__all__ = ()
