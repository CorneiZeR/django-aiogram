"""The measurement a driver decision rests on, kept so it can be re-taken.

Not a test: it needs a broker, and what it produces is a number to read rather than a pass or a
fail. `README.md` beside it says how to run it and what it answered.

A package rather than a loose file because `_timing` is shared with whatever measures next, and
because a number is only comparable to another number taken the same way.
"""

__all__ = ()
