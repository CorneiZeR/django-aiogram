"""The measurements behind two driver decisions and every quoted ratio, kept to be re-taken.

Not tests: each needs a broker, and what they produce is a number to read rather than a pass or
a fail. `README.md` beside them says how to run each one and what it answered.

A package rather than loose files because they share `_timing`, and because a number is only
comparable to another number taken the same way — which is the whole reason `redis_baseline` is
here beside the two driver comparisons rather than being remembered from an earlier release.
"""

__all__ = ()
