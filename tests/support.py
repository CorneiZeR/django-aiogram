"""Helpers this suite shares, and the one place that starts an interpreter of our own.

Eight cases needed a fresh process: what an import costs, what a boot pulls in, what a
`manage.py` invocation loads. Each ran `subprocess.run` itself and each carried the same
`noqa: S603` with the same sentence explaining it — a suppression repeated is a suppression
nobody reads, and the eighth copy said nothing the first did not.

It is here once now, and the reason is what makes it load-bearing rather than habitual: the
executable is `sys.executable`, the argument is a string written in the test above the call, and
neither reaches this function from outside the suite.
"""

import os
import subprocess
import sys

#: generous on purpose: a cold interpreter on a loaded CI runner is the slow case, and a timeout
#: that fires there is a flake rather than a finding
SUBPROCESS_TIMEOUT = 120


def run_python(
    script: str,
    *,
    env: dict[str, str] | None = None,
    check: bool = False,
    timeout: float = SUBPROCESS_TIMEOUT,
) -> 'subprocess.CompletedProcess[str]':
    """Run ``script`` in a fresh interpreter and return what it did.

    ``env`` is added to this process's environment rather than replacing it, which is what every
    caller wanted: a subprocess with an empty environment finds no ``PATH`` and no virtualenv.
    """
    return subprocess.run(  # noqa: S603 - our own interpreter, and a script the caller wrote above it
        [sys.executable, '-c', script],
        capture_output=True,
        text=True,
        check=check,
        timeout=timeout,
        env={**os.environ, **(env or {})},
    )
