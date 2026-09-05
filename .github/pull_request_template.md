## What and why

<!-- What changes, and what problem it solves. If it fixes a bug, describe the
failure it produces. -->

## Checklist

The first three apply to every pull request. Strike out the rest with
`~~...~~` when they do not apply, rather than leaving them unticked.

- [ ] `ruff check .` and `ruff format --check .` pass
- [ ] `mypy` passes
- [ ] `python -m pytest` passes
- [ ] Behaviour changes are covered by a test that fails without the change
- [ ] Nothing new runs at import time
- [ ] `CHANGELOG.md` updated for user-visible changes
- [ ] Documentation in `docs/wiki/` follows the behaviour it describes
- [ ] Every promise the documentation gains has a case named after it — a claim
      about repetition or idempotence needs a case that runs the thing twice
- [ ] A documented bound has a case *at* it and one *past* it — running twice
      says nothing about where a limit stops
- [ ] New flags and settings were tried with the wrong values: negative, zero,
      empty, absent, enormous, and whatever the help text calls special
