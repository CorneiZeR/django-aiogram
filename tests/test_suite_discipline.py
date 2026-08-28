"""Rules the suite holds itself to, because a test that cannot fail is worse than no test.

A green case reads as coverage. One that was green before the change and green after it reads as
coverage too, and there is nothing on its face to tell the two apart -- so the reader who comes
next writes no case for the thing this one appeared to cover.

This repository has been bitten by that four times, three of them in one release: a shutdown
recipe that passed on master, a join deadline asserted as `11` when every transport defaulted to
ten so both designs gave the same number, an `assert finished.is_set()` on an event the test set
itself, and the line this module was written for -- `assert real is not None` with a comment
claiming it checked that `monkeypatch` had restored the method, which happens at teardown, after
the body has returned.

So the shape gets a rule rather than another round of reading.
"""

import ast
import collections
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent

#: assertions whose truth is fixed before the code under test runs, and are meant to be. Keyed by
#: `path:line` at the time of writing -- a line number is a poor key, and that is deliberate: it
#: makes an exemption cheap to notice and awkward to keep, which is the right way round for a list
#: of things allowed to be inert
EXEMPT = {
    'tests/db/test_recorder.py': (
        'two cases read `needs_rollback` off the connection *after* the write under test, and the '
        'read is the observation -- the binding is call-free, which is what this sweep looks for, '
        'but the flag it copies was set by the code under test one line above'
    ),
}


def inert_assertions(path):
    """Assertions in one file whose names nothing in the test can have touched.

    A name counts as inert when it is bound without calling anything and appears nowhere else in
    the test -- not passed to a call, not mutated, not captured by a closure the code under test
    runs. Anything looser drowns in recorder lists: `handled = []` is call-free too, and filling
    it is the whole point of the case that follows.
    """
    tree = ast.parse(path.read_text(encoding='utf-8'))
    for func in [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef | ast.AsyncFunctionDef)]:
        if not func.name.startswith('test_'):
            continue
        uses = collections.Counter(node.id for node in ast.walk(func) if isinstance(node, ast.Name))
        plain = {
            node.targets[0].id: ast.unparse(node.value)
            for node in ast.walk(func)
            if isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and not any(isinstance(inner, ast.Call) for inner in ast.walk(node.value))
        }
        for node in ast.walk(func):
            if not isinstance(node, ast.Assert) or any(isinstance(n, ast.Call) for n in ast.walk(node.test)):
                continue
            names = {n.id for n in ast.walk(node.test) if isinstance(n, ast.Name)}
            inert = [name for name in names if name in plain and uses[name] <= 2]
            if not names:
                yield node.lineno, ast.unparse(node.test), 'both sides are constants'
            elif inert and names <= set(plain):
                yield node.lineno, ast.unparse(node.test), '; '.join(f'{n} = {plain[n]}' for n in sorted(inert))


@pytest.mark.parametrize('path', sorted(ROOT.glob('tests/**/test_*.py')), ids=lambda path: path.name)
def test_no_assertion_is_true_before_the_code_under_test_runs(path):
    """One rule, per file, so a failure names the file rather than the whole suite."""
    relative = path.relative_to(ROOT).as_posix()
    found = list(inert_assertions(path))

    if relative in EXEMPT:
        assert found, f'{relative} is exempt and has nothing to exempt; drop the entry: {EXEMPT[relative]}'
        return
    assert found == [], '\n'.join(f'{relative}:{line}  assert {text}    [{why}]' for line, text, why in found)


def test_the_sweep_can_see_the_shape_it_is_for():
    """The rule above passes when the parser is broken, so the parser is put to work here.

    Written as the defect this module exists for, and taken from the line that prompted it: a
    reference captured before the patch, asserted after the body ran, with a comment claiming it
    proves a restoration that happens at teardown.
    """
    source = """
def test_something(monkeypatch):
    real = SomeClass.method
    monkeypatch.setattr(SomeClass, 'method', lambda self: None)
    do_the_thing()
    assert real is not None  # the patched method is restored by monkeypatch
"""
    written = ROOT / 'tests' / '__inert_probe__.py'
    written.write_text(source, encoding='utf-8')
    try:
        found = list(inert_assertions(written))
    finally:
        written.unlink()

    assert [text for _line, text, _why in found] == ['real is not None'], found
