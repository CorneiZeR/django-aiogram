"""Rules the suite and the published documentation hold themselves to.

A test that cannot fail is worse than no test, and a command that cannot work is worse than no
command: both read as covered ground. The two rules here are the ones this repository has been
bitten by, and each is a sweep kept rather than a sweep run once.

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
import re

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


#: where a reader is sent to copy commands from. The wiki pages and the two root files a
#: contributor reads before anything else -- and `CHANGELOG.md` deliberately not: a historical
#: entry quoting a command from its own release is a record, not an instruction
PUBLISHED = (
    'README.md',
    'CONTRIBUTING.md',
    'AGENTS.md',
    *[f'docs/wiki/{page.name}' for page in sorted((ROOT / 'docs' / 'wiki').glob('*.md'))],
)

#: a line that is only `NAME=value`, which in a shell block is a no-op for the reader's purpose:
#: it sets a variable in that shell and does not pass it to anything the shell starts, so the
#: command they run next does not see it
BARE_ASSIGNMENT = re.compile(r'^[A-Z][A-Z0-9_]*=\S*$')


def shell_blocks(text):
    """Every fenced block a reader would paste into a shell, with the line each one starts at.

    The line count is the newlines in everything before the block, and nothing else: the fence
    markers carry none of their own, so summing them per segment is exact. The first version added
    one per preceding segment and drifted -- a second block was reported two lines late, and the
    only thing that would have noticed is an assertion on the numbers, which the self-test below
    now makes.
    """
    seen = 0
    for index, chunk in enumerate(text.split('```')):
        if index % 2 and chunk.split('\n', 1)[0].strip() in {'shell', 'bash', 'sh', 'console'}:
            # `seen + 1` is the fence line, so the first line inside the block is one past it
            yield seen + 2, chunk.split('\n', 1)[1] if '\n' in chunk else ''
        seen += chunk.count('\n')


@pytest.mark.parametrize('relative', PUBLISHED, ids=lambda relative: relative.rsplit('/', 1)[-1])
def test_no_shell_block_tells_the_reader_to_run_nothing(relative):
    """A block whose line is only an assignment does nothing when it is pasted.

    `DJANGO_AIOGRAM_MODE=webhook` was such a block: the prose around it was right -- the setting
    can come from the environment -- and the block set a shell variable that the process started
    next does not inherit, so a reader who followed it exactly changed nothing and had no way to
    see that. `export`, or the variable shown where the process is actually started, is what the
    sentence means.

    The same sweep found a `CONTRIBUTING.md` block whose two variables never reached the `pytest`
    process they were for, which silenced the very cases the change that added them introduced.
    """
    path = ROOT / relative
    offences = [
        f'{relative}:{start + offset}  {stripped}'
        for start, block in shell_blocks(path.read_text(encoding='utf-8'))
        for offset, stripped in enumerate(line.strip() for line in block.split('\n'))
        if BARE_ASSIGNMENT.fullmatch(stripped)
    ]

    assert offences == [], '\n'.join(
        [*offences, 'a bare assignment in a shell block is a no-op: export it, or show where the process is started']
    )


def test_the_shell_sweep_can_see_the_shape_it_is_for():
    """As above, the rule passes when the parser sees nothing, so the parser gets its own case."""
    page = """Set the mode from the environment:

```shell
DJANGO_AIOGRAM_MODE=webhook
```

and then, correctly:

```shell
export DJANGO_AIOGRAM_MODE=webhook
python manage.py start_tgbot
```
"""
    blocks = list(shell_blocks(page))

    assert len(blocks) == 2, blocks
    # the numbers, not just the count: a line the reader is pointed at is the whole value of this
    # sweep, and an arithmetic drift is invisible to a case that only counts blocks
    assert [start for start, _block in blocks] == [4, 10], [start for start, _block in blocks]
    bare = [
        stripped
        for _start, block in blocks
        for stripped in (line.strip() for line in block.split('\n'))
        if BARE_ASSIGNMENT.fullmatch(stripped)
    ]
    assert bare == ['DJANGO_AIOGRAM_MODE=webhook'], bare
