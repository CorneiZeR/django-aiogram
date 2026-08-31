"""What the package says it runs on has to be one answer, not five.

The supported range is stated in `pyproject.toml` twice — `requires-python` and the
classifiers — in the CI matrix, and in prose on three pages. Every floor is stated in
`pyproject.toml` and again in the leg that pins the floors, which is the only leg that
would notice a floor nobody can install. Nothing tied any of those to each other.

The failure this exists for was measured rather than imagined: `confluent-kafka>=2.3`
stood in the Kafka extra while the package claimed Python 3.10-3.14, and that driver
compiles librdkafka — the first wheel for 3.13 is 2.6.0 and the first for 3.14 is 2.12.1,
so on two of the five interpreters the declared floor cannot be installed at all. `pip
install confluent-kafka==2.6.0` on Python 3.14.6 asks for `librdkafka/rdkafka.h`.

A test cannot resolve wheels — that needs a network and five interpreters — so what is
checked here is the half that rots on its own: the numbers agreeing with each other.

Read as text rather than through `tomllib`, which the 3.10 floor does not have.
"""

import pathlib
import re

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
PYPROJECT = (ROOT / 'pyproject.toml').read_text(encoding='utf-8')
WORKFLOW = (ROOT / '.github' / 'workflows' / 'ci.yml').read_text(encoding='utf-8')

#: the three the floors leg pins, which are the three every install resolves
PINNED_IN_CI = ('django', 'aiogram', 'redis')

#: pages that state the supported Python range in prose. The README and Home are read
#: before anything else, and Installation is where somebody checks before upgrading
PAGES_STATING_THE_RANGE = ('README.md', 'docs/wiki/Home.md', 'docs/wiki/Installation.md')


def version_tuple(text: str) -> tuple[int, ...]:
    """`6.2` and `6.2.0` are one floor, and a short tuple would sort below a long one."""
    numbers = [int(number) for number in text.split('.')]
    return tuple(numbers + [0] * (3 - len(numbers)))


def declared_floor(package: str) -> str:
    """The lowest version `pyproject.toml` allows for one package, wherever it declares it."""
    found = re.findall(rf'"{package}(?:\[\w+\])?>=([\d.]+)', PYPROJECT)
    assert found, f'{package} no longer declares a lower bound the way this test reads it'
    return min(found, key=version_tuple)


@pytest.mark.parametrize('package', PINNED_IN_CI)
def test_the_floors_leg_pins_the_floor_that_is_declared(package):
    """A leg pinning something else tests a configuration nobody is promised.

    Above the floor it hides the breakage it exists to find; below it, it fails on a
    version this package already refuses. The pins are written `==X.Y.0` because a floor
    is a series and the leg wants its first release.
    """
    pinned = re.search(rf"'{package}==([\d.]+)'", WORKFLOW)

    assert pinned, f'the floors leg no longer pins {package}'
    assert version_tuple(pinned.group(1)) == version_tuple(declared_floor(package)), (
        f'CI pins {package}=={pinned.group(1)} against a declared floor of {declared_floor(package)}'
    )


@pytest.mark.parametrize('page', PAGES_STATING_THE_RANGE)
def test_every_page_states_the_python_range_requires_python_allows(page):
    """Three pages say "Python 3.10-3.14", and `requires-python` is what a resolver reads.

    The upper bound is exclusive there and inclusive in prose — `<3.15` is 3.14 — which is
    exactly the arithmetic a reader should not have to do twice.
    """
    lower, upper = re.search(r'requires-python = ">=(\d+\.\d+),<(\d+\.\d+)"', PYPROJECT).groups()
    last = f'{upper.split(".")[0]}.{int(upper.split(".")[1]) - 1}'
    # the en dash the pages use, built rather than written: `ruff`'s RUF001 refuses the
    # character in a literal, and the pages are typeset prose rather than code
    stated = f'Python {lower}{chr(0x2013)}{last}'

    assert stated in (ROOT / page).read_text(encoding='utf-8'), f'{page} does not say {stated!r}'


def test_the_classifiers_cover_exactly_the_range_and_no_more():
    """A classifier is what PyPI shows and what a resolver on an old interpreter reads.

    Both directions: a missing one hides support that exists, and a spare one advertises an
    interpreter `requires-python` refuses, which is the shape that survives a bump — the
    range moves and a classifier for the version that left stays behind.
    """
    lower, upper = re.search(r'requires-python = ">=(\d+)\.(\d+)', PYPROJECT).groups()
    _, cap = re.search(r'requires-python = ">=[\d.]+,<(\d+)\.(\d+)"', PYPROJECT).groups()
    expected = {f'{lower}.{minor}' for minor in range(int(upper), int(cap))}
    declared = set(re.findall(r'"Programming Language :: Python :: (\d+\.\d+)"', PYPROJECT))

    assert declared == expected, f'classifiers say {sorted(declared)}, requires-python allows {sorted(expected)}'


def test_the_unit_matrix_runs_every_interpreter_the_package_claims():
    """A claim is only as good as the leg behind it, and the matrix is the leg.

    Django's own axis is excluded where it has to be — 6.0 and 6.1 need Python 3.12 — so
    what is asserted is that every claimed interpreter appears, not that every pair does.
    """
    lower, upper = re.search(r'requires-python = ">=(\d+)\.(\d+)', PYPROJECT).groups()
    _, cap = re.search(r'requires-python = ">=[\d.]+,<(\d+)\.(\d+)"', PYPROJECT).groups()
    expected = {f'{lower}.{minor}' for minor in range(int(upper), int(cap))}
    matrix = re.search(r'python-version: \[([^\]]+)\]', WORKFLOW).group(1)
    tested = set(re.findall(r"'(\d+\.\d+)'", matrix))

    assert tested == expected, f'the matrix runs {sorted(tested)} for a claimed {sorted(expected)}'


def wiki() -> str:
    """Every page as one flattened string, because a claim wraps and a table cell does not."""
    pages = '\n'.join(path.read_text(encoding='utf-8') for path in sorted((ROOT / 'docs' / 'wiki').glob('*.md')))
    return re.sub(r'\s+', ' ', pages)


def test_installation_documents_every_extra_and_its_floor():
    """The page a reader checks before upgrading carries the same numbers as the metadata.

    Every specifier in an extra, not just the transports': `[hiredis]` is opt-in and
    documented on **Deployment**, so the floor is looked for across the wiki rather than on
    one page — what must not happen is a floor that lives only in `pyproject.toml`.
    """
    extras = PYPROJECT.split('[project.optional-dependencies]')[1].split('\n[')[0]
    specifiers = re.findall(r'"([\w-]+)(?:\[[\w]+\])?>=([\d.]+)', extras)
    pages = wiki()

    missing = [f'{name}>={floor}' for name, floor in specifiers if f'{name}>={floor}' not in pages]

    assert not missing, f'floors documented nowhere in the wiki: {missing}'


def test_a_floor_that_depends_on_the_interpreter_is_documented_against_it():
    """A number is documented; the Python it applies to has to be documented with it.

    Two floors for one package is the shape a marker produces, and the case above is satisfied
    by both numbers appearing *anywhere* — including a page that pairs them the wrong way
    round, which is worse than not saying it: the Kafka driver's floor exists precisely because
    an older pin cannot be installed on a newer Python, so a reader given the pair backwards
    hits the failure the marker was added to prevent.

    So each marked specifier is looked for within a stretch of prose that also names its
    Python. The window is generous on purpose — the table writes them a few words apart, and
    the point is that they are stated together rather than in the same file.
    """
    extras = PYPROJECT.split('[project.optional-dependencies]')[1].split('\n[')[0]
    marked = re.findall(r'"([\w-]+)>=([\d.]+); python_version ([<>=]+) \'([\d.]+)\'"', extras)
    assert marked, 'no floor depends on the interpreter any more; drop this case with the marker'

    pages = wiki()
    for name, floor, operator, python in marked:
        specifier = f'{name}>={floor}'
        stated = [
            window for window in re.findall(rf'.{{0,160}}{re.escape(specifier)}.{{0,160}}', pages) if python in window
        ]
        assert stated, f'{specifier} applies {operator} Python {python} and no page says so within reach of the number'
