"""The pages are published as a site, so a broken link ships as a broken link.

Until 4.0 these were GitHub wiki pages and linked each other with `[[Page]]`, which the
wiki resolved leniently: it folded spaces into dashes and ignored case, so three spellings
of one link all worked and the tests here had to police which one was written.

The site is static files. `/latest/rate-limits/` is not `/latest/Rate-limits/` — it is a
404 — so the rules got stricter and simpler at once: a link is a relative path to a file
that exists, spelled exactly. `mkdocs build --strict` says the same thing in the `docs` CI
job; this file says it on every leg, including the ones with no mkdocs installed, and it is
what a contributor sees before pushing.
"""

import re
import unicodedata
from datetime import date
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
WIKI = ROOT / 'docs' / 'wiki'
MKDOCS = ROOT / 'mkdocs.yml'
#: `[label](Target.md)`, `[label](Target.md#anchor)` and the same-page `[label](#anchor)`.
#: External links start with a scheme and are somebody else's to keep.
#:
#: The third form is the one the corpus actually has -- its only anchor points at a heading on
#: its own page -- so a pattern that required a file name checked every link except that one.
#: Found in review, after the anchor case had been written and had passed against nothing
LINK = re.compile(r'\]\(([^):#]*\.md)?(#[^)]+)?\)')
#: the syntax the wiki used. It renders literally in Markdown, so it may not survive anywhere
WIKI_LINK = re.compile(r'\[\[')

PAGES = sorted(WIKI.glob('*.md'))


def page_names() -> set[str]:
    """Every page by its stem, which is how the README and the nav name one."""
    return {path.stem for path in PAGES}


def page_files() -> set[str]:
    """Every page by its file name, which is what a relative link has to spell exactly."""
    return {path.name for path in PAGES}


def headings_of(path: Path) -> set[str]:
    """The anchors a page offers, slugified the way the toc extension does it.

    Its algorithm, not an approximation of it: punctuation is **removed** and only
    whitespace becomes a dash. `[^a-z0-9]+ -> -` was close enough while no heading held a
    dot inside a word, and wrong the day one did -- `bot.outcome()` publishes as
    `botoutcome-...`, where that rule computed `bot-outcome-...` and reported a live anchor
    as dangling. Checked against the ids in `site/` by
    `test_the_slugs_match_the_ones_the_build_publishes`.
    """
    slugs = set()
    for line in visible(path.read_text(encoding='utf-8')).splitlines():
        found = ATX.match(line)
        if found:
            text = re.sub(r'`|\*|\[|\]|\(.*?\)', '', found.group(1))
            slugs.add(slugify(text))
    return slugs


def slugify(text: str) -> str:
    """Turn heading text into its anchor, as `markdown.extensions.toc.slugify` does."""
    stripped = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')
    return re.sub(r'[\s-]+', '-', re.sub(r'[^\w\s-]', '', stripped).strip().lower())


def test_the_wiki_has_pages():
    assert PAGES, 'docs/wiki is empty'


@pytest.mark.parametrize('path', PAGES, ids=lambda path: path.name)
def test_every_link_resolves(path):
    """A relative link names a file in this directory, spelled exactly.

    Case matters now and did not before: GitHub's wiki resolved `rate-limits` to
    `Rate-limits`, and GitHub Pages serves what is on disk. A link that only differs in
    case used to work and now 404s, which is a failure no reader can diagnose.
    """
    known = page_files()
    broken = [target for target, _ in LINK.findall(path.read_text(encoding='utf-8')) if target and target not in known]
    assert not broken, f'{path.name} links to missing pages: {broken}'


@pytest.mark.parametrize('path', PAGES, ids=lambda path: path.name)
def test_the_slugs_match_the_ones_the_build_publishes(path):
    """`headings_of` models the toc extension, so the model is held to the output.

    Skipped where `site/` has not been built, which is most runs -- the point is the CI leg
    that builds the docs, where a heading whose anchor this file cannot compute would
    otherwise make `test_every_anchor_resolves` report a live link as dangling. That is what
    happened to a heading holding `bot.outcome()`.
    """
    if not (ROOT / 'site').exists():
        pytest.skip('site/ is not built in this run')
    # `index.md` is the site root rather than a directory of its own, and asking for
    # `site/index/index.html` skipped the front page for as long as the skip was per file --
    # so a missing page is a failure now and only an unbuilt `site/` is a skip
    built = ROOT / 'site' / ('index.html' if path.stem == 'index' else f'{path.stem}/index.html')
    assert built.exists(), f'site/ is built and has no page for {path.name}: expected {built}'
    # `_1` and `_2` are what the toc extension appends to a repeated heading, and `headings_of`
    # answers with a set -- so the suffixes are dropped rather than modelled, and a page with
    # two `## Run migrate` sections still compares equal
    published = {
        re.sub(r'_\d+$', '', found)
        for found in re.findall(r'<h[1-6][^>]*\bid="([^"]+)"', built.read_text(encoding='utf-8'))
    }
    computed = headings_of(path)

    # both directions, and non-empty. `computed <= published` was the first draft, and it holds
    # for a `headings_of` that computes nothing at all -- which is the shape of the very bug this
    # is here to catch, since a slug this file cannot compute is one `test_every_anchor_resolves`
    # reports as dangling
    assert computed, f'{path.name} has headings and this file computed none'
    assert computed == published, (
        f'computed but not published: {sorted(computed - published)}; '
        f'published but not computed: {sorted(published - computed)}'
    )


@pytest.mark.parametrize('path', PAGES, ids=lambda path: path.name)
def test_every_anchor_resolves(path):
    """The fragment half of a link, against the headings the target page actually has.

    One link in the corpus carries an anchor, and a heading it points at is exactly the kind
    of thing a rewrite renames without looking. `mkdocs build --strict` checks this too, with
    `validation.links.anchors` — pinned by `test_the_config_validates_what_it_claims`.
    """
    dangling = []
    for target, fragment in LINK.findall(path.read_text(encoding='utf-8')):
        if not fragment:
            continue
        if target and target not in page_files():
            continue
        # no file name means the anchor is on this page, which is the form the corpus uses
        page = WIKI / target if target else path
        if fragment.lstrip('#') not in headings_of(page):
            dangling.append(f'{target}{fragment}')
    assert not dangling, f'{path.name} links to headings that do not exist: {dangling}'


@pytest.mark.parametrize('path', [*PAGES, ROOT / 'README.md', ROOT / 'CHANGELOG.md'], ids=lambda path: path.name)
def test_the_wiki_link_syntax_is_gone(path):
    """`[[Page]]` renders as literal brackets everywhere except a GitHub wiki.

    It was banned from the README and the changelog while the wiki existed; now that the
    pages are a site, the ban covers the pages too — this is the check that the conversion of
    all 120 of them was complete, and that nobody writes the 121st.
    """
    assert not WIKI_LINK.search(path.read_text(encoding='utf-8')), f'{path.name} still uses wiki link syntax'


README = ROOT / 'README.md'
PYPROJECT = ROOT / 'pyproject.toml'
# the site's root. `project.urls.Documentation` points here rather than at a version, so
# PyPI's sidebar follows mike's default-version redirect instead of freezing on whichever
# series was current the day a release shipped
SITE_URL = 'https://corneizer.github.io/django-aiogram/'
# and the version every link in the README names. Absolute, because the README is also the
# PyPI description and PyPI does not rewrite relative links — `../../wiki/<page>` used to
# resolve to `pypi.org/wiki/<page>`
DOCS_URL = f'{SITE_URL}latest/'
#: `](.../latest/Delivery/)`, with the trailing slash a directory URL carries
README_PAGE_LINK = re.compile(rf'\]\({re.escape(DOCS_URL)}([^)#/]+)/?\)')


def test_readme_page_links_resolve():
    """The README links into the site by page name; a rename there breaks them silently.

    And the spelling is exact now. The wiki folded case and turned spaces into dashes, so
    `../../wiki/rate-limits` reached the page; the site is static files, where it is a 404
    that no reader can diagnose and no gate here used to catch.
    """
    known = page_names()
    targets = README_PAGE_LINK.findall(README.read_text(encoding='utf-8'))
    assert targets, 'no documentation links found in the README'
    broken = [target for target in targets if target not in known]
    assert not broken, f'README links to pages that do not exist: {broken}'


#: a path into this repository, as prose writes one. Rooted at a directory that exists here,
#: which is what separates `tests/test_wiki.py` from the `tgbot/tg_router.py` a reader is told
#: to create in their own project.
#:
#: The left edge is a lookbehind and not `\b`, because a word boundary is exactly what a
#: backtick followed by `.github` does not have: two non-word characters in a row. Written with
#: `\b` first, and the falsification caught it -- a dangling `.github/workflows/...` still
#: passed, because the pattern had never matched a dotted root at all
REPO_PATH = re.compile(r'(?<![\w./-])((?:\.github|src|tests|scripts|docs)/[\w./-]*[\w-])')
#: the changelog is exempt, and is the reason this test exists: naming a file a release deleted
#: is what a release note is for, while a live page naming one is a reader sent to a 404
PROSE = [README, ROOT / 'AGENTS.md', ROOT / 'CONTRIBUTING.md', *PAGES]


@pytest.mark.parametrize('path', PROSE, ids=lambda path: path.name)
def test_every_repository_path_in_the_prose_exists(path):
    """Documentation that names a file names one that is there.

    Deleting a workflow, a script or a test module is the moment its mentions rot, and every
    gate stays green while they do: the sentence is still grammatical, the link still is not
    one. Found the other way round in review -- a release note naming a workflow it had
    deleted, which is correct, next to a sentence that named the same kind of file by
    description because nothing forced it to be exact.
    """
    missing = sorted(
        {match for match in REPO_PATH.findall(path.read_text()) if not (ROOT / match).exists()},
    )
    assert not missing, f'{path.name} names paths that do not exist: {", ".join(missing)}'


def test_every_documented_log_message_is_still_emitted():
    """The other direction from the table's own purpose.

    `Logging.md` lists the events worth alerting on — a curated subset, not every
    warning the package writes, so requiring the code to be exhausted by it would
    be wrong. What is worth enforcing is the reverse: a row describing a message
    nothing emits any more sends someone to build an alert that can never fire.
    """
    import ast

    page = (ROOT / 'docs' / 'wiki' / 'Logging.md').read_text(encoding='utf-8')
    documented = re.findall(r'^\| `([a-z][^`]+)` \| (?:ERROR|WARNING|INFO) \|', page, re.MULTILINE)
    assert documented, 'no message rows found on the Logging page'

    emitted = []
    for path in sorted((ROOT / 'src' / 'django_aiogram').rglob('*.py')):
        for node in ast.walk(ast.parse(path.read_text(encoding='utf-8'))):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr not in {'debug', 'info', 'warning', 'error', 'exception'}:
                continue
            if node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
                emitted.append(node.args[0].value)

    # matched on the name of the event, by equality — not by containment anywhere in any
    # literal, which kept a row green after its own event was renamed away as long as its
    # words survived inside some other message. A row names either the whole line or the
    # part before the remedy, because the table carries the remedy in its own column; both
    # are anchored at the start of the literal and end where its punctuation does
    names = set(emitted)
    for literal in emitted:
        names.update(literal.split(separator, 1)[0] for separator in (':', ';') if separator in literal)
    stale = [message for message in documented if message not in names]
    assert not stale, f'documented but no longer emitted: {stale}'


CHANGELOG = ROOT / 'CHANGELOG.md'
#: the newest released heading. Everything below it describes what shipped, in the words
#: it shipped with, and is not ours to edit — a spelling pass reached three lines under
#: here and no test noticed
HISTORY_BEGINS = '## 3.0.0 - 2026-08-09'
#: words the released entries spell the way those releases spelled them
#: written the way those releases wrote them, and **not** to be Americanised: this tuple
#: is the guard, so a sweep that edits it here edits the thing doing the guarding — which
#: a mechanical en-US pass did, and this test caught
HISTORICAL_SPELLINGS = ('`VACUUM` afterwards', 'about its behaviour changed', 'for the old behaviour.')


def test_the_newest_changelog_entry_is_this_version_and_is_dated():
    """A release shipped as `unreleased` reads as a nightly to whoever installs it.

    Two ways to get this wrong, and one test for both: publishing with the heading still
    saying `unreleased`, and bumping `__version__` without writing the entry. Read from
    `__version__` so the next release inherits the demand rather than the date.
    """
    from django_aiogram import __version__

    # `visible()`, because a heading inside a fenced block is not a release: without it
    # the real one could be deleted and an example in a code block would satisfy this
    lines = visible(CHANGELOG.read_text(encoding='utf-8')).splitlines()
    heading = next(line for line in lines if line.startswith('## '))

    # a version under development announces the entry it is preparing -- `4.0.0.dev0`
    # belongs to the `4.0.0` heading -- and only a final version may carry a date. Dating an
    # entry while the version still says `dev` is how a nightly reads as shipped.
    #
    # Split on `.dev` rather than on the release triple, so a post release keeps its own
    # entry: `4.0.0.post1.dev0` announces `## 4.0.0.post1`, where a regex that stopped at the
    # third number would have demanded the `4.0.0` heading -- already dated, and about a
    # different set of changes
    announced, _, developing = __version__.partition('.dev')

    assert heading.startswith(f'## {announced} - '), f'the newest entry is not {announced}: {heading!r}'
    stamp = heading.removeprefix(f'## {announced} - ')
    if developing:
        assert stamp == 'unreleased', f'{__version__} is not released, so the entry cannot be dated {stamp!r}'
        return
    # parsed, not pattern-matched: `2026-13-45` has the shape of a date and is not one,
    # and a release dated by hand is exactly where that typo lands
    try:
        date.fromisoformat(stamp)
    except ValueError as wrong:
        raise AssertionError(f'not a date: {stamp!r} ({wrong})') from wrong


def test_the_released_entries_keep_the_words_they_shipped_with():
    """A changelog entry is a record of what was said, not prose to be improved.

    The en-US pass for 3.1.0 rewrote three lines below `## 3.0.0` — `afterwards` and two
    `behaviour` — while the pull request describing it said history was left alone. Nothing
    failed, because no test reads down there.

    Narrow on purpose: this pins the words a sweep would reach for, not the whole section,
    so an entry can still be corrected on its facts if one is ever found to be wrong.
    """
    text = CHANGELOG.read_text(encoding='utf-8')
    assert HISTORY_BEGINS in text, f'{HISTORY_BEGINS!r} is gone; this test no longer knows where history starts'
    history = text[text.index(HISTORY_BEGINS) :]
    rewritten = [phrase for phrase in HISTORICAL_SPELLINGS if phrase not in history]

    assert not rewritten, f'a released entry was reworded: {rewritten}'


def test_the_upgrade_page_covers_the_version_being_shipped():
    """`Upgrading.md` had no 3.1 section while `__version__` already said 3.1.0.

    Nothing pointed it out, because the page is prose and the version is code — so the
    release that changed the acknowledgement point, added a migration and moved the
    shutdown arithmetic shipped with an upgrade page whose newest entry was the release
    before it. Read from `__version__`, so the next release inherits the same demand.
    """
    from django_aiogram import __version__

    series = '.'.join(__version__.split('.')[:2])
    # `visible()` for the same reason: a heading in a code block is not a section
    page = visible((WIKI / 'Upgrading.md').read_text(encoding='utf-8'))
    # through the same two helpers the rest of this file reads headings with, so setext
    # and a closing `##` are headings here too — a hand-matched `#{1,6} ` saw neither
    headings = [normalized(heading) for heading in sections(page)]

    # the heading, not the page text: `to 3.1` appears in any prose that mentions
    # upgrading to it, so the broad search passed on a page with no such section.
    # Any level: `startswith('# ')` matched only level one, so moving the section under
    # `## ` would have failed a page that documents the series perfectly well
    assert any(heading.endswith(normalized(f' to {series}')) for heading in headings), (
        f'no section heading upgrading to {series}: {headings}'
    )


def test_every_documented_cap_formula_states_the_floor():
    """A page writing the consumer's cap has to write the whole of it, floor included.

    `take_ceiling` computes `min(HEARTBEAT_INTERVAL, floor(deadline) - 1)`, never below 1, and the
    floor is not a rounding detail: two of the four transport deadlines accept fractions, so at
    `KAFKA_TIMEOUT = 2.6` the raw subtraction states 1.6 where the consumer applies 1. Three pages
    stated it that way and the suite could not tell, because nothing held prose to the helper.

    A grep-shaped rule on purpose: what went wrong was a formula written out a fourth time, and any
    test that computes the cap instead would have agreed with all four.
    """
    stated = [
        (path.name, line)
        for path in PAGES
        for line in path.read_text(encoding='utf-8').splitlines()
        if 'min(HEARTBEAT_INTERVAL' in line
    ]
    assert stated, 'no page states the cap any more, so this rule guards nothing'

    without = [(name, line.strip()[:90]) for name, line in stated if 'floor(' not in line]
    assert without == [], f'the cap is stated without its floor in: {without}'


def navigated() -> set[str]:
    """Every page `mkdocs.yml` puts in the navigation, however deeply it is grouped.

    Parsed rather than pattern-matched: this file's meaning is YAML's, and `pyyaml` is
    already a test dependency for the same reason on `.coderabbit.yaml`.
    """

    def walk(entry) -> list[str]:
        if isinstance(entry, str):
            return [entry]
        if isinstance(entry, dict):
            return [name for value in entry.values() for name in walk(value)]
        if isinstance(entry, list):
            return [name for item in entry for name in walk(item)]
        return []

    return set(walk(yaml.safe_load(MKDOCS.read_text(encoding='utf-8'))['nav']))


def test_the_site_has_a_front_page():
    """`index.md` is what answers at the root of a version directory.

    It was `Home.md` while these were wiki pages, because that is the name a GitHub wiki
    serves first. Renaming it is what lets mike's default-version redirect land on a page
    rather than on a 404.
    """
    assert (WIKI / 'index.md').is_file()


def test_the_nav_lists_every_page():
    """The sidebar used to be a page anybody could forget to edit; now it is the config.

    Same rule as before and the same failure it prevents: a page that exists and appears in
    no navigation is a page reachable only by search. `mkdocs build --strict` also refuses
    it -- `validation.omitted_files` -- and this says it without mkdocs installed.
    """
    missing = {f'{name}.md' for name in page_names()} - navigated()
    assert not missing, f'pages missing from the nav in mkdocs.yml: {sorted(missing)}'


def test_the_config_validates_what_it_claims():
    """`--strict` only escalates the warnings MkDocs is configured to emit.

    Anchors are not checked by default, so `validation.links.anchors` is the setting that
    makes the strict build mean what the `docs` job says it means. Pinned here because
    dropping one line from `mkdocs.yml` would take a whole class of checking with it and
    leave every gate green.
    """
    validation = yaml.safe_load(MKDOCS.read_text(encoding='utf-8'))['validation']

    assert validation['nav']['omitted_files'] == 'warn'
    assert validation['links']['anchors'] == 'warn'
    assert validation['links']['unrecognized_links'] == 'warn'
    assert validation['links']['absolute_links'] == 'warn'


#: the README is a front page, not the documentation. It was 351 lines of
#: material the wiki already carried, and it drifted from those pages.
#:
#: Raised from 140 in 4.0.0 for the transport table, which is the one thing a front page
#: has to carry that no wiki page can: four rows saying what `BROKER`, the extra and the
#: required settings are for each transport. Routing, not prose — the number is here to
#: refuse a second copy of the documentation, and it still does.
README_BUDGET = 148
#: `## Title`, with the three leading spaces markdown still renders as a heading
ATX = re.compile(r'^ {0,3}#{1,6}\s+(.+?)\s*$', re.MULTILINE)
#: `Title` underlined with `===` or `---`, which renders as one too
SETEXT = re.compile(r'^ {0,3}(\S.*?)\s*\n {0,3}(?:=+|-+)\s*$', re.MULTILINE)
#: a fence opens with at least three of either marker, indented up to three
#: spaces, and runs to a matching close or to the end of the file
FENCE = re.compile(r'^ {0,3}(?P<mark>`{3,}|~{3,}).*?(?:^ {0,3}(?P=mark)[`~]*[ \t]*$|\Z)', re.MULTILINE | re.DOTALL)
COMMENT = re.compile(r'<!--.*?-->', re.DOTALL)


def visible(text: str) -> str:
    """What a reader actually sees.

    A heading inside a fenced block is not a section, and a link inside one is
    not a link — so neither may satisfy the checks below, in either direction.
    """
    return COMMENT.sub('', FENCE.sub('', text))


def sections(text: str) -> list[str]:
    """Every heading a reader sees, in either of the two markdown spellings."""
    return ATX.findall(text) + SETEXT.findall(text)


def normalized(title: str) -> str:
    """`## Rate  limits ##` and `## Rate-limits` name the same page."""
    return '-'.join(re.sub(r'\s*#+\s*$', '', title).split()).lower()


def test_normalized_reads_the_heading_forms_markdown_allows():
    """Written out, because each of these once slipped past the check below."""
    assert normalized('Delivery') == 'delivery'
    assert normalized('Delivery ##') == 'delivery'
    assert normalized('  Rate   limits  ') == 'rate-limits'
    assert normalized('Rate-limits') == 'rate-limits'


def test_visible_drops_what_is_not_rendered():
    text = (
        '## Real\n'
        '```\n## Fenced\n```\n'
        '~~~\n### Tilde fenced\n~~~\n'
        '   ```\n## Indented fence\n   ```\n'
        '````\n## Long marker\n````\n'
        '<!-- ## Commented -->\n'
        '  ### Indented heading\n'
        'Underlined\n=========\n'
    )

    # every heading form counts as a section, and no fence style does
    assert sorted(sections(visible(text))) == ['Indented heading', 'Real', 'Underlined']


def test_an_unclosed_fence_hides_everything_after_it():
    """Which is what GitHub renders: the rest of the file becomes code.

    The link in the fixture is a site link, and that is load-bearing: it carried the old wiki
    URL through the move, which `README_PAGE_LINK` does not match at all -- so the second
    assertion held whether or not `visible()` still removed the fence, and the case could not
    fail. Found in review.
    """
    text = f'## Real\n```\n## Never closed\n[API]({DOCS_URL}API/)\n'

    rendered = visible(text)

    assert sections(rendered) == ['Real']
    assert README_PAGE_LINK.findall(rendered) == []


def test_the_readme_stays_a_front_page():
    lines = README.read_text(encoding='utf-8').splitlines()
    assert len(lines) <= README_BUDGET, (
        f'the README is {len(lines)} lines; anything this long belongs on a documentation page'
    )


def test_no_readme_section_duplicates_a_wiki_page():
    """A section named after a page is that page's material coming back."""
    pages = {normalized(name) for name in page_names()} - {'index'}
    duplicated = [
        title for title in sections(visible(README.read_text(encoding='utf-8'))) if normalized(title) in pages
    ]

    assert not duplicated, f'these belong on a documentation page, not in the README: {duplicated}'


def test_the_readme_links_to_every_page():
    """A new page nobody can find from the front page is a page nobody reads.

    Spelled exactly, on both sides. While these were wiki pages both were normalised, because
    GitHub resolved a link case-insensitively and folded spaces into dashes — so
    `../../wiki/rate-limits` reached the page and had to count as reaching it. On a static
    site it reaches a 404, so the leniency went with the wiki.
    """
    linked = set(README_PAGE_LINK.findall(visible(README.read_text(encoding='utf-8'))))
    missing = page_names() - linked - {'index'}

    assert not missing, f'pages the README does not link to: {sorted(missing)}'


def test_a_link_in_the_wrong_case_does_not_count(tmp_path, monkeypatch):
    """The inverse of what this asserted while the pages were a wiki.

    It used to prove that `../../wiki/rate-limits` counted as linking `Rate-limits`, because
    GitHub resolved it and demanding one spelling would have been pedantry. The site serves
    files: that URL is a 404, and a check that accepts it is a check that ships one.
    """
    readme = tmp_path / 'README.md'
    rows = '\n'.join(f'[{name}]({DOCS_URL}{name.lower()}/)' for name in page_names())
    readme.write_text(rows + '\n', encoding='utf-8')
    monkeypatch.setattr('tests.test_wiki.README', readme)

    with pytest.raises(AssertionError, match='does not link to'):
        test_the_readme_links_to_every_page()


def a_readme(tmp_path, monkeypatch, body: str):
    """Point the checks at a README of our own, through the name they read."""
    readme = tmp_path / 'README.md'
    rows = '\n'.join(f'[{name}]({DOCS_URL}{name}/)' for name in page_names())
    readme.write_text(rows + '\n' + body, encoding='utf-8')
    monkeypatch.setattr('tests.test_wiki.README', readme)
    return readme


def test_a_duplicate_heading_inside_a_fence_is_not_a_section(tmp_path, monkeypatch):
    """Driving the check itself, so it fails if it goes back to raw text."""
    a_readme(tmp_path, monkeypatch, '```\n## Delivery\n```\n<!-- ## Troubleshooting -->\n')

    test_no_readme_section_duplicates_a_wiki_page()


def test_a_duplicate_heading_outside_a_fence_is_caught(tmp_path, monkeypatch):
    """The other half: the check must still do its job."""
    a_readme(tmp_path, monkeypatch, '## Delivery\n\nTwo consumers are available.\n')

    with pytest.raises(AssertionError, match='belong on a documentation page'):
        test_no_readme_section_duplicates_a_wiki_page()


def test_a_link_only_inside_a_fence_does_not_count(tmp_path, monkeypatch):
    """Reading raw text here would call an unreachable page linked."""
    readme = a_readme(tmp_path, monkeypatch, '')
    text = readme.read_text(encoding='utf-8')
    row = next(line for line in text.splitlines() if f'{DOCS_URL}Webhook/)' in line)
    readme.write_text(text.replace(row + '\n', '') + '```\n' + row + '\n```\n', encoding='utf-8')

    with pytest.raises(AssertionError, match='does not link to'):
        test_the_readme_links_to_every_page()


def test_a_link_only_inside_a_comment_does_not_count(tmp_path, monkeypatch):
    readme = a_readme(tmp_path, monkeypatch, '')
    text = readme.read_text(encoding='utf-8')
    row = next(line for line in text.splitlines() if f'{DOCS_URL}API/)' in line)
    readme.write_text(text.replace(row + '\n', '') + f'<!-- {row} -->\n', encoding='utf-8')

    with pytest.raises(AssertionError, match='does not link to'):
        test_the_readme_links_to_every_page()


def test_the_readme_has_no_relative_links():
    """The README is also the PyPI long description. PyPI serves it from
    pypi.org and does not rewrite links, so a relative one points at a page
    that does not exist there — which is how 2.0.0 shipped a documentation
    table of dead links.
    """
    relative = re.findall(r'\]\((?!https?://|#)([^)]+)\)', README.read_text(encoding='utf-8'))

    assert not relative, f'relative links break on the PyPI page: {relative}'


def test_the_documentation_url_is_declared():
    """PyPI builds its sidebar from project.urls, not from the description.

    Read as text rather than through tomllib, which the 3.10 floor lacks.
    """
    declared = f'Documentation = "{SITE_URL}"'

    assert declared in PYPROJECT.read_text(encoding='utf-8')
