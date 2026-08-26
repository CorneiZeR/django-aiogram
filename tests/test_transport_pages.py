"""A page per transport, held to what that transport actually declares.

`Settings.md` is pinned already: every key in `DEFAULTS` and in every shipped broker's
`OPTIONS` has to appear in its tables. These pages are a second place the same settings are
written down, and a second place is where documentation goes wrong — a transport gaining an
option leaves the page that reader trusts silently incomplete, and nothing about the code
would look amiss.

So the pages are checked against the declarations rather than proof-read:

* every setting the broker declares is named on its page;
* nothing is named that the broker does not declare, which catches a copied row as surely as
  a missing one;
* a page marking a setting **required** agrees with `OPTIONS` about whether it has a default,
  because that is the one detail a reader acts on before the first run.

What is deliberately *not* pinned is the prose. These pages exist to explain what a transport
guarantees and costs, and a test that asserted sentences would pin this wording rather than
the facts under it.
"""

import pathlib
import re

import pytest
from django.utils.module_loading import import_string

from django_aiogram.broker.base import REQUIRED
from django_aiogram.broker.registry import SHIPPED

WIKI = pathlib.Path(__file__).resolve().parent.parent / 'docs' / 'wiki'

#: which page belongs to which shipped broker. Written out rather than derived from the dotted
#: path: a page is named for a reader, and a mapping generated from the module names would
#: agree with any rename of either side
PAGES = {
    'django_aiogram.broker.redis_list.RedisListBroker': 'Redis-list.md',
    'django_aiogram.broker.redis_streams.RedisStreamsBroker': 'Redis-Streams.md',
    'django_aiogram.broker.rabbitmq.RabbitMQBroker': 'RabbitMQ.md',
    'django_aiogram.broker.kafka.KafkaBroker': 'Kafka.md',
}

#: settings every transport reads, which belong to the package rather than to one of them.
#: A page may mention one without declaring it
SHARED = frozenset({'BROKER', 'ENABLED', 'MAX_IN_FLIGHT', 'HEARTBEAT_INTERVAL', 'WORKER_NAME'})


def options(path: str) -> dict[str, object]:
    """What this broker declares, read off the class."""
    return dict(import_string(path).OPTIONS)


def tabulated(text: str) -> set[str]:
    """The settings this page gives a table row, which is the only mention that counts.

    Searching the whole page was the first version and it could not fail properly: a setting
    named anywhere in the prose satisfied "the page documents it" while its row was gone, and
    a row is what a reader configures from. Shared with the case below rather than written
    twice, so the two cannot drift into disagreeing about what counts as documented.
    """
    return set(re.findall(r'^\| `([A-Z_]+)` \|', text, re.MULTILINE))


def test_every_shipped_broker_has_a_page():
    """A transport nobody can read about is a transport nobody should be offered.

    Both halves: a broker with no entry in the mapping, and an entry pointing at a file that
    is not there. Comparing the two key sets alone passed with the page deleted, which made
    this the one case in the file that could not fail for the reason it names.
    """
    assert sorted(PAGES) == sorted(SHIPPED), (
        f'pages are mapped for {sorted(PAGES)}, brokers shipped are {sorted(SHIPPED)}'
    )
    absent = sorted(name for name in PAGES.values() if not (WIKI / name).is_file())
    assert absent == [], f'mapped but missing from docs/wiki: {absent}'


@pytest.mark.parametrize('path', sorted(PAGES))
def test_the_page_names_every_setting_the_broker_declares(path):
    """A transport gaining an option must not leave its own page half true."""
    page = WIKI / PAGES[path]
    missing = sorted(set(options(path)) - tabulated(page.read_text(encoding='utf-8')))

    assert missing == [], f'{page.name} gives no table row to {missing}, which {path} declares'


@pytest.mark.parametrize('path', sorted(PAGES))
def test_the_page_names_nothing_the_broker_does_not_declare(path):
    """The other direction, which is what catches a row copied from a neighbouring page.

    Only settings **another** transport declares are looked for: a page is free to mention
    the package's own keys, and `MAX_IN_FLIGHT` on the Kafka page is the point rather than a
    mistake.
    """
    page = WIKI / PAGES[path]
    # everything not this broker's and not the package's, rather than only a neighbour's:
    # looking for a *known* stray let a typo through, and a row nobody declares is the worse
    # of the two — a neighbour's setting is at least a real setting somewhere
    strays = sorted(tabulated(page.read_text(encoding='utf-8')) - set(options(path)) - SHARED)

    assert strays == [], f'{page.name} tabulates {strays}, which {path} does not declare'


@pytest.mark.parametrize('path', sorted(PAGES))
def test_the_page_agrees_about_what_is_required(path):
    """The one detail a reader acts on before the first run, so it must not drift.

    `REQUIRED` is a sentinel rather than a default, and a page saying **required** about a
    setting that has one sends somebody looking for a value they did not need — while the
    reverse leaves them starting a bot that cannot connect.
    """
    page = WIKI / PAGES[path]
    text = page.read_text(encoding='utf-8')
    for name, default in options(path).items():
        row = re.search(rf'^\| `{name}` \| (.+?) \|', text, re.MULTILINE)
        assert row, f'{page.name} has no table row for `{name}`'
        # the marker, not the word: a substring test reads "not required" as required, and
        # would match the word anywhere in a description cell that happened to use it
        says_required = re.search(r'\*\*required\*\*', row.group(1), re.IGNORECASE) is not None
        assert says_required == (default is REQUIRED), (
            f'{page.name} says {row.group(1).strip()!r} for `{name}`, '
            f'which {path} declares as {"REQUIRED" if default is REQUIRED else default!r}'
        )


SETTINGS_PAGE = WIKI / 'Settings.md'


def declared_row(path: str) -> tuple[set[str], set[str]]:
    """What `Settings.md`'s transport table says this broker takes, and what it requires.

    The table is the page an operator reads to pick a transport, so it is the one place where
    being wrong sends somebody to configure the wrong keys. Parsed rather than eyeballed: the
    row is `| dotted.path | `A`, `B` | **`A`** |`, and the third cell names the required ones in
    bold or says none.
    """
    row = re.search(rf'^\| `{re.escape(path)}` \| (.+?) \| (.+?) \|$', SETTINGS_PAGE.read_text(), re.MULTILINE)
    assert row, f'Settings.md has no transport-table row for {path}'
    return set(re.findall(r'`([A-Z_]+)`', row.group(1))), set(re.findall(r'\*\*`([A-Z_]+)`\*\*', row.group(2)))


@pytest.mark.parametrize('path', sorted(PAGES))
def test_the_settings_table_lists_what_the_broker_takes(path):
    """Exactly what it declares — a missing name hides a setting, a stray one invents it."""
    listed, _required = declared_row(path)

    assert listed == set(options(path)), (
        f'Settings.md lists {sorted(listed)} for {path}, which declares {sorted(options(path))}'
    )


@pytest.mark.parametrize('path', sorted(PAGES))
def test_the_settings_table_agrees_about_what_is_required(path):
    """The column somebody reads before their first run, so it must match the sentinel.

    Named in bold or not at all: the row for a broker with no required settings says so in
    words, and a `**`NAME`**` anywhere in that cell is a claim this checks against `OPTIONS`.
    """
    _listed, required = declared_row(path)
    expected = {name for name, default in options(path).items() if default is REQUIRED}

    assert required == expected, (
        f'Settings.md marks {sorted(required)} required for {path}, which requires {sorted(expected)}'
    )
