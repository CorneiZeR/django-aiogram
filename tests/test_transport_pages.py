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


def test_every_shipped_broker_has_a_page():
    """A transport nobody can read about is a transport nobody should be offered."""
    assert sorted(PAGES) == sorted(SHIPPED), (
        f'pages are mapped for {sorted(PAGES)}, brokers shipped are {sorted(SHIPPED)}'
    )


@pytest.mark.parametrize('path', sorted(PAGES))
def test_the_page_names_every_setting_the_broker_declares(path):
    """A transport gaining an option must not leave its own page half true."""
    page = WIKI / PAGES[path]
    text = page.read_text(encoding='utf-8')
    missing = sorted(name for name in options(path) if f'`{name}`' not in text)

    assert missing == [], f'{page.name} does not name {missing}, which {path} declares'


@pytest.mark.parametrize('path', sorted(PAGES))
def test_the_page_names_nothing_the_broker_does_not_declare(path):
    """The other direction, which is what catches a row copied from a neighbouring page.

    Only settings **another** transport declares are looked for: a page is free to mention
    the package's own keys, and `MAX_IN_FLIGHT` on the Kafka page is the point rather than a
    mistake.
    """
    page = WIKI / PAGES[path]
    text = page.read_text(encoding='utf-8')
    mine = set(options(path))
    others = {name for other in SHIPPED if other != path for name in options(other)}
    # in a table cell, so a passing mention in prose is not what this is about
    rows = set(re.findall(r'^\| `([A-Z_]+)` \|', text, re.MULTILINE))
    strays = sorted(rows & (others - mine) - SHARED)

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
        says_required = 'required' in row.group(1).lower()
        assert says_required == (default is REQUIRED), (
            f'{page.name} says {row.group(1).strip()!r} for `{name}`, '
            f'which {path} declares as {"REQUIRED" if default is REQUIRED else default!r}'
        )
