"""Every setting that names a dotted path refuses a bad one in this package's own words.

Three settings take a path — `BROKER`, `DELIVERY`, `FSM_STORAGE` — and each resolver has gone to
some trouble to turn a settings mistake into a sentence naming the setting. A path with an **empty
module part** slipped past all three: `'.Storage'` reaches `import_module('')`, which raises
`ValueError` rather than `ImportError`, so the caller met Django's `Empty module name` instead.

Not exotic. It is what a copied path, an editor's autocomplete or a relative import written from
memory produces, and the message it used to give named nothing a reader could act on.

One table, three shapes of wrong path, because the defect was one class with three addresses: a
case per site would have been fixed one site at a time, which is how it survived the first two.
"""

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.test import override_settings

from django_aiogram.broker.exceptions import BrokerNotConfiguredError
from django_aiogram.broker.registry import broker_class
from django_aiogram.consumer.delivery import DeliveryNotConfiguredError, delivery_class
from django_aiogram.producer.from_settings import build_storage

SETTINGS = {'TOKEN': '42:x', 'REDIS_URL': 'redis://localhost'}

#: the setting, what resolves it, and the refusal it owes a reader
RESOLVERS = [
    ('BROKER', broker_class, BrokerNotConfiguredError),
    ('DELIVERY', delivery_class, DeliveryNotConfiguredError),
    ('FSM_STORAGE', build_storage, ImproperlyConfigured),
]

#: every way a path can be wrong before anything can be imported, and what each one raises inside
#: `import_string`: `ValueError` for the empty module part, `ImportError` for the other two
PATHS = ['.Thing', 'nomodule.Thing', 'Thing']


@pytest.mark.parametrize(('key', 'resolve', 'refusal'), RESOLVERS, ids=[row[0] for row in RESOLVERS])
@pytest.mark.parametrize('path', PATHS, ids=['an empty module part', 'no such module', 'no module at all'])
def test_a_path_that_cannot_resolve_is_refused_by_this_package(key, resolve, refusal, path):
    """The refusal is the package's own, and it names the setting rather than the import machinery."""
    with override_settings(TELEGRAM_BOT={**SETTINGS, key: path}), pytest.raises(refusal) as refused:
        resolve()

    assert key in str(refused.value), f'the refusal does not name {key}: {refused.value}'
