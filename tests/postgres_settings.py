"""The database-backed suite, against PostgreSQL instead of SQLite.

`tests/db_settings.py` runs on SQLite in memory, which is fast and covers almost everything --
but not the thing that decides whether a copied table can be written to afterwards. A sequence is
where the two backends genuinely differ: SQLite's `sqlite_sequence` follows an explicit id on its
own, and PostgreSQL's does not, so a copy that inserts explicit ids leaves the next insert
colliding on the primary key. A case about that is worth nothing unless it runs here.

Everything else is `tests.db_settings`, imported name by name rather than with a star: ruff runs
over `tests/` with every rule selected, and F403 is not in the per-file ignores.
"""

import os

from tests.db_settings import (
    INSTALLED_APPS,
    MIDDLEWARE,
    ROOT_URLCONF,
    SECRET_KEY,
    STATIC_URL,
    TELEGRAM_BOT,
    TEMPLATES,
    USE_TZ,
)

__all__ = [
    'DATABASES',
    'INSTALLED_APPS',
    'MIDDLEWARE',
    'ROOT_URLCONF',
    'SECRET_KEY',
    'STATIC_URL',
    'TELEGRAM_BOT',
    'TEMPLATES',
    'USE_TZ',
]


def _postgres(name: str) -> dict[str, object]:
    """One connection, configured from the environment CI and a local container both set."""
    return {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': name,
        'USER': os.environ.get('DJANGO_AIOGRAM_TEST_PG_USER', 'dra'),
        'PASSWORD': os.environ.get('DJANGO_AIOGRAM_TEST_PG_PASSWORD', 'dra'),
        'HOST': os.environ.get('DJANGO_AIOGRAM_TEST_PG_HOST', '127.0.0.1'),
        'PORT': os.environ.get('DJANGO_AIOGRAM_TEST_PG_PORT', '55439'),
    }


DATABASES = {
    'default': _postgres(os.environ.get('DJANGO_AIOGRAM_TEST_PG_NAME', 'dra')),
    # the second alias the router cases need, and a database of its own so the two do not
    # share a schema: Django creates `test_<name>` for each
    'logs': _postgres(os.environ.get('DJANGO_AIOGRAM_TEST_PG_LOGS', 'dra_logs')),
}
