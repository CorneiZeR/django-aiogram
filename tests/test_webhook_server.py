"""The server a bot container runs when it receives its own updates.

What is checked here is the shape of the thing rather than uvicorn: the routes come from the
URL Telegram was given, the extra is required as a group, and the flag is refused where there
would be nothing to serve.
"""

import importlib
import re
from pathlib import Path

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.core.management import CommandError, call_command
from django.test import override_settings

from django_aiogram.consumer import serving_http
from django_aiogram.exceptions import WebhookServerDependencyError

SETTINGS = {
    'TOKEN': '42:x',
    'REDIS_URL': 'redis://localhost:6379/0',
    'MODE': 'webhook',
    'WEBHOOK_URL': 'https://example.test/tg/9c1f2b7a/',
    'WEBHOOK_SECRET': 'a-long-random-string',
}


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_the_route_is_the_one_telegram_was_given():
    """Nothing configures the path twice: `WEBHOOK_URL` is what `tgbot_webhook set` registers."""
    assert serving_http.webhook_route() == 'tg/9c1f2b7a'


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'WEBHOOK_URL': ''})
def test_serving_without_a_url_refuses_by_name():
    """There is nothing to serve and nothing to guess: a default path would be one Telegram never got."""
    with pytest.raises(ImproperlyConfigured, match='WEBHOOK_URL'):
        serving_http.webhook_route()


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_both_routes_are_declared_whatever_this_deployment_has():
    """One bot is posted to on the shorter route; a bot from the table needs the identity one.

    Declared together so that serving a second bot is a row rather than a redeploy.
    """
    patterns = [str(route.pattern) for route in serving_http.urlpatterns()]

    assert any('bot_id' in pattern for pattern in patterns), patterns
    assert any(re.fullmatch(r'\^?tg/9c1f2b7a/\$?', pattern) for pattern in patterns), patterns


def test_the_extra_is_read_from_the_metadata_rather_than_listed_here():
    """A package added to the group is required from the next release, with nothing else edited.

    This is the invariant the group buys: the names come from what the installed distribution
    declares, so `pyproject` is the one place that says what serving needs.
    """
    # 3.10 has no TOML parser in its standard library, and this package ships no dependency
    # for one: the invariant is pinned on every other version the suite runs on
    tomllib = pytest.importorskip('tomllib', reason='stdlib TOML parsing landed in 3.11')
    declared = tomllib.loads((Path(__file__).resolve().parent.parent / 'pyproject.toml').read_text())
    named = declared['project']['optional-dependencies'][serving_http.WEBHOOK_EXTRA]
    expected = {re.split(r'[\[(<>=!~; ]', requirement.strip(), maxsplit=1)[0] for requirement in named}

    assert set(serving_http.webhook_requirements()) == expected


def test_a_missing_package_is_refused_by_name_with_the_install_line(monkeypatch):
    """The reader needs the extra, which an ImportError naming a module does not give them."""
    monkeypatch.setattr(serving_http, 'missing_requirements', lambda: ('uvicorn',))

    with pytest.raises(WebhookServerDependencyError) as refusal:
        serving_http.require_dependencies()

    assert 'pip install "django-aiogram[webhook]"' in str(refusal.value)
    assert 'uvicorn' in str(refusal.value)


def test_nothing_is_missing_in_an_environment_that_installed_the_extra():
    """The suite installs it, so the check has to pass here or it is checking the wrong thing."""
    pytest.importorskip('uvicorn')

    assert serving_http.missing_requirements() == ()


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'MODE': 'polling'})
def test_serving_is_refused_where_updates_are_polled_for():
    """`--serve` on a polling deployment binds a port nothing will ever post to."""
    with pytest.raises(CommandError, match='nothing to serve'):
        call_command('start_tgbot', '--serve')


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_serving_and_no_updates_together_are_refused():
    """One says receive updates here, the other says receive none: the pair has no meaning."""
    with pytest.raises(CommandError, match='receive none'):
        call_command('start_tgbot', '--serve', '--no-updates')


@override_settings(TELEGRAM_BOT_DEFAULTS={**SETTINGS, 'WEBHOOK_URL': ''})
def test_importing_the_urlconf_reads_no_settings():
    """Importing a module of this package must never resolve a setting, and this one is imported.

    The suite imports every module there is, and Django imports whatever `ROOT_URLCONF`
    names. Building the routes at import would make both of those read `WEBHOOK_URL` --
    refusing, in a project that does not serve the webhook at all. The routes are built when
    the attribute is *read* instead, which is what the case below proves is still true.
    """
    module = importlib.reload(importlib.import_module('django_aiogram.consumer.webhook_urls'))

    # the module attribute itself, not what reading it answers: an import that built the
    # routes would have left them here, and with no URL to build them from it would have
    # raised on the line above instead
    assert 'urlpatterns' not in vars(module)


@override_settings(TELEGRAM_BOT_DEFAULTS=SETTINGS)
def test_the_urlconf_answers_the_routes_on_access():
    """What Django reads off the module is the pair of routes, resolved then and not before."""
    urls = importlib.import_module('django_aiogram.consumer.webhook_urls')

    assert len(urls.urlpatterns) == 2
