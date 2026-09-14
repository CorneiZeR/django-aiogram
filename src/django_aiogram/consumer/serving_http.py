"""The HTTP server a bot container runs when it receives its own updates.

Webhook mode has always worked by serving :func:`~django_aiogram.consumer.webhook.telegram_webhook`
from the project's web tier. That still works and needs nothing here. What it costs is a
coupling nobody chose: an update is handled in a web process, beside the requests people are
waiting on, and the handler competes with them for that process's threads and its database
connections.

**This is the other shape**: `start_tgbot --serve` runs a server whose only route is the
webhook, in the container that already has the bot's loop. The web tier then serves no
updates at all, and a deployment that never registered the view in its `urls.py` needs no
route there.

The path is not configured twice. It is read from ``WEBHOOK_URL`` -- the URL
`manage.py tgbot_webhook set` registers with Telegram -- so the one place that says where
updates arrive is the place that told Telegram. A deployment serving several bots gets the
identity segment as well, for the same reason the view takes ``bot_id``.
"""

import logging
import re
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from django.core.exceptions import ImproperlyConfigured
from django.urls import path, re_path

from django_aiogram.config.settings import SETTINGS_NAME, conf
from django_aiogram.consumer.webhook import telegram_webhook
from django_aiogram.exceptions import WebhookServerDependencyError

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop
    from collections.abc import Mapping
    from collections.abc import Sequence as Seq

    from django.core.handlers.asgi import ASGIHandler

logger = logging.getLogger('django_aiogram')

#: what `--serve` binds when nothing says otherwise: every interface, because the
#: proxy that terminates TLS is in another container
DEFAULT_HOST = '0.0.0.0'  # noqa: S104 - a container port, reached through the proxy
DEFAULT_PORT = 8080


def webhook_route(settings: 'Mapping[str, Any] | None' = None) -> str:
    """Answer the path part of ``WEBHOOK_URL``, without its leading slash.

    Django route patterns are relative to the mount point, and there is none here: this
    server exists to serve one path, so the pattern *is* the path Telegram was given.
    """
    resolved = conf if settings is None else settings
    url = str(resolved['WEBHOOK_URL'] or '').strip()
    if not url:
        msg = f"{SETTINGS_NAME}['WEBHOOK_URL'] is required to serve the webhook."
        raise ImproperlyConfigured(msg)
    return urlsplit(url).path.strip('/')


def urlpatterns(settings: 'Mapping[str, Any] | None' = None) -> 'Seq[Any]':
    """Build the two routes this server answers: one bot's, and the process's own.

    The identity segment is last and the unguessable one first, which is the order
    `tgbot_webhook` registers -- see **Webhook** in the documentation. Both are declared
    whatever a deployment has: a process with one bot is posted to on the shorter route and
    never sees the other, and refusing to declare it would make "several bots" a redeploy
    rather than a row.
    """
    route = webhook_route(settings)
    prefix = f'{route}/' if route else ''
    return [
        path(f'{prefix}<int:bot_id>/', telegram_webhook),
        # escaped: the prefix is a path somebody chose, and `.` or `+` in it would otherwise
        # match more than the one route Telegram was given
        re_path(rf'^{re.escape(prefix)}$', telegram_webhook),
    ]


#: the extra whose packages this server needs, and the only place its name is written
WEBHOOK_EXTRA = 'webhook'


def webhook_requirements() -> tuple[str, ...]:
    """Read the distributions the `webhook` extra asks for, out of the installed metadata.

    The group rather than a name: `uvicorn` is what it holds today, and a release that adds
    to it should not also have to remember a list in here. Read from the metadata of the
    *installed* package, which is what a deployment actually resolved.

    Answers nothing where the metadata cannot be read at all -- a source tree that was never
    installed, an environment that lost the dist-info -- because refusing to start over an
    unreadable `Requires-Dist` would be a refusal about this package rather than about the
    deployment.
    """
    from importlib.metadata import PackageNotFoundError, requires  # noqa: PLC0415 - only when serving

    try:
        declared = requires('django-aiogram') or ()
    except PackageNotFoundError:
        return ()
    names = []
    for requirement in declared:
        expression, _, marker = requirement.partition(';')
        if f"extra == '{WEBHOOK_EXTRA}'" not in marker and f'extra == "{WEBHOOK_EXTRA}"' not in marker:
            continue
        # `uvicorn>=0.30`, `redis[hiredis]>=6.2`: the name ends at the first character
        # that can follow it, and every one of those is punctuation
        name = expression.strip()
        for boundary in '[(<>=!~; ':
            name = name.split(boundary, 1)[0]
        if name:
            names.append(name)
    return tuple(dict.fromkeys(names))


def missing_requirements() -> tuple[str, ...]:
    """Answer which of those are not installed, in the order they are declared."""
    from importlib.metadata import PackageNotFoundError, version  # noqa: PLC0415 - as above

    def installed(name: str) -> bool:
        """Whether this distribution is present, by the name its metadata declares."""
        try:
            version(name)
        except PackageNotFoundError:
            return False
        return True

    return tuple(name for name in webhook_requirements() if not installed(name))


def require_dependencies() -> None:
    """Refuse to serve without the extra, by name and with the install line."""
    missing = missing_requirements()
    if missing:
        raise WebhookServerDependencyError(missing, WEBHOOK_EXTRA)


def application() -> 'ASGIHandler':
    """Build the ASGI application this server runs: Django, with a urlconf of our own.

    `ROOT_URLCONF` is replaced rather than added to, because this process serves the webhook
    and nothing else: a project's own routes belong to its web tier, and answering them from
    the bot container would be a second, unannounced deployment of them.
    """
    from django.conf import settings as django_settings  # noqa: PLC0415 - only when serving
    from django.core.asgi import get_asgi_application  # noqa: PLC0415 - as above

    django_settings.ROOT_URLCONF = 'django_aiogram.consumer.webhook_urls'
    return get_asgi_application()


def serve(loop: 'AbstractEventLoop', host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> None:
    """Run the server on the bot's own loop until it is signalled.

    On *that* loop rather than one of its own, which is what keeps the container to a single
    one: an update the view accepts is handed to the loop already turning under it, and the
    consumers hand their sends to the same. `uvicorn.run` would build a second loop and leave
    the bot's needing a thread to turn it.

    The dependency refusal comes first: a missing `uvicorn` is a deployment that installed the
    wrong extra, and finding that out through an `ImportError` in the middle of a startup log
    is worse than being told before anything is bound.
    """
    require_dependencies()
    import uvicorn  # noqa: PLC0415 - behind the extra, and only when serving

    route = webhook_route()
    logger.info('serving the webhook', extra={'tg_host': host, 'tg_port': port, 'tg_route': f'/{route}'})
    # `log_config=None`: the project configured logging when Django booted, and uvicorn's own
    # dictConfig would replace every handler it set up
    server = uvicorn.Server(uvicorn.Config(application(), host=host, port=port, log_config=None))
    loop.run_until_complete(server.serve())
