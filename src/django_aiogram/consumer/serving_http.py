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
    """Answer the path part of ``WEBHOOK_URL``, exactly as Telegram was given it.

    Django route patterns are relative to the mount point, and there is none here: this
    server exists to serve one path, so the pattern *is* the path Telegram was given --
    **including whether it ends in a slash**. Telegram posts to the URL it was registered
    with, character for character, so a route that dropped the trailing slash would answer
    404 to every update a slashed registration delivers, and the other way round.
    """
    resolved = conf if settings is None else settings
    url = str(resolved['WEBHOOK_URL'] or '').strip()
    if not url:
        msg = f"{SETTINGS_NAME}['WEBHOOK_URL'] is required to serve the webhook."
        raise ImproperlyConfigured(msg)
    return urlsplit(url).path.lstrip('/')


def urlpatterns(settings: 'Mapping[str, Any] | None' = None) -> 'Seq[Any]':
    """Build the two routes this server answers: one bot's, and the process's own.

    The identity segment is last and the unguessable one first, which is the order
    `tgbot_webhook` registers -- see **Webhook** in the documentation. Both are declared
    whatever a deployment has: a process with one bot is posted to on the shorter route and
    never sees the other, and refusing to declare it would make "several bots" a redeploy
    rather than a row.
    """
    route = webhook_route(settings)
    # the identity route is `<prefix>/<id>/` whatever the prefix ends in, because that is
    # what `tgbot_webhook` registers: it appends the identity to a URL it has stripped
    identity = f'{route.rstrip("/")}/<int:bot_id>/'.lstrip('/')
    return [
        path(identity, telegram_webhook),
        # escaped, and anchored on the route as it stands: the path is one somebody chose,
        # and a `.` or a `+` in it would otherwise match more than the one Telegram posts to
        re_path(rf'^{re.escape(route)}$', telegram_webhook),
    ]


#: the extra whose packages this server needs, and the only place its name is written
WEBHOOK_EXTRA = 'webhook'
#: what to require when the metadata cannot be read: the server imports this by name, so
#: its absence is the failure the refusal is about whatever `Requires-Dist` says
FALLBACK_REQUIREMENT = 'uvicorn'


def webhook_requirements() -> tuple[str, ...]:
    """Read the distributions the `webhook` extra asks for, out of the installed metadata.

    The group rather than a name: `uvicorn` is what it holds today, and a release that adds
    to it should not also have to remember a list in here. Read from the metadata of the
    *installed* package, which is what a deployment actually resolved.

    Falls back to the one package the server imports where the metadata cannot be read at
    all -- a source tree that was never installed, an environment that lost its dist-info.
    Answering *nothing* there would have let the refusal pass and the start fail on an
    `ImportError` instead, which is the shape this exists to replace.
    """
    from importlib.metadata import PackageNotFoundError, requires  # noqa: PLC0415 - only when serving

    try:
        declared = requires('django-aiogram') or ()
    except PackageNotFoundError:
        return (FALLBACK_REQUIREMENT,)
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
            names.append(name.strip())
    return tuple(dict.fromkeys(names))


def _below_floor(installed: str, floor: str) -> bool:
    """Whether an installed version is older than the one the extra asks for.

    A `>=` floor and nothing else: that is what this package's extras declare, and a
    comparison that guessed at the rest -- `!=`, `~=`, an epoch, a pre-release -- would be
    a packaging library written badly rather than a check. Anything it cannot read is
    treated as satisfied, so the refusal is never about this function being unsure.
    """

    def numbers(version: str) -> tuple[int, ...]:
        """Read the leading numeric release of a version, as far as it stays numeric."""
        parts = []
        for piece in version.split('.'):
            if not piece.isdigit():
                break
            parts.append(int(piece))
        return tuple(parts)

    wanted, running = numbers(floor), numbers(installed)
    return bool(wanted) and bool(running) and running < wanted


def webhook_floors() -> dict[str, str]:
    """Answer the `>=` floor each package of the extra declares, where it declares one."""
    from importlib.metadata import PackageNotFoundError, requires  # noqa: PLC0415 - only when serving

    try:
        declared = requires('django-aiogram') or ()
    except PackageNotFoundError:
        return {}
    floors = {}
    for requirement in declared:
        expression, _, marker = requirement.partition(';')
        if f"extra == '{WEBHOOK_EXTRA}'" not in marker and f'extra == "{WEBHOOK_EXTRA}"' not in marker:
            continue
        name, separator, floor = expression.strip().partition('>=')
        if separator:
            floors[name.split('[', 1)[0].strip()] = floor.strip()
    return floors


def missing_requirements() -> tuple[str, ...]:
    """Answer which of those are absent or older than the extra declares, in order.

    A version under the floor counts as missing on purpose: the extra names it because the
    server needs what that release added, and letting it through would move the failure to
    whichever line first depends on it.
    """
    from importlib.metadata import PackageNotFoundError, version  # noqa: PLC0415 - as above

    floors = webhook_floors()

    def unusable(name: str) -> bool:
        """Whether this distribution is absent, or present and too old to be asked for."""
        try:
            installed = version(name)
        except PackageNotFoundError:
            return True
        floor = floors.get(name)
        return floor is not None and _below_floor(installed, floor)

    return tuple(name for name in webhook_requirements() if unusable(name))


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
