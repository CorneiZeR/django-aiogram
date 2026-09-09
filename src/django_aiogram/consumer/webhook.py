"""Receive updates over HTTP instead of polling for them.

Long polling needs a process that runs forever. A webhook does not: Telegram
posts each update to a URL, so the update arrives in whichever process serves
that URL — normally the web one.

**With several bots the URL carries the identity**, and each bot has a secret of its own.
Both halves matter: one URL for every bot would make the update's own contents the only clue
about who it is for, and one shared secret would let a leak from one client's bot post as
every other. The path segment is what a `setWebhook` call registers, so it is also what tells
this view which bot's handlers -- and which bot's token -- an update belongs to.

**An update for a bot this deployment does not serve is refused without a database read.** The
identities come from the same watermark-cached read the supervisor uses, so an unknown one
costs a set lookup rather than a query: a webhook that queried per request would be a way for
a stranger to load the database by posting nonsense at it.

The view is deliberately synchronous. An async view would run on the server's
own loop under ASGI but on a throwaway loop per request under WSGI, and the
bot's HTTP session binds to the first loop that uses it. Driving the bot's own
loop works the same under both.
"""

import hmac
import json
import logging
import secrets
import time
from typing import TYPE_CHECKING, Any

from aiogram.types import Update
from django.core.exceptions import ImproperlyConfigured
from django.core.signals import setting_changed
from django.dispatch import receiver
from django.http import HttpRequest, HttpResponse, HttpResponseNotAllowed
from django.views.decorators.csrf import csrf_exempt
from pydantic import ValidationError

from django_aiogram import bot
from django_aiogram.config.bots import BOTS_SETTINGS_NAME
from django_aiogram.config.defaults import DEFAULTS
from django_aiogram.config.enums import UpdateMode, as_member, choices
from django_aiogram.config.settings import SETTINGS_NAME, conf
from django_aiogram.exceptions import LoopUnavailableError

if TYPE_CHECKING:
    from collections.abc import Mapping

logger = logging.getLogger('django_aiogram')

#: what Telegram sends the configured secret back in
SECRET_HEADER = 'HTTP_X_TELEGRAM_BOT_API_SECRET_TOKEN'  # noqa: S105 - a header name, not the secret it carries
#: plain strings, so argparse choices and messages read as the settings do
MODES = choices(UpdateMode)


def current_mode() -> str:
    """Which of the two ways of receiving updates this deployment uses.

    Read through `as_member`, so `UpdateMode.WEBHOOK` and `'webhook'` are the same setting. They
    were not: `str()` on a member gives its *name*, so the spelling `API.md` documents --
    ``'MODE': UpdateMode.POLLING`` -- raised here at startup, naming a value nobody typed.

    The refusal quotes what the project wrote rather than the normalised form, for the same reason.
    """
    member = as_member(conf['MODE'], UpdateMode)
    if member is None:
        msg = f"{SETTINGS_NAME}['MODE'] must be one of {sorted(MODES)}, got {conf['MODE']!r}."
        raise ImproperlyConfigured(msg)
    return member.value


def _refused_by_mode() -> 'HttpResponse | None':
    """Refuse where this deployment does not serve updates over HTTP, or say nothing.

    Two refusals with one answer: a `MODE` that cannot be read at all -- unguarded that left
    an unauthenticated 500 with a traceback, from a view whose every other refusal is a
    status code -- and a `MODE` that says this deployment polls, where serving here would
    mean two sources of updates and no way to tell which handled what.
    """
    try:
        mode = current_mode()
    except ImproperlyConfigured:
        logger.exception('webhook is not configured to serve updates')
        return HttpResponse(status=503)
    if mode != UpdateMode.WEBHOOK:
        logger.warning(
            'webhook received an update while this deployment polls',
            extra={'tg_mode': mode},
        )
        return HttpResponse(status=503)
    return None


def _read_update(request: HttpRequest, telegram: Any) -> 'Update | None':  # noqa: ANN401 - an aiogram `Bot`
    """Read one update out of the body, or ``None`` where the body was not one.

    The body is whoever posted it, so the type of the failure is all that goes in the log: a
    traceback here would spread unvalidated input through the handlers and into every log
    shipper downstream.
    """
    try:
        payload = json.loads(request.body)
        return Update.model_validate(payload, context={'bot': telegram})
    except (json.JSONDecodeError, UnicodeDecodeError, ValidationError, TypeError) as error:
        logger.warning(
            'webhook could not read an update',
            extra={'tg_error': type(error).__name__},
        )
        return None


def _serving_bot(bot_id: 'int | None') -> 'tuple[Any, str, HttpResponse | None]':
    """Resolve the bot the path names and the secret it is served under, or refuse.

    Three answers in one place, because each is a refusal the view returns rather than a
    question the caller can act on: a 404 for an identity this deployment does not serve, a
    503 for a configuration this cannot read, and the bot with its own secret otherwise.

    The secret is the addressed bot's rather than the process's wherever the path names one:
    one secret shared by every bot would make a leak from one client's bot a way to post as
    all of them.
    """
    try:
        serving = _addressed(bot_id)
    except LookupError:
        # not a database read: the identities come from the cache the supervisor's own pass
        # fills, so a stranger posting nonsense at this URL costs a set lookup
        logger.warning(
            'webhook refused an update for a bot this deployment does not serve',
            extra={'tg_bot_id': bot_id},
        )
        return None, '', HttpResponse(status=404)
    except ImproperlyConfigured:
        logger.exception('webhook cannot resolve the bot the path names', extra={'tg_bot_id': bot_id})
        return None, '', HttpResponse(status=503)
    try:
        secret = webhook_secret() if bot_id is None else webhook_secret(serving.settings)
    except (ImproperlyConfigured, KeyError):
        # a bot with no secret of its own is not served rather than served under somebody
        # else's: a shared secret makes one client's leak everybody's.
        #
        # `KeyError` because `BOT_PROVIDERS` is a seam: a project's own provider hands back
        # records this package did not resolve, so a mapping without the key is its answer to
        # give -- and unguarded it would be an unauthenticated 500 with a traceback, from the
        # one branch whose whole job is to refuse
        logger.exception('webhook has no secret to serve this update with', extra={'tg_bot_id': bot_id})
        return None, '', HttpResponse(status=503)
    return serving, secret, None


def _addressed(bot_id: 'int | None') -> Any:  # noqa: ANN401 - a `TelegramBot`, without importing aiogram here
    """Return the bot the path names, or the process's own where it names none.

    Raises ``LookupError`` for an identity this deployment does not serve, which the view
    answers with a 404 -- and it is answered from the cached set rather than a query, so the
    cost of a stranger's traffic is a set lookup. A configuration this cannot read raises
    ``ImproperlyConfigured``, which is a 503: ours to fix, not the caller's.
    """
    if bot_id is None:
        return bot
    # asked twice on purpose: the cache answers a served bot without a query, and an
    # identity missing from it is re-read once before it is refused -- a bot registered a
    # second ago must not be turned away for the rest of the interval
    found = served_records().get(bot_id) or served_records(refresh=True).get(bot_id)
    if found is None:
        msg = f'this deployment serves no bot with the identity {bot_id}'
        raise LookupError(msg)
    # deferred: the registry reaches aiogram, and this module is imported by `urls.py`
    from django_aiogram.runtime.registry import bots  # noqa: PLC0415 - as above

    # from the record this already holds, rather than by identity: `by_id` would ask the
    # providers again, and that is the watermark read the cache above is here to avoid
    return bots.for_record(found)


def served_records(*, refresh: bool = False) -> 'Mapping[int, Any]':
    """Every bot this deployment serves, by identity, cached so a request is not a query.

    Two caches, and the second is why this function exists. The providers already answer an
    unchanged table without reading its rows, but that still costs the watermark -- two
    aggregates -- and a webhook is asked per request. So the set itself is held for
    ``BOT_REFRESH_INTERVAL``, and an update for a bot already in it costs a set lookup and
    nothing else.

    ``refresh`` is what the view asks for when an identity is *not* in the cache: a bot
    registered a second ago would otherwise be refused for the rest of the interval. It is
    still bounded -- at most one read a second, `MISS_GRACE` -- because the alternative is a
    stranger posting unknown identities as fast as the database can answer.

    The **resolved records** rather than the identities alone, because the bot is built from
    one: asking the providers again to turn an identity into a bot would put the watermark
    back on every request, which is the read this cache exists to remove.

    A read that failed answers with what was last known rather than with nothing: refusing
    every update because a database blinked would be an outage.
    """
    global _served, _read_at  # noqa: PLW0603 - one cache per process, like the bots it names
    if _read_at is not None:
        age = time.monotonic() - _read_at
        if age < (MISS_GRACE if refresh else _cache_for()):
            return _served
    # deferred: the providers reach the ORM, and this module is imported by `urls.py`
    from django_aiogram.runtime.providers import desired  # noqa: PLC0415 - as above

    # stamped whichever way it goes: a failure that left the old timestamp would make every
    # following request re-enter the read, so an unreachable database would be one attempt per
    # webhook request -- exactly the load this cache is here to remove
    _read_at = time.monotonic()
    try:
        _served = {record.bot_id: record for record in desired() if record.bot_id is not None}
    except Exception:
        logger.exception('webhook could not re-read the bots it serves; going by the last read')
    return _served


def _cache_for() -> float:
    """How long the identity set is believed, never below a second.

    `BOT_REFRESH_INTERVAL` because it is the same question the supervisor asks of the same
    table, and a webhook that noticed a change sooner than the container serving it would be
    accepting updates for a bot with no handlers running.
    """
    try:
        return max(1.0, float(conf['BOT_REFRESH_INTERVAL']))
    except (TypeError, ValueError, ImproperlyConfigured):
        return float(DEFAULTS['BOT_REFRESH_INTERVAL'])


#: how often a *miss* may re-read: enough that a bot registered a moment ago is served, and
#: little enough that unknown identities arriving in a flood cannot be a query each
MISS_GRACE = 1.0

#: what `served_records` last read, so a failure answers with it rather than with nothing,
#: and when: `None` is "never read", which is not the same as "read and empty"
_served: dict[int, Any] = {}
_read_at: float | None = None


@receiver(setting_changed, dispatch_uid='django_aiogram.consumer.webhook')
def _forget_the_bots_it_serves(**kwargs: Any) -> None:
    """Drop the cache when either dict a bot is configured in changes.

    An interval of stale records is an interval of the wrong answers: a bot whose section was
    removed would still be routed, and one whose ``WEBHOOK_SECRET`` was rotated would still
    accept the old one -- for up to `BOT_REFRESH_INTERVAL` after the change, which is the
    window a rotation exists to close.
    """
    global _served, _read_at  # noqa: PLW0603 - one cache per process, like the bots it names
    if kwargs.get('setting') in {SETTINGS_NAME, BOTS_SETTINGS_NAME}:
        _served = {}
        _read_at = None


def webhook_secret(settings: 'Mapping[str, Any] | None' = None) -> str:
    """Return the shared secret, which the view refuses to run without."""
    resolved = conf if settings is None else settings
    secret = str(resolved['WEBHOOK_SECRET'] or '').strip()
    if not secret:
        msg = (
            f"{SETTINGS_NAME}['WEBHOOK_SECRET'] is required to serve the webhook: without it "
            'anyone who finds the URL can feed your bot updates.'
        )
        raise ImproperlyConfigured(msg)
    return secret


@csrf_exempt
def telegram_webhook(  # noqa: PLR0911 - a guard-clause chain is the readable shape
    request: HttpRequest,
    bot_id: int | None = None,
) -> HttpResponse:
    """Feed one update to the dispatcher, through the bot the path names.

    ``bot_id`` comes from the URL -- ``path('tg/<int:bot_id>/<secret>/', telegram_webhook)`` --
    and is what a deployment serving several bots registers with `setWebhook`. Left out, the
    view serves the process's own bot, which is every 4.x deployment and every project with
    one bot.

    Answers 200 for anything Telegram should not retry, including a handler that
    raised — a non-2xx makes Telegram redeliver the same update, and a handler
    that fails once will fail again. A *refusal* is the other case: when nothing
    ran at all, because this process is shutting down, redelivery is exactly
    what should happen, so that answers 503.
    """
    if request.method != 'POST':
        return HttpResponseNotAllowed(['POST'])

    if not bot.enabled:
        logger.warning('webhook received an update while the bot is disabled')
        return HttpResponse(status=503)

    refused = _refused_by_mode()
    if refused is not None:
        return refused

    # after the mode check, so a polling deployment is never asked for a secret it has no
    # reason to have set: reading it first told every such deployment its configuration was
    # unreadable instead of that it polls
    serving, secret, refusal = _serving_bot(bot_id)
    if refusal is not None:
        return refusal

    given = request.META.get(SECRET_HEADER, '')
    # bytes, not str: `compare_digest` refuses str arguments outside ASCII, so a header
    # with one non-ASCII character used to raise TypeError here — an unauthenticated
    # 500 with a traceback, from the branch whose whole job is to answer 403
    if not hmac.compare_digest(given.encode(), secret.encode()):
        logger.warning('webhook rejected an update with a wrong secret')
        return HttpResponse(status=403)

    try:
        telegram = serving.bot
    except ImproperlyConfigured:
        # a missing token is our problem, not a bad request
        logger.exception('webhook cannot build the bot', extra={'tg_bot_id': bot_id})
        return HttpResponse(status=503)

    update = _read_update(request, telegram)
    if update is None:
        return HttpResponse(status=400)

    try:
        serving.feed_update(update)
    except LoopUnavailableError:
        # nothing ran, so this update is still Telegram's to redeliver — which a
        # 2xx would tell it not to. The shutdown window is not a handler that
        # failed, and the two must not answer the same way
        logger.warning('webhook refused an update', extra={'tg_update': update.update_id})
        return HttpResponse(status=503)
    except Exception:
        logger.exception('webhook handler failed', extra={'tg_update': update.update_id})

    return HttpResponse(status=200)


def webhook_settings(settings: 'Mapping[str, Any] | None' = None, bot_id: 'int | None' = None) -> dict[str, Any]:
    """Everything `setWebhook` needs, resolved from settings.

    ``bot_id`` is what makes the URL that bot's own: appended as a path segment, which is
    what :func:`telegram_webhook` reads back out of the request. One URL for every bot would
    make the update's own contents the only clue about who it is for.
    """
    resolved = conf if settings is None else settings
    url = str(resolved['WEBHOOK_URL'] or '').strip()
    if not url:
        msg = f"{SETTINGS_NAME}['WEBHOOK_URL'] is required to register a webhook."
        raise ImproperlyConfigured(msg)
    allowed = resolved['WEBHOOK_ALLOWED_UPDATES']
    return {
        'url': url if bot_id is None else f'{url.rstrip("/")}/{bot_id}/',
        'secret_token': webhook_secret(resolved),
        # `[]` rather than `None`, and that is not a formality: Telegram reads an *omitted*
        # `allowed_updates` as "keep whatever was registered before", so a bot narrowed to
        # `['message']` once would stay narrowed for ever while the settings said default.
        # An empty list is the documented way to ask for the default set
        'allowed_updates': list(allowed) if allowed else [],
        'drop_pending_updates': False,
    }


def new_secret() -> str:
    """Generate a webhook secret, because a typed one is a shared one.

    Telegram accepts 1-256 characters of ``A-Za-z0-9_-``, which is exactly what
    ``token_urlsafe`` produces. Generated rather than typed for the reason a per-bot secret
    exists at all: a person setting one secret for twenty bots is the leak that lets one
    client's bot post as all of them.
    """
    return secrets.token_urlsafe(32)


def registered(desired: dict[str, Any], info: Any) -> bool:  # noqa: ANN401 - aiogram's `WebhookInfo`
    """Whether Telegram already has what :func:`webhook_settings` asks for.

    The URL and the allowed updates are what `getWebhookInfo` reports, and they are what a
    reconciliation can compare. **The secret is not reported at all**, so a rotated one looks
    like no change from here -- which is why the pass takes `--force` and why rotating a
    secret means registering again.

    Telegram omits ``allowed_updates`` where the default set is registered, so both sides are
    normalised to a list: read as "nothing to compare", a bot narrowed to one update type
    once would look correctly registered for ever against settings asking for the default.
    """
    if str(getattr(info, 'url', '') or '') != desired['url']:
        return False
    # both sides normalised to a list, because Telegram omits the field where the default set
    # is registered: read as "nothing to compare", a bot narrowed once would look correct for
    # ever against settings asking for the default
    wanted = desired['allowed_updates'] or []
    return sorted(getattr(info, 'allowed_updates', None) or []) == sorted(wanted)
