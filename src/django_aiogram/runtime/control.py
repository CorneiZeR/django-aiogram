"""Telling another process that the bots have changed, so it does not wait for its next poll.

A bot added through a project's own interface is a row in the web process, and the bot
container is somewhere else — Django's signals are in-process, so the row alone reaches
nothing. The transport is the one thing both of them already talk to, so the notice rides it.

**The message says "read again", never "set the token to X".** Level-triggered, like the
supervisor it wakes: a duplicate costs nothing, a loss costs one poll interval, and two of
them arriving out of order cannot leave a process serving the wrong bots. A message carrying
the change itself would have all three of those problems and a stale write besides.

**It reaches the queue it is published to**, which is one queue today. A deployment with a
queue per pool needs a notice per pool, and that arrives with the pools; the poll is what
makes a change arrive at all, and this is what makes it arrive in a second.

Published on the caller's commit, so a save the transaction rolls back announces nothing.
"""

import logging
from typing import TYPE_CHECKING

from django.db import DEFAULT_DB_ALIAS
from django.db.models.signals import post_delete, post_save

from django_aiogram.producer.committing import after_commit

if TYPE_CHECKING:
    from typing import Any

__all__ = ('CONTROL_KEY', 'announce', 'apply_control', 'connect', 'is_control', 'pack_reload')

logger = logging.getLogger('django_aiogram')

#: what each receiver is registered under, so an autoreload cannot stack duplicates
_UID = 'django_aiogram.runtime.control'

#: marks a payload as a notice rather than a call. Deliberately not an envelope version: a
#: consumer that does not know this key refuses the payload as unreadable and acknowledges it,
#: which is the right answer -- an old consumer polls, and a notice it cannot read is one it
#: does not need
CONTROL_KEY = '__control__'

#: the only notice there is. Named rather than implied, so a second one can be added without
#: a consumer guessing what an unknown notice means
RELOAD = 'reload'


def pack_reload() -> 'dict[str, Any]':
    """Build the notice that asks a process to read its bots again."""
    return {CONTROL_KEY: RELOAD}


def is_control(payload: object) -> bool:
    """Whether a decoded payload is a notice rather than a queued call.

    By the key alone, and before anything reads it as an envelope: the two shapes share a
    queue, and a notice put through `unpack` would be a message nobody can deliver.
    """
    return isinstance(payload, dict) and CONTROL_KEY in payload


def apply_control(payload: object) -> None:
    """Act on a notice, which today means asking the supervisor for a pass.

    Nothing raises out of here: this runs on the consumer thread, where an exception ends
    delivery for the life of the container, and a notice is the least important thing that
    thread carries.
    """
    said = payload.get(CONTROL_KEY) if isinstance(payload, dict) else None
    if said != RELOAD:
        logger.warning('ignoring a control message this version does not know', extra={'tg_control': said})
        return
    # deferred: the supervisor imports the providers, which reach the ORM
    from django_aiogram.runtime.supervisor import serving  # noqa: PLC0415 - as above

    supervisor = serving()
    if supervisor is None:
        # a process that serves no bots -- a pure sender, or one still starting -- has nothing
        # to reconcile, and the notice is not an error there
        return
    try:
        supervisor.reconcile()
    except Exception:
        logger.exception('a reconciliation asked for by a control message failed')


def announce(using: str = DEFAULT_DB_ALIAS) -> None:
    """Publish the notice, on the caller's commit, and let nothing about it reach the caller.

    ``using`` is the database the change was written on, which Django hands to a model signal:
    waiting on the wrong connection would run the publish immediately, and a rollback would
    then have announced a bot that does not exist.

    A save that cannot be announced is still a save: the poll picks the change up within
    ``BOT_REFRESH_INTERVAL``, so a broker that is down costs seconds rather than the write. It
    is logged and swallowed for that reason -- an admin page that refuses to save because a
    queue is unreachable would be the wrong trade.
    """

    def publish() -> None:
        """Put one notice on the queue this process publishes to."""
        try:
            # both deferred, and the serializer is the one that matters: it encodes aiogram
            # models, so importing it costs aiogram -- and this module is imported by
            # `apps.ready` in every enabled process, including the ones that never send.
            # `tests/test_lazy_init.py` is what would notice
            from django_aiogram.broker.registry import get_broker  # noqa: PLC0415 - as above
            from django_aiogram.wire.serializers import get_serializer  # noqa: PLC0415 - as above

            get_broker().publish([get_serializer().dumps(pack_reload())])
        except Exception:
            logger.exception('could not announce a bot change; the next poll will pick it up')

    after_commit(publish, using=using)


#: the rows a bot is configured in, by name. Connected per model rather than for every save in
#: the project, and that is not tidiness: Django checks for `post_delete` receivers before it
#: takes its fast-delete path, so a receiver registered without a sender makes **every**
#: queryset delete in the project fetch its rows first. Measured -- it turned the event log's
#: prune, whose whole design is bounded ranges, into `DELETE ... WHERE id IN (...)`, and
#: `tests/db/test_prune.py` is what said so
CONFIGURED_IN = ('TelegramBot', 'TelegramBotProfile', 'TelegramQueue')


def connect() -> None:
    """Listen for a change to the rows a bot is configured in.

    Called from ``AppConfig.ready``, where the models can be looked up by name -- importing
    them at module scope would be a cycle, and this module is imported by processes that never
    touch a database.
    """
    # deferred: the registry is populated by the time `ready` runs, and not before
    from django.apps import apps  # noqa: PLC0415 - as above

    for name in CONFIGURED_IN:
        model = apps.get_model('django_aiogram', name)
        post_save.connect(_announce_a_configuration_change, sender=model, dispatch_uid=f'{_UID}.saved.{name}')
        post_delete.connect(_announce_a_configuration_change, sender=model, dispatch_uid=f'{_UID}.deleted.{name}')


def _announce_a_configuration_change(using: str = DEFAULT_DB_ALIAS, **kwargs: 'Any') -> None:
    """Announce that one of those rows changed, whichever of them it was and wherever it went."""
    announce(using=using)
