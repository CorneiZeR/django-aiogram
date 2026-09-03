"""Send the event log to its own database, when one is configured.

Optional. The writer and the admin always name the alias explicitly, so the log
lands in the right database with no router installed at all. What the router
adds is ``migrate`` creating the table there, and third-party code reaching the
right alias through the plain manager.

Not ``routers.py``: that name already means aiogram router autodiscovery.
"""

from typing import Any

from django.db.models import Model

from django_aiogram.config.settings import conf

APP_LABEL = 'django_aiogram'
#: the model this router is about, by name rather than by import: this module is read while
#: the app registry is still loading, and importing `models` from here would be a cycle.
#:
#: **The app's other table is deliberately not in it.** `TelegramScheduledSend` is
#: operational state a producer writes and a mover consumes -- it belongs with the caller's
#: own writes, on the default connection, which is also what lets a scheduled send roll back
#: with the transaction that made it. Routed by app label, as this was until 4.1, a log
#: database would have taken the schedule with it and `allow_migrate` would have created the
#: table *only* there, so nothing on `default` would have had one at all
LOG_MODELS = frozenset({'telegramevent'})


def event_log_database() -> str | None:
    """Return the configured alias for the log, or None when it lives in the default one."""
    return str(conf['EVENT_LOG_DATABASE'] or '').strip() or None


class TelegramEventLogRouter:
    """Routes the event log to ``EVENT_LOG_DATABASE`` and nothing else anywhere."""

    def _alias_for(self, model: type[Model]) -> str | None:
        """Return the alias this model belongs to, or None to express no opinion."""
        alias = event_log_database()
        # _meta is how Django itself asks a model what it is
        meta = model._meta  # noqa: SLF001
        if not alias or meta.app_label != APP_LABEL or meta.model_name not in LOG_MODELS:
            return None
        return alias

    def db_for_read(self, model: type[Model], **hints: Any) -> str | None:
        """Read this app's models from the log database."""
        return self._alias_for(model)

    def db_for_write(self, model: type[Model], **hints: Any) -> str | None:
        """Write this app's models to the log database."""
        return self._alias_for(model)

    def allow_relation(self, *objects: Model, **hints: Any) -> bool | None:
        """Express no opinion: this app owns no relations in either direction."""
        return None

    def allow_migrate(self, db: str, app_label: str, **hints: Any) -> bool | None:
        """Create the log's table on the log database only, and only when one is set.

        None rather than False for other apps: where somebody else's table belongs is not
        this router's decision to make.

        This app's *other* table is refused on the log alias and left alone everywhere else.
        Django passes the model as a migration hint, so the two are separable -- and `None`
        for the schedule was not enough: no opinion means Django creates it, so with a log
        database configured the table appeared on **both** aliases, measured. The copy on the
        log alias is never read or written, which is exactly why it should not exist.
        """
        alias = event_log_database()
        if alias is None or app_label != APP_LABEL:
            return None
        model = hints.get('model')
        name = getattr(getattr(model, '_meta', None), 'model_name', None)
        if name is not None and name not in LOG_MODELS:
            # not the log's table: keep it off the log's database, and say nothing about
            # where else it goes -- that is the project's own routing to decide
            return False if db == alias else None
        return db == alias
