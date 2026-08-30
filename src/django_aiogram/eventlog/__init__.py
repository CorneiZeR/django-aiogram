"""The optional table, and the metrics seam that is not the table.

`recorder` is the queue and the one thread that drains it. Four modules sit under it, each
holding a part that is not about that thread: `records` is what crosses the queue, `pacing`
the numbers it runs by, `bookkeeping` the two counts several threads touch, and
`publishing` the fan-out to receivers. Around them, `writer` puts a batch into the
database; `events` names an event and the worker that produced it; `instrumentation`
describes an update without keeping its contents; `signals` is the seam a project connects
a metrics exporter to; `moving` knows what 3.x's table was; `dbrouter` sends this app's
tables to their own alias.

The model itself is `django_aiogram.models`, at the app root, because Django looks for it
there and moving it would cost an `app_label` on the model and a `MIGRATION_MODULES` in
every project.
"""

#: deliberately empty: callers import from the modules in this package, not from the
#: package itself. A re-export here would make a second path to every name, and the one
#: nobody chose is the one that cannot be moved later
__all__: tuple[str, ...] = ()
