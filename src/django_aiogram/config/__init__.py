"""What a project configures, and what refuses a bad value.

`settings` reads the `TELEGRAM_BOT` dict and the `DJANGO_AIOGRAM_` environment twins;
`defaults` is the only place a default lives; `enums` holds the values a setting accepts;
`checks` judges the result and is the only module here that reports to Django.

Nothing in this package may import from `producer`, `consumer` or `broker`: configuration
is read by them and reads nothing of them, which is what keeps `manage.py check` from
paying for aiogram.
"""

#: deliberately empty: callers import from the modules in this package, not from the
#: package itself. A re-export here would make a second path to every name, and the one
#: nobody chose is the one that cannot be moved later
__all__: tuple[str, ...] = ()
