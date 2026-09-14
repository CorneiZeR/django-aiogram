"""The URLconf the webhook server runs on, and nothing else.

A module rather than a list built in place, because `ROOT_URLCONF` names one and Django
imports it.

``urlpatterns`` is resolved on **attribute access** (PEP 562) rather than at import, for the
reason everything in this package is: importing a module must not read the settings. The
suite imports every module there is, and a deployment that never serves the webhook has no
`WEBHOOK_URL` to be refused over.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence


def __getattr__(name: str) -> Any:  # noqa: ANN401 - the module attribute Django reads
    """Answer ``urlpatterns`` by building the routes, and nothing else."""
    if name == 'urlpatterns':
        from django_aiogram.consumer.serving_http import urlpatterns as routes  # noqa: PLC0415 - see above

        return routes()
    msg = f'module {__name__!r} has no attribute {name!r}'
    raise AttributeError(msg)


if TYPE_CHECKING:
    urlpatterns: 'Sequence[Any]'
