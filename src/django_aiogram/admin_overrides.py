"""The form that makes a bot's sparse overrides something a person can edit.

``overrides`` is a JSON column and a raw JSON box is the wrong way to edit one: the keys are
not discoverable, a typo is silent, and the interesting question -- *what will this bot
actually use?* -- is answered nowhere on the page. So each setting is rendered as a pair: a
checkbox that says whether this bot decides it, and a field for the value, with the value it
would inherit written next to it and the layer that would decide it named.

**The checkbox is the model.** ``overrides`` is sparse *by key presence* -- ``RATE_LIMIT:
None`` is a value, it switches pacing off -- so "inherit" cannot be spelled as an empty field
or a null. It has to be its own answer, and this is what makes it one a person can give.

**One law about what a valid value is.** The keys are read from
:data:`~django_aiogram.config.defaults.DEFAULTS` and validated by the *check registry* --
`E012` is what refuses a `MAX_RETRIES` of zero, here as much as at boot. A second, slightly
different copy in a `clean_` method is how the two drift until a page accepts a configuration
the deployment then refuses to start with.
"""

from typing import TYPE_CHECKING, Any

from django import forms
from django.core.exceptions import ValidationError

from django_aiogram.config.bots import resolve
from django_aiogram.config.defaults import DEFAULTS, PROCESS_SCOPED

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from django_aiogram.models import TelegramBot, TelegramBotProfile

__all__ = (
    'GROUPS',
    'OVERRIDABLE',
    'field_name',
    'fields_for',
    'group_fields',
    'inherited_note',
    'read_overrides',
    'switch_name',
)

#: settings a row must not carry, with the reason each is left out. Kept as a mapping rather
#: than a set so the page and this module cannot disagree about *why* one is missing
EXCLUDED: 'Mapping[str, str]' = {
    # write-only and stored through `TOKEN_STORAGE`; the credentials section owns it
    'TOKEN': 'the credentials section',
    # the row's own foreign key decides it, so two boxes would disagree about one answer
    'QUEUE': "the bot's queue",
    # a callable, and nothing typed into a form is one
    'DEFAULT_KWARGS': 'a callable, which a form cannot hold',
    # the row's `enabled` column is the switch a person reaches for
    'ENABLED': "the bot's own switch",
}

#: every setting a bot's row may decide: what the package reads, less what the process owns
#: and less the four above
OVERRIDABLE: tuple[str, ...] = tuple(key for key in DEFAULTS if key not in PROCESS_SCOPED and key not in EXCLUDED)

#: the sections the fields are shown in, in the order a person reads them. Grouped because a
#: flat list of twenty-two settings is a page nobody finds anything on, and named after the
#: question each group answers rather than after the module the settings live in
GROUPS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        'Limits',
        ('RATE_LIMIT', 'MAX_RETRIES', 'RAISE_EXCEPTION', 'MAX_IN_FLIGHT', 'MAX_IN_FLIGHT_PER_BOT'),
    ),
    (
        'Updates',
        ('MODE', 'WEBHOOK_URL', 'WEBHOOK_SECRET', 'WEBHOOK_ALLOWED_UPDATES'),
    ),
    (
        'Delivery',
        (
            'BROKER',
            'DELIVERY',
            'SERIALIZER',
            'ALLOW_PICKLE',
            'TRANSACTIONAL',
            'REQUIRE_CRASH_SAFE',
            'BLPOP_TIMEOUT',
            'DRAIN_TIMEOUT',
            'REDIS_URL',
            'REDIS_TIMEOUT',
        ),
    ),
    (
        'Bot behaviour',
        ('DEFAULT_BOT_PROPERTIES', 'HEARTBEAT_INTERVAL', 'HEALTHCHECK_MAX_QUEUE'),
    ),
)


def switch_name(key: str) -> str:
    """Name the checkbox that says whether this bot decides ``key``."""
    return f'set_{key}'


def field_name(key: str) -> str:
    """Name the field the value for ``key`` is typed into."""
    return f'val_{key}'


def group_fields() -> 'Iterator[tuple[str, tuple[str, ...]]]':
    """Every section with the form field names in it, for an admin's ``fieldsets``."""
    for title, keys in GROUPS:
        named = tuple(name for key in keys if key in OVERRIDABLE for name in (switch_name(key), field_name(key)))
        if named:
            yield title, named


def _widget_for(key: str) -> forms.Field:
    """Build the field one setting's value is typed into, from the shape of its default.

    From the default rather than from a table written by hand: a setting added to the package
    appears on this page with the right kind of box, and one whose type changes cannot leave a
    number field in front of a mapping.
    """
    shipped = DEFAULTS[key]
    if isinstance(shipped, bool):
        # a real choice rather than a checkbox: a checkbox cannot say `False`, and `False` is
        # exactly what somebody overriding a defaulted-on setting means
        return forms.TypedChoiceField(
            required=False,
            choices=(('true', 'Yes'), ('false', 'No')),
            coerce=lambda given: given == 'true',
        )
    if isinstance(shipped, int):
        return forms.IntegerField(required=False)
    if isinstance(shipped, float):
        return forms.FloatField(required=False)
    if isinstance(shipped, str):
        return forms.CharField(required=False)
    # a mapping or a collection: JSON, because that is what the column holds and what the
    # settings dict would have held. `None` is legitimate for `RATE_LIMIT`, so null is allowed
    return forms.JSONField(required=False)


def inherited_note(key: str, inherited: 'Mapping[str, Any]', origins: 'Mapping[str, str]') -> str:
    """Say what leaving this setting alone would mean, and which layer decides it.

    On the page because "inherit" is the answer most of these boxes should keep, and a person
    can only choose it when they can see what it leaves in place.
    """
    value = inherited.get(key, DEFAULTS[key])
    # the origin as a place rather than as a lookup: `resolve` labels a value
    # `<layer>['<KEY>']` for a finding that has to be greppable, and the key is already the
    # label of the line this sits under
    where = origins.get(key, 'the deployment defaults').removesuffix(f"['{key}']")
    return f'Inherited: {value!r} — from {where}.'


def fields_for(inherited: 'Mapping[str, Any]', origins: 'Mapping[str, str]') -> 'dict[str, forms.Field]':
    """Build the checkbox and the value field for every overridable setting.

    ``inherited`` is what this bot would use with nothing of its own, and ``origins`` says
    which layer decided each of those -- both go into the help text, because "leave it alone"
    is a decision somebody can only make when they can see what it leaves.
    """
    built: dict[str, forms.Field] = {}
    for key in OVERRIDABLE:
        built[switch_name(key)] = forms.BooleanField(
            required=False,
            label=key,
            help_text=inherited_note(key, inherited, origins),
        )
        field = _widget_for(key)
        field.label = 'value'
        built[field_name(key)] = field
    return built


def read_overrides(
    bot: 'TelegramBot | None',
    profile: 'TelegramBotProfile | None',
) -> tuple[dict[str, Any], dict[str, str]]:
    """Return what this bot would inherit, and where each of those values comes from.

    Resolved through the same function the runtime uses, with this bot's own layer left out:
    what a person needs to see is the value the checkbox would leave in place.
    """
    alias = str(bot.bot_id) if bot is not None and bot.bot_id else 'default'
    layers = []
    if profile is not None:
        layers.append((f'the profile {profile.name!r}', profile.overrides))
    record = resolve(alias, *layers, provided=True)
    return dict(record.resolved), dict(record.origins)


def validate_one(key: str, value: Any, inherited: 'Mapping[str, Any]') -> None:  # noqa: ANN401 - a setting holds anything
    """Run the package's own checks about ``key`` against ``value``, and raise what they found.

    The registry rather than a second copy of the rules: one law about what a valid value is,
    and a page that accepts what the deployment would refuse to start with is the drift this
    avoids.
    """
    # deferred: the checks reach the transports and the ORM, and this module is imported by
    # `admin_bots`, which `admin.autodiscover` imports while the registry is still loading
    from django_aiogram.config.checks import CHECKS  # noqa: PLC0415 - as above

    record = resolve('admin', ('this form', {**inherited, key: value}), provided=True)
    found = [
        problem.message
        for check in CHECKS
        if check.key == key and not check.per_process
        for problem in check.validate(key, record)
    ]
    if found:
        raise ValidationError([f'{key} {message}' for message in found])
