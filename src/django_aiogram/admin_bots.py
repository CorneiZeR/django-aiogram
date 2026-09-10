"""The admin a SaaS deployment configures its bots from.

For a deployment whose clients bring their own bot, this page *is* the interface: bots are
added here, tokens rotated here, a noisy client throttled here, and a client who left switched
off here. So everything the package can be told has to be reachable from it -- which is why
the profile is a row rather than a dict in ``settings.py``.

**Fieldsets are permission boundaries, not decoration.** Support staff need the limits and the
switch; they do not need the credential or the transport. A section a user may not see is not
rendered at all, so nothing is masked in a page that also carries the real value somewhere.

**The token is write-only.** An empty field means *keep the one that is there*, so an operator
can rotate a credential without ever being able to read the old one. What is rendered instead
is a mask, and only for a user holding ``view_telegrambot_token`` -- the same split the event
feed already makes about payloads: being allowed to switch a bot off is a different question
from being allowed to read what sends as it.

**And the request never talks to Telegram.** Saving a row moves its watermark and publishes a
notice; a supervisor is what acts on it, seconds later. That is what makes an action over five
hundred rows survivable, and it is why nothing here calls ``getMe``.

Nothing registers itself: ``admin.autodiscover`` imports this while the app registry is still
loading, and :meth:`~django_aiogram.apps.TelegramBotAppConfig.ready` is where settings are
safe to read.
"""

from typing import TYPE_CHECKING, Any

from django import forms
from django.contrib import admin, messages
from django.core.exceptions import ValidationError
from django.db.models import Count
from django.urls import reverse
from django.utils import timezone
from django.utils.html import format_html

from django_aiogram.admin_overrides import (
    OVERRIDABLE,
    field_name,
    fields_for,
    group_fields,
    inherited_note,
    read_overrides,
    switch_name,
    validate_one,
)
from django_aiogram.config.bots import parse_bot_id
from django_aiogram.models import TelegramBot, TelegramBotProfile, TelegramQueue
from django_aiogram.tokens import store_token

if TYPE_CHECKING:
    from django.db.models import QuerySet
    from django.http import HttpRequest

    BotAdminBase = admin.ModelAdmin[TelegramBot]
    ProfileAdminBase = admin.ModelAdmin[TelegramBotProfile]
    QueueAdminBase = admin.ModelAdmin[TelegramQueue]
    BotFormBase = forms.ModelForm[TelegramBot]
else:
    BotAdminBase = admin.ModelAdmin
    ProfileAdminBase = admin.ModelAdmin
    QueueAdminBase = admin.ModelAdmin
    BotFormBase = forms.ModelForm

#: what the mask shows in place of a token: the identity, which is not a secret -- it is in
#: every queued message and every log line -- and nothing of the half that is
MASK = '{bot_id}:{hidden}'
HIDDEN = '•' * 8

#: the permission the credentials section is behind
BOT_CHANGELIST = 'admin:django_aiogram_telegrambot_changelist'

TOKEN_PERMISSION = 'django_aiogram.view_telegrambot_token'  # noqa: S105 - a permission name, not a secret


def may_see_tokens(request: 'HttpRequest') -> bool:
    """Whether this user may see anything at all about a bot's credential.

    Split from change access the way the feed splits payloads from rows: support staff switch
    a bot off and change its limits, and neither needs the token.
    """
    checker = getattr(request.user, 'has_perm', None)
    return bool(checker and checker(TOKEN_PERMISSION))


class TelegramBotForm(BotFormBase):
    """The bot's form, with the token write-only and the identity read off it."""

    #: not the model field: a `TextInput` bound to it would render the credential into the
    #: page. Empty means keep, which is what lets a rotation happen without a read
    token = forms.CharField(
        required=False,
        widget=forms.PasswordInput(render_value=False),
        label='New token',
        help_text='Leave empty to keep the token this bot already has.',
    )

    class Meta:
        """Every editable column; the fieldsets decide which of them a user is shown.

        ``overrides`` is not among them: the column is built from the checkbox-and-value pairs
        this form grows instead, because a raw JSON box makes the keys undiscoverable and a
        typo silent.
        """

        model = TelegramBot
        fields = ('label', 'token', 'profile', 'queue', 'enabled')

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Grow one checkbox and one value field per overridable setting, and fill them in.

        The inherited value is resolved for *this* bot -- through its profile, if it has one --
        so what a person reads next to an unchecked box is what leaving it unchecked means.
        """
        super().__init__(*args, **kwargs)
        bot = self.instance if self.instance.pk else None
        profile = bot.profile if bot is not None else None
        self.inherited, origins = read_overrides(bot, profile)
        # the pairs are on the class, so Django's form factory knows about them; what belongs
        # to *this* bot is the inherited value each line reports
        for key in OVERRIDABLE:
            self.fields[switch_name(key)].help_text = inherited_note(key, self.inherited, origins)
        held = dict(bot.overrides) if bot is not None else {}
        for key in OVERRIDABLE:
            if key not in held:
                continue
            self.initial[switch_name(key)] = True
            value = held[key]
            self.initial[field_name(key)] = str(value).lower() if isinstance(value, bool) else value

    def clean_token(self) -> str:
        """Refuse a string Telegram would not have issued, and keep an empty one empty.

        Refused here rather than at the first send: a token with no identity in it is a bot
        nothing can address -- no queued message could name it -- and the row would sit in the
        table looking configured.
        """
        given = str(self.cleaned_data.get('token') or '').strip()
        if not given:
            return ''
        if parse_bot_id(given) is None:
            msg = 'This is not a bot token: it has no identity in front of the colon.'
            raise ValidationError(msg)
        return given

    def clean(self) -> 'dict[str, Any] | None':
        """Require a token on a new bot, and judge every setting this bot decides."""
        cleaned = super().clean() or {}
        given = str(cleaned.get('token') or '').strip()
        if not given and not self.instance.pk:
            self.add_error('token', 'A new bot needs a token: its identity is the number inside one.')
        self.overrides = self._decided(cleaned)
        return cleaned

    def _decided(self, cleaned: 'dict[str, Any]') -> 'dict[str, Any]':
        """Collect the settings whose box is checked, refusing what the package would refuse.

        Judged by the check registry rather than by a second copy of the rules here: a page
        that accepted a value the deployment then refuses to start with is worse than one that
        refuses it, because the refusal arrives at the next restart and in another person's
        terminal.
        """
        decided: dict[str, Any] = {}
        for key in OVERRIDABLE:
            if not cleaned.get(switch_name(key)):
                continue
            value = cleaned.get(field_name(key))
            try:
                validate_one(key, value, self.inherited)
            except ValidationError as refused:
                self.add_error(field_name(key), refused)
                continue
            decided[key] = value
        return decided

    def save(self, commit: bool = True) -> TelegramBot:  # noqa: FBT001, FBT002 - Django's signature
        """Store a new token through the seam, and leave the old one alone when none was given.

        The identity is written from the token rather than typed: it is the one thing about a
        bot that holds still, everything else joins on it, and a person copying it by hand is
        a mis-keyed digit away from a row that serves somebody else's bot.
        """
        bot = super().save(commit=False)
        # the pairs, not a JSON box: `clean` has already judged each of them
        bot.overrides = getattr(self, 'overrides', bot.overrides)
        given = str(self.cleaned_data.get('token') or '').strip()
        identity = parse_bot_id(given)
        if identity is not None:
            # both from the same string: `clean_token` has already refused one with no
            # identity in it, so the two move together or not at all
            bot.bot_id = identity
            # through the seam, so a deployment with an encrypting `TOKEN_STORAGE` has no
            # plaintext path through the admin either
            bot.token = store_token(given)
        elif self.instance.pk:
            # `fields` names the token, so the unbound value would otherwise be written back
            # as an empty column -- which is the credential deleted by leaving a box alone
            bot.token = TelegramBot.objects.values_list('token', flat=True).get(pk=self.instance.pk)
        if commit:
            bot.save()
        return bot


#: the checkbox-and-value pairs, declared on the class rather than grown per instance:
#: Django's model-form factory validates a `fieldsets` naming them against the form's fields,
#: and a field that only appears in `__init__` is one the admin refuses to render
_PAIRS = fields_for({}, {})
TelegramBotForm.base_fields.update(_PAIRS)
# `declared_fields` as well, and that is the load-bearing half: the admin builds its form with
# `modelform_factory`, which *subclasses* this one, and a subclass inherits the fields a class
# declared rather than whatever was added to its `base_fields` afterwards
TelegramBotForm.declared_fields.update(_PAIRS)


class TelegramBotProfileAdmin(ProfileAdminBase):
    """The settings a group of bots shares, as a row."""

    list_display = ('name', 'bots', 'updated_at')
    search_fields = ('name',)
    ordering = ('name',)

    def get_queryset(self, request: 'HttpRequest') -> 'QuerySet[TelegramBotProfile]':
        """Count the bots in one query rather than one per row."""
        return super().get_queryset(request).annotate(_bots=Count('bots'))

    @admin.display(description='bots', ordering='_bots')
    def bots(self, obj: TelegramBotProfile) -> str:
        """How many bots take their settings from this profile, as a link to them.

        A count is the answer to "is this profile in use"; the next question is always *which
        bots*, and a number nobody can click leaves that to a search somebody has to compose.
        """
        return _bots_link('profile__id__exact', obj.pk, getattr(obj, '_bots', 0))


class TelegramQueueAdmin(QueueAdminBase):
    """The queues this deployment has declared, and the pools that serve them."""

    list_display = ('name', 'pool', 'bots', 'created_at')
    list_filter = ('pool',)
    search_fields = ('name', 'pool')
    ordering = ('pool', 'name')

    def get_queryset(self, request: 'HttpRequest') -> 'QuerySet[TelegramQueue]':
        """Count the bots in one query rather than one per row."""
        return super().get_queryset(request).annotate(_bots=Count('bots'))

    @admin.display(description='bots', ordering='_bots')
    def bots(self, obj: TelegramQueue) -> str:
        """How many bots publish to this queue, as a link to them."""
        return _bots_link('queue__id__exact', obj.pk, getattr(obj, '_bots', 0))


class TelegramBotAdmin(BotAdminBase):
    """One client's bot: what it is, what sends as it, and whether it is being served."""

    form = TelegramBotForm
    list_display = ('label', 'bot_id', 'profile', 'queue', 'pool', 'own_settings', 'enabled', 'serving')
    list_filter = ('enabled', 'profile', 'queue', 'queue__pool')
    search_fields = ('label', 'bot_id')
    actions = ('switch_on', 'switch_off')
    # the two foreign keys the list renders: without this the changelist asks for each of them
    # per row, which is the N+1 a page over five hundred clients cannot afford
    list_select_related = ('profile', 'queue')
    readonly_fields = ('bot_id', 'token_mask', 'quarantine_reason', 'quarantined_until', 'created_at', 'updated_at')
    ordering = ('label', 'bot_id')

    #: the sections, in the order they are read. `Credentials` is the one a user without
    #: `view_telegrambot_token` never sees -- not a masked copy of it, none of it
    FIELDSETS = (
        ('Bot', {'fields': ('label', 'bot_id')}),
        (
            'Credentials',
            {
                'fields': ('token_mask', 'token'),
                'description': 'The token is never rendered. An empty field keeps the one this bot has.',
            },
        ),
        ('Placement', {'fields': ('profile', 'queue')}),
        *(
            (
                title,
                {
                    'fields': named,
                    'classes': ('collapse',),
                    'description': (
                        'Tick a setting to have this bot decide it. Left alone, it comes from '
                        "the profile, and then from the deployment's defaults — which is what "
                        'each line says it would inherit.'
                    ),
                },
            )
            for title, named in group_fields()
        ),
        ('State', {'fields': ('enabled', 'quarantine_reason', 'quarantined_until', 'created_at', 'updated_at')}),
    )

    def get_fieldsets(
        self,
        request: 'HttpRequest',
        obj: TelegramBot | None = None,  # noqa: ARG002 - as above; the sections do not depend on the row
    ) -> Any:  # noqa: ANN401 - Django's own signature
        """Drop the credentials section for a user who may not see the token.

        Dropped rather than masked: a page that renders a section is a page that posts it
        back, and leaving the token field there would let a user who cannot see the credential
        replace it -- which is the same thing as taking the bot.
        """
        if may_see_tokens(request):
            return self.FIELDSETS
        return tuple(section for section in self.FIELDSETS if section[0] != 'Credentials')

    def get_form(
        self,
        request: 'HttpRequest',
        obj: TelegramBot | None = None,
        change: bool = False,  # noqa: FBT001, FBT002 - as above
        **kwargs: Any,
    ) -> Any:  # noqa: ANN401 - the factory's return type is Django's own
        """Build the form from its own declaration rather than from the sections.

        Django hands `modelform_factory` the fields it found in `fieldsets`, and every
        checkbox-and-value pair is a form field with no column behind it -- so the factory
        refuses them as unknown. The form already says which columns it edits, and the pairs
        are on the class beside them.
        """
        kwargs['fields'] = None
        built = super().get_form(request, obj, change=change, **kwargs)
        if may_see_tokens(request):
            return built
        # the section is not *rendered* for this user, and that is not enough: `fields=None`
        # above hands the factory the form's own declaration, which still declares `token`, so
        # a POST carrying one would be accepted and stored -- the bot given away by somebody
        # who may not read its credential. Dropped from the class the factory built, which is
        # this request's own subclass rather than the form every request shares
        return _without_the_token_field(built)

    def has_add_permission(self, request: 'HttpRequest') -> bool:
        """Adding a bot means setting its token, so it needs the permission for one.

        Otherwise the add form is one a user cannot fill in: a new bot has no token to keep,
        and the identity everything else joins on is read out of the one they cannot give.
        """
        return bool(super().has_add_permission(request)) and may_see_tokens(request)

    @admin.display(description='pool', ordering='queue__pool')
    def pool(self, obj: TelegramBot) -> str:
        """Which pool of containers serves this bot's queue.

        The queue name is a client's and changes with them; the pool is the deployment axis a
        container is started with, and it is what says *where* this bot's work is done.
        """
        return obj.queue.pool if obj.queue is not None else '—'

    @admin.display(description='own settings')
    def own_settings(self, obj: TelegramBot) -> str:
        """Which settings this bot decides for itself, so a surprise is visible from the list.

        A bot behaving unlike its neighbours is nearly always one carrying an override
        somebody set months ago, and finding that out currently means opening every row.
        """
        keys = sorted(str(key) for key in (obj.overrides or {}))
        return ', '.join(keys) if keys else '—'

    @admin.display(description='serving', boolean=True)
    def serving(self, obj: TelegramBot) -> bool:
        """Whether anything is serving this bot right now, by its own two answers.

        Read from the row rather than from Telegram: the supervisor writes what happened here,
        and a page that asked the API would be five hundred round trips per load.
        """
        return bool(obj.enabled and not obj.quarantine_reason)

    @admin.action(description='Switch on the selected bots')
    def switch_on(self, request: 'HttpRequest', queryset: 'QuerySet[TelegramBot]') -> None:
        """Let a supervisor serve these bots again, in one statement."""
        self._switch(request, queryset, on=True)

    @admin.action(description='Switch off the selected bots')
    def switch_off(self, request: 'HttpRequest', queryset: 'QuerySet[TelegramBot]') -> None:
        """Stop serving these bots without deleting anything they hold."""
        self._switch(request, queryset, on=False)

    def _switch(self, request: 'HttpRequest', queryset: 'QuerySet[TelegramBot]', *, on: bool) -> None:
        """Move the switch on every selected row, and move their watermark with it.

        `update()` rather than a save per row: an action over five hundred clients is one
        statement, and none of this talks to Telegram. `updated_at` is passed by hand because
        `update()` moves no `auto_now` column, and that column is what every supervisor polls
        to notice the change at all -- without it the switch would take effect at the next
        restart.
        """
        moved = queryset.update(enabled=on, updated_at=timezone.now())
        self.message_user(
            request,
            f'{moved} bot(s) switched {"on" if on else "off"}. A supervisor picks this up within a poll.',
            messages.SUCCESS,
        )

    @admin.display(description='token')
    def token_mask(self, obj: TelegramBot) -> str:
        """Show that a token is there, and its identity, without showing the token."""
        if not obj.token:
            return 'not set'
        return MASK.format(bot_id=obj.bot_id, hidden=HIDDEN)


def _without_the_token_field(form: 'type[BotFormBase]') -> 'type[BotFormBase]':
    """Return the same form with no ``token`` field, for a user who may not set one.

    Taken away in ``__init__`` rather than from ``base_fields``: a form's metaclass rebuilds
    that mapping from what the class *declared*, so a subclass handing it a filtered copy gets
    the unfiltered one back -- measured, and the reason this is done to the bound fields.
    Without it the credentials section is merely not rendered, and a POST carrying a token is
    still accepted: the bot given away by somebody who may not read its credential.

    Built with ``type()`` rather than a ``class`` statement because the base is decided at run
    time -- it is whatever `modelform_factory` produced for this request.
    """

    def drop_it(self: 'BotFormBase', *args: Any, **kwargs: Any) -> None:
        """Build the form, then take the field away before anything can bind to it."""
        form.__init__(self, *args, **kwargs)
        self.fields.pop('token', None)

    return type(form.__name__, (form,), {'__init__': drop_it, '__doc__': form.__doc__})


def _bots_link(lookup: str, value: int, count: int) -> str:
    """Render a count as a link into the bot changelist, filtered to what it counted."""
    if not count:
        return '0'
    return format_html('<a href="{}?{}={}">{}</a>', reverse(BOT_CHANGELIST), lookup, value, count)


def register_bot_admin(site: admin.AdminSite | None = None) -> None:
    """Register the three models. Called from ``ready``, like the feed's own admin."""
    target = site or admin.site
    if not target.is_registered(TelegramBotProfile):
        target.register(TelegramBotProfile, TelegramBotProfileAdmin)
    if not target.is_registered(TelegramQueue):
        target.register(TelegramQueue, TelegramQueueAdmin)
    if not target.is_registered(TelegramBot):
        target.register(TelegramBot, TelegramBotAdmin)
