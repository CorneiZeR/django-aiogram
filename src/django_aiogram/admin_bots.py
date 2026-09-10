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
from django.contrib import admin
from django.core.exceptions import ValidationError
from django.db.models import Count

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
        """Every editable column; the fieldsets decide which of them a user is shown."""

        model = TelegramBot
        fields = ('label', 'token', 'profile', 'queue', 'overrides', 'enabled')

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
        """Require a token on a new bot, and take the identity from whichever one applies."""
        cleaned = super().clean()
        given = str((cleaned or {}).get('token') or '').strip()
        if not given and not self.instance.pk:
            self.add_error('token', 'A new bot needs a token: its identity is the number inside one.')
        return cleaned

    def save(self, commit: bool = True) -> TelegramBot:  # noqa: FBT001, FBT002 - Django's signature
        """Store a new token through the seam, and leave the old one alone when none was given.

        The identity is written from the token rather than typed: it is the one thing about a
        bot that holds still, everything else joins on it, and a person copying it by hand is
        a mis-keyed digit away from a row that serves somebody else's bot.
        """
        bot = super().save(commit=False)
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


class TelegramBotProfileAdmin(ProfileAdminBase):
    """The settings a group of bots shares, as a row."""

    list_display = ('name', 'bots', 'updated_at')
    search_fields = ('name',)
    ordering = ('name',)

    def get_queryset(self, request: 'HttpRequest') -> 'QuerySet[TelegramBotProfile]':
        """Count the bots in one query rather than one per row."""
        return super().get_queryset(request).annotate(_bots=Count('bots'))

    @admin.display(description='bots', ordering='_bots')
    def bots(self, obj: TelegramBotProfile) -> int:
        """How many bots take their settings from this profile."""
        return getattr(obj, '_bots', 0)


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
    def bots(self, obj: TelegramQueue) -> int:
        """How many bots publish to this queue."""
        return getattr(obj, '_bots', 0)


class TelegramBotAdmin(BotAdminBase):
    """One client's bot: what it is, what sends as it, and whether it is being served."""

    form = TelegramBotForm
    list_display = ('label', 'bot_id', 'profile', 'queue', 'enabled', 'quarantine_reason')
    list_filter = ('enabled', 'profile', 'queue')
    search_fields = ('label', 'bot_id')
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
        (
            'Delivery and limits',
            {
                'fields': ('overrides',),
                'description': (
                    'Only what this bot decides. Anything absent comes from its profile, and '
                    "then from the deployment's defaults."
                ),
            },
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

    @admin.display(description='token')
    def token_mask(self, obj: TelegramBot) -> str:
        """Show that a token is there, and its identity, without showing the token."""
        if not obj.token:
            return 'not set'
        return MASK.format(bot_id=obj.bot_id, hidden=HIDDEN)


def register_bot_admin(site: admin.AdminSite | None = None) -> None:
    """Register the three models. Called from ``ready``, like the feed's own admin."""
    target = site or admin.site
    if not target.is_registered(TelegramBotProfile):
        target.register(TelegramBotProfile, TelegramBotProfileAdmin)
    if not target.is_registered(TelegramQueue):
        target.register(TelegramQueue, TelegramQueueAdmin)
    if not target.is_registered(TelegramBot):
        target.register(TelegramBot, TelegramBotAdmin)
