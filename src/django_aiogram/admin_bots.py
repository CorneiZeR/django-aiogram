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

import logging
from typing import TYPE_CHECKING, Any

from django import forms
from django.contrib import admin, messages
from django.core.exceptions import ImproperlyConfigured, ValidationError
from django.db.models import Count
from django.urls import reverse
from django.utils import timezone
from django.utils.html import format_html

from django_aiogram.admin_overrides import (
    OVERRIDABLE,
    SENSITIVE,
    field_name,
    fields_for,
    group_fields,
    inherited_note,
    problems_in,
    read_overrides,
    switch_name,
)
from django_aiogram.broker.exceptions import BrokerError
from django_aiogram.config.bots import parse_bot_id
from django_aiogram.models import TelegramBot, TelegramBotProfile, TelegramQueue
from django_aiogram.tokens import read_token, store_token

if TYPE_CHECKING:
    from django.db.models import QuerySet
    from django.http import HttpRequest, HttpResponse

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
logger = logging.getLogger('django_aiogram')

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
        # re-resolved against the profile *this submission* chose, not the one the row held: a
        # form that moved a bot to another profile would otherwise judge its settings against
        # the values it is leaving -- and a rule that reads a neighbouring setting, as `W004`
        # reads the broker timeout, would accept or refuse by the wrong numbers.
        #
        # The identity comes from the token being submitted where there is no row yet: a bot's
        # own environment variables are keyed by it, so resolving a new bot as `default` would
        # read another bot's variables
        self.inherited, _ = read_overrides(
            self.instance if self.instance.pk else None,
            cleaned.get('profile'),
            parse_bot_id(given),
        )
        self.overrides = self._decided(cleaned)
        return cleaned

    def clean_queue(self) -> Any:  # noqa: ANN401 - a model choice, whose type is Django's
        """Refuse moving a bot off a queue that still holds its messages.

        A queue is where this bot's backlog is: point the bot somewhere else and the messages
        already in the old one have nobody to take them -- a consumer serves the queues it was
        told about, and nothing points at that one any more. So the edit is refused with the
        thing to do first, which is to let it drain.

        **A queue that cannot be reached is not a queue with messages in it.** The depth read
        goes over the network, and an admin page that refuses every edit because Redis blinked
        would be worse than one that lets a rare mistake through -- the same trade the
        supervisor makes about a provider it could not read. A transport that cannot be *built*
        is refused instead: see `_waiting_in`.

        **And this is a guard rather than a guarantee.** The read and the save are two steps,
        so a message published between them lands in the queue this bot is leaving. Nothing
        here can close that without holding a lock across every send in the deployment, which
        is a cost this package will not put on `bot.send()` for an edit somebody makes twice a
        year. What it catches is the ordinary mistake -- moving a client with a visible backlog
        -- and the message names the order that has no race in it: switch the bot off, let the
        queue drain, then move it.
        """
        chosen = self.cleaned_data.get('queue')
        held = self.instance.queue if self.instance.pk else None
        if held is None or chosen == held:
            return chosen
        try:
            waiting = _waiting_in(held.name)
        except (ImproperlyConfigured, BrokerError) as refused:
            # the transport cannot be *built*, so nobody can say what that queue holds -- now
            # or after the edit. Refused rather than allowed: a misconfigured deployment would
            # otherwise find every dangerous edit permitted
            msg = (
                f'{held.name} cannot be read, so this edit cannot be judged: {refused}. '
                'Fix the transport settings, or switch the bot off and drain it deliberately.'
            )
            raise ValidationError(msg) from refused
        if waiting:
            msg = (
                f'{held.name} still holds {waiting} message(s). Moving this bot would strand them: '
                'let the queue drain first, or switch the bot off and drain it deliberately.'
            )
            raise ValidationError(msg)
        return chosen

    def _decided(self, cleaned: 'dict[str, Any]') -> 'dict[str, Any]':
        """Collect the settings whose box is checked, refusing what the package would refuse.

        Judged by the check registry rather than by a second copy of the rules here: a page
        that accepted a value the deployment then refuses to start with is worse than one that
        refuses it, because the refusal arrives at the next restart and in another person's
        terminal.
        """
        decided: dict[str, Any] = {
            key: cleaned.get(field_name(key)) for key in OVERRIDABLE if cleaned.get(switch_name(key))
        }
        # collected first and judged together: the rules read each other, so a setting judged
        # against the inherited value of its neighbour refuses a pair that is right together
        refused = problems_in(decided, self.inherited)
        for key, said in refused.items():
            self.add_error(field_name(key), ValidationError(said))
        return {key: value for key, value in decided.items() if key not in refused}

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
    FIELDSETS: 'tuple[tuple[str, dict[str, Any]], ...]' = (
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
        obj: TelegramBot | None = None,
    ) -> Any:  # noqa: ANN401 - Django's own signature
        """Drop the credentials section for a user who may not see the token.

        Dropped rather than masked: a page that renders a section is a page that posts it
        back, and leaving the token field there would let a user who cannot see the credential
        replace it -- which is the same thing as taking the bot.
        """
        if obj is not None and not self.has_change_permission(request, obj):
            # a page somebody may read and not edit: the pairs are form fields, and a section
            # naming one on a form that has none would be a page that cannot render. What is
            # left is what the row *is*, which is what a reader came for
            return tuple(section for section in self.FIELDSETS if section[0] in {'Bot', 'State'})
        if may_see_tokens(request):
            return self.FIELDSETS
        hidden = {name for key in SENSITIVE for name in (switch_name(key), field_name(key))}
        shown = []
        for title, options in self.FIELDSETS:
            if title == 'Credentials':
                continue
            settings: dict[str, Any] = dict(options)
            settings['fields'] = tuple(name for name in settings['fields'] if name not in hidden)
            shown.append((title, settings))
        return tuple(shown)

    def get_urls(self) -> list[Any]:
        """Add the one page that shows a credential, so revealing it is a deliberate act.

        A page rather than a column: a token rendered into the changeform is a token in a
        browser history, a proxy log and every screenshot of that page, whether anybody meant
        to read it or not. Here it takes a click, from a user holding the permission, and it
        leaves a row in the feed saying who.
        """
        from django.urls import path  # noqa: PLC0415 - the URL conf, and this module is imported early

        return [
            path(
                '<int:pk>/token/',
                self.admin_site.admin_view(self.reveal_token),
                name='django_aiogram_telegrambot_token',
            ),
            *super().get_urls(),
        ]

    def reveal_token(self, request: 'HttpRequest', pk: int) -> 'HttpResponse':
        """Show one bot's token to somebody holding the permission, and record that.

        `bot.token_revealed` goes into the append-only feed, so *who saw this token* has an
        answer during the incident where somebody has to ask. Recorded before the value is
        rendered: a response that reached the browser and no row is the one order that leaves
        the question unanswerable.
        """
        from django.http import Http404, HttpResponseForbidden  # noqa: PLC0415 - as in `get_urls`
        from django.template.response import TemplateResponse  # noqa: PLC0415 - as above

        if not (may_see_tokens(request) and self.has_view_permission(request)):
            return HttpResponseForbidden('You may not read bot tokens.')
        bot = self.get_queryset(request).filter(pk=pk).first()
        if bot is None:
            raise Http404
        shown = read_token(bot.token) if request.method == 'POST' else ''
        if shown:
            _record_reveal(bot, request)
        return TemplateResponse(
            request,
            'admin/django_aiogram/reveal_token.html',
            {
                **self.admin_site.each_context(request),
                'title': f'Token for {bot}',
                'bot': bot,
                'token': shown,
                'opts': self.opts,
            },
        )

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
        if obj is not None and not self.has_change_permission(request, obj):
            # Django extends its exclude list with `fields` where the user may not change the
            # row, so `None` reaches `list.extend` and the page 500s. A reader needs no form
            # at all: the sections above are the read-only ones
            kwargs['fields'] = []
            return super().get_form(request, obj, change=change, **kwargs)
        kwargs['fields'] = None
        built = super().get_form(request, obj, change=change, **kwargs)
        if may_see_tokens(request):
            return built
        # the section is not *rendered* for this user, and that is not enough: `fields=None`
        # above hands the factory the form's own declaration, which still declares `token`, so
        # a POST carrying one would be accepted and stored -- the bot given away by somebody
        # who may not read its credential. Dropped from the class the factory built, which is
        # this request's own subclass rather than the form every request shares
        return _without_the_credentials(built)

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
        """Show that a token is there, and its identity, with a way to read the rest.

        The link is a page of its own rather than a value in this one: see `reveal_token`.
        """
        if not obj.token:
            return 'not set'
        masked = MASK.format(bot_id=obj.bot_id, hidden=HIDDEN)
        if not obj.pk:
            return masked
        target = reverse('admin:django_aiogram_telegrambot_token', args=(obj.pk,))
        return format_html('{} <a href="{}">show it</a>', masked, target)


def _waiting_in(queue: str) -> int:
    """How many messages one queue still holds, or zero where that cannot be read.

    Zero for a transport that could not be *reached*: it has not said the queue is empty, but
    it has not said it is full either, and a page that refused every edit while Redis blinked
    would be worse than the rare mistake -- see `TelegramBotForm.clean_queue`.

    **A transport that cannot be built is a different answer.** A `BROKER` that names nothing
    importable, or a driver that is not installed, means nobody can read this queue *at all* --
    including after the edit -- so it is raised rather than turned into a zero. Otherwise a
    misconfigured deployment would find every dangerous edit allowed, which is the one shape
    where the refusal is needed most.
    """
    # deferred: building a transport imports its driver, and this module is imported by
    # `admin.autodiscover` while the app registry is still loading
    from django_aiogram.broker.exceptions import BrokerError  # noqa: PLC0415 - as above
    from django_aiogram.broker.registry import broker_class  # noqa: PLC0415 - as above
    from django_aiogram.runtime.queues import settings_for  # noqa: PLC0415 - as above

    settings = settings_for(queue)
    # outside the guard below: `BrokerError` and `ImproperlyConfigured` from here are the
    # settings being wrong, and `clean_queue` turns them into a refusal a person can act on
    broker = broker_class(settings).configured(settings)
    try:
        return int(broker.depth()) + int(broker.inflight_depth())
    except BrokerError:
        # the transport's own way of saying it could not answer -- unreachable, or a depth it
        # does not keep. Not the settings, so not a refusal
        logger.warning('could not read the depth of %s; allowing the edit', queue, exc_info=True)
        return 0
    except OSError:
        # a socket, which is the other half of "could not be reached": every driver here
        # raises its own class, and they all derive from this one
        logger.warning('could not reach the transport for %s; allowing the edit', queue, exc_info=True)
        return 0


def _record_reveal(bot: TelegramBot, request: 'HttpRequest') -> None:
    """Write the feed row that says whose token was read, and by whom.

    Best effort, like every other write into the feed: a recorder that is off or a database
    that refused must not be the reason a page 500s. What it costs is the row -- and the
    recorder logs its own failures.
    """
    # deferred: the recorder reaches the ORM and the settings, and this module is imported by
    # `admin.autodiscover` while the app registry is still loading
    from django_aiogram.config.enums import EventKind  # noqa: PLC0415 - as above
    from django_aiogram.eventlog.recorder import recorder  # noqa: PLC0415 - as above
    from django_aiogram.eventlog.records import Event  # noqa: PLC0415 - as above

    if not recorder.active:
        return
    who = getattr(request.user, 'get_username', lambda: '')() or 'anonymous'
    recorder.record(
        Event(
            kind=EventKind.BOT_TOKEN_REVEALED.value,
            bot_id=bot.bot_id,
            # the username in `detail` rather than in a column: the feed's columns are the
            # ones its indexes serve, and nothing queries this by user
            detail={'by': who, 'label': bot.label},
        )
    )


def _without_the_credentials(form: 'type[BotFormBase]') -> 'type[BotFormBase]':
    """Return the same form with no credential fields, for a user who may not set one.

    The token, and the settings whose value *is* a credential: a webhook secret is what tells
    Telegram's requests from anybody else's, and `REDIS_URL` carries the password to the
    broker. Same boundary, because they are the same kind of secret.

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
        for name in ('token', *(name for key in SENSITIVE for name in (switch_name(key), field_name(key)))):
            self.fields.pop(name, None)

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
