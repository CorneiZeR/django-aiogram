"""Pace outgoing calls to stay under Telegram's published limits.

Retrying on ``TelegramRetryAfter`` is reactive: the message has already been
refused and the bot has already been told to back off. These limits are
documented, so the sane thing is not to exceed them in the first place.

Telegram enforces three at once:

* roughly 30 messages per second overall
* about one message per second to the same chat
* 20 messages per minute to the same group or channel

A limiter belongs to one bot. Limits are per token, so a second bot must not
share this budget — which is what makes the multi-bot case work unchanged.
"""

import asyncio
import logging
import threading
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from django.core.exceptions import ImproperlyConfigured
from django.core.signals import setting_changed

from django_aiogram.config.bots import BotRecord
from django_aiogram.config.defaults import DEFAULTS
from django_aiogram.config.enums import KNOWN_RATE_LIMIT_KEYS, RateLimitKey
from django_aiogram.config.settings import SETTINGS_NAME, conf

logger = logging.getLogger('django_aiogram')

Clock = Callable[[], float]
Sleeper = Callable[[float], Awaitable[None]]


# the shipped limits live in defaults.py; duplicating them here would drift
RATE_LIMIT_DEFAULTS: dict[str, float] = dict(DEFAULTS['RATE_LIMIT'])

# chats a bot talks to at once; beyond this the idle ones are dropped
MAX_TRACKED_CHATS = 4096
#: how many of the oldest buckets to look at before evicting one regardless
EVICTION_CANDIDATES = 8


class TokenBucket:
    """A token bucket by its behavior, GCRA by its implementation.

    The name is kept because that is what the limits are described as, but nothing
    counts tokens. Each caller claims the next free slot under the lock and sleeps
    until exactly that instant, which gives the same pacing for two reasons a
    counter cannot:

    * **Wakeups are O(1) per admitted call.** Counting meant every waiter computed
      the same wait from the same shared state, so N waiters woke together, one
      won and N-1 recomputed — about N²/2 wakeups. Measured here, on the design that
      ships: 35 wakeups for 40 queued sends and 495 for 500, which is one per send that
      *had to wait* — the burst goes through without sleeping at all, so the count is
      ``max(0, N - max(1, floor(capacity)))`` rather than ``N`` — clamped, because a
      batch smaller than the burst sleeps not at all: measured, three calls against a
      capacity of five wake nobody. The floor matters because
      ``capacity`` is a float: measured, 1.5 admits one call without sleeping and 5.5
      admits five, so a fraction of a slot buys nothing. The ``max`` matters because a
      capacity *below* one still admits the first call — ``_burst`` clamps to zero and the
      claim starts at ``now`` — so 0.5 gives 39 wakeups for 40 calls, not 40.

      The old shape is quoted as N²/2 rather than as a number, because it is gone and a
      number for it would be invented.
    * **Admission is strict FIFO.** A herd re-racing for the same token admits in
      whatever order the loop happens to resume, so the message that waited
      longest had no claim on going first.

    It also removes the reason `TOKEN_EPSILON` existed: refilling accumulated
    float error, a full bucket landed on 0.9999999999, and the wait shrank to
    intervals too small to advance the clock at all. There is no loop to spin in
    here — a slot is claimed once and waited for once.
    """

    def __init__(
        self,
        rate: float,
        capacity: float | None = None,
        *,
        clock: Clock = time.monotonic,
        sleep: Sleeper = asyncio.sleep,
    ) -> None:
        """Build a bucket admitting ``rate`` calls per second, ``capacity`` at once."""
        if rate <= 0:
            msg = 'rate must be positive'
            raise ValueError(msg)
        self.rate = rate
        self.capacity = capacity if capacity is not None else max(rate, 1.0)
        self._clock = clock
        self._sleep = sleep
        self._interval = 1 / rate
        # (capacity - 1), not capacity: the first call takes a slot rather than
        # only credit for one, so the full form admits capacity + 1 in a burst
        self._burst = max(0.0, (self.capacity - 1) * self._interval)
        # a fresh bucket owes nothing, which is the whole burst available at once
        self._next_free = clock() - self._burst
        self._guard = threading.Lock()

    def is_idle(self) -> bool:
        """Report whether the bucket owes no wait, and so is free to forget.

        Against `now - burst` rather than `now`: a bucket that has spent part of
        its burst still owes that part, and calling it idle would let `_evict`
        forget a chat that is mid-conversation — which is the bounded-loss
        argument that method rests on.
        """
        with self._guard:
            return self._next_free <= self._clock() - self._burst

    async def acquire(self) -> None:
        """Claim the next free slot, then wait until it arrives.

        The guard is a threading lock, not an asyncio one: a limiter is shared
        per token and may be reached from more than one loop or thread, and an
        asyncio primitive binds itself to the first loop that awaits it. It is
        held only across the claim, never across the sleep.
        """
        with self._guard:
            now = self._clock()
            # the claim cannot start further back than the burst allows, or an
            # idle bucket would bank credit without limit
            slot = max(self._next_free, now - self._burst)
            self._next_free = slot + self._interval
        # read again, outside the lock: `now` was sampled while claiming, and
        # anything between then and here — the GIL, another thread, a slow
        # logger — makes it stale. Sleeping `slot - stale` overshoots the slot by
        # exactly that gap, which is throttling nobody asked for
        wait = slot - self._clock()
        if wait > 0:
            await self._sleep(wait)


class RateLimiter:
    """Holds the three buckets Telegram applies to a single bot."""

    def __init__(
        self,
        overall_per_second: float = RATE_LIMIT_DEFAULTS[RateLimitKey.OVERALL_PER_SECOND.value],
        per_chat_per_second: float = RATE_LIMIT_DEFAULTS[RateLimitKey.PER_CHAT_PER_SECOND.value],
        group_per_minute: float = RATE_LIMIT_DEFAULTS[RateLimitKey.GROUP_PER_MINUTE.value],
        *,
        clock: Clock = time.monotonic,
        sleep: Sleeper = asyncio.sleep,
    ) -> None:
        """Build the buckets for one bot; a rate of 0 switches that budget off.

        The parameter names are the ``RATE_LIMIT`` keys, so a settings mapping
        can be splatted straight in.
        """
        self._clock = clock
        self._sleep = sleep
        self._overall = self._bucket(overall_per_second)
        self._per_chat_rate = per_chat_per_second
        self._group_rate = group_per_minute / 60 if group_per_minute else 0
        self._group_capacity = group_per_minute or None
        self._chats: OrderedDict[int, TokenBucket] = OrderedDict()
        self._groups: OrderedDict[int, TokenBucket] = OrderedDict()
        # threading, not asyncio: see TokenBucket.acquire
        self._lock = threading.Lock()

    def _bucket(self, rate: float, capacity: float | None = None) -> TokenBucket | None:
        """Build a bucket, or None when this limit is switched off.

        ``None`` for a zero rate is what lets every caller treat *unlimited* as *no
        bucket* and skip the wait entirely, rather than each one repeating the check
        against the setting.
        """
        if not rate:
            return None
        return TokenBucket(rate, capacity, clock=self._clock, sleep=self._sleep)

    def _for(
        self,
        chats: OrderedDict[int, TokenBucket],
        key: int,
        rate: float,
        capacity: float | None = None,
    ) -> TokenBucket | None:
        """Return this key's bucket, creating it on first use, and mark it as used.

        The ``move_to_end`` is the whole reason this is one method rather than a
        ``setdefault``: the map is bounded, and eviction draws its candidates from the
        least recently used end — so a lookup that did not record itself would leave a
        busy chat sitting at the front of that queue, and be thrown away while it is
        still owed a wait.
        """
        bucket = chats.get(key)
        if bucket is None:
            created = self._bucket(rate, capacity)
            if created is None:
                return None
            bucket = chats[key] = created
            self._evict(chats)
        chats.move_to_end(key)
        return bucket

    @staticmethod
    def _evict(chats: OrderedDict[int, TokenBucket]) -> None:
        """Keep the map at the cap, preferring buckets that owe no wait time.

        When every candidate is still busy the least recently used one goes
        anyway, and its debt goes with it. That is a bounded loss rather than a
        way around the limit: a bucket is only evicted once MAX_TRACKED_CHATS
        other chats have been more recently active, which at the overall limit
        takes minutes, while per-chat debt clears in about a second. The
        alternative is an unbounded map, which is a leak.
        """
        while len(chats) > MAX_TRACKED_CHATS:
            # stopping at the first busy bucket left the map uncapped: one chat
            # that keeps sending pinned everything behind it
            candidates = [chats.popitem(last=False) for _ in range(min(EVICTION_CANDIDATES, len(chats)))]
            evict = next((index for index, (_, bucket) in enumerate(candidates) if bucket.is_idle()), 0)
            del candidates[evict]
            for key, bucket in reversed(candidates):
                chats[key] = bucket
                chats.move_to_end(key, last=False)

    @staticmethod
    def is_group(chat_id: int) -> bool:
        """Report whether ``chat_id`` is a group: those all carry a negative id.

        Supergroups and channels count as groups here, since Telegram gives the
        three of them the same per-minute budget.
        """
        return chat_id < 0

    async def acquire(self, chat_id: int | str | None = None) -> None:
        """Wait until sending to ``chat_id`` stays inside every limit."""
        buckets: list[TokenBucket] = []
        if self._overall is not None:
            buckets.append(self._overall)

        key = self._chat_key(chat_id)
        if key is not None:
            with self._lock:
                per_chat = self._for(self._chats, key, self._per_chat_rate)
                group = (
                    self._for(self._groups, key, self._group_rate, self._group_capacity) if self.is_group(key) else None
                )
            buckets.extend(bucket for bucket in (per_chat, group) if bucket is not None)

        for bucket in buckets:
            await bucket.acquire()

    @staticmethod
    def _chat_key(chat_id: int | str | None) -> int | None:
        """Return the bucket key for ``chat_id``, or None when it has none.

        Per-chat limits only apply to numeric ids: an ``@channel`` name cannot
        be keyed. The runtime check is wider than the annotation because the id
        comes from caller kwargs, where anything at all can turn up — including
        a bool, which int() would otherwise fold into chat 1.
        """
        if isinstance(chat_id, bool) or not isinstance(chat_id, (int, str)):
            return None
        try:
            return int(chat_id)
        except (TypeError, ValueError):
            return None


def build_rate_limiter(settings: 'Mapping[str, Any] | None' = None) -> RateLimiter | None:
    """Build the limiter one bot's settings describe, or None when disabled.

    ``settings`` is that bot's resolved record: Telegram meters the token, so the numbers are
    a bot's own -- a noisy client throttled below the shared default, a client with paid
    broadcasting above it -- and reading the process-wide dict here would have given every bot
    the same budget whatever its row said.
    """
    resolved = conf if settings is None else settings
    limits = resolved['RATE_LIMIT']
    if not limits:
        return None

    unknown = sorted(str(key) for key in limits if key not in KNOWN_RATE_LIMIT_KEYS)
    if unknown:
        label = settings.label('RATE_LIMIT') if isinstance(settings, BotRecord) else f"{SETTINGS_NAME}['RATE_LIMIT']"
        msg = f'{label} has unknown keys: {", ".join(unknown)}.'
        raise ImproperlyConfigured(msg)
    return RateLimiter(**limits)


def _numbers(settings: 'Mapping[str, Any] | None') -> tuple[tuple[str, Any], ...]:
    """Return the limits as something comparable, for a cache that has to notice a change.

    The rates live in rows since 5.0, and a row moves without `setting_changed` firing -- so
    a registry that only cleared on that signal would pace a client against the numbers their
    previous plan had. Compared rather than versioned because a record carries no revision:
    what matters is whether the numbers this limiter was built from are still the numbers.
    """
    resolved = conf if settings is None else settings
    limits = resolved['RATE_LIMIT'] or {}
    return tuple(sorted((str(key), value) for key, value in limits.items()))


def _owner(settings: 'Mapping[str, Any] | None') -> str | None:
    """Name the record a token's numbers came from, or ``None`` where nothing named one.

    Two configurations may legally hold one token -- a row and a section of the same
    identity -- and Telegram meters the token, so they share one
    limiter. Which of them the numbers came from is what decides whether a different
    snapshot is a plan that moved or the other configuration disagreeing.
    """
    if not isinstance(settings, BotRecord):
        return None
    return f'{"row" if settings.provided else "section"}:{settings.alias}'


class _LimiterRegistry:
    """The limiters in use, one per bot token.

    Telegram applies its limits per bot, so two ``TelegramBot`` objects holding
    the same token must draw on one budget; separate limiters would let them
    send at twice the rate.
    """

    def __init__(self) -> None:
        """Start empty: a limiter is built on the first send with that token."""
        self._limiters: dict[str, RateLimiter] = {}
        #: the numbers each limiter was built from, so a change in a row is noticed. A
        #: `setting_changed` receiver cannot see one: the rates live in rows since 5.0
        self._numbers: dict[str, tuple[tuple[str, Any], ...]] = {}
        #: which record each token's numbers came from, so a second configuration holding
        #: the same token cannot rebuild it -- a rebuild starts the buckets full, and two
        #: records disagreeing would hand a full burst to every send. Kept for a token whose
        #: owner switched pacing *off* as well, which is why the decision is read from here
        #: and not from `_limiters`: a decision not to pace is a decision, and forgetting it
        #: would let the other record take the token over on the next send
        self._owners: dict[str, str | None] = {}
        #: tokens whose collision has been reported, so the log says it once
        self._told: set[str] = set()
        # threading, not asyncio: see TokenBucket.acquire
        self._guard = threading.Lock()

    def get(self, token: str, settings: 'Mapping[str, Any] | None' = None) -> RateLimiter | None:
        """Return the limiter for ``token``, building it if this is the first ask.

        Rebuilt where the bot's numbers have moved since: a client whose plan changed is
        paced by the new budget on the next send rather than at the next restart. The buckets
        start full, which is the same state a fresh process would give them.

        **Only the record the numbers came from may rebuild it.** Where a second
        configuration holds the same token and asks for different numbers, it is answered
        with what that record decided: alternating sends would otherwise rebuild the limiter
        on every call, and a limiter built a moment ago has a full burst to give away, which
        is pacing switched off rather than shared. ``RATE_LIMIT: {}`` is one of those
        decisions and is owned like any other: the owner's ``{}`` leaves the token paced by
        nothing for both of them, and an owner that paces paces the other one too -- Telegram
        meters the token, so the alternative is the second record spending a budget the first
        is being held to.
        """
        with self._guard:
            asked = _numbers(settings)
            owner = _owner(settings)
            # by the owner rather than by a limiter: a token whose owner switched pacing off
            # has an answer here -- ``None`` -- and no entry in `_limiters` at all
            known = token in self._owners
            if known and self._numbers.get(token) == asked:
                if owner is not None and self._owners[token] is None:
                    # claimed rather than rebuilt: the limiter already holds these numbers,
                    # and leaving the token unowned would let the *next* record take it --
                    # after which this one's rate changes would be refused as a disagreement
                    self._owners[token] = owner
                return self._limiters.get(token)
            if known and not self._may_rebuild(token, owner):
                if token not in self._told:
                    self._told.add(token)
                    logger.warning(
                        'two configurations hold one token with different limits; pacing by the first',
                        extra={
                            'tg_bot': settings.alias if isinstance(settings, BotRecord) else None,
                            'tg_paced_by': self._owners.get(token),
                        },
                    )
                return self._limiters.get(token)
            limiter = build_rate_limiter(settings)
            self._numbers[token] = asked
            self._owners[token] = owner
            if limiter is None:
                # the limiter goes, the decision stays: a bot asked not to be paced must not
                # be paced by whatever it built a moment ago, and the ownership above is what
                # keeps a second record holding this token from pacing it instead
                self._limiters.pop(token, None)
                return None
            self._limiters[token] = limiter
            return limiter

    def _may_rebuild(self, token: str, owner: str | None) -> bool:
        """Whether ``owner`` is the record this token's pacing answers to.

        An unrecorded owner is one nothing named -- a caller that passed the process-wide
        settings -- and a named record takes it over rather than being refused by it.
        """
        held = self._owners.get(token)
        return held is None or held == owner

    def clear(self) -> None:
        """Forget every limiter, so the next ask reads the settings again."""
        with self._guard:
            self._limiters.clear()
            self._numbers.clear()
            self._owners.clear()
            self._told.clear()


_registry = _LimiterRegistry()


def get_rate_limiter(token: str, settings: 'Mapping[str, Any] | None' = None) -> RateLimiter | None:
    """Return the limiter for ``token``, shared across bot instances.

    Keyed by the token because that is what Telegram meters: two objects holding one token
    draw on one budget, and separate limiters would let them send at twice the rate. The
    *numbers* come from ``settings`` -- that bot's resolved record -- so a client throttled in
    their row is throttled here.
    """
    return _registry.get(token, settings)


def reset_rate_limiters() -> None:
    """Forget the shared limiters, so changed settings take effect."""
    _registry.clear()


def _reset_on_setting_change(
    sender: object,  # noqa: ARG001 - Django sends this to every receiver, named
    setting: str,
    **kwargs: Any,
) -> None:
    """Forget the shared limiters when the setting they were built from changes.

    The registry is keyed by token and outlives any one bot, so without this a test or a
    runtime change of the rates would keep pacing against the numbers a previous
    configuration was built with.
    """
    if setting == SETTINGS_NAME:
        reset_rate_limiters()


setting_changed.connect(_reset_on_setting_change, dispatch_uid='django_aiogram.producer.throttling')
