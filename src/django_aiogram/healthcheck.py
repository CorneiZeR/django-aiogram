"""Answer whether the bot container is doing its job, without booting Django.

``manage.py tgbot_healthcheck`` has always been correct and could not be used: a
management command runs ``django.setup()`` first, which populates the app registry
and executes every ``AppConfig.ready()`` in the *host* project. Measured in one
consumer — Django 5.2, twenty apps, one registering adapters in ``ready()`` — that
was 17.89s on top of 2.45s for the settings module, against ~0.01s for the probe's
own three Redis calls. Docker killed it at every timeout, and the container read
``unhealthy`` for the best part of an hour while the bot was fine and its heartbeat
six seconds old.

So this module is the check, and both entry points are thin:

* ``python -m django_aiogram.healthcheck`` — for a container healthcheck. It
  reads ``DJANGO_SETTINGS_MODULE`` the way any Django code does and never calls
  ``django.setup()``.
* ``manage.py tgbot_healthcheck`` — unchanged, because consumers have it in compose
  files today. It pays for ``django.setup()`` like every other command, and the
  Deployment page says why you would not put it in a healthcheck.

**This module must not import anything that needs the app registry.** No models, no
aiogram, no :mod:`django_aiogram.producer.client`. Reading
``django.conf.settings.TELEGRAM_BOT`` imports the settings module and nothing more,
which is the whole saving. ``tests/test_lazy_init.py`` asserts the registry is still
unpopulated after ``main()`` returns.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING

from django.core.exceptions import ImproperlyConfigured

from django_aiogram.broker.base import Broker
from django_aiogram.broker.exceptions import BrokerDependencyError, BrokerError
from django_aiogram.broker.registry import get_broker
from django_aiogram.config.settings import SETTINGS_NAME, coerce_bool, conf
from django_aiogram.redis import (
    get_redis,
    heartbeat_ttl,
    processing_key,
    processing_pattern,
    queue_key,
)

if TYPE_CHECKING:
    from redis import Redis

try:  # redis-py is an extra since 4.0: a Kafka or RabbitMQ deployment installs none of it
    from redis.exceptions import ConnectionError as _RedisConnectionError
    from redis.exceptions import RedisError as _RedisError
except ImportError:

    class _RedisError(Exception):  # type: ignore[no-redef]
        """Stand-in for the driver's error class where there is no driver.

        Naming ``redis`` at import time meant `python -m django_aiogram.healthcheck` could
        not start at all on a deployment that carries messages some other way -- measured,
        ``ModuleNotFoundError: No module named 'redis'`` out of the probe a container runs on
        a timer, so the container was unhealthy for ever and the one thing that would say why
        was the thing that crashed.

        Nothing raises this, so every ``except`` clause below reads the same on both installs
        and matches nothing where there is no Redis to fail.
        """

    class _RedisConnectionError(_RedisError):  # type: ignore[no-redef]
        """The same, for the one driver failure this module reports differently.

        A subclass of the stand-in above, so the clauses keep the order they have with the
        real classes: redis-py's ``ConnectionError`` derives from ``RedisError`` and nothing
        else -- measured, its MRO is ``ConnectionError -> RedisError -> Exception`` -- so the
        builtin of the same name would not catch it.
        """


logger = logging.getLogger('django_aiogram')

# round trips, not keys: MATCH filters on the server but SCAN walks the whole
# keyspace either way, and this probe runs on a timer
STRANDED_SCAN_ROUNDS = 20


@dataclass(frozen=True)
class Report:
    """What the probe concluded: the verdict, the line to print, anything else to say.

    ``warnings`` is separate from ``message`` because a warning must never change the
    verdict — a stranded in-flight list may be one another worker is sending this
    second, and an exit code that said otherwise would restart a healthy container.
    """

    ok: bool
    message: str
    warnings: tuple[str, ...] = ()
    #: whether anything was actually examined. False only when this process is
    #: disabled, which is not a verdict about the bot — and is why the management
    #: command reports that one plainly rather than in success green, as it always has
    checked: bool = True


class _UnhealthyError(Exception):
    """One reason the probe is about to answer no.

    Private, and raised only between the helpers below and :func:`check`, which turns
    it back into a :class:`Report`. It exists because the check is a sequence of
    reads where any one of them ends the answer — written as early returns, that was
    twelve branches in one function and the reason for each was harder to see than the
    control flow around it.
    """


def _setting_int(key: str) -> int:
    """Read one integer setting, refusing in a line rather than a traceback.

    `E023` and `E024` say this in ``manage.py check`` — which runs under
    ``django.setup()``, the thing this entry point exists to skip. So this is the one path
    where a value like ``os.environ.get('HB', '')`` written straight into the settings
    dict reaches ``int()`` with nothing between, and the probe is what has to say so.
    """
    raw = conf[key]
    try:
        return int(raw)
    # `OverflowError` with them: `int(float('inf'))` raises that and neither of the other two, and
    # this probe's whole contract is to report -- a traceback out of a container health check is a
    # restart loop with nothing to read, on a value it has a sentence ready for
    except (TypeError, ValueError, OverflowError) as error:
        msg = f"{SETTINGS_NAME}['{key}'] is not a number: {raw!r}"
        raise _UnhealthyError(msg) from error


def _connected() -> Redis:
    """Return the shared connection, having proved it answers.

    Two non-Redis failures are caught beside ``RedisError``, because from a probe's
    point of view a connection it cannot build is a Redis it cannot reach — which is
    what this command has always said about it, back when it caught ``Exception``:

    * ``ImproperlyConfigured`` — an empty ``REDIS_URL``, which
      :func:`~django_aiogram.redis.build_client` raises on.
    * ``ValueError`` — a ``REDIS_URL`` with no scheme, which ``Redis.from_url`` rejects,
      and a non-numeric ``REDIS_TIMEOUT``, which ``read_timeout()`` does. Both are
      misconfiguration reaching us as a builtin, and both used to read as one line.

    Narrowing to ``RedisError`` alone turned those into tracebacks and, in the management
    command, into a bare exception where a ``CommandError`` belongs.

    ``ImportError`` joins them for the same reason: since 4.0 the driver is an extra, so a
    deployment on another transport has no redis-py at all, and asking it for a client is a
    failure to reach Redis rather than a crash. Only the two Redis-only reports come here.

    Which is why what this raises is no longer a line the probe prints: the verdict asks the
    broker, and both callers here degrade instead of refusing. The message travels to their
    warning as ``tg_reason``, so an operator who asked for ``--guarantee`` still learns why
    it came back ``unknown``.
    """
    try:
        connection = get_redis()
        connection.ping()
    except (_RedisError, ImproperlyConfigured, ValueError, ImportError) as error:
        msg = f'redis is unreachable: {error}'
        raise _UnhealthyError(msg) from error
    return connection


def _liveness_age(broker: Broker, *, limit: int, ttl: int) -> int | None:
    """How long ago the consumer last said it was turning, as the transport measures it.

    Asked of the broker rather than read out of a key, because the key is one transport's
    answer. A Redis list has nothing that knows a consumer exists, so it writes a
    timestamp with a TTL; a stream's consumer group already records when each member last
    spoke, so there is nothing to write and nothing to expire — and a probe that read the
    key anyway would report a bot that is running perfectly as dead.

    ``None`` means the transport says liveness is not observable from outside. That is not
    a failure and not a pass: the caller says so and moves on to the depth, which is the
    honest position for a probe that has been told there is nothing to look at.

    Both numbers are needed for the refusal, not for the verdict: ``limit`` is what it is
    judged by, and ``ttl`` is what a written marker can actually show. A limit above it is
    unreachable, so the message names that instead of leaving an operator wondering why
    ``--max-age 600`` still refused at a minute.
    """
    try:
        report = broker.liveness()
    # before the clause below, and the order is the whole point: `UnicodeDecodeError` *is* a
    # `ValueError`, so a heartbeat this client could not decode would otherwise be reported as
    # a settings problem. It is bytes on the wire — `decode_responses` in a URL shared with a
    # cache backend makes redis-py decode this key, and what is there is not ours to promise
    # anything about
    except UnicodeDecodeError as error:
        msg = f'could not read the consumer liveness: {error}'
        raise _UnhealthyError(msg) from error
    except (ImproperlyConfigured, ValueError, _RedisConnectionError) as error:
        # a transport that cannot be addressed is one this probe cannot reach, which is the
        # argument the Redis ping made for three releases and the wording it used. Only the
        # subject changed: the ping ran before every check and named Redis, so a deployment
        # carrying messages another way could not run the probe at all. These arrive here
        # instead, because this call is where the client is built now
        #
        # a refused connection belongs here rather than with the clause below, and this is the
        # first call that would meet one: 3.x pinged before every check and answered `redis is
        # unreachable` for a Redis that was down, which is the failure an operator greps for.
        # Only redis-py's own class, because it is the only driver whose hierarchy is measured
        # here -- another transport's connection failure reads as the clause below until it is
        msg = f'the broker is unreachable: {error}'
        raise _UnhealthyError(msg) from error
    except (_RedisError, BrokerError) as error:
        # addressable and did not answer: down, failing over, or a key this replica cannot
        # serve
        msg = f'could not read the consumer liveness: {error}'
        raise _UnhealthyError(msg) from error
    if not report.reported:
        return None
    if report.age is None:
        # the applied limit, not the TTL: with `--max-age 600` the old wording said
        # "within 30s", which contradicts the number the probe is judging by
        detail = report.detail or 'the consumer has not reported'
        msg = f'{detail}: nothing within {limit}s, or the consumer never started'
        if limit > ttl and _writes_a_marker(broker):
            # only where something expires. On a transport that reports from the group's
            # own bookkeeping there is no key to outlive, so this advice would be noise
            msg = (
                f'{msg}. A limit over {ttl}s cannot be observed anyway, because the marker expires then: '
                f"raise {SETTINGS_NAME}['HEARTBEAT_INTERVAL'] instead"
            )
        raise _UnhealthyError(msg)
    # compared before truncating: `int(5.9)` is 5, so `--max-age 5` used to accept a consumer
    # already past the limit. The message keeps whole seconds, which is what an operator set
    if report.age > limit:
        msg = f'the consumer last reported {int(report.age)}s ago, over the {limit}s limit'
        raise _UnhealthyError(msg)
    return int(report.age)


def _writes_a_marker(broker: Broker) -> bool:
    """Whether this transport writes something that can expire.

    Read off whether the broker overrides :meth:`Broker.alive`, which is exactly the
    question: the base class does nothing, and a transport that has to say "still here"
    is the one with a marker that outlives nothing.
    """
    return type(broker).alive is not Broker.alive


def _depth(broker: Broker, *, limit: int) -> int:
    """How many messages are waiting, refusing when that is over the limit.

    Through the broker, so this counts the transport that is configured. Reading a list
    length here would report an empty queue on a deployment carrying its messages
    somewhere else — a healthcheck that says "healthy, 0 queued" about a backlog.
    """
    try:
        queued = broker.depth()
    except _RedisConnectionError as error:
        # the same split as the liveness read above: a broker that refused the connection is
        # unreachable, not a broker that would not answer one question
        msg = f'the broker is unreachable: {error}'
        raise _UnhealthyError(msg) from error
    except (_RedisError, BrokerError) as error:
        msg = f'could not read the queue length: {error}'
        raise _UnhealthyError(msg) from error
    if limit and queued > limit:
        msg = f'{queued} messages are queued, over the limit of {limit}'
        raise _UnhealthyError(msg)
    return queued


def check(
    *,
    max_queue: int | None = None,
    max_age: int | None = None,
    stranded: bool = False,
    guarantee: bool = False,
) -> Report:
    """Read Redis, then ask the broker for consumer liveness and queue depth, in that order.

    ``max_age`` defaults to three ``HEARTBEAT_INTERVAL``s, so one missed refresh is not a
    failure — which is also the most a written marker can show; ``max_queue`` to
    ``HEALTHCHECK_MAX_QUEUE``, where 0 disables the limit.

    Liveness and depth are asked of the configured broker, not read out of a key and a
    list. Both are one transport's answer: a stream's consumer group records when each
    member last spoke, so nothing is written and nothing expires, and its waiting count is
    a group's lag rather than a list's length.

    ``stranded`` and ``guarantee`` are off by default and the management command turns
    them on, which is the one place the two entry points differ. Both cost more than
    everything else here and neither can change the verdict: the scan is up to twenty
    ``SCAN`` rounds over a keyspace often shared with a cache backend, and the
    guarantee probe is a write — a no-op ``LMOVE`` on a missing key, but still a write,
    and on a read-only replica it answers ``unknown`` rather than the truth. Twice a
    minute for a line nobody acts on is the wrong trade for a container healthcheck and
    the right one for a command a person ran deliberately.
    """
    if not coerce_bool(conf['ENABLED'], f"{SETTINGS_NAME}['ENABLED']"):
        # nothing is meant to be running here, so nothing is wrong
        return Report(ok=True, message='disabled in this process; nothing to check', checked=False)

    try:
        # inside the guard: these two are read before Redis is touched, so an
        # unreadable one is the first thing the probe meets rather than the last
        ttl = heartbeat_ttl(max(1, _setting_int('HEARTBEAT_INTERVAL')))
        age_limit = ttl if max_age is None else max_age
        queue_limit = _setting_int('HEALTHCHECK_MAX_QUEUE') if max_queue is None else max_queue
        broker = get_broker()
        age = _liveness_age(broker, limit=age_limit, ttl=ttl)
        queued = _depth(broker, limit=queue_limit)
    except _UnhealthyError as refusal:
        return Report(ok=False, message=str(refusal))

    # `None` is the transport saying nobody outside can see whether a consumer is turning.
    # Reported as such rather than as an age of zero, which would read as a fresh heartbeat
    reported = f'{age}s old' if age is not None else 'not observable from outside'
    healthy = f'healthy: consumer {reported}, {queued} queued'
    if guarantee:
        # its own client, and only here: the two reports below are the only Redis-shaped
        # things this probe does, and building one in the default path is what made the
        # whole probe impossible to start without the driver
        healthy = f'{healthy}, {_guarantee()}'
    warnings: list[str] = []
    # the per-worker in-flight list is one transport's bookkeeping, and a transport that
    # answers `needs_identity` false has none — scanning for keys that cannot exist would
    # report a reassuring zero about a question this deployment does not have
    if stranded and broker.needs_identity:
        found, swept = _stranded()
        if found:
            # not a failure: another worker may be sending them right now. But an
            # invisible pile is how a stranded list stays stranded
            warnings.append(
                f'{found if swept else f"at least {found}"} message(s) are in flight under '
                'other worker names. If one of those workers is gone, '
                '`manage.py tgbot_reclaim --worker <name>` requeues them.'
            )
    return Report(ok=True, message=healthy, warnings=tuple(warnings))


def _guarantee() -> str:
    """Which delivery guarantee this Redis can actually give.

    Asked of the server, not of a consumer: ``Delivery.crash_safe`` starts true and is
    only lowered by ``reclaim()``, which this probe must never call — requeueing a
    running worker's in-flight list would send those messages twice.

    It asks the same question ``reclaim()`` does, on a key that does not exist:
    rotating an empty list is a no-op on a server that has ``LMOVE``, and
    ``unknown command`` on one that does not.

    ``COMMAND INFO lmove`` would be a read rather than a write, and does not work:
    against Redis 6.0 — the servers where the answer actually differs — redis-py's own
    response parser raises ``TypeError`` on the nil entry the server returns for an
    unknown command. Catching a library's parser failing in a particular way is a
    worse dependency than a no-op write, so the write stays and the *caller* decides
    whether to pay for it.
    """
    try:
        connection = _connected()
    except _UnhealthyError as refusal:
        # unreachable, misconfigured, or an install with no driver: this report is extra
        # credit, and `unknown` is what it already says for every other way it cannot answer.
        # The reason travels as a field rather than as a refusal, because the verdict is the
        # broker's now -- this client exists only for the two Redis-shaped extras
        logger.warning(
            'could not establish which delivery guarantee is in force',
            extra={'tg_reason': str(refusal)},
        )
        return 'unknown'

    from redis.exceptions import ResponseError  # noqa: PLC0415 - an extra, and a client exists by here

    probe = f'{queue_key()}:lmove-probe'
    try:
        connection.lmove(probe, probe, 'LEFT', 'RIGHT')
    except ResponseError as error:
        if 'unknown command' in str(error).lower():
            return 'at-most-once'
        logger.warning('could not establish which delivery guarantee is in force')
        return 'unknown'
    except _RedisError:
        logger.warning('could not establish which delivery guarantee is in force')
        return 'unknown'
    return 'at-least-once'


def _stranded() -> tuple[int, bool]:
    """Count what is in flight under a worker name that is not this one.

    Read rather than acted on: a message under another name may be one another worker
    is sending this second, and taking it back would send it twice.

    Bounded, and returns whether it finished. ``MATCH`` filters on the server but
    ``SCAN`` still walks the whole keyspace, and on a Redis shared with a cache
    backend — which the settings page suggests is common — an unbounded sweep is a full
    pass over someone else's keys. A partial answer is worth having; one that pretends
    to be complete is not.

    Builds its own client, and only when asked: this is one of the two Redis-shaped reports,
    and the probe's verdict never needs one. A deployment on another transport does not reach
    here at all -- the caller asks the broker first, and only the transport that keys its
    in-flight list on a worker name answers yes.
    """
    try:
        connection = _connected()
    except _UnhealthyError as refusal:
        # nothing to report rather than a refusal: the verdict is already decided, and this
        # sweep is a warning line on top of it
        logger.warning('could not scan for stranded in-flight lists', extra={'tg_reason': str(refusal)})
        return 0, True

    pattern = processing_pattern()
    mine = processing_key()
    # SCAN may return the same key more than once when the keyspace changes
    # size mid-iteration, and counting one twice would invent a backlog
    seen: set[str] = set()
    total = 0
    cursor = 0
    try:
        for _ in range(STRANDED_SCAN_ROUNDS):
            cursor, keys = connection.scan(cursor=cursor, match=pattern, count=100)
            for key in keys:
                name = key.decode('utf-8') if isinstance(key, bytes) else str(key)
                if name == mine or name in seen:
                    continue
                seen.add(name)
                total += int(connection.llen(name) or 0)
            if cursor == 0:
                return total, True
    except (_RedisError, UnicodeDecodeError):
        # the decode too: a foreign key on a shared Redis can match this pattern and hold
        # bytes that are not UTF-8, and aborting the whole probe over a warning nobody
        # acts on is the opposite of what this sweep is for
        logger.warning('could not scan for stranded in-flight lists', extra={'tg_key': pattern})
        return total, False
    return total, False


def add_limit_flags(parser: argparse.ArgumentParser) -> None:
    """Declare the two limits on any parser, so the entry points cannot drift.

    The management command adds them to the parser Django hands it, which is why this
    takes one rather than making its own: the flags, their types and their defaults are
    a property of :func:`check`, and a reader comparing ``--help`` of the two forms is
    entitled to the same answer from both.
    """
    parser.add_argument(
        '--max-queue',
        type=int,
        default=None,
        help=f"messages allowed to be waiting; defaults to {SETTINGS_NAME}['HEALTHCHECK_MAX_QUEUE'], 0 disables",
    )
    parser.add_argument(
        '--max-age',
        type=int,
        default=None,
        help=(
            f"seconds the heartbeat may be stale; defaults to three {SETTINGS_NAME}['HEARTBEAT_INTERVAL']s, "
            "which is also the key's TTL and so the most that can be observed"
        ),
    )


def build_parser() -> argparse.ArgumentParser:
    """Declare the container form's flags: the shared limits, plus what it leaves off."""
    parser = argparse.ArgumentParser(
        prog='python -m django_aiogram.healthcheck',
        description='Exit 0 when the bot container is healthy, non-zero with a reason otherwise',
    )
    add_limit_flags(parser)
    parser.add_argument(
        '--stranded',
        action='store_true',
        help=(f'also scan for in-flight lists left by other worker names (up to {STRANDED_SCAN_ROUNDS} SCAN rounds)'),
    )
    parser.add_argument(
        '--guarantee',
        action='store_true',
        help='also report which delivery guarantee this Redis gives (issues a no-op write)',
    )
    return parser


def _names_the_settings_module(error: ImportError) -> bool:
    """Whether what failed to import is the configured settings module itself.

    ``ModuleNotFoundError`` carries the dotted name it could not find, and a missing
    parent package reports the parent — so ``core.settings`` with no ``core`` on the path
    is recognized too.
    """
    missing = getattr(error, 'name', None)
    configured = os.environ.get('DJANGO_SETTINGS_MODULE')
    if not missing or not configured:
        return False
    return configured == missing or configured.startswith(f'{missing}.')


def _cannot_read(error: Exception) -> int:
    """Write the one-line refusal and give the exit code that goes with it."""
    sys.stderr.write(f'cannot read the settings: {error}\n')
    return 1


def main(argv: list[str] | None = None) -> int:
    """Run the check and print its report. Returns the exit code.

    Nothing here calls ``django.setup()``, which is the point of the module. Reading a
    setting still needs ``DJANGO_SETTINGS_MODULE`` in the environment, which a container
    running ``manage.py`` does not necessarily have — see the refusal below.
    """
    options = build_parser().parse_args(argv)
    try:
        report = check(
            max_queue=options.max_queue,
            max_age=options.max_age,
            stranded=options.stranded,
            guarantee=options.guarantee,
        )
    except BrokerDependencyError as error:
        # the deployment this module was rewritten for: BROKER names a transport whose driver
        # this image does not carry, because every driver is an extra since 4.0. The settings
        # are perfectly readable, so the refusal below would be the wrong sentence -- and the
        # exception already carries the one command that fixes it, which a traceback buries
        sys.stderr.write(f'{error}\n')
        return 1
    except ImproperlyConfigured as error:
        # the failure this form meets that the management command cannot: `manage.py` sets
        # DJANGO_SETTINGS_MODULE inside its own process, so a container running it does not
        # necessarily export the variable — and a healthcheck is a separate process. A
        # traceback here would say "unhealthy" without saying why, from a probe whose whole
        # job is to say why
        return _cannot_read(error)
    except ImportError as error:
        # the recipe asks an operator to write that module name by hand, so a mistyped one
        # is at least as likely as a missing variable, and it deserves the same line. Only
        # that one: an import the settings module itself fails at is not ours to flatten,
        # and its traceback is where the answer is
        if not _names_the_settings_module(error):
            raise
        return _cannot_read(error)
    stream = sys.stdout if report.ok else sys.stderr
    stream.write(f'{report.message}\n')
    # stdout is block-buffered when it is not a tty and stderr is not buffered at all, so
    # without this the warnings reach a merged `docker inspect` log before the verdict
    # they qualify
    stream.flush()
    for warning in report.warnings:
        sys.stderr.write(f'{warning}\n')
    return 0 if report.ok else 1


if __name__ == '__main__':
    raise SystemExit(main())
