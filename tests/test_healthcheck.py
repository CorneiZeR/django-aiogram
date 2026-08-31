"""`docker ps` cannot tell whether the consumer is consuming.

The heartbeat is the only thing another process can observe about the consumer
thread, and `tgbot_healthcheck` is what reads it.
"""

import re
import textwrap
import time
from io import StringIO
from unittest.mock import patch

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.core.management import CommandError, call_command
from django.test import override_settings

# redis-py raises its own `RedisConnectionError`, which is a `RedisError` and not an `OSError` --
# a fake raising the builtin is pretending to be a failure no real client produces, and a
# guard narrowed to either would stay green against it. Imported under a name of its own:
# the hazard is the shadowing, and recording it in a comment did not remove it
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError, ResponseError

from django_aiogram.broker.exceptions import BrokerDependencyError
from django_aiogram.consumer.delivery import BlpopDelivery
from django_aiogram.healthcheck import build_parser, check, main
from django_aiogram.management.commands.tgbot_healthcheck import Command as TgbotHealthcheck
from tests.support import run_python

QUEUE = 'TELEGRAM_BOT_MESSAGE'
WORKER = 'tests'
HEARTBEAT = f'{QUEUE}:heartbeat:{WORKER}'
SETTINGS = {
    'TOKEN': '42:x',
    'REDIS_URL': 'redis://localhost:6379/0',
    'WORKER_NAME': WORKER,
    'BLPOP_TIMEOUT': 1,
}
#: what the fakes below raise with, named up here so each raise stays one line
REFUSED = 'Connection refused'
READONLY = 'READONLY You cannot write against a read only replica'
RESET = 'Connection reset by peer'
STOP_AFTER_ONE_READ = 'stop here'


def healthcheck(**options):
    out = StringIO()
    call_command('tgbot_healthcheck', stdout=out, **options)
    return out.getvalue()


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_consumer_writes_a_heartbeat(redis_server):
    delivery = BlpopDelivery(handler=lambda **kwargs: None)

    delivery.heartbeat()

    assert redis_server.get(HEARTBEAT) is not None
    assert redis_server.ttl(HEARTBEAT) > 0, 'the heartbeat must expire on its own'


@override_settings(TELEGRAM_BOT={**SETTINGS, 'HEARTBEAT_INTERVAL': 30})
def test_the_heartbeat_is_paced(redis_server):
    """Refreshing per message would be a write per message."""
    delivery = BlpopDelivery(handler=lambda **kwargs: None)

    delivery.heartbeat()
    first = redis_server.get(HEARTBEAT)
    redis_server.delete(HEARTBEAT)
    delivery.heartbeat()

    assert first is not None
    assert redis_server.get(HEARTBEAT) is None, 'it wrote again inside the interval'


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_redis_that_refuses_the_write_does_not_stop_the_loop(redis_server, caplog):
    class Refuses:
        def set(self, *args, **kwargs):
            raise RedisConnectionError(REFUSED)

        def __getattr__(self, name):
            return getattr(redis_server, name)

    delivery = BlpopDelivery(handler=lambda **kwargs: None)
    with pytest.MonkeyPatch.context() as patch, caplog.at_level('ERROR'):
        patch.setattr('django_aiogram.broker.redis_list.broker.get_redis', Refuses)
        delivery.heartbeat()  # must not raise

    assert 'could not write the heartbeat' in caplog.text


@override_settings(TELEGRAM_BOT={**SETTINGS, 'WORKER_NAME': 'worker-b'})
def test_the_key_is_per_worker(redis_server):
    assert BlpopDelivery(handler=lambda **kwargs: None).heartbeat_key.endswith(':worker-b')


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_healthy_when_the_heartbeat_is_fresh(redis_server):
    redis_server.set(HEARTBEAT, str(int(time.time())))

    assert 'healthy' in healthcheck()


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_unhealthy_when_there_is_no_heartbeat(redis_server):
    with pytest.raises(CommandError, match='no heartbeat'):
        healthcheck()


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_unhealthy_when_the_heartbeat_is_stale(redis_server):
    """The failure this command exists for: the thread died, the process lives."""
    redis_server.set(HEARTBEAT, str(int(time.time()) - 300))

    with pytest.raises(CommandError, match='last reported'):
        healthcheck()


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_unhealthy_when_the_transport_cannot_be_reached(monkeypatch):
    """The probe pings nothing now, so the first broker call is what finds a transport that
    is down -- and it has to report it rather than raise.

    Patched where the broker reads rather than where the probe used to ping: since the driver
    became an extra the probe opens no client of its own, which is what lets it start on a
    deployment that carries messages some other way.

    The wording is 3.x's, and deliberately: `redis is unreachable` is the line operators grep
    out of `docker inspect` for the commonest failure there is, and dropping the ping must not
    turn a Redis that is down into a report about one question it would not answer. Only the
    subject moved -- see the case below for the other side of that split.
    """

    class Down:
        def get(self, *args, **kwargs):
            raise RedisConnectionError(REFUSED)

        def __getattr__(self, name):
            msg = f'reached for {name} on a Redis that is down'
            raise RedisConnectionError(msg)

    monkeypatch.setattr('django_aiogram.broker.redis_list.broker.get_redis', Down)

    with pytest.raises(CommandError, match='the broker is unreachable'):
        healthcheck()


@override_settings(TELEGRAM_BOT={**SETTINGS, 'HEALTHCHECK_MAX_QUEUE': 2})
def test_unhealthy_when_the_queue_is_over_the_limit(redis_server):
    redis_server.set(HEARTBEAT, str(int(time.time())))
    for _ in range(3):
        redis_server.rpush(QUEUE, b'{}')

    with pytest.raises(CommandError, match='3 messages are queued'):
        healthcheck()


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_queue_check_is_off_by_default(redis_server):
    redis_server.set(HEARTBEAT, str(int(time.time())))
    for _ in range(50):
        redis_server.rpush(QUEUE, b'{}')

    assert 'healthy' in healthcheck()


@override_settings(TELEGRAM_BOT={**SETTINGS, 'HEALTHCHECK_MAX_QUEUE': 100})
def test_the_limits_can_be_given_on_the_command_line(redis_server):
    redis_server.set(HEARTBEAT, str(int(time.time())))
    for _ in range(3):
        redis_server.rpush(QUEUE, b'{}')

    with pytest.raises(CommandError, match='over the limit of 2'):
        healthcheck(max_queue=2)


@override_settings(TELEGRAM_BOT={**SETTINGS, 'ENABLED': False})
def test_a_disabled_process_is_not_unhealthy():
    """Nothing is meant to be running there, so nothing is wrong."""
    assert 'disabled' in healthcheck()


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_heartbeat_that_is_not_a_timestamp_is_reported(redis_server):
    redis_server.set(HEARTBEAT, b'soon')

    with pytest.raises(CommandError, match='not a timestamp'):
        healthcheck()


@override_settings(TELEGRAM_BOT={**SETTINGS, 'BLPOP_TIMEOUT': 300, 'HEARTBEAT_INTERVAL': 5})
def test_a_long_blocking_read_cannot_outlast_the_heartbeat(redis_server, monkeypatch):
    """The loop beats between reads, so a read longer than the interval would
    let the key expire under a consumer that is doing fine."""
    seen = []

    class Spy:
        def blmove(self, source, destination, timeout, *args, **kwargs):
            seen.append(timeout)
            raise RedisConnectionError(STOP_AFTER_ONE_READ)  # one read is enough to observe

        def __getattr__(self, name):
            return getattr(redis_server, name)

    monkeypatch.setattr('django_aiogram.broker.redis_list.broker.get_redis', Spy)
    delivery = BlpopDelivery(handler=lambda **kwargs: None)
    thread = delivery.start_thread()
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not seen:
            time.sleep(0.02)
    finally:
        delivery.stop()
        thread.join(timeout=10)

    assert seen, 'the consumer never reached the blocking read'
    assert max(seen) <= 5, f'it blocked for {max(seen)}s with a 5s heartbeat interval'


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_heartbeat_read_that_fails_is_reported(redis_server, monkeypatch):
    """A transport that answers one command and not the next must not surface as a traceback.

    A `ResponseError`, which is what redis-py raises for this: the server answered, and what
    it said was no. That is what separates this line from `the broker is unreachable` above --
    a refused connection is a transport that is not there, and a replica refusing a read is
    one that is.
    """

    class FailsTheRead:
        def ping(self):
            return True

        def get(self, *args, **kwargs):
            raise ResponseError(READONLY)

        def __getattr__(self, name):
            return getattr(redis_server, name)

    # the broker's name is the one that matters: the liveness read goes through it, because a
    # heartbeat key is one transport's answer. `healthcheck.get_redis` is patched too, since
    # the two Redis-only reports still build a client from it
    monkeypatch.setattr('django_aiogram.healthcheck.get_redis', FailsTheRead)
    monkeypatch.setattr('django_aiogram.broker.redis_list.broker.get_redis', FailsTheRead)

    with pytest.raises(CommandError, match='could not read the consumer liveness'):
        healthcheck()


@override_settings(TELEGRAM_BOT={**SETTINGS, 'HEALTHCHECK_MAX_QUEUE': 5})
def test_a_queue_read_that_fails_is_reported(redis_server, monkeypatch):
    """The heartbeat read got through and the count did not, which is its own line."""

    class FailsTheCount:
        def ping(self):
            return True

        def get(self, *args, **kwargs):
            return str(int(time.time())).encode()

        def llen(self, *args, **kwargs):
            raise ResponseError(READONLY)

        def __getattr__(self, name):
            return getattr(redis_server, name)

    # as above: the count is the broker's answer now, so the failure has to be arranged
    # where the broker looks
    monkeypatch.setattr('django_aiogram.healthcheck.get_redis', FailsTheCount)
    monkeypatch.setattr('django_aiogram.broker.redis_list.broker.get_redis', FailsTheCount)

    with pytest.raises(CommandError, match='could not read the queue length'):
        healthcheck()


@override_settings(TELEGRAM_BOT={**SETTINGS, 'HEALTHCHECK_MAX_QUEUE': 5})
def test_a_queue_read_on_a_dropped_connection_reads_as_unreachable(redis_server, monkeypatch):
    """The depth read splits the same way the liveness read does, and both halves are pinned.

    Without this case the depth clause could report a connection that dropped mid-probe as one
    question the broker would not answer -- the wording 3.x reserved for a server that is
    there. The heartbeat is fresh here on purpose: the count is the only thing that fails.
    """

    class DropsTheCount:
        def ping(self):
            return True

        def get(self, *args, **kwargs):
            return str(int(time.time())).encode()

        def llen(self, *args, **kwargs):
            raise RedisConnectionError(RESET)

        def __getattr__(self, name):
            return getattr(redis_server, name)

    monkeypatch.setattr('django_aiogram.healthcheck.get_redis', DropsTheCount)
    monkeypatch.setattr('django_aiogram.broker.redis_list.broker.get_redis', DropsTheCount)

    with pytest.raises(CommandError, match='the broker is unreachable'):
        healthcheck()


@override_settings(TELEGRAM_BOT={**SETTINGS, 'HEALTHCHECK_MAX_QUEUE': 3})
def test_the_queue_limit_is_inclusive(redis_server):
    """Exactly at the limit is still healthy; the docs say so."""
    redis_server.set(HEARTBEAT, str(int(time.time())))
    for _ in range(3):
        redis_server.rpush(QUEUE, b'{}')

    assert 'healthy' in healthcheck()

    redis_server.rpush(QUEUE, b'{}')
    with pytest.raises(CommandError, match='4 messages are queued'):
        healthcheck()


@override_settings(TELEGRAM_BOT={**SETTINGS, 'WORKER_NAME': 'mine'})
def test_the_probe_says_which_guarantee_is_in_force(redis_server):
    """A probe that only says "healthy" cannot tell at-least-once from
    at-most-once, and the difference is whether a kill loses a message."""
    redis_server.set(f'{QUEUE}:heartbeat:mine', str(int(time.time())))
    out = StringIO()

    call_command('tgbot_healthcheck', stdout=out)

    assert 'at-least-once' in out.getvalue()


@override_settings(TELEGRAM_BOT={**SETTINGS, 'WORKER_NAME': 'mine'})
def test_messages_stranded_under_another_worker_are_reported(redis_server):
    """A stranded list is invisible otherwise: nothing reads it and nothing
    counts it, which is how it stays stranded."""
    redis_server.set(f'{QUEUE}:heartbeat:mine', str(int(time.time())))
    redis_server.rpush(f'{QUEUE}:processing:gone', b'{}', b'{}')
    out = StringIO()

    call_command('tgbot_healthcheck', stdout=out)

    reported = out.getvalue()
    assert '2 message(s) are in flight under other worker names' in reported
    assert 'tgbot_reclaim' in reported


@override_settings(TELEGRAM_BOT={**SETTINGS, 'WORKER_NAME': 'mine'})
def test_an_old_server_is_not_reported_as_crash_safe(redis_server, monkeypatch):
    """This command builds its own `Delivery`, and a fresh one says it is crash
    safe until something proves otherwise — the consumer learns that from
    `reclaim()`, which a probe must not call. Reporting the default would tell an
    operator on a pre-6.2 Redis that messages survive a kill, which is the one
    thing they need to know is untrue."""

    def no_lmove(*args, **kwargs):
        msg = "unknown command 'LMOVE'"
        raise ResponseError(msg)

    redis_server.set(f'{QUEUE}:heartbeat:mine', str(int(time.time())))
    monkeypatch.setattr(redis_server, 'lmove', no_lmove)
    out = StringIO()

    call_command('tgbot_healthcheck', stdout=out)

    reported = out.getvalue()
    assert 'at-most-once' in reported, reported
    assert 'at-least-once' not in reported, reported


@override_settings(TELEGRAM_BOT={**SETTINGS, 'WORKER_NAME': 'mine'})
def test_the_stranded_sweep_is_bounded_and_says_when_it_stopped_early(redis_server):
    """`MATCH` filters on the server, but `SCAN` walks the whole keyspace.

    The compose recipe runs this probe every thirty seconds, and the settings
    page suggests sharing one Redis with a cache backend — so an unbounded sweep
    is a full pass over someone else's keys twice a minute. It stops instead, and
    a count it cannot stand behind is reported as a floor rather than a total.
    """
    redis_server.set(f'{QUEUE}:heartbeat:mine', str(int(time.time())))
    redis_server.rpush(f'{QUEUE}:processing:gone', b'{}')
    # more keys than the bound can reach at a hundred a round
    for index in range(4000):
        redis_server.set(f'unrelated:{index}', b'x')
    out = StringIO()

    call_command('tgbot_healthcheck', stdout=out)

    reported = out.getvalue()
    assert 'healthy' in reported
    assert 'at least' in reported, reported


@override_settings(TELEGRAM_BOT={**SETTINGS, 'WORKER_NAME': 'mine'})
def test_a_scan_that_fails_does_not_make_the_container_unhealthy(redis_server, monkeypatch):
    """The probe answers about this worker. A scan it could not finish is not a
    reason to restart a container that is doing its job."""
    redis_server.set(f'{QUEUE}:heartbeat:mine', str(int(time.time())))

    def refuse(*args, **kwargs):
        raise RedisError('NOPERM')

    # the method the sweep actually calls: patching scan_iter left the handler
    # below unexercised while the test went on passing
    monkeypatch.setattr(redis_server, 'scan', refuse)
    out = StringIO()

    call_command('tgbot_healthcheck', stdout=out)

    assert 'healthy' in out.getvalue()
    # nothing was scanned, so there is no floor to report and nothing to hedge about
    assert 'in flight' not in out.getvalue(), out.getvalue()


@override_settings(TELEGRAM_BOT={**SETTINGS, 'WORKER_NAME': 'mine'})
def test_a_sweep_that_stops_partway_reports_what_it_did_see(redis_server, monkeypatch):
    """A count found before the failure is worth having, said as a floor.

    The old command threw it away and printed the healthy line alone, which reads as
    *no stranded messages* — the one conclusion a partial sweep cannot support. What it
    must not do is claim the number is complete, so the wording says `at least`.
    """
    redis_server.set(f'{QUEUE}:heartbeat:mine', str(int(time.time())))
    redis_server.rpush(f'{QUEUE}:processing:gone', b'{}', b'{}')
    rounds = []
    real_scan = redis_server.scan

    def once_then_refuse(*args, **kwargs):
        rounds.append(1)
        if len(rounds) > 1:
            raise RedisError('NOPERM')
        # a non-zero cursor, so the sweep comes back for a second round it will not get
        return 99, real_scan(*args, **kwargs)[1]

    monkeypatch.setattr(redis_server, 'scan', once_then_refuse)
    out = StringIO()

    call_command('tgbot_healthcheck', stdout=out)

    printed = out.getvalue()
    assert 'healthy' in printed, printed
    assert 'at least 2 message(s) are in flight' in printed, printed


@override_settings(TELEGRAM_BOT={**SETTINGS, 'REDIS_URL': 'localhost:6379/0'})
def test_a_url_with_no_scheme_reads_as_an_unreachable_broker():
    """The other half of the empty-URL case, and the one the narrowing missed.

    `Redis.from_url` rejects a URL without a scheme with a plain `ValueError`, so
    catching `RedisError` and `ImproperlyConfigured` still left this one as a traceback
    from both entry points — while `except Exception` had always reported it as a line.
    A non-numeric `REDIS_TIMEOUT` arrives the same way.
    """
    report = check()

    assert not report.ok
    assert report.message.startswith('the broker is unreachable: '), report.message

    with pytest.raises(CommandError, match='the broker is unreachable'):
        healthcheck()


@override_settings(TELEGRAM_BOT={**SETTINGS, 'REDIS_TIMEOUT': 'soon'})
def test_an_unreadable_timeout_reads_as_an_unreachable_broker():
    """`read_timeout()` coerces the setting while the client is being built."""
    report = check()

    assert not report.ok
    assert report.message.startswith('the broker is unreachable: '), report.message


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_heartbeat_that_cannot_be_decoded_is_reported(redis_server, monkeypatch):
    """`decode_responses` in a shared URL makes redis-py decode this key for us.

    The settings page says one Redis often serves the cache backend too, and that is how
    `decode_responses` gets into the URL. The bytes under the heartbeat key are then
    decoded by the client, and a `UnicodeDecodeError` is a `ValueError` — not a
    `RedisError`, so narrowing the guard turned a readable refusal into a traceback.
    """

    class CannotDecode:
        def ping(self):
            return True

        def get(self, *args, **kwargs):
            b'\xff'.decode('utf-8')

        def __getattr__(self, name):
            return getattr(redis_server, name)

    monkeypatch.setattr('django_aiogram.healthcheck.get_redis', CannotDecode)
    monkeypatch.setattr('django_aiogram.broker.redis_list.broker.get_redis', CannotDecode)

    with pytest.raises(CommandError, match='could not read the consumer liveness'):
        healthcheck()


@override_settings(
    TELEGRAM_BOT={
        **SETTINGS,
        'BROKER': 'django_aiogram.broker.redis_streams.RedisStreamsBroker',
        'REDIS_STREAM_KEY': 'TELEGRAM_BOT_STREAM',
    }
)
def test_probing_a_stream_creates_nothing(redis_server, monkeypatch):
    """Observation is read-only, and on this transport that had to be made true.

    Publishing and consuming create the consumer group; reading how much is waiting must not.
    A container healthcheck runs twice a minute in a fresh process, so a group-creating probe
    is a write per probe — refused outright on a read-only replica, and by an ACL user allowed
    to run `XINFO` and nothing else.

    Asserted on the command and on the answer. The answer here is a *refusal*, and it should
    be: nothing has ever read from this group, so no worker has started — the same verdict the
    list gives when no heartbeat has been written. What matters is that it refuses with that
    sentence rather than with an error about Redis, and that it did not make the group in order
    to find out.
    """
    created: list[str] = []
    monkeypatch.setattr(
        redis_server,
        'xgroup_create',
        lambda *args, **kwargs: created.append('xgroup_create'),
    )

    report = check()

    assert created == [], 'probing a stream created the consumer group'
    assert not report.ok, report.message
    assert 'no consumer has joined the group' in report.message, report.message


@override_settings(
    TELEGRAM_BOT={
        **SETTINGS,
        'BROKER': 'django_aiogram.broker.redis_streams.RedisStreamsBroker',
        'REDIS_STREAM_KEY': 'TELEGRAM_BOT_STREAM',
    }
)
def test_probing_a_working_stream_writes_nothing_either(redis_server, monkeypatch):
    """The same property, on the path that reaches every read the probe makes.

    The case above stops at liveness, because a group nobody has joined is a refusal — so it
    says nothing about the depth read behind it. Here a worker has been and gone: liveness
    answers with an age, the depth is reached, and neither may create anything.

    The group is made through the broker's own producer and consumer, which is where creating
    it belongs. Everything after that is observation.
    """
    from django_aiogram.broker.redis_streams import RedisStreamsBroker

    broker = RedisStreamsBroker()
    broker.publish([b'{}'])
    assert broker.take_nowait() is not None, 'the fixture did not deliver anything'

    created: list[str] = []
    monkeypatch.setattr(
        redis_server,
        'xgroup_create',
        lambda *args, **kwargs: created.append('xgroup_create'),
    )

    report = check()

    assert created == [], 'probing an established stream created a group'
    assert report.ok, report.message
    assert 'queued' in report.message, report.message


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_fractional_age_over_the_limit_is_refused(redis_server, monkeypatch):
    """5.9 seconds is over a 5-second limit, and truncating hid that.

    The message keeps whole seconds because that is what an operator set, but the comparison
    cannot: `int(5.9)` is 5, so the probe used to accept a consumer already past its limit —
    for the whole second before the age rolled over.
    """
    from django_aiogram.broker.models import Liveness
    from django_aiogram.broker.redis_list import RedisListBroker

    monkeypatch.setattr(RedisListBroker, 'liveness', lambda self: Liveness(reported=True, age=5.9))

    report = check(max_age=5)

    assert not report.ok, report.message
    assert 'over the 5s limit' in report.message, report.message


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_transport_that_cannot_answer_liveness_is_not_called_dead(redis_server, monkeypatch):
    """`reported=False` is "nobody outside can see this", and that is not a failure.

    The contract allows a transport whose group membership *is* the liveness signal and which
    therefore writes nothing an outside probe can read. Judging that as a stale heartbeat would
    report every such deployment as unhealthy for ever; treating it as an age of zero would be
    worse, because the line would claim a consumer had just checked in.

    So the probe says so and the depth carries the verdict, which is the only thing left to
    look at.
    """
    from django_aiogram.broker.models import Liveness
    from django_aiogram.broker.redis_list import RedisListBroker

    monkeypatch.setattr(
        RedisListBroker,
        'liveness',
        lambda self: Liveness(reported=False, age=None, detail='this transport tracks its own consumers'),
    )

    report = check()

    assert report.ok, report.message
    assert 'not observable from outside' in report.message, report.message


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_liveness_that_is_too_old_still_refuses(redis_server, monkeypatch):
    """The other half of the same branch: an age *is* judged, whoever measured it.

    Without this the case above could pass on a probe that had stopped judging liveness
    altogether — the same green for "nothing to look at" and for "nobody has reported in an
    hour".
    """
    from django_aiogram.broker.models import Liveness
    from django_aiogram.broker.redis_list import RedisListBroker

    monkeypatch.setattr(RedisListBroker, 'liveness', lambda self: Liveness(reported=True, age=3600.0))

    report = check()

    assert not report.ok
    assert 'over the' in report.message, report.message


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_container_form_neither_scans_nor_writes(redis_server, monkeypatch):
    """The one place the two entry points differ, and the reason the split exists.

    A healthcheck runs twice a minute. `_stranded` is up to twenty `SCAN` rounds over a
    keyspace the settings page says is often shared with a cache backend, and
    `_guarantee` is a write — a no-op `LMOVE` on a missing key, but a write, which a
    read-only replica refuses outright. Neither can change the verdict, so neither
    belongs on that path by default.

    Asserted on the calls, not on the output: a message that happens not to mention
    stranded lists proves only that none were found.
    """
    # written before the recorder is installed, or the probe gets blamed for the heartbeat
    # this test put there
    redis_server.set(HEARTBEAT, str(int(time.time())))
    calls: list[str] = []
    # every write the probe could reach for, not the two that existed when this was written:
    # asking the broker for liveness and depth put `XGROUP CREATE` on this path, and a test
    # watching `scan` and `lmove` alone called that clean
    for name in ('scan', 'lmove', 'xgroup_create', 'xadd', 'set', 'xack', 'xclaim', 'xautoclaim'):
        original = getattr(redis_server, name)

        def recording(*args, _name=name, _original=original, **kwargs):
            """Note that this command was issued, then let it through."""
            calls.append(_name)
            return _original(*args, **kwargs)

        monkeypatch.setattr(redis_server, name, recording)

    report = check()

    assert report.ok, report.message
    assert calls == [], f'the container form paid for {sorted(set(calls))}'
    # the whole line, not `'once' not in report.message`: `_guarantee` answers `unknown`
    # on a read-only replica, so that substring is absent from a probe that did run.
    #
    # The age is matched rather than compared: it is wall-clock seconds, so a run that
    # crosses a second boundary between the write above and the read reports `1s` and the
    # equality failed on CI for a probe that was working perfectly. The digits are not
    # what this test is about — the shape of the line is
    assert re.fullmatch(r'healthy: consumer \d+s old, 0 queued', report.message), report.message
    assert report.warnings == (), report.warnings


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_the_management_command_still_scans_and_reports_the_guarantee(redis_server, monkeypatch):
    """The other half: the command's output must not change for anyone using it.

    Without this, moving the defaults to off would silently take the guarantee line and
    the stranded warning out of a command people read by hand — which is the sort of
    quiet removal a changelog entry cannot make up for. Both halves are asserted on the
    output as well as on the calls, because a `SCAN` that is issued and then not reported
    is the same loss to the person reading it.
    """
    redis_server.rpush(f'{QUEUE}:processing:gone', b'{}')
    calls: list[str] = []
    for name in ('scan', 'lmove'):
        original = getattr(redis_server, name)

        def recording(*args, _name=name, _original=original, **kwargs):
            """Note that this command was issued, then let it through."""
            calls.append(_name)
            return _original(*args, **kwargs)

        monkeypatch.setattr(redis_server, name, recording)

    redis_server.set(HEARTBEAT, str(int(time.time())))
    out = StringIO()
    call_command('tgbot_healthcheck', stdout=out)

    assert 'at-least-once' in out.getvalue(), out.getvalue()
    assert '1 message(s) are in flight' in out.getvalue(), out.getvalue()
    assert 'scan' in calls, f'the command stopped scanning: {calls}'
    assert 'lmove' in calls, f'the command stopped probing the guarantee: {calls}'


@override_settings(TELEGRAM_BOT={**SETTINGS, 'REDIS_URL': ''})
def test_a_missing_redis_url_reads_as_an_unreachable_broker():
    """A client that cannot be built is a transport this probe cannot reach.

    `build_client` raises `ImproperlyConfigured` on an empty `REDIS_URL`, which is not a
    `RedisError` — so narrowing the guard from `except Exception` turned a readable line
    into a traceback, and turned the command's `CommandError` into an
    `ImproperlyConfigured`.

    The sentence is the one compose logs have shown for three releases; only its subject
    changed. `redis is unreachable` became `the broker is unreachable` in 4.0, because the
    client is built on the liveness call now and a deployment on another transport has no
    Redis to name — so a log line matching on `unreachable` still matches, and one matching
    on `redis is` does not. That is in `Upgrading.md`.
    """
    report = check()

    assert not report.ok
    assert report.message.startswith('the broker is unreachable: '), report.message
    assert 'REDIS_URL' in report.message

    with pytest.raises(CommandError, match='the broker is unreachable'):
        healthcheck()


@override_settings(TELEGRAM_BOT={**SETTINGS, 'ENABLED': False})
def test_a_disabled_process_is_not_unhealthy_and_is_not_reported_as_healthy():
    """Documented on the Deployment page and, until now, tested nowhere.

    Two things about it. It exits 0, because nothing is meant to be running here — and
    it says so *plainly*: the message goes through `self.style.SUCCESS` for a healthy
    bot and must not for this one, which examined nothing. `Report.checked` carries that
    distinction rather than the wrapper sniffing the string.
    """
    report = check()

    assert report.ok, report.message
    assert report.checked is False, 'a disabled process examined nothing, so it cannot claim to have'
    assert report.message == 'disabled in this process; nothing to check'

    out = StringIO()
    # force_color, not no_color=False: `self.style` is a no-op when the stream is not a
    # tty, so a StringIO cannot tell a styled write from a plain one otherwise — which
    # is how the first version of this test passed with the styling put back
    call_command('tgbot_healthcheck', stdout=out, force_color=True)

    assert out.getvalue().strip() == report.message, repr(out.getvalue())
    assert '\x1b[' not in out.getvalue(), 'the disabled line was colored as a success'


@override_settings(TELEGRAM_BOT=SETTINGS)
def test_a_healthy_process_is_reported_in_success_green(redis_server):
    """The other half of the distinction `Report.checked` exists to carry.

    Without this the styling could be dropped from the command outright and the suite
    would stay green — the disabled case asserts only that *its* line is plain, which is
    also true of a command that has stopped colouring anything. Then `checked` becomes a
    field nothing reads.
    """
    redis_server.set(HEARTBEAT, str(int(time.time())))
    out = StringIO()

    call_command('tgbot_healthcheck', stdout=out, force_color=True)

    assert 'healthy: consumer' in out.getvalue(), out.getvalue()
    assert '\x1b[' in out.getvalue(), 'the healthy line lost its success colour'


@override_settings(TELEGRAM_BOT={**SETTINGS, 'HEARTBEAT_INTERVAL': 10})
def test_the_missing_heartbeat_message_names_the_limit_it_judged_by(redis_server):
    """`--max-age 600` and a message saying "within 30s" send an operator to the wrong
    number — and the one it named was a default the flag had already overridden."""
    report = check(max_age=600)

    assert not report.ok
    assert 'within 600s' in report.message, report.message
    assert 'within 30s' not in report.message, report.message


def test_a_probe_with_no_settings_module_says_so_instead_of_raising(monkeypatch, capsys):
    """The one failure this form meets that the management command cannot.

    `manage.py` sets `DJANGO_SETTINGS_MODULE` with `os.environ.setdefault` *inside its
    own process*, and a healthcheck is a different process — so a container that runs
    `manage.py` may never export it, and the recipe on the Deployment page has to put it
    in `environment:`. Without it the probe used to answer with a traceback: exit 1, so
    Docker read unhealthy, from a probe whose whole job is to say *why*.
    """

    def unreadable(*args, **kwargs):
        message = (
            'Requested setting TELEGRAM_BOT, but settings are not configured. You must '
            'either define the environment variable DJANGO_SETTINGS_MODULE or call '
            'settings.configure() before accessing settings.'
        )
        raise ImproperlyConfigured(message)

    monkeypatch.setattr('django_aiogram.healthcheck.check', unreadable)

    code = main([])

    assert code == 1
    captured = capsys.readouterr()
    assert captured.out == '', captured.out
    assert captured.err.startswith('cannot read the settings: '), captured.err
    assert 'DJANGO_SETTINGS_MODULE' in captured.err


@pytest.mark.parametrize('flag', ['--max-queue', '--max-age'])
def test_both_entry_points_declare_the_same_limits(flag):
    """One form silently defaulting differently from the other is the drift to prevent.

    Both take these from `add_limit_flags`, so this fails the moment either restates a
    flag: a reader comparing `--help` of the two forms is entitled to the same answer,
    and `check()` owns what the default means.
    """
    command = TgbotHealthcheck().create_parser('manage.py', 'tgbot_healthcheck')

    module_action = next(a for a in build_parser()._actions if flag in a.option_strings)
    command_action = next(a for a in command._actions if flag in a.option_strings)

    assert module_action.default is command_action.default is None
    assert module_action.type is command_action.type is int
    assert module_action.help == command_action.help


def test_a_probe_with_a_mistyped_settings_module_says_so_instead_of_raising(monkeypatch, capsys):
    """The recipe asks an operator to write that module name by hand.

    A typo is then at least as likely as a missing variable, and it arrives as
    `ModuleNotFoundError` rather than `ImproperlyConfigured` — so guarding only the
    latter left the likelier mistake answering with a traceback. Raised with `name=`,
    which is what the import machinery sets and what tells this apart from the case
    below; the first version of this test left it unset and so asserted on a shape no
    real failure has.
    """

    def mistyped(*args, **kwargs):
        raise ModuleNotFoundError("No module named 'core.settingz'", name='core.settingz')

    monkeypatch.setenv('DJANGO_SETTINGS_MODULE', 'core.settingz')
    monkeypatch.setattr('django_aiogram.healthcheck.check', mistyped)

    code = main([])

    assert code == 1
    captured = capsys.readouterr()
    assert captured.out == '', captured.out
    assert captured.err == "cannot read the settings: No module named 'core.settingz'\n", captured.err


def test_a_dependency_the_settings_module_imports_keeps_its_traceback(monkeypatch):
    """Our own failure is ours to summarize into one line. This one is not.

    A settings module that imports something uninstalled raises `ModuleNotFoundError`
    too, and flattening it would report `cannot read the settings: No module named 'yaml'`
    with no hint of where the import was — for a fault that is not the healthcheck's and
    is not fixed by anything on the Deployment page.
    """

    def missing_dependency(*args, **kwargs):
        raise ModuleNotFoundError("No module named 'yaml'", name='yaml')

    monkeypatch.setenv('DJANGO_SETTINGS_MODULE', 'core.settings')
    monkeypatch.setattr('django_aiogram.healthcheck.check', missing_dependency)

    with pytest.raises(ModuleNotFoundError, match='yaml'):
        main([])


def test_a_settings_module_whose_parent_package_is_missing_is_still_ours(monkeypatch, capsys):
    """`DJANGO_SETTINGS_MODULE=coree.settings` reports the missing *parent*.

    So comparing the name for equality alone would send the commonest typo of all — one
    in the package part — out as a traceback.
    """

    def missing_parent(*args, **kwargs):
        raise ModuleNotFoundError("No module named 'coree'", name='coree')

    monkeypatch.setenv('DJANGO_SETTINGS_MODULE', 'coree.settings')
    monkeypatch.setattr('django_aiogram.healthcheck.check', missing_parent)

    assert main([]) == 1
    assert capsys.readouterr().err == "cannot read the settings: No module named 'coree'\n"


@override_settings(TELEGRAM_BOT={**SETTINGS, 'WORKER_NAME': 'mine'})
def test_a_key_that_cannot_be_decoded_does_not_abort_the_sweep(redis_server, caplog):
    """The sweep is the one part that must never fail the container over what it found.

    A foreign key on a Redis shared with a cache backend can match this pattern and hold
    bytes that are not UTF-8. Decoding it raised straight out of `check()` — a traceback
    and an unhealthy container, from an optional warning nobody acts on.
    """
    redis_server.set(f'{QUEUE}:heartbeat:mine', str(int(time.time())))
    redis_server.rpush(f'{QUEUE}:processing:'.encode() + b'\xff\xfe', b'{}')

    report = check(stranded=True)

    assert report.ok, report.message
    assert 'healthy' in report.message, report.message
    assert 'could not scan for stranded in-flight lists' in caplog.text


@override_settings(TELEGRAM_BOT={**SETTINGS, 'WORKER_NAME': 'mine'})
def test_the_verdict_is_flushed_before_anything_qualifies_it(redis_server, monkeypatch):
    """`docker inspect` merges the two streams, and stdout is the buffered one.

    Written to a pipe, stdout is block-buffered while stderr is not, so an unflushed
    verdict arrives *after* the warning about it — the qualifier reaching the operator
    ahead of the thing it qualifies.
    """
    redis_server.set(f'{QUEUE}:heartbeat:mine', str(int(time.time())))
    redis_server.rpush(f'{QUEUE}:processing:gone', b'{}')
    events = []

    class Recording:
        def __init__(self, name):
            self.name = name

        def write(self, text):
            events.append((self.name, 'write'))

        def flush(self):
            events.append((self.name, 'flush'))

    monkeypatch.setattr('sys.stdout', Recording('out'))
    monkeypatch.setattr('sys.stderr', Recording('err'))

    assert main(['--stranded']) == 0

    assert events[0] == ('out', 'write'), events
    assert ('err', 'write') in events, events
    assert events.index(('out', 'flush')) < events.index(('err', 'write')), events


@override_settings(TELEGRAM_BOT={**SETTINGS, 'HEARTBEAT_INTERVAL': 'abc'})
def test_an_unreadable_interval_is_refused_in_a_line():
    """This entry point is the one where `manage.py check` never ran.

    `E023` and `E024` catch these values — under `django.setup()`, which the container
    form skips by design. A value like `os.environ.get('HB', '')` written straight into
    the settings dict therefore reaches `int()` with nothing in between, and a traceback
    from a probe is a container that says unhealthy without saying why.
    """
    report = check()

    assert not report.ok
    assert report.message == "TELEGRAM_BOT['HEARTBEAT_INTERVAL'] is not a number: 'abc'", report.message


@override_settings(TELEGRAM_BOT={**SETTINGS, 'HEALTHCHECK_MAX_QUEUE': 'lots'})
def test_an_unreadable_queue_limit_is_refused_in_a_line():
    """The other one read before Redis is touched. A flag still overrides it."""
    report = check()

    assert not report.ok
    assert 'HEALTHCHECK_MAX_QUEUE' in report.message, report.message


@override_settings(TELEGRAM_BOT={**SETTINGS, 'HEARTBEAT_INTERVAL': 10})
def test_a_limit_over_the_keys_ttl_says_it_cannot_be_observed(redis_server):
    """`--max-age 60` against a key that expires at 30s can never see a stale heartbeat.

    The Deployment page published exactly that recipe. Raising the limit past the TTL does
    nothing at all unless `HEARTBEAT_INTERVAL` goes up too, and the refusal is the only
    place an operator is looking when they find out.
    """
    over = check(max_age=60)
    within = check(max_age=20)

    assert not over.ok, over.message
    assert not within.ok, within.message
    assert 'cannot be observed anyway' in over.message, over.message
    assert "raise TELEGRAM_BOT['HEARTBEAT_INTERVAL'] instead" in over.message, over.message
    assert 'cannot be observed' not in within.message, within.message


@override_settings(TELEGRAM_BOT={**SETTINGS, 'WORKER_NAME': 'mine'})
def test_the_sweep_matches_the_keys_workers_actually_write(redis_server):
    """Derived from `processing_key`, not spelled out a second time.

    A probe holding its own copy of the scheme keeps scanning the old one after a rename:
    every failing test would be one that hardcodes the literal, so updating those is how
    the rename gets done — and the probe reports no stranded messages for ever, green.
    """
    from django_aiogram.redis import processing_key

    redis_server.set(f'{QUEUE}:heartbeat:mine', str(int(time.time())))
    redis_server.rpush(processing_key('gone'), b'{}')

    report = check(stranded=True)

    assert report.ok, report.message
    assert len(report.warnings) == 1, report.warnings
    assert '1 message(s) are in flight' in report.warnings[0], report.warnings


@override_settings(TELEGRAM_BOT={**SETTINGS, 'WORKER_NAME': 'mine'})
def test_a_sweep_that_could_not_run_says_so_rather_than_reporting_a_clean_zero(redis_server, monkeypatch):
    """A sweep that never happened must not read like one that found nothing.

    The two are the same line otherwise: the caller warns on a non-zero count, so a client
    that could not be built returned zero, warned nobody, and left the report saying the
    healthy thing about a question nothing had asked. Which is the failure this whole change
    is about, one layer in -- the driver being an extra is exactly how the client stops
    being available.

    Arranged one-sided on purpose: the broker's own accessor keeps working, so the verdict
    is reached and the report is healthy. Only the Redis-shaped extra is unavailable, and
    the reason travels into the warning where an operator reads it.
    """
    redis_server.set(f'{QUEUE}:heartbeat:mine', str(int(time.time())))

    def cannot_build():
        raise ImproperlyConfigured('REDIS_URL is empty')

    monkeypatch.setattr('django_aiogram.healthcheck.get_redis', cannot_build)

    report = check(stranded=True)

    assert report.ok, report.message
    assert len(report.warnings) == 1, report.warnings
    assert 'did not finish' in report.warnings[0], report.warnings
    # the reason, not just the fact: `--stranded` on an install with no driver is the case
    # this exists for, and "something went wrong" would send the operator to the logs
    assert 'REDIS_URL is empty' in report.warnings[0], report.warnings


@override_settings(TELEGRAM_BOT={**SETTINGS, 'WORKER_NAME': 'mine'})
def test_a_sweep_that_finished_adds_no_second_warning(redis_server):
    """The control, and it is the one that keeps the case above from being free.

    A warning appended unconditionally would satisfy every assertion up there while adding
    a line to every healthy probe on the list transport -- twice a minute, for nothing.
    """
    redis_server.set(f'{QUEUE}:heartbeat:mine', str(int(time.time())))

    report = check(stranded=True)

    assert report.ok, report.message
    assert report.warnings == (), report.warnings


# one key per metacharacter `_escaped` quotes, and a decoy for the two that would
# otherwise match their own literal: unescaped, `TG?one` and `TG*all` are patterns that
# also select the decoy, and only a second list makes that visible
@pytest.mark.parametrize(
    ('key', 'decoy'),
    [
        ('TG_OK', None),
        ('TG[prod]', None),
        ('TG?one', 'TGXone'),
        ('TG*all', 'TGXall'),
        ('TG\\path', None),
    ],
)
def test_a_queue_key_with_glob_characters_is_still_swept(redis_server, key, decoy):
    """`SCAN MATCH` takes a glob and a queue key is an operator's string.

    `REDIS_MESSAGES_KEY = 'tg[staging]'` became a character class matching nothing this
    package writes, so the sweep found zero stranded lists — and called the scan
    *complete*, which is the one answer a wrong pattern must not give.

    Missing it in the other direction is just as wrong, and needs the decoy: unescaped,
    `TG?one` and `TG*all` still match the key they came from, so those two cases passed
    against the unescaped version. With a foreign list one character away, the unescaped
    pattern collects it and the count says so — a queue reporting another queue's
    messages as its own in flight.
    """
    with override_settings(TELEGRAM_BOT={**SETTINGS, 'WORKER_NAME': 'mine', 'REDIS_MESSAGES_KEY': key}):
        from django_aiogram.healthcheck import _stranded
        from django_aiogram.redis import processing_key

        redis_server.rpush(processing_key('gone'), b'{}')
        if decoy:
            redis_server.rpush(f'{decoy}:processing:gone', b'{}')

        sweep = _stranded()

        assert (sweep.found, sweep.complete) == (1, True), f'the sweep answered wrongly under {key!r}'


@pytest.mark.parametrize('value', [float('inf'), float('nan'), 'nine', {}], ids=repr)
def test_a_setting_that_is_not_a_number_is_reported_not_raised(value):
    """The probe's whole contract is to report, and `int()` has three ways to refuse.

    `TypeError` and `ValueError` were caught; `int(float('inf'))` raises `OverflowError`, which is
    neither -- so a container health check ended in a traceback rather than the sentence it has
    ready, and an operator reading `docker logs` saw a crash where the message names the setting.

    `nan` is in the cases because it is the one that gets *through* `int()`: it raises `ValueError`
    there, which was already caught, and the case is here so the widened clause cannot quietly
    become "catch everything and hope".
    """
    with override_settings(TELEGRAM_BOT={**SETTINGS, 'HEARTBEAT_INTERVAL': value}):
        report = check(max_age=None, max_queue=None, guarantee=False, stranded=False)

    assert report.ok is False, report
    assert 'HEARTBEAT_INTERVAL' in report.message, report.message
    assert 'is not a number' in report.message, report.message


def test_the_probe_still_runs_on_an_install_with_no_driver():
    """The probe a container runs on a timer, on a deployment that has no redis-py.

    `redis-py` became an extra in 4.0, so a RabbitMQ or Kafka install carries none of it — and
    this module named `redis` at import time, which meant `python -m django_aiogram.healthcheck`
    could not start there at all. Measured before the fix, with `redis` hidden:
    `ModuleNotFoundError` out of `healthcheck.py`, at the `from redis import Redis` line, before
    a single check ran. The one thing that would have said why the container was unhealthy was
    the thing that crashed.

    A subprocess with `redis` hidden from `sys.modules`, because assigning `None` there makes
    `import redis` raise the way a missing distribution does. The suite cannot arrange it any
    other way: the dev venv installs every extra, and the `[redis]`-only leg in CI installs that
    one.

    Asserted through `main`, which is what the container calls, and on the report rather than on
    the import alone — starting and then failing to say anything is the same outage with a
    tidier traceback. The settings here name the Redis transport, so the honest report is the
    refusal that names the missing extra; a transport whose driver *is* present reports a real
    verdict, measured on a Kafka-only install: `healthy: consumer not observable from outside`.
    """
    script = textwrap.dedent("""
        import sys

        sys.modules['redis'] = None
        sys.modules['redis.exceptions'] = None

        import django

        django.setup()

        from django_aiogram.healthcheck import main

        code = main([])
        print('exit', code)
    """)
    finished = run_python(script, env={'DJANGO_SETTINGS_MODULE': 'tests.settings'})

    assert finished.returncode == 0, finished.stderr
    assert 'Traceback' not in finished.stderr, finished.stderr
    # the refusal names the package and the extra that installs it, which is the whole point of
    # reporting instead of crashing: the container log says what to do. On stderr, where every
    # unhealthy verdict this module writes goes
    assert "needs the 'redis' package" in finished.stderr, finished.stderr
    assert 'django-aiogram[redis]' in finished.stderr, finished.stderr
    # and it is a verdict rather than a crash: the code came back from `main`, so the container
    # sees the same exit status an unreachable broker gives it
    assert 'exit 1' in finished.stdout, finished.stdout + finished.stderr


def test_the_command_reports_a_missing_driver_instead_of_tracebacking():
    """The same refusal, in the shape a management command reports with.

    The class the module form fixed is not the module's alone: `manage.py tgbot_healthcheck`
    calls the same `check`, so a `BROKER` whose driver this install does not carry gave it a
    bare `BrokerDependencyError` and a traceback — from a command whose whole output is a
    diagnosis. A `CommandError` keeps the install line on one line and the exit status
    non-zero.

    The exception is raised rather than arranged for real, because arranging it means hiding
    `redis` from a running interpreter and this suite needs it for the next case. What it
    stands in for is measured in the case above.
    """
    refusal = BrokerDependencyError('KafkaBroker', 'aiokafka', 'kafka')

    with (
        patch('django_aiogram.management.commands.tgbot_healthcheck.check', side_effect=refusal),
        pytest.raises(CommandError) as caught,
    ):
        call_command('tgbot_healthcheck')

    assert "needs the 'aiokafka' package" in str(caught.value)
    assert 'django-aiogram[kafka]' in str(caught.value)
