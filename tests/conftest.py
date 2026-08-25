import fakeredis
import fakeredis.aioredis
import pytest

from django_aiogram import conf

# `django_aiogram.bot` is the singleton instance, so the class lives in
# `client`; patching the wrong one silently leaves the real connection in place.
PATCH_TARGETS = (
    'django_aiogram.redis.get_redis',
    # the consumer stopped holding a client in 4.0: it asks the broker, and the broker is
    # what holds one. Patching where the connection is *used* is the whole point of this
    # tuple, so the target moved with the use
    'django_aiogram.broker.redis_list.broker.get_redis',
    # every transport module gets its own entry, and a missing one fails in a way worth
    # knowing: a broker module imported lazily *during* a test binds that test's fake and
    # keeps it for the whole session, so later tests read entries the first one left behind
    'django_aiogram.broker.redis_streams.broker.get_redis',
    'django_aiogram.producer.client.get_redis',
    # `django_aiogram.get_redis` is not here any more: 4.0 stopped exporting a name for one
    # transport's client from a package that carries four, and the callers reach for
    # `django_aiogram.redis.get_redis` — the first entry above — instead
    # the probe's decision moved out of the command in 3.1.0, so it can run without
    # django.setup(); the command is a wrapper and holds no connection of its own
    'django_aiogram.healthcheck.get_redis',
    'django_aiogram.management.commands.tgbot_reclaim.get_redis',
)


@pytest.fixture(autouse=True)
def _uncached_settings():
    """Drop the settings cache after every test, whatever put something in it.

    `override_settings` fires `setting_changed` and the package resets on it, so those
    tests clean up after themselves. `monkeypatch.setenv` fires nothing: the variable is
    restored at teardown and the value read through it stays cached, so the next test to
    ask for that key gets an answer from an environment that no longer exists. Measured
    before this existed — a test setting `DJANGO_AIOGRAM_BLPOP_TIMEOUT=3` and calling
    `conf.reset()` left the next test reading 3 with the variable already gone.
    """
    yield
    conf.reset()


@pytest.fixture
def redis_server(monkeypatch):
    """Swap the shared connection for an in-memory one, sync and async alike.

    The async half patches `build_async_client` rather than `aget_redis`, so the
    per-loop registry runs for real and hands out fakes — patching the accessor
    would leave the thing under test untested. One `FakeServer` behind both, so a
    message queued through `asend` is visible to a synchronous read.
    """
    server = fakeredis.FakeServer()
    client = fakeredis.FakeRedis(server=server)
    for target in PATCH_TARGETS:
        monkeypatch.setattr(target, lambda *args, client=client, **kwargs: client)
    monkeypatch.setattr(
        'django_aiogram.redis.build_async_client',
        lambda server=server: fakeredis.aioredis.FakeRedis(server=server),
    )
    return client
