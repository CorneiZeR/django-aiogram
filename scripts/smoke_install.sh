#!/usr/bin/env bash
# Install the built wheel into a throwaway Django project and check that a
# project boots with no credentials at all.
#
# The unit suite imports from the source tree, so it cannot catch a packaging
# mistake: a missing py.typed, a module that only resolves because `src/` is on
# the path. This does.
set -euo pipefail

# a PYTHONPATH pointing at src/ would let imports resolve from the source tree,
# so a wheel missing a module would still pass — the one thing this must catch
unset PYTHONPATH PYTHONHOME MYPYPATH

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# a directory of already-built artifacts, when the caller has one. The release workflow
# does: it builds once, uploads that wheel, and this must check *that* file rather than
# one built again here — same source, but not the same bytes, and the bytes are what
# PyPI keeps for ever
if [ -n "${1:-}" ]; then
    echo "--- using the artifacts already built in $1"
    # absolute, and that is the whole point of the `cd`: every step below runs from a
    # throwaway project directory, so a relative `dist/...` — which is exactly what the
    # release workflow passes — stops resolving the moment the script moves. It failed
    # there and nowhere else, because this argument is used by the release build alone
    given="$(cd "$1" && pwd)"
    wheel="$(ls "$given"/*.whl)"
    sdist="$(ls "$given"/*.tar.gz)"
else
    echo "--- building the wheel and the sdist"
    python -m build --sdist --wheel --outdir "$work/dist" "$root" >/dev/null
    wheel="$(ls "$work"/dist/*.whl)"
    sdist="$(ls "$work"/dist/*.tar.gz)"
fi
echo "checking $(basename "$wheel") and $(basename "$sdist")"

echo "--- the sdist must carry what a rebuild and a review need"
python - "$sdist" <<'PY'
import sys, tarfile

with tarfile.open(sys.argv[1]) as archive:
    # the leading directory is <name>-<version>/, which no expectation should
    # depend on, so compare on what follows it
    names = {name.split('/', 1)[1] for name in archive.getnames() if '/' in name}
for expected in (
    'src/django_aiogram/__init__.py',
    'tests/test_public_surface.py',
    'docs/wiki/Delivery.md',
    'scripts/smoke_install.sh',
    'CONTRIBUTING.md',
    'SECURITY.md',
    'AGENTS.md',
):
    assert expected in names, f'{expected} missing from the sdist'
print('sources, tests, docs and the contributor files all travel with the sdist')
PY

echo "--- the wheel must carry what a consumer needs"
python - "$wheel" <<'PY'
import sys, zipfile

names = set(zipfile.ZipFile(sys.argv[1]).namelist())
for expected in (
    'django_aiogram/py.typed',
    'django_aiogram/api.py',
    'django_aiogram/management/commands/start_tgbot.py',
    'django_aiogram/migrations/__init__.py',
    'django_aiogram/migrations/0001_initial.py',
    # every migration, not just the first: 3.1.0 ships 0002 and the Upgrading page
    # tells operators to run it, so a wheel that dropped it would break the upgrade
    # it documents
    'django_aiogram/migrations/0002_kind_id_index.py',
):
    assert expected in names, f'{expected} missing from the wheel'
# 3.0 removed the 1.x package name; packaging it again would silently recreate
# the site-packages collision the rename was made to fix
shim = [name for name in names if name == 'telegram_bot.py' or name.startswith('telegram_bot/')]
assert not shim, f'the removed telegram_bot package is back in the wheel: {shim}'
print('py.typed and the management command are present; the 1.x name is absent')
PY

echo "--- installing the base wheel, with no extra"
python -m venv "$work/venv"
"$work/venv/bin/pip" install -q --upgrade pip
"$work/venv/bin/pip" install -q "$wheel"

mkdir -p "$work/project"
cat > "$work/project/settings.py" <<'PY'
SECRET_KEY = 'smoke'
INSTALLED_APPS = ['django.contrib.contenttypes', 'django.contrib.auth', 'django_aiogram']
DATABASES = {'default': {'ENGINE': 'django.db.backends.sqlite3', 'NAME': 'smoke.sqlite3'}}
USE_TZ = True
PY
cat > "$work/project/manage.py" <<'PY'
import os, sys
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')
from django.core.management import execute_from_command_line
execute_from_command_line(sys.argv)
PY

cd "$work/project"

# Since 4.0 no transport driver is a dependency, so this is the install a reader gets from
# `pip install django-aiogram` — and the only place the promise around it can be checked:
# that the package imports without a driver, and that what a project hears instead is the
# name of the extra it needs. The unit suite cannot ask this. It runs with `redis` present,
# because everything else in it needs a server.
echo "--- the base install carries no driver"
"$work/venv/bin/python" - <<'PY'
import importlib.util

assert importlib.util.find_spec('redis') is None, 'the base install pulled a transport driver'
print('    redis is not installed')
PY

echo "--- and imports anyway, all the way to a send that names the extra"
DJANGO_SETTINGS_MODULE=settings DJANGO_AIOGRAM_TOKEN=42:x "$work/venv/bin/python" - <<'PY'
import sys

import django

django.setup()

# the import a project writes, and the one that used to fail: a module-scope
# `from redis import Redis` anywhere on this path turns the message below into
# `ModuleNotFoundError: No module named 'redis'`
from django_aiogram import bot

assert 'redis' not in sys.modules, 'importing the package pulled the driver'

try:
    bot.send(chat_id=1, text='hi')
except Exception as error:  # noqa: BLE001 - the class is the package's own, asserted by name below
    assert type(error).__name__ == 'BrokerDependencyError', f'{type(error).__name__}: {error}'
    assert 'pip install "django-aiogram[redis]"' in str(error), str(error)
    print(f'    the send refused with the install line: {error}')
else:
    raise AssertionError('a send with no driver installed did not refuse')
PY

echo "--- the check names the missing extra, and fails the build"
check_status=0
"$work/venv/bin/python" manage.py check > "$work/base.out" 2>&1 || check_status=$?
sed 's/^/    /' "$work/base.out"
[ "$check_status" = 1 ] || { echo "    expected exit 1 with no driver, got $check_status"; exit 1; }
grep -q 'django_aiogram.E047' "$work/base.out" || { echo '    E047 was not reported'; exit 1; }
grep -q 'pip install "django-aiogram\[redis\]"' "$work/base.out" ||
    { echo '    the hint did not carry the install line'; exit 1; }

# and the probe, in the same window, because this is the only place an install with no
# driver at all exists: 4.0.0 named `redis` at module scope here, so on a Kafka or RabbitMQ
# image `python -m django_aiogram.healthcheck` -- the command Deployment puts in a compose
# healthcheck -- could not start, and the container was unhealthy for ever with a
# ModuleNotFoundError as its only explanation
echo "--- the healthcheck reports on an install with no driver"
probe_status=0
DJANGO_SETTINGS_MODULE=settings "$work/venv/bin/python" -m django_aiogram.healthcheck \
    > "$work/nodriver.out" 2> "$work/nodriver.err" || probe_status=$?
! grep -q 'ModuleNotFoundError' "$work/nodriver.err" ||
    { echo '    the probe could not start at all:'; sed 's/^/      /' "$work/nodriver.err"; exit 1; }
grep -q 'pip install "django-aiogram\[redis\]"' "$work/nodriver.err" ||
    { echo '    the refusal did not name the extra:'; sed 's/^/      /' "$work/nodriver.err"; exit 1; }
[ "$probe_status" = 1 ] || { echo "    expected exit 1 with no driver, got $probe_status"; exit 1; }
echo "    it named the extra and exited 1, rather than failing to import"

echo "--- installing the extra the hint asked for"
"$work/venv/bin/pip" install -q "$wheel[redis]"
"$work/venv/bin/python" -c "import redis; print('    redis', redis.__version__)"

echo "--- a project with neither TOKEN nor REDIS_URL"
echo "check:"
check_status=0
"$work/venv/bin/python" manage.py check > "$work/check.out" 2>&1 || check_status=$?
sed 's/^/    /' "$work/check.out"
[ "$check_status" = 0 ] || { echo "    the check still refuses once the extra is installed"; exit 1; }
! grep -q 'django_aiogram.E047' "$work/check.out" ||
    { echo '    E047 survived installing the extra'; exit 1; }
echo "    E047 is gone, and the credential warnings are all that is left"

echo "--- the shipped migration applies"
"$work/venv/bin/python" manage.py migrate --noinput 2>&1 | sed 's/^/    /'
"$work/venv/bin/python" manage.py makemigrations --check --dry-run 2>&1 | sed 's/^/    /'

echo "--- the healthcheck runs from an install, without django.setup()"
# the Deployment page tells readers to put this exact command in a compose file, and
# an install is the only place a packaging-level break in it shows up: a module missing
# from the wheel, or one that has grown an import needing the app registry
DJANGO_SETTINGS_MODULE=settings DJANGO_AIOGRAM_REDIS_URL=redis://127.0.0.1:1/0 \
    "$work/venv/bin/python" -m django_aiogram.healthcheck > "$work/probe.out" 2> "$work/probe.err" \
    && probe_status=0 || probe_status=$?
grep -q 'the broker is unreachable' "$work/probe.err" \
    || { echo "    the probe did not reach the broker at all:"; sed 's/^/      /' "$work/probe.err"; exit 1; }
[ "$probe_status" = 1 ] || { echo "    expected exit 1 from an unreachable Redis, got $probe_status"; exit 1; }
echo "    python -m django_aiogram.healthcheck refused an unreachable broker with exit 1"

# and the path a healthy container takes, which is the one compose depends on: a probe
# that only ever answered "unreachable" would pass the check above while returning
# non-zero for every healthy container, and nothing here would notice
if [ -n "${DJANGO_AIOGRAM_TEST_REDIS_URL:-}" ]; then
    # a reachable server is not a healthy bot: the probe also wants a heartbeat, which is
    # what a running consumer writes. Written here by hand, because starting a consumer
    # would need a Telegram token — and what is under test is the probe's success path,
    # not the consumer's
    DJANGO_AIOGRAM_REDIS_URL="$DJANGO_AIOGRAM_TEST_REDIS_URL" \
        "$work/venv/bin/python" - <<PROBE
import time

import redis

# the same write the consumer makes: an epoch second as text, with a TTL. The probe
# reads it as a timestamp, so a placeholder value reads as a worker last seen in 1970
client = redis.Redis.from_url("$DJANGO_AIOGRAM_TEST_REDIS_URL")
client.set('TELEGRAM_BOT_MESSAGE:heartbeat:smoke', str(int(time.time())), ex=60)
PROBE
    DJANGO_SETTINGS_MODULE=settings DJANGO_AIOGRAM_REDIS_URL="$DJANGO_AIOGRAM_TEST_REDIS_URL" \
        DJANGO_AIOGRAM_WORKER_NAME=smoke \
        "$work/venv/bin/python" -m django_aiogram.healthcheck > "$work/ok.out" 2> "$work/ok.err" \
        && healthy_status=0 || healthy_status=$?
    [ "$healthy_status" = 0 ] || {
        echo "    a reachable Redis still gave exit $healthy_status:"
        sed 's/^/      /' "$work/ok.err"
        exit 1
    }
    grep -q . "$work/ok.out" || { echo '    the healthy probe printed nothing on stdout'; exit 1; }
    echo "    and answered a reachable Redis with exit 0 and a line on stdout"
else
    echo "    (set DJANGO_AIOGRAM_TEST_REDIS_URL to check the success path too)"
fi

echo "--- the 1.x package name is gone from the installed environment"
"$work/venv/bin/python" - <<'PY'
import importlib.util

assert importlib.util.find_spec('telegram_bot') is None, 'telegram_bot is still importable'
print('    telegram_bot no longer resolves')
PY

echo "--- disabled, the command exits cleanly"
disabled_output="$(DJANGO_AIOGRAM_ENABLED=0 "$work/venv/bin/python" manage.py start_tgbot 2>&1)"
echo "$disabled_output" | sed 's/^/    /'
case "$disabled_output" in
  *disabled*) ;;
  *) echo "the command exited 0 but said nothing about being disabled" >&2; exit 1 ;;
esac

echo "--- types are visible to a consumer"
"$work/venv/bin/pip" install -q mypy
cat > uses_it.py <<'PY'
# typing.assert_type is 3.11 and up, and this script runs on whatever python a
# contributor has. mypy installs typing_extensions itself, so this adds nothing
import uuid

from typing_extensions import assert_type

from redis import Redis

from django_aiogram import bot
from django_aiogram.redis import redis_conn

def notify(chat_id: int) -> None:
    bot.send(chat_id=chat_id, text='hi')

async def notify_from_async(chat_id: int) -> None:
    # 3.1.0's own surface, checked against the installed package rather than the
    # checkout: `asend` returns the correlation id, `aqueue_depth` an int, and
    # `close` takes the drain budget as a float
    identifier = await bot.asend(chat_id=chat_id, text='hi')
    assert_type(identifier, uuid.UUID)
    assert_type(await bot.aqueue_depth(), int)
    bot.close(drain_timeout=0.5)

# redis_conn forwards through __getattr__, so it resolved to Any while get_redis() did not.
# Nothing is called here: this is the installed package's typing, checked without a server.
#
# Imported from `django_aiogram.redis` since 4.0 rather than from the package: the shortcut was
# removed, and this line is also the check that the move kept the annotation — a re-export is
# the easiest place to lose one
assert_type(redis_conn, Redis)
PY
"$work/venv/bin/mypy" --strict uses_it.py 2>&1 | sed 's/^/    /'

echo "--- the metadata a consumer resolves against"
"$work/venv/bin/python" - <<'META'
from importlib.metadata import metadata

fields = metadata('django-aiogram')
classifiers = fields.get_all('Classifier') or []
for expected in ('Framework :: AsyncIO', 'Framework :: Django :: 6.1', 'Typing :: Typed'):
    assert expected in classifiers, f'{expected} is missing from the wheel metadata'
extras = fields.get_all('Provides-Extra') or []
# every transport's refusal names the extra that fixes it, so an extra missing here turns that
# hint into a dead end — the one metadata field this release's install story rests on. One name
# per transport plus hiredis, which is the only optional speed-up
for expected in ('redis', 'hiredis', 'rabbitmq', 'kafka'):
    assert expected in extras, f'{expected} is missing from Provides-Extra: {extras}'
# dev and measure are for this repository, not for anyone installing the package: both are
# dependency groups, and a group that leaks into Provides-Extra is a packaging mistake
for refused in ('dev', 'measure'):
    assert refused not in extras, f'the {refused} group shipped as an extra: {extras}'
print('    classifiers, one extra per transport, and no dev or measure among them')
META

echo
echo "smoke install passed"
