"""The surfaces that must not name one transport, pinned so a revert fails.

4.0 made the queue a setting, and the prose describing it had to follow — but prose has no
gate. Every claim in this file was true of a Redis-only package and false of this one, and
each was found by a reviewer rather than by the suite, one file at a time, because nothing
here could fail. So the class gets a test instead of another round of reading.

Two rules make it worth having rather than merely strict:

* it names *surfaces*, not files at large. A transport's own module is entitled to say Redis
  as often as it likes, and `Delivery.md` compares the four by name on purpose. What is
  pinned is the small set of places whose subject is the queue *in general* — the trust
  boundary, the serializer's warning about who can execute code, the settings table.
* it looks for Redis named as **the** queue, not for the word. "Redis with ``requirepass``,
  RabbitMQ with a user that is not ``guest``, Kafka with SASL" is exactly right and must
  keep passing; "the Redis behind it must stay inside your trust boundary" must not.

The whitespace of each file is flattened before matching, because prose wraps and a phrase
broken across a line break is one a plain search does not find. That is how several of these
survived earlier sweeps.
"""

import ast
import pathlib
import re

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent

#: files whose subject is the queue itself rather than one implementation of it
NEUTRAL_SURFACES = (
    'SECURITY.md',
    'README.md',
    'src/django_aiogram/wire/serializers.py',
    'src/django_aiogram/wire/envelope.py',
    'src/django_aiogram/producer/client.py',
    'docs/wiki/Sending-messages.md',
    'docs/wiki/Installation.md',
    # the wiki's own front page, added for the same reason the README is here: it is read
    # before any transport page and its subject is the queue in general
    'docs/wiki/Home.md',
    # the trust boundary is stated on this page, and it was stated as Redis's: whoever can
    # write to *the queue* runs code in the bot container, and that is true of all four
    'docs/wiki/Serialization.md',
    # the brief handed to a coding agent, which is the surface that propagates: it taught a
    # `redis-cli llen` as the way to read a depth, where `queue_depth()` asks any transport
    'docs/wiki/AI-assistants.md',
    # the kinds fire on four transports, and two rows of the table named one
    'docs/wiki/Event-log.md',
)

#: `config/defaults.py` is deliberately absent: it is the file that *names* the default
#: transport, so "defaulted to the Redis list" is the truth rather than a leftover, and a
#: guard here would have to be argued away every time. Its own over-claim -- `ENABLED`
#: described as gating everything -- is pinned by `test_enabled_flag.py`, which asserts the
#: behaviour instead of the wording

#: Redis presented as *the* queue rather than as one of the four. Each of these was a real
#: sentence in one of the files above, and each was reported by review rather than by a test
THE_ONLY_TRANSPORT = (
    # a call handed *to* a Redis list, which is where the README's first paragraph left the
    # reader for the whole of 4.0's development: every pattern below matched nothing there,
    # and it took the project's owner reading the published front page to see it
    r'(?:onto|to|on) a Redis list',
    r'the Redis list',
    r'Redis queue',
    r'the Redis behind',
    r'write to the Redis\b',
    r'a Redis nothing',
    r'through Redis\b',
    r'(?:reach|reaches|reaching) Redis\b',
    r'queues? the call through Redis',
    r'bounded by ``?REDIS_TIMEOUT',
    # the flag gates sending, and the depth reads answer either way. This spelling claimed
    # otherwise in six files, and each was reported separately because nothing failed
    r'(?:Telegram|the broker)[^.]{0,24}at all',
)

#: names 4.0 removed. A docstring or a page citing one sends a reader after something gone;
#: `test_public_surface.py` pins their absence from the code, and this pins it from the prose
REMOVED_NAMES = (r'\bsend_redis\b', r'\basend_redis\b', r'\bbot\.redis_conn\b')


def flattened(relative: str) -> str:
    """The file as one line, so a claim wrapped across a line break still matches."""
    return re.sub(r'\s+', ' ', (ROOT / relative).read_text())


@pytest.mark.parametrize('relative', NEUTRAL_SURFACES)
@pytest.mark.parametrize('pattern', THE_ONLY_TRANSPORT)
def test_the_surface_does_not_name_one_transport_as_the_queue(relative, pattern):
    """A neutral surface may name Redis beside the others; it may not name it as the queue."""
    found = re.search(pattern, flattened(relative), re.IGNORECASE)
    assert found is None, (
        f'{relative} says {found.group(0)!r}, which is true of one transport out of four; '
        f'name what `BROKER` selects, or move the claim to that transport'
    )


@pytest.mark.parametrize('relative', NEUTRAL_SURFACES)
@pytest.mark.parametrize('pattern', REMOVED_NAMES)
def test_the_surface_does_not_cite_a_removed_name(relative, pattern):
    """The rename table in `Upgrading.md` is where an old name belongs, and the only place."""
    found = re.search(pattern, flattened(relative))
    assert found is None, (
        f'{relative} cites {found.group(0)!r}, removed in 4.0; see the rename table in '
        f'docs/wiki/Upgrading.md for what to say instead'
    )


def test_the_pickle_warning_says_who_can_execute_code():
    """The one claim in that docstring worth pinning by its content and not its wording.

    `ALLOW_PICKLE` turns "can write to the queue" into "can execute code in this container",
    and the module docstring is where a reader meets that. A refactor that trims it to
    "pickle is unsafe" loses the part that tells an operator what to go and secure.

    Read out of the **module docstring** rather than out of the file. Searching the whole text
    made this pass for the wrong reason: delete the warning and the same phrases survive in the
    comments and function docstrings below it, so the case guarded its own wording rather than
    the paragraph a reader arrives at.
    """
    source = (ROOT / 'src/django_aiogram/wire/serializers.py').read_text(encoding='utf-8')
    warning = re.sub(r'\s+', ' ', ast.get_docstring(ast.parse(source)) or '')

    assert warning, 'the serializer module has no docstring to warn in'
    assert 'write to the queue' in warning, 'the warning no longer names who is trusted'
    assert 'execute code' in warning, 'the warning no longer names what they can do'
    assert '``BROKER``' in warning, 'the warning no longer says the queue is a setting'
