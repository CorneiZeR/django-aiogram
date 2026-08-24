"""What `KafkaBroker` does when the driver will not cooperate: a full queue, an unjoined group.

Both need a producer or a consumer that can be told to refuse, so they are here with doubles.

`produce` is asynchronous but not unconditional: it refuses with `BufferError` once the driver's
own queue is full, which is what a broker that has stopped accepting looks like from inside the
process after enough sends. That path needs a producer that can be told to refuse, so it is
here with a double rather than in the integration suite — filling a real broker's local queue
means a hundred thousand records and a broker that has actually gone away.
"""

import time

import pytest
from django.test import override_settings

from django_aiogram.broker.kafka import KafkaBroker
from django_aiogram.broker.kafka.exceptions import ProduceRefusedError
from django_aiogram.wire.serializers import JsonSerializer

SETTINGS = {
    'TOKEN': '42:x',
    'RATE_LIMIT': None,
    'KAFKA_BOOTSTRAP': '127.0.0.1:9093',
    'KAFKA_TOPIC': 'publish-double',
    'KAFKA_GROUP': 'publish-double',
    # short, because two of these cases wait the whole timeout out: the double refuses for ever
    # and the point is what `publish` says when the deadline passes, not how long it waits
    'KAFKA_TIMEOUT': 0.5,
    # named rather than defaulted, so anything reaching for `get_broker()` cannot quietly
    # answer with the default list and pass this case against the wrong transport
    'BROKER': 'django_aiogram.broker.kafka.KafkaBroker',
}


def payload(chat_id):
    return JsonSerializer().dumps({'function': 'send_message', 'chat_id': chat_id})


class FullOnce:
    """A producer whose queue is full for the first ``refusals`` calls, then takes everything.

    It answers each accepted record on the next ``poll``, which is what librdkafka does: the
    callback belongs to whoever polls, not to whoever produced.
    """

    def __init__(self, refusals=1):
        # `float('inf')` for a queue that never drains: a large integer would not do, because
        # the retry loop spins as fast as `poll` returns and gets through ten thousand of them
        # long before a half-second deadline
        self.refusals = refusals
        self.accepted = []
        self.polls = 0
        self._pending = []

    def produce(self, topic, payload, on_delivery):
        """Refuse while the queue is 'full', then accept and remember the callback."""
        if self.refusals:
            self.refusals -= 1
            raise BufferError('Local: Queue full')
        self.accepted.append((topic, payload))
        self._pending.append(on_delivery)

    def poll(self, _timeout):
        """Serve every callback that is waiting, as a drained queue would."""
        self.polls += 1
        pending, self._pending = self._pending, []
        for callback in pending:
            callback(None, object())
        return len(pending)


@pytest.fixture
def broker():
    with override_settings(TELEGRAM_BOT=SETTINGS):
        yield KafkaBroker()


def test_a_full_queue_is_waited_out_rather_than_raised(broker, monkeypatch):
    """A refused record is retried until there is room, and every record still gets answered.

    Without the retry the `BufferError` left `publish` mid-batch: the records already accepted
    were in flight with nobody waiting for their callbacks, and the caller was told the whole
    batch had failed — so retrying it sent the accepted prefix a second time.
    """
    double = FullOnce(refusals=1)
    monkeypatch.setattr('django_aiogram.broker.kafka.broker.shared_producer', lambda _bootstrap: double)

    broker.publish([payload(1), payload(2), payload(3)])

    assert len(double.accepted) == 3, f'only {len(double.accepted)} of three records were queued'
    assert double.polls, 'the full queue was never polled, so nothing could have drained it'


def test_a_record_that_never_fits_is_reported_with_what_did(broker, monkeypatch):
    """A batch that only partly went has to say so, because a blind retry would duplicate it.

    The refusal names both halves: how many never reached the queue and how many were accepted
    before them. An operator reading it can tell that retrying the whole batch sends the
    accepted ones twice — which is the choice this transport leaves to the caller, since Kafka
    has no way to un-send them.
    """
    double = FullOnce(refusals=float('inf'))
    monkeypatch.setattr('django_aiogram.broker.kafka.broker.shared_producer', lambda _bootstrap: double)

    with pytest.raises(ProduceRefusedError) as refusal:
        broker.publish([payload(1), payload(2)])

    assert 'never reached the local queue' in str(refusal.value), str(refusal.value)
    assert '2 message(s)' in str(refusal.value), str(refusal.value)


def test_the_accepted_prefix_is_counted_in_the_refusal(broker, monkeypatch):
    """The count of what did get in has to be the truth, not the batch size.

    Arranged so the first record is accepted and the second is refused for ever, which is the
    case a caller has to reason about: half a batch is on its way and the other half is not.
    """
    double = FullOnce(refusals=0)
    monkeypatch.setattr('django_aiogram.broker.kafka.broker.shared_producer', lambda _bootstrap: double)
    original = double.produce
    state = {'seen': 0}

    def produce(topic, payload, on_delivery):
        state['seen'] += 1
        if state['seen'] > 1:
            raise BufferError('Local: Queue full')
        original(topic, payload, on_delivery)

    double.produce = produce

    with pytest.raises(ProduceRefusedError) as refusal:
        broker.publish([payload(1), payload(2)])

    assert '1 message(s) of this batch never reached' in str(refusal.value), str(refusal.value)
    assert 'the 1 before them were accepted' in str(refusal.value), str(refusal.value)


class NeverAssigned:
    """A consumer that never joins its group and answers every poll with nothing.

    What a broker that is up but slow to hand out partitions looks like, and what a broker
    behind a network problem looks like for as long as the problem lasts.
    """

    def __init__(self):
        self.polled = []

    def assignment(self):
        """Never assigned, which is the whole point of it."""
        return []

    def poll(self, timeout):
        """Spend the asked-for time and answer nothing, as a poll with no data does.

        The whole time, not a capped slice of it: a double that returns early from a long poll
        makes 'how long did `take` spend' unmeasurable, which is the only thing these cases ask.
        """
        self.polled.append(timeout)
        time.sleep(timeout)


class AssignedAfter:
    """A consumer that joins partway through, which is the case that ends the join early.

    `NeverAssigned` cannot reach the branch below it: a join that never finishes means the
    caller's whole timeout goes on joining, and the poll that follows never runs. What breaks
    the contract is the *other* path — the assignment lands, the join loop exits with time to
    spare, and a poll for the full timeout then doubles the wait.
    """

    def __init__(self, after=2):
        self.after = after
        self.polls = []

    def assignment(self):
        """Nothing until `after` polls have gone by, then one partition."""
        return [] if len(self.polls) < self.after else ['a partition']

    def poll(self, timeout):
        """Spend the asked-for time and answer nothing, as `NeverAssigned` does and why."""
        self.polls.append(timeout)
        time.sleep(timeout)


def test_take_does_not_poll_again_for_time_it_already_spent_joining(broker, monkeypatch):
    """A join that finishes early must leave the poll only what is left, not the whole timeout.

    This is the path `NeverAssigned` cannot exercise, and it is the one that doubles the wait:
    the join loop returns as soon as the assignment lands, and polling for the full timeout
    after that makes `take(0.3)` come back at 0.6.
    """
    consumer = AssignedAfter(after=2)
    monkeypatch.setattr(broker, '_consumer', lambda: consumer)

    # one second, because the join polls in slices of `_JOIN_SLICE` and this needs the
    # assignment to land with time to spare: two slices is 0.4s, leaving 0.6s for the poll that
    # follows. Polling for the whole second there instead would come back at 1.4
    started = time.monotonic()
    assert broker.take(1.0) is None
    spent = time.monotonic() - started

    assert spent < 1.15, f'take(1.0) spent {spent:.2f}s, so the poll ignored what the join had used'
    assert len(consumer.polls) > 2, 'the poll after the join never ran, so this proves nothing'


def test_take_does_not_spend_longer_than_it_was_given_on_joining(broker, monkeypatch):
    """`take(timeout)` has to come back within its timeout even if the group never forms.

    The join used to get a budget of its own — `KAFKA_TIMEOUT` — spent before the caller's
    timeout began. A consumer loop whose iteration is supposed to last half a second would then
    block for the whole of it, and the heartbeat it owes is what pays: the arithmetic that keeps
    a worker looking alive is built on `take` returning when it says it will.

    Bounded at four times the asked-for timeout rather than at exactly it, because the loop
    polls in slices and the last slice can overshoot; `KAFKA_TIMEOUT` here is ten times that
    bound, so the case cannot pass by accident.
    """
    consumer = NeverAssigned()
    monkeypatch.setattr(broker, '_consumer', lambda: consumer)

    started = time.monotonic()
    assert broker.take(0.05) is None
    spent = time.monotonic() - started

    assert spent < 0.2, f'take(0.05) spent {spent:.2f}s, so it was still joining on its own budget'


def test_take_nowait_still_gives_the_join_its_own_budget(broker, monkeypatch):
    """The one caller with no deadline keeps the behaviour that a join needs more than a fetch.

    Measured, a local assignment took 3.05 seconds against the 1.5-second fetch budget this
    method allows itself, so bounding its join by that budget would make it answer "nothing"
    about a topic with something in it — which is exactly what it exists not to do.
    """
    consumer = NeverAssigned()
    monkeypatch.setattr(broker, '_consumer', lambda: consumer)

    started = time.monotonic()
    assert broker.take_nowait() is None
    spent = time.monotonic() - started

    # `KAFKA_TIMEOUT` is 0.5 in this module's settings, so the join budget is what is being
    # observed here rather than a wall-clock guess: it has to exceed the 0.05 a `take` would get
    assert spent > 0.3, f'take_nowait gave the join only {spent:.2f}s, less than KAFKA_TIMEOUT'
