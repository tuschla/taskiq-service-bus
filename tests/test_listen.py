import asyncio
import sys
from collections import Counter
from datetime import UTC, datetime, timedelta

import pytest
from azure.servicebus.exceptions import MessageLockLostError, ServiceBusConnectionError
from taskiq import BrokerMessage

from taskiq_service_bus import ServiceBusBroker
from taskiq_service_bus import broker as broker_module

LOCK = timedelta(seconds=60)


class FakeMessage:
    def __init__(self, body: bytes) -> None:
        self.body = [body]
        self.locked_until_utc = datetime.now(UTC) + LOCK


class FakeReceiver:
    """Yields its messages, then fails with `error`, stops, or waits forever."""

    def __init__(
        self, messages=(), *, error=None, open_error=None, stop=False, lost=()
    ):
        self.messages = [FakeMessage(m) for m in messages]
        self.error = error
        self.open_error = open_error
        self.stop = stop
        self.running = False
        self.closed = False
        self.completed = []
        self.abandoned = []
        self.fetched = 0
        self.renewed = 0
        self.lost = lost

    async def __aenter__(self):
        if self.open_error:
            raise self.open_error
        self.running = True
        return self

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        for message in self.messages:
            self.fetched += 1
            yield message
        if self.error:
            self.running = False
            raise self.error
        if not self.stop:
            await asyncio.Event().wait()

    async def complete_message(self, message) -> None:
        if not self.running:
            raise ValueError("handler has already been shutdown")
        self.completed.append(message)

    async def renew_message_lock(self, message) -> None:
        if not self.running:
            raise ValueError("handler has already been shutdown")
        if message.body[0] in self.lost:
            raise MessageLockLostError(message="lock lost")
        self.renewed += 1
        message.locked_until_utc = datetime.now(UTC) + LOCK

    async def abandon_message(self, message) -> None:
        self.abandoned.append(message)

    async def close(self) -> None:
        self.running = False
        self.closed = True


class FakeClient:
    """Hands out the given receivers per queue, in order."""

    def __init__(self, receivers) -> None:
        self.receivers = {q: list(r) for q, r in receivers.items()}
        self.created = []

    def get_queue_receiver(self, queue_name, auto_lock_renewer=None):
        receiver = self.receivers[queue_name].pop(0)
        self.created.append(receiver)
        return receiver


def make_broker(queues="q", **receivers):
    broker = ServiceBusBroker(
        "unused", queues, reconnect_delay=0, max_reconnect_delay=0
    )
    broker._client = FakeClient(receivers)
    return broker


async def take(listen, n, pause=0.0):
    messages = []
    for _ in range(n):
        await asyncio.sleep(pause)  # let the readers refill, like a busy worker
        messages.append(await anext(listen))
    return messages


DROPPED = ServiceBusConnectionError(message="connection closed by the service")


@pytest.mark.asyncio
async def test_listen_reopens_after_the_receiver_fails_or_stops():
    broker = make_broker(
        q=[
            FakeReceiver([b"a"], error=DROPPED),
            FakeReceiver([b"b"], stop=True),
            FakeReceiver([b"c"]),
        ]
    )
    messages = await take(broker.listen(), 3)
    assert [m.data for m in messages] == [b"a", b"b", b"c"]


@pytest.mark.asyncio
async def test_listen_retries_when_opening_the_receiver_fails():
    broker = make_broker(
        q=[
            FakeReceiver(open_error=DROPPED),
            FakeReceiver(open_error=DROPPED),
            FakeReceiver([b"a"]),
        ]
    )
    (message,) = await take(broker.listen(), 1)
    assert message.data == b"a"
    assert [r.closed for r in broker._client.created] == [True, True, False]


@pytest.mark.asyncio
async def test_ack_after_the_receiver_was_replaced_settles_on_the_new_one():
    old = FakeReceiver([b"a"], error=DROPPED)
    new = FakeReceiver([b"b"])
    broker = make_broker(q=[old, new])
    first, second = await take(broker.listen(), 2)

    assert not old.closed  # still has an unacknowledged message
    await first.ack()
    await second.ack()

    assert [m.body for m in new.completed] == [[b"a"], [b"b"]]
    assert old.closed


@pytest.mark.asyncio
async def test_ack_on_a_live_receiver_settles_there():
    receiver = FakeReceiver([b"a"])
    broker = make_broker(q=[receiver])
    (message,) = await take(broker.listen(), 1)
    await message.ack()
    assert [m.body for m in receiver.completed] == [[b"a"]]
    assert not receiver.closed


@pytest.mark.asyncio
async def test_listen_raises_before_startup():
    broker = ServiceBusBroker("unused", "q")
    with pytest.raises(RuntimeError):
        await anext(broker.listen())


def test_reconnect_delay_backs_off(monkeypatch):
    delays = []
    real_sleep = asyncio.sleep

    async def fake_sleep(delay):
        if delay:
            delays.append(delay)
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    broker = ServiceBusBroker("unused", "q", reconnect_delay=1, max_reconnect_delay=4)
    failing = [FakeReceiver(open_error=DROPPED) for _ in range(4)]
    broker._client = FakeClient({"q": [*failing, FakeReceiver([b"a"])]})
    asyncio.run(take(broker.listen(), 1))
    assert delays[:4] == [1, 2, 4, 4]  # then the lock renewal of "a"


@pytest.mark.asyncio
async def test_backlogged_queues_share_messages_by_weight():
    broker = make_broker(
        {"high": 3, "low": 1},
        high=[FakeReceiver([b"h"] * 50)],
        low=[FakeReceiver([b"l"] * 50)],
    )
    messages = await take(broker.listen(), 40, pause=0.001)
    assert Counter(m.data for m in messages) == {b"h": 30, b"l": 10}


@pytest.mark.asyncio
async def test_an_empty_queue_does_not_hold_up_the_others():
    broker = make_broker(
        {"busy": 1, "empty": 10},
        busy=[FakeReceiver([b"a", b"b", b"c"])],
        empty=[FakeReceiver()],
    )
    messages = await asyncio.wait_for(take(broker.listen(), 3), timeout=1)
    assert [m.data for m in messages] == [b"a", b"b", b"c"]


@pytest.mark.asyncio
async def test_a_failing_queue_does_not_hold_up_the_others():
    broker = make_broker(
        {"down": 1, "up": 1},
        down=[FakeReceiver(open_error=DROPPED) for _ in range(100)],
        up=[FakeReceiver([b"a", b"b"])],
    )
    messages = await asyncio.wait_for(take(broker.listen(), 2), timeout=1)
    assert [m.data for m in messages] == [b"a", b"b"]


@pytest.mark.asyncio
async def test_closing_the_listener_abandons_messages_not_handed_out():
    receiver_a = FakeReceiver([b"a1", b"a2"])
    receiver_b = FakeReceiver([b"b1"])
    broker = make_broker({"a": 1, "b": 1}, a=[receiver_a], b=[receiver_b])
    listen = broker.listen()
    (first,) = await take(listen, 1, pause=0.01)
    await asyncio.sleep(0.01)
    await listen.aclose()
    # Each queue holds back one message; a2 replaced a1 once a1 was handed out.
    assert first.data == b"a1"
    abandoned = {m.body[0] for r in (receiver_a, receiver_b) for m in r.abandoned}
    assert abandoned == {b"a2", b"b1"}


@pytest.mark.parametrize("weights", [{}, {"q": 0}, {"q": 1.5}, {"q": True}])
def test_invalid_weights_are_rejected(weights):
    with pytest.raises(ValueError, match="weights"):
        ServiceBusBroker("unused", weights)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("labels", "expected"), [({}, "first"), ({"queue": "second"}, "second")]
)
async def test_kick_sends_to_the_queue_label_or_the_first_queue(
    monkeypatch, labels, expected
):
    sent_to = []

    class FakeSender:
        def __init__(self, queue_name):
            self.queue_name = queue_name

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def send_messages(self, message):
            sent_to.append(self.queue_name)

    class FakeSendClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        def get_queue_sender(self, queue_name):
            return FakeSender(queue_name)

    monkeypatch.setattr(
        broker_module.ServiceBusClient,
        "from_connection_string",
        staticmethod(lambda *a, **k: FakeSendClient()),
    )
    broker = ServiceBusBroker("unused", {"first": 1, "second": 2})
    message = BrokerMessage(task_id="t", task_name="n", message=b"x", labels=labels)
    await broker.kick(message)
    assert sent_to == [expected]


@pytest.mark.asyncio
async def test_kick_rejects_an_unknown_queue_label():
    broker = ServiceBusBroker("unused", {"first": 1})
    message = BrokerMessage(
        task_id="t", task_name="n", message=b"x", labels={"queue": "other"}
    )
    with pytest.raises(ValueError, match="other"):
        await broker.kick(message)


@pytest.mark.asyncio
async def test_each_queue_holds_back_at_most_one_message():
    receiver = FakeReceiver([b"a", b"b", b"c"])
    broker = make_broker(q=[receiver])
    listen = broker.listen()
    await take(listen, 1, pause=0.01)
    await asyncio.sleep(0.01)
    # "a" was handed out and "b" waits; "c" must not be received (and locked) yet.
    assert receiver.fetched == 2
    await listen.aclose()
    assert [m.body for m in receiver.abandoned] == [[b"b"]]


@pytest.mark.asyncio
async def test_a_held_back_message_keeps_its_lock(monkeypatch):
    monkeypatch.setattr(sys.modules[__name__], "LOCK", timedelta(seconds=0.2))
    receiver = FakeReceiver([b"a", b"b"])
    broker = make_broker(q=[receiver])
    listen = broker.listen()
    await take(listen, 1)
    await asyncio.sleep(0.5)  # the worker is busy; "b" waits
    assert receiver.renewed >= 2
    (second,) = await take(listen, 1)
    assert second.data == b"b"
    await listen.aclose()


@pytest.mark.asyncio
async def test_a_held_back_message_whose_lock_is_lost_is_not_handed_out(monkeypatch):
    monkeypatch.setattr(sys.modules[__name__], "LOCK", timedelta(seconds=0.1))
    receiver = FakeReceiver([b"a", b"lost", b"b"], lost={b"lost"})
    broker = make_broker(q=[receiver])
    listen = broker.listen()
    (first,) = await take(listen, 1)
    await asyncio.sleep(0.2)
    (second,) = await take(listen, 1)
    assert (first.data, second.data) == (b"a", b"b")
    await listen.aclose()


@pytest.mark.asyncio
async def test_a_handed_out_message_stays_locked_until_acknowledged(monkeypatch):
    monkeypatch.setattr(sys.modules[__name__], "LOCK", timedelta(seconds=0.2))
    receiver = FakeReceiver([b"a"])
    broker = make_broker(q=[receiver])
    (message,) = await take(broker.listen(), 1)
    await asyncio.sleep(0.5)
    renewed = receiver.renewed
    assert renewed >= 2
    await message.ack()
    await asyncio.sleep(0.3)
    assert receiver.renewed == renewed
    assert not broker._renewals


@pytest.mark.asyncio
async def test_renewal_stops_after_max_lock_renewal_duration(monkeypatch):
    monkeypatch.setattr(sys.modules[__name__], "LOCK", timedelta(seconds=0.2))
    receiver = FakeReceiver([b"a"])
    broker = make_broker(q=[receiver])
    broker._max_lock_renewal_duration = 0.3
    await take(broker.listen(), 1)
    await asyncio.sleep(0.8)
    assert 1 <= receiver.renewed <= 3
    assert not broker._renewals


@pytest.mark.asyncio
async def test_renewal_retries_while_the_receiver_reconnects(monkeypatch):
    monkeypatch.setattr(sys.modules[__name__], "LOCK", timedelta(seconds=0.4))
    monkeypatch.setattr(broker_module, "RENEW_RETRY_DELAY", 0.01)
    receiver = FakeReceiver([b"a"])
    broker = make_broker(q=[receiver])
    (message,) = await take(broker.listen(), 1)
    receiver.running = False  # the SDK reconnecting: renewals raise ValueError
    await asyncio.sleep(0.25)
    assert receiver.renewed == 0
    receiver.running = True
    await asyncio.sleep(0.1)
    assert receiver.renewed == 1
    await message.ack()
    assert [m.body for m in receiver.completed] == [[b"a"]]


@pytest.mark.asyncio
async def test_renewal_uses_the_current_receiver_after_a_replacement(monkeypatch):
    monkeypatch.setattr(sys.modules[__name__], "LOCK", timedelta(seconds=0.2))
    old = FakeReceiver([b"a"], error=DROPPED)
    new = FakeReceiver()
    broker = make_broker(q=[old, new])
    listen = broker.listen()
    await take(listen, 1)
    await asyncio.sleep(0.5)
    assert old.renewed == 0
    assert new.renewed >= 2
    await listen.aclose()


@pytest.mark.asyncio
async def test_a_receiver_shut_down_while_holding_is_reopened_for_renewal(monkeypatch):
    monkeypatch.setattr(sys.modules[__name__], "LOCK", timedelta(seconds=0.2))
    receiver = FakeReceiver([b"a", b"b"])
    broker = make_broker(q=[receiver])
    listen = broker.listen()
    (first,) = await take(listen, 1)
    await asyncio.sleep(0.01)  # "b" is now held, so nothing is receiving
    receiver.running = False  # the SDK shut the handler down
    await asyncio.sleep(0.15)
    assert receiver.running
    assert receiver.renewed >= 1
    await first.ack()
    assert [m.body for m in receiver.completed] == [[b"a"]]
    await listen.aclose()
