import asyncio

import pytest
from azure.servicebus.exceptions import ServiceBusConnectionError

from taskiq_service_bus import ServiceBusBroker


class FakeMessage:
    def __init__(self, body: bytes) -> None:
        self.body = [body]


class FakeReceiver:
    """Yields its messages, then fails with `error` or stops."""

    def __init__(self, messages, error=None, open_error=None) -> None:
        self.messages = [FakeMessage(m) for m in messages]
        self.error = error
        self.open_error = open_error
        self.running = False
        self.closed = False
        self.completed = []

    async def __aenter__(self):
        if self.open_error:
            raise self.open_error
        self.running = True
        return self

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        for message in self.messages:
            yield message
        if self.error:
            self.running = False
            raise self.error

    async def complete_message(self, message) -> None:
        if not self.running:
            raise ValueError("handler has already been shutdown")
        self.completed.append(message)

    async def close(self) -> None:
        self.running = False
        self.closed = True


class FakeClient:
    def __init__(self, receivers) -> None:
        self.receivers = list(receivers)
        self.created = []

    def get_queue_receiver(self, queue_name, auto_lock_renewer=None):
        receiver = self.receivers.pop(0)
        self.created.append(receiver)
        return receiver


def make_broker(*receivers):
    broker = ServiceBusBroker("unused", "q", reconnect_delay=0, max_reconnect_delay=0)
    broker._client = FakeClient(receivers)
    return broker


async def take(listen, n):
    return [await anext(listen) for _ in range(n)]


DROPPED = ServiceBusConnectionError(message="connection closed by the service")


@pytest.mark.asyncio
async def test_listen_reopens_after_the_receiver_fails_or_stops():
    broker = make_broker(
        FakeReceiver([b"a"], error=DROPPED),
        FakeReceiver([b"b"]),  # ends without an error
        FakeReceiver([b"c"]),
    )
    messages = await take(broker.listen(), 3)
    assert [m.data for m in messages] == [b"a", b"b", b"c"]


@pytest.mark.asyncio
async def test_listen_retries_when_opening_the_receiver_fails():
    broker = make_broker(
        FakeReceiver([], open_error=DROPPED),
        FakeReceiver([], open_error=DROPPED),
        FakeReceiver([b"a"]),
    )
    (message,) = await take(broker.listen(), 1)
    assert message.data == b"a"
    assert [r.closed for r in broker._client.created] == [True, True, False]


@pytest.mark.asyncio
async def test_ack_after_the_receiver_was_replaced_settles_on_the_new_one():
    old = FakeReceiver([b"a"], error=DROPPED)
    new = FakeReceiver([b"b"])
    broker = make_broker(old, new)
    listen = broker.listen()
    first, second = await take(listen, 2)

    assert not old.closed  # still has an unacknowledged message
    await first.ack()
    await second.ack()

    assert [m.body for m in new.completed] == [[b"a"], [b"b"]]
    assert old.closed


@pytest.mark.asyncio
async def test_ack_on_a_live_receiver_settles_there():
    receiver = FakeReceiver([b"a"])
    broker = make_broker(receiver)
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

    async def fake_sleep(delay):
        delays.append(delay)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    broker = ServiceBusBroker("unused", "q", reconnect_delay=1, max_reconnect_delay=4)
    failing = [FakeReceiver([], open_error=DROPPED) for _ in range(4)]
    broker._client = FakeClient([*failing, FakeReceiver([b"a"])])
    asyncio.run(take(broker.listen(), 1))
    assert delays == [1, 2, 4, 4]
