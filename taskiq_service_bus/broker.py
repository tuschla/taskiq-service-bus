import asyncio
import logging
from collections.abc import AsyncGenerator, Callable, Mapping
from datetime import UTC, datetime, timedelta
from functools import partial
from itertools import chain
from typing import TypeVar

from azure.servicebus import ServiceBusMessage, ServiceBusReceivedMessage
from azure.servicebus.aio import (
    AutoLockRenewer,
    ServiceBusClient,
    ServiceBusReceiver,
    ServiceBusSender,
)
from taskiq import AckableMessage, AsyncBroker, AsyncResultBackend, BrokerMessage

T = TypeVar("T")

logger = logging.getLogger(__name__)


TASK_ID_KEY = "__task_id"
TASK_NAME_KEY = "__task_name"
QUEUE_LABEL = "queue"


class ServiceBusBroker(AsyncBroker):
    def __init__(  # noqa: PLR0913
        self,
        connection_string: str,
        queue_name: str | Mapping[str, int],
        max_lock_renewal_duration: float = 300,
        result_backend: AsyncResultBackend[T] | None = None,
        task_id_generator: Callable[[], str] | None = None,
        *,
        reconnect_delay: float = 1.0,
        max_reconnect_delay: float = 60.0,
    ) -> None:
        """ServiceBusBroker.

        Args:
            connection_string (str): Azure service bus connection string.
            queue_name (str | Mapping[str, int]): Name of the service bus queue to
                use, or a mapping of queue names to integer weights. With several
                queues, a worker takes messages from the queues that have messages in
                proportion to their weights. Tasks are sent to the queue named in
                their "queue" label, else to the first queue.
            max_lock_renewal_duration (float, optional): Maximum message lock renewal
                duration in seconds. Defaults to 300.
            result_backend (AsyncResultBackend | None, optional): Result backend to use
            task_id_generator (Callable[[], str] | None): custom task ID generator
                function. If not provided, taskiq will use `uuid4`
            reconnect_delay (float, optional): Seconds to wait before reopening the
                receiver after it failed or stopped. Doubles on each consecutive
                failure. Defaults to 1.
            max_reconnect_delay (float, optional): Upper bound for the reconnect delay
                in seconds. Defaults to 60.


        """
        super().__init__(result_backend, task_id_generator)

        self._connection_string = connection_string
        weights = {queue_name: 1} if isinstance(queue_name, str) else dict(queue_name)
        if not weights or any(
            not isinstance(w, int) or isinstance(w, bool) or w < 1
            for w in weights.values()
        ):
            raise ValueError("Queue weights must be positive integers.")
        self._weights = weights
        self._queue_name = next(iter(weights))
        self._max_lock_renewal_duration = max_lock_renewal_duration
        self._reconnect_delay = reconnect_delay
        self._max_reconnect_delay = max_reconnect_delay

        self._client: ServiceBusClient | None = None
        self._auto_lock_renewer: AutoLockRenewer | None = None
        # The receiver each queue currently reads from.
        self._receivers: dict[str, ServiceBusReceiver] = {}
        # Messages not yet acknowledged, per receiver, so a replaced receiver is
        # closed only once its last message has been settled.
        self._unacked: dict[ServiceBusReceiver, int] = {}
        self._replaced: set[ServiceBusReceiver] = set()

    async def startup(self) -> None:
        """Do something when starting broker."""
        await super().startup()
        self._client = ServiceBusClient.from_connection_string(self._connection_string)
        if self.is_worker_process:
            self._auto_lock_renewer = AutoLockRenewer(
                max_lock_renewal_duration=self._max_lock_renewal_duration
            )


    async def shutdown(self) -> None:
        """Close the broker.

        This method is called when the broker is closing.
        """
        await super().shutdown()
        if self._client is not None:

            # This closes senders and receivers as well
            await self._client.close()

        self._client = None
        self._auto_lock_renewer = None
        self._receivers.clear()
        self._unacked.clear()
        self._replaced.clear()

    async def kick(self, message: BrokerMessage) -> None:
        """Send message to workers.

        Args:
            message (BrokerMessage): message to send

        Returns:
            None
        """

        sb_message = ServiceBusMessage(
            message.message,
            application_properties={
                TASK_ID_KEY: message.task_id,
                TASK_NAME_KEY: message.task_name,
                **message.labels,
            },
        )

        # handle "delay" convention
        # see: https://taskiq-python.github.io/extending-taskiq/broker.html#conventions
        delay = message.labels.get("delay")
        queue_name = message.labels.get(QUEUE_LABEL, self._queue_name)
        if queue_name not in self._weights:
            raise ValueError(f"Queue {queue_name!r} is not one of the broker's queues.")

        async with ServiceBusClient.from_connection_string(self._connection_string) as client:
            async with client.get_queue_sender(queue_name) as sender:

                if delay is not None:
                    delay_seconds = float(delay)
                    scheduled_time = datetime.now(UTC) + timedelta(seconds=delay_seconds)
                    await sender.schedule_messages(sb_message, scheduled_time)
                else:
                    await sender.send_messages(sb_message)

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """Yield messages retrieved from the queues.

        Each queue is read by its own receiver, which holds at most one message
        until it is yielded. When several queues have a message ready, they are
        yielded by smooth weighted round-robin, so under load each queue gets its
        share of the messages, and a queue without messages never delays the
        others.

        If a receiver fails or stops (for example after the service closed the
        connection), it is replaced by a new one after a backoff instead of ending
        the worker's message loop.

        Yields:
            AckableMessage: message received from one of the queues
        """
        if self._client is None:
            raise RuntimeError("Broker is not initialized. Call startup() first.")

        ready_event = asyncio.Event()
        ready = {name: asyncio.Queue(maxsize=1) for name in self._weights}
        current = dict.fromkeys(self._weights, 0)
        readers = [
            asyncio.create_task(self._read(name, ready[name], ready_event))
            for name in self._weights
        ]
        try:
            while True:
                ready_event.clear()
                available = [name for name in self._weights if not ready[name].empty()]
                if not available:
                    await ready_event.wait()
                    continue
                for name in available:
                    current[name] += self._weights[name]
                name = max(available, key=current.__getitem__)
                current[name] -= sum(self._weights[n] for n in available)
                receiver, message = ready[name].get_nowait()
                yield AckableMessage(
                    data=bytes(chain.from_iterable(message.body)),
                    ack=partial(self._ack, name, receiver, message),
                )
        finally:
            for reader in readers:
                reader.cancel()
            await asyncio.gather(*readers, return_exceptions=True)
            # Give back messages that were received but never handed to the worker.
            for queue in ready.values():
                while not queue.empty():
                    receiver, message = queue.get_nowait()
                    try:
                        await receiver.abandon_message(message)
                    except Exception:
                        logger.debug("Abandoning a message failed", exc_info=True)

    async def _read(
        self,
        queue_name: str,
        ready: "asyncio.Queue[tuple[ServiceBusReceiver, ServiceBusReceivedMessage]]",
        ready_event: asyncio.Event,
    ) -> None:
        if self._client is None:
            raise RuntimeError("Broker is not initialized. Call startup() first.")
        delay = self._reconnect_delay
        while True:
            receiver = self._client.get_queue_receiver(
                queue_name, auto_lock_renewer=self._auto_lock_renewer
            )
            self._receivers[queue_name] = receiver
            self._unacked[receiver] = 0
            try:
                # Open before iterating, so a failure to connect is retried here and
                # acks can fall back to this receiver while it iterates.
                await receiver.__aenter__()
                async for message in receiver:
                    delay = self._reconnect_delay
                    self._unacked[receiver] += 1
                    await ready.put((receiver, message))
                    ready_event.set()
                logger.warning("%s: receiver stopped, retry in %ss", queue_name, delay)
            except Exception:
                logger.exception("%s: receiver failed, retry in %ss", queue_name, delay)
            await self._retire(receiver)
            await asyncio.sleep(delay)
            delay = min(delay * 2, self._max_reconnect_delay)

    async def _ack(
        self,
        queue_name: str,
        receiver: ServiceBusReceiver,
        message: ServiceBusReceivedMessage,
    ) -> None:
        try:
            try:
                await receiver.complete_message(message)
            except ValueError:
                # The receiver was shut down. Another receiver of the same queue can
                # settle the message by its lock token, if the lock is still valid.
                current = self._receivers.get(queue_name)
                if current is None or current is receiver:
                    raise
                await current.complete_message(message)
        finally:
            if receiver in self._unacked:  # cleared by shutdown()
                self._unacked[receiver] -= 1
                if receiver in self._replaced:
                    await self._retire(receiver)

    async def _retire(self, receiver: ServiceBusReceiver) -> None:
        self._replaced.add(receiver)
        if self._unacked.get(receiver):
            return  # the last ack closes it
        self._replaced.discard(receiver)
        self._unacked.pop(receiver, None)
        try:
            await receiver.close()
        except Exception:
            logger.debug("Closing a replaced receiver failed", exc_info=True)
