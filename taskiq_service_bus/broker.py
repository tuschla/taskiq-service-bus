import asyncio
import logging
from collections.abc import AsyncGenerator, Callable
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


class ServiceBusBroker(AsyncBroker):
    def __init__(  # noqa: PLR0913
        self,
        connection_string: str,
        queue_name: str,
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
            queue_name (str): Name of the service bus queue to use.
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
        self._queue_name = queue_name
        self._max_lock_renewal_duration = max_lock_renewal_duration
        self._reconnect_delay = reconnect_delay
        self._max_reconnect_delay = max_reconnect_delay

        self._client: ServiceBusClient | None = None
        self._auto_lock_renewer: AutoLockRenewer | None = None
        self._receiver: ServiceBusReceiver | None = None
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
        self._receiver = None
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

        async with ServiceBusClient.from_connection_string(self._connection_string) as client:
            async with client.get_queue_sender(self._queue_name) as sender:

                if delay is not None:
                    delay_seconds = float(delay)
                    scheduled_time = datetime.now(UTC) + timedelta(seconds=delay_seconds)
                    await sender.schedule_messages(sb_message, scheduled_time)
                else:
                    await sender.send_messages(sb_message)

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """Yield messages retrieved from the queue.

        If the receiver fails or stops (for example after the service closed the
        connection), it is replaced by a new one after a backoff instead of ending
        the worker's message loop.

        Yields:
            AckableMessage: message received from the queue
        """
        if self._client is None:
            raise RuntimeError("Broker is not initialized. Call startup() first.")

        delay = self._reconnect_delay
        while True:
            receiver = self._client.get_queue_receiver(
                self._queue_name, auto_lock_renewer=self._auto_lock_renewer
            )
            self._receiver = receiver
            self._unacked[receiver] = 0
            try:
                # Open before iterating, so a failure to connect is retried here and
                # acks can fall back to this receiver while it iterates.
                await receiver.__aenter__()
                async for message in receiver:
                    delay = self._reconnect_delay
                    self._unacked[receiver] += 1
                    yield AckableMessage(
                        data=bytes(chain.from_iterable(message.body)),
                        ack=partial(self._ack, receiver, message),
                    )
                logger.warning("Receiver stopped; reopening in %.1fs", delay)
            except Exception:
                logger.exception("Receiver failed; reopening in %.1fs", delay)
            await self._retire(receiver)
            await asyncio.sleep(delay)
            delay = min(delay * 2, self._max_reconnect_delay)

    async def _ack(
        self, receiver: ServiceBusReceiver, message: ServiceBusReceivedMessage
    ) -> None:
        try:
            try:
                await receiver.complete_message(message)
            except ValueError:
                # The receiver was shut down. Another receiver can settle the
                # message by its lock token, if the lock is still valid.
                current = self._receiver
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
