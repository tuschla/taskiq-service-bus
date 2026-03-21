from collections.abc import AsyncGenerator, Callable
from datetime import UTC, datetime, timedelta
from functools import partial
from itertools import chain
from typing import TypeVar

from azure.servicebus import ServiceBusMessage
from azure.servicebus.aio import (
    AutoLockRenewer,
    ServiceBusClient,
    ServiceBusReceiver,
    ServiceBusSender,
)
from taskiq import AckableMessage, AsyncBroker, AsyncResultBackend, BrokerMessage

T = TypeVar("T")


TASK_ID_KEY = "__task_id"
TASK_NAME_KEY = "__task_name"


class ServiceBusBroker(AsyncBroker):
    def __init__(
        self,
        connection_string: str,
        queue_name: str,
        max_lock_renewal_duration: float = 300,
        result_backend: AsyncResultBackend[T] | None = None,
        task_id_generator: Callable[[], str] | None = None,
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


        """
        super().__init__(result_backend, task_id_generator)

        self._connection_string = connection_string
        self._queue_name = queue_name
        self._max_lock_renewal_duration = max_lock_renewal_duration

        self._client: ServiceBusClient | None = None
        self._receiver: ServiceBusReceiver | None = None
        self._sender: ServiceBusSender | None = None

    async def startup(self) -> None:
        """Do something when starting broker."""
        await super().startup()
        self._client = ServiceBusClient.from_connection_string(self._connection_string)
        if self.is_worker_process:
            auto_lock_renewer = AutoLockRenewer(
                max_lock_renewal_duration=self._max_lock_renewal_duration
            )
            self._receiver = self._client.get_queue_receiver(
                self._queue_name, auto_lock_renewer=auto_lock_renewer
            )

        self._sender = self._client.get_queue_sender(self._queue_name)

    async def shutdown(self) -> None:
        """Close the broker.

        This method is called when the broker is closing.
        """
        await super().shutdown()
        if self._client is not None:

            # This closes senders and receivers as well
            await self._client.close()

        self._client = None
        self._sender = None
        self._receiver = None

    async def kick(self, message: BrokerMessage) -> None:
        """Send message to workers.

        Args:
            message (BrokerMessage): message to send

        Returns:
            None
        """
        if self._sender is None:
            raise RuntimeError("Sender is not initialized. Call startup() first.")

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

        if delay is not None:
            delay_seconds = float(delay)
            scheduled_time = datetime.now(UTC) + timedelta(seconds=delay_seconds)
            await self._sender.schedule_messages(sb_message, scheduled_time)
        else:
            await self._sender.send_messages(sb_message)

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """Yield messages retrieved from the queue.

        Yields:
            AckableMessage: message received from the queue
        """
        if self._receiver is None:
            raise RuntimeError("Receiver is not initialized. Call startup() first.")

        async for message in self._receiver:
            ack_message = partial(self._receiver.complete_message, message)

            yield AckableMessage(
                data=bytes(chain.from_iterable(message.body)),
                ack=ack_message,
            )
