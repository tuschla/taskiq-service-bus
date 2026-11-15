# Azure Service Bus broker for Taskiq


Example:

```python
from taskiq_service_bus import ServiceBusBroker

SERVICE_BUS_CONNECTION_STRING = "your_connection_string_here"

broker = ServiceBusBroker(SERVICE_BUS_CONNECTION_STRING, "my-queue")

@broker.task
def example_task():
    print("Task executed successfully")

```



## Broker Parameters

- `connection_string` - Service Bus Namespace connection string. Instructions on how to find it can be found here: [Get the connection string](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-python-how-to-use-queues?tabs=connection-string#get-the-connection-string)
- `queue_name` - Name of the Service Bus queue to use, or a mapping of queue names to integer weights (see below)
- `max_lock_renewal_duration` - How long, in seconds, the worker keeps renewing the lock of a message after starting its task. Optional, defaults to 300. The broker renews locks itself instead of using the SDK's `AutoLockRenewer`, and keeps retrying while a receiver reconnects.
- `result_backend` - Result backend to use. Optional.
- `task_id_generator` - custom task ID generator function. Optional, if not provided, taskiq will use `uuid4`.
- `reconnect_delay` - Seconds to wait before reopening the receiver after it failed, stopped, or could not connect; doubles on each consecutive failure. Optional, defaults to 1.
- `max_reconnect_delay` - Upper bound for `reconnect_delay` in seconds. Optional, defaults to 60.

## Multiple weighted queues

Pass a mapping of queue names to weights to read from several queues:

```python
broker = ServiceBusBroker(
    SERVICE_BUS_CONNECTION_STRING, {"high-priority": 3, "default": 1}
)

@broker.task(queue="high-priority")
async def urgent_task():
    ...

@broker.task  # sent to the first queue, "high-priority"
async def other_task():
    ...
```

Tasks are sent to the queue named in their `queue` label, or to the first queue. A worker holds back at most one message per queue (renewing its lock) and, when several queues have a message, hands them out by smooth weighted round-robin: while all queues are backlogged, the example above runs about three `high-priority` tasks for each `default` task. The ratio is approximate, because a queue fetches its next message only after the previous one was handed out. A queue without messages never delays the others, and each queue has its own receiver, so a failing queue doesn't affect the others.
