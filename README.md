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
- `queue_name` - Name of the Service Bus queue to use
- `max_lock_renewal_duration` - Maximum message lock renewal duration in seconds. Optional, defaults to 300.
- `result_backend` - Result backend to use. Optional.
- `task_id_generator` - custom task ID generator function. Optional, if not provided, taskiq will use `uuid4`.
- `reconnect_delay` - Seconds to wait before reopening the receiver after it failed, stopped, or could not connect; doubles on each consecutive failure. Optional, defaults to 1.
- `max_reconnect_delay` - Upper bound for `reconnect_delay` in seconds. Optional, defaults to 60.