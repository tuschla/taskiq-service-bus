# Changelog

## Unreleased
- Message locks are renewed by the broker instead of the SDK's `AutoLockRenewer`, which silently stopped renewing when a receiver reconnected. Renewal is retried while the receiver reconnects, and `max_lock_renewal_duration` counts from when the task starts.
- `queue_name` also accepts a mapping of queue names to weights. Workers read every queue and hand out messages by weight; tasks are sent to the queue in their `queue` label, or the first queue.
- The worker reopens the Service Bus receiver with a backoff when it fails, stops, or cannot connect, instead of ending its message loop or crashing. Messages received on a replaced receiver are acknowledged through the new one. New keyword-only parameters `reconnect_delay` and `max_reconnect_delay`.

## Version 0.2 - 2026-03-20
- Fix `max_lock_renewal_duration` parameter not being respected (`8bac3631`). Thanks @sujh!

