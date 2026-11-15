# Changelog

## Unreleased
- The worker reopens the Service Bus receiver with a backoff when it fails, stops, or cannot connect, instead of ending its message loop or crashing. Messages received on a replaced receiver are acknowledged through the new one. New keyword-only parameters `reconnect_delay` and `max_reconnect_delay`.

## Version 0.2 - 2026-03-20
- Fix `max_lock_renewal_duration` parameter not being respected (`8bac3631`). Thanks @sujh!

