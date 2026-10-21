# Workflow Task heartbeat notification PoC

This PoC shortens the time a Signal waits behind a started Workflow Task that
is blocked on a long Local Activity. History sends a best-effort notification
to the worker's existing control queue. The Go SDK checks the exact task
identity and uses its existing forced Workflow Task completion path to obtain a
replacement task. The Local Activity keeps running.

## Scope

- Signal delivery is the trigger in this PoC. Updates and other history-backed
  inputs are not yet wired to notifications.
- `system.enableWorkflowTaskHeartbeatNotification` is a namespace-scoped dynamic
  config setting and defaults to `false`.
- The worker advertises support on Workflow Task polls. History records the
  control queue for that started task, and only sends a notification when a
  Signal is the first buffered event behind it.
- The command contains run ID, scheduled and started event IDs, attempt, and
  started timestamp. The SDK ignores stale and duplicate commands.
- If a notification is lost, the existing SDK heartbeat at about 80% of the
  Workflow Task timeout remains the fallback.

## Trying it

The runnable Signal benchmark and measurement report are in
`~/projects/demo-whn`. It uses the matching `poc-whn` API Go and SDK Go
branches. Enable `frontend.WorkerCommandsEnabled` for the benchmark namespace
alongside this feature flag so the worker polls its control queue. The demo
starts a Local Activity, sends a steering Signal while it runs, cancels it
after Workflow code receives the Signal, and starts a replacement Local
Activity. With a 10-second Workflow Task timeout, 100 local trials per mode
measured 9.577 ms p90 from Signal send to replacement start with notifications
enabled, versus 7805.402 ms p90 with notifications disabled. See the demo
repository for per-trial data and the five-minute activity model.

The notification does not carry the Signal payload. History remains the source
of truth, and the SDK does not add a new heartbeat RPC.
