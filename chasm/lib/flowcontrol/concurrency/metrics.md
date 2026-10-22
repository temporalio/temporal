# Concurrency limiter slot metrics

These metrics instrument the experimental limiter at commit `710295b0`. They do
not change its admission, dispatch, or reservation-expiry decisions.

## Metric families

- `flowcontrol_concurrency_slots`: last successful Batch observation, with
  `state=RESERVED|COMMITTED|AVAILABLE|LIMIT`.
- `flowcontrol_concurrency_slots_observed_timestamp`: observation time as Unix
  seconds, for freshness and ownership-change filtering.
- `flowcontrol_concurrency_slot_operations`: individual operation results, with
  `operation=RESERVE|COMMIT|RELEASE|CANCEL` and
  `outcome=APPLIED|DUPLICATE|REJECTED|NOOP`.
- `flowcontrol_concurrency_expiration_attempts`: reservations removed during
  transition attempts, including staged wakes. A persistence retry can count the
  same expiration again; this is intentionally an attempt counter.
- `flowcontrol_concurrency_reservation_to_commit_latency`: initial reservation
  to first commit. Duplicate commits and legacy reservations without timestamps
  produce no latency sample.
- `flowcontrol_concurrency_release_latency`: activity release scheduling to a
  successful release RPC response, including task queueing, batching and RPC
  latency. It is not SDK activity completion to release. The scheduling timestamp
  survives task retries. Failed RPCs, legacy tasks without timestamps, and negative
  durations produce no sample. Successful duplicate releases can produce samples.

Operation results, slot observations, and reservation-to-commit latency are emitted
only after CHASM returns success. Internal failed transition attempts do not emit
these metrics. RPC-level failures remain available in the existing RPC metrics.
As with ordinary success telemetry, a committed update whose response is lost can
be absent from these counters.

## Labels and gauge interpretation

Metrics carry `archetype=concurrency_limiter` and `namespace_id`. The CHASM Batch
path also retains its context's namespace label. Whole-queue limiters additionally
carry the existing `taskqueue` and `task_type` labels, parsed from their stable
whole-queue key. Task queue names containing slashes are supported. No slot IDs or
arbitrary business IDs become labels. Gauges are suppressed for per-task or
unrecognized limiter keys to avoid collisions between such limiters.

Retain these labels in the metric exporter, including `state`, `operation`, and
`outcome`. Dropping queue labels makes different queues indistinguishable.

Gauges are event-driven observations, not periodic scrapes of limiter state. After
idle time or expiration in a staged wake, they remain unchanged until the next
successful Batch. Inspect the observation timestamp; no sample means unknown,
not zero. Reservation expiration is lazy, matching the limiter's existing behavior.

Do not sum gauges across history pods: an old owner can continue exposing its last
observation after ownership moves. Select the pod with the newest observation
for each `(namespace_id, taskqueue, task_type)`, and apply a freshness threshold
appropriate to the load. Sampling can miss short gaps; gauge averages are not
time-integrated permit occupancy. A limit reduction may temporarily leave held
slots above the new limit, with available slots clamped to zero.

Use the sum of operation outcome rates for acknowledged operation attempts, and
`outcome=APPLIED` for newly applied changes. `DUPLICATE` is an idempotent successful
reserve or commit. `NOOP` is a release or cancellation that removed no eligible
slot. Retries do not count as new applications.

The two added proto timestamps are optional and backward compatible. Reservation
timestamps are cleared on commit. Release timestamps persist with the existing
release task; neither requires additional CHASM tasks or database calls. Histograms
use the metric backend's existing timer unit configuration.
