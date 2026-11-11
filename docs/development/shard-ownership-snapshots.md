# Read-only acquired-shard snapshots

`shard.OwnershipSnapshotProvider` is an optional capability separate from
`shard.Controller`: an ordinary controller lookup can create and acquire a shard.
The concrete controller's `OwnershipSnapshot(maxContexts)` reads only existing
in-memory contexts. It does not start acquisition, query persistence or wait for
controller/state/range locks. A busy range renewal declines the read immediately.

The result includes inactive contexts with a zero range ID and their lifecycle
revisions. A positive range requires acquired state, a ready engine and a
successfully renewed range. State transitions advance each context's revision;
successful renewal records its range acquisition time. `Since` is the later of
the current state transition and that renewal. Controller insertion/removal has
a separate revision and timestamp. Saturated revisions make reads unavailable
rather than wrapping.

The controller holds its context map fixed and reads each context twice.
Matching values across both passes establish a common stable cut. Lock contention,
unsupported contexts, intervening transitions, shutdown and exceeding the caller's
context bound return an empty result with `false`. Unavailable authority is
different from a successful snapshot containing no acquired ranges.

Returned maps belong to the caller. Consumers must compare the full controller
and context revisions, retain their own process identity and freshness bound, and
revalidate before acting. The controller revision alone does not identify
acquisition/loss on existing contexts. These are local acquired-state observations,
not a global lease or protection against a subsequent ownership change.
Persistence retains its existing range fencing.

Tests cover all context states, copied maps, lock contention, range/engine
readiness, bounds, unsupported contexts, revision saturation, changes between
passes, removal and loss/stop transitions. A native context test blocks its stubbed
persistence renewal, verifies unavailable reads during I/O, then checks the renewed
range and actual lifecycle hooks. This is source-level validation; it does not
add a policy, maintenance loop or persistence polling path.
