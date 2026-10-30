# Authoritative namespace replication

## Activation and rollback

Enable the `chasm` namespace replication transport only after all participating
clusters have compatible frontend and history binaries and authoritative
replication metrics/events deployed. This includes history binaries that decode
persisted mutations and understand `replicate_only`. There is no automatic
fallback to legacy replication when a peer rejects an authoritative request.

`system.namespaceReplicationTransportMode` selects the source transport. A
receiver in legacy mode still accepts authoritative requests so mixed-mode
operation and previously started executions can drain. Switching a source back
to legacy stops starting new authoritative executions; it does not stop existing
executions. A binary rollback must remain compatible with their persisted state.

## Removal snapshots

An explicit removal delivery can create a nonmember namespace record. Its config
version prevents a delayed older addition from restoring stale membership. If an
operator deletes that record while deliveries remain outstanding, a removal
retry can recreate it. Removal delivery does not provide a durable local-delete
tombstone.

## Ambiguous local commits

The metadata write and the CHASM state save do not share a transaction. Readback
usually recovers a committed write after a failed handoff. If a subsequent
namespace update overwrites the evidence before reconciliation, the component
records `INDETERMINATE` and suppresses peer fan-out instead of propagating an
unproven snapshot.

Later snapshots can converge current peers. A removed peer can remain stale
because it no longer appears in subsequent delivery lists. Caller retries can
return success without delivering the removal to that former peer. This is an
accepted recovery limitation; it does not introduce durable commit receipts or
an additional superseded-mutation state.

Operator recovery must explicitly inspect and repair the former destination.
Verification that discovers destinations only from current source membership
can miss it. Use the retained mutation's peer list or the removal operation's
records to identify the former peer.

## Observation boundaries

The authoritative receive outcome counter records one final outcome per admin
RPC. The receiver may perform several persistence attempts within that RPC to
absorb transient contention. Its response describes the final successful attempt:
`NO_OP_STALE` means that attempt made no change, not that an earlier attempt could
not have committed before returning a timeout.

Processed receiver events describe executor outcomes and include the persistence
request when available. An UPDATE that loses a missing-row CREATE race is retried
through apply-if-higher; the intermediate collision is not emitted as a completed
duplicate delivery. A genuine duplicate CREATE remains observable.

Source local-apply observations are emitted by the component independently of
the frontend's start/poll result. A caller can receive a polling timeout after the
local mutation commits. For that case, use the replication lifecycle event with
`mode=authoritative`, `apply_stage=local`, and `outcome=applied` to observe the local
resolution. A replicate-only no-op uses `outcome=no_change` and does not write
local metadata.

The existing `namespace_registered` and `namespace_updated` events are emitted by
the frontend after a successful response from the component. They can be absent
when polling fails after a local commit; the component's replication lifecycle
event does not emit those CRUD event names retroactively. Consumers that need to
observe authoritative local resolution should use the component-side event.

All of these logs and metrics are best-effort telemetry. A crash after a commit
but before observation can still lose an event; they are not a durable audit log
or an exactly-once record of writes.
