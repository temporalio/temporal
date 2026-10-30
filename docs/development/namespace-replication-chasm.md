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
