package replication

// NamespaceThrottler tracks per-namespace HIGH-priority (live) replication task load on
// the receiver and reports which namespaces are overwhelming the shared lane. The
// reported namespace IDs travel back to the sender in SyncReplicationState acks
// (throttle_high_namespace_ids), which isolates those namespaces' live replication onto
// dedicated throttled lanes so they cannot stall the default lane.
//
// Load and throttle decisions can be scoped to the receiver's local shard: a namespace
// overwhelming one shard's lane is not necessarily throttled on others, letting it
// keep full speed on lower-traffic shards. The throttler is shared across all stream
// receivers on this host, so implementations must be safe for concurrent use.
type NamespaceThrottler interface {
	// RecordTask records an incoming HIGH-priority task for the given namespace on
	// the given local shard.
	RecordTask(shardID int32, namespaceID string)
	// ThrottledNamespaceIDs returns the namespace IDs the throttler currently
	// considers overwhelming on the given local shard. How that is decided
	// (thresholds, windows, decay) is internal to the implementation.
	ThrottledNamespaceIDs(shardID int32) []string
}

// NoopNamespaceThrottler is the default implementation which never throttles.
type NoopNamespaceThrottler struct{}

func (NoopNamespaceThrottler) RecordTask(_ int32, _ string)           {}
func (NoopNamespaceThrottler) ThrottledNamespaceIDs(_ int32) []string { return nil }
