package replication

// NamespaceThrottler tracks per-namespace HIGH-priority (live) replication task load on
// the receiver and reports which namespaces are overwhelming the shared lane. The
// reported namespace IDs travel back to the sender in SyncReplicationState acks
// (pause_high_namespace_ids), which isolates those namespaces' live replication onto
// dedicated throttled lanes so they cannot stall the default lane.
type NamespaceThrottler interface {
	// RecordTask records an incoming HIGH-priority task for the given namespace.
	RecordTask(namespaceID string)
	// ThrottledNamespaceIDs returns the namespace IDs that exceeded the throttle
	// threshold in the current observation window and resets the window.
	ThrottledNamespaceIDs() []string
}

// NoopNamespaceThrottler is the default implementation which never throttles.
type NoopNamespaceThrottler struct{}

func (NoopNamespaceThrottler) RecordTask(_ string)             {}
func (NoopNamespaceThrottler) ThrottledNamespaceIDs() []string { return nil }
