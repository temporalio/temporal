package replication

// NamespaceThrottler tracks per-namespace LOW-priority task load on the receiver and
// reports which namespaces are overwhelming the shared lane. The sender uses the reported
// namespace IDs to create dedicated readers, keeping the default LOW reader from stalling.
type NamespaceThrottler interface {
	// RecordTask records an incoming LOW-priority task for the given namespace.
	RecordTask(namespaceID string)
	// ThrottledNamespaceIDs returns the namespace IDs that exceeded the throttle
	// threshold in the current observation window and resets the window.
	ThrottledNamespaceIDs() []string
}

// NoopNamespaceThrottler is the default implementation which never throttles.
type NoopNamespaceThrottler struct{}

func (NoopNamespaceThrottler) RecordTask(_ string)             {}
func (NoopNamespaceThrottler) ThrottledNamespaceIDs() []string { return nil }
