package replication

// NamespaceReplicationThrottler tracks per-namespace replication rate limits on the receiver side.
// When a namespace exceeds its allowed rate, it is considered throttled and its ID is included
// in SyncReplicationState ACKs so the sender can demote it to low priority.

type NamespaceThrottler interface {
	// Allow records an incoming task for the given namespace and reports whether it is within
	// its rate limit. It must be called for every incoming task — including tasks already at
	// THROTTLED priority — so the throttler can track the current load and determine when a
	// namespace has recovered enough to be promoted back to HIGH priority.
	// Returns true if the namespace is within its limit (not throttled).
	Allow(namespaceID string) bool
	// ThrottledNamespaceIDs returns the set of namespace IDs that are currently throttled.
	ThrottledNamespaceIDs() []string
}

// NoopNamespaceReplicationThrottler is the default implementation which never throttles.
type NoopNamespaceReplicationThrottler struct{}

func (NoopNamespaceReplicationThrottler) Allow(_ string) bool             { return true }
func (NoopNamespaceReplicationThrottler) ThrottledNamespaceIDs() []string { return nil }
