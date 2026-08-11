package wideevents

import "context"

type sourceTaskIDCtxKey struct{}

// SetReplicationSourceTaskID stamps the source cluster's replication-queue task id onto ctx for wide
// events emitted below the replication task. Diagnostic only: it must never affect control flow.
func SetReplicationSourceTaskID(ctx context.Context, sourceTaskID int64) context.Context {
	return context.WithValue(ctx, sourceTaskIDCtxKey{}, sourceTaskID)
}

// ReplicationSourceTaskIDFromContext returns the stamped id, or 0 when ctx carries none.
func ReplicationSourceTaskIDFromContext(ctx context.Context) int64 {
	if sourceTaskID, ok := ctx.Value(sourceTaskIDCtxKey{}).(int64); ok {
		return sourceTaskID
	}
	return 0
}
