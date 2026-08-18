package wideevents

import "context"

type replicationTaskOriginCtxKey struct{}
type namespaceReplicationTaskContextKey struct{}

// ReplicationTaskOrigin identifies where a replicated artifact came from; diagnostic only. TaskID is
// unset when the applied artifact was re-fetched rather than being that queue entry's own payload.
type ReplicationTaskOrigin struct {
	ShardID int32
	TaskID  int64
}

// SetReplicationTaskOrigin stamps origin onto ctx.
func SetReplicationTaskOrigin(ctx context.Context, origin ReplicationTaskOrigin) context.Context {
	return context.WithValue(ctx, replicationTaskOriginCtxKey{}, origin)
}

// ReplicationTaskOriginFromContext returns the stamped origin, or the zero value when ctx has none.
func ReplicationTaskOriginFromContext(ctx context.Context) ReplicationTaskOrigin {
	if origin, ok := ctx.Value(replicationTaskOriginCtxKey{}).(ReplicationTaskOrigin); ok {
		return origin
	}
	return ReplicationTaskOrigin{}
}

// NamespaceReplicationTaskContext contains receiver-side lifecycle metadata for a namespace task.
type NamespaceReplicationTaskContext struct {
	SourceCluster string
	TargetCluster string
	SourceTaskID  int64
	AttemptCount  int
}

// SetNamespaceReplicationTaskContext stamps namespace replication metadata onto ctx.
func SetNamespaceReplicationTaskContext(
	ctx context.Context,
	metadata NamespaceReplicationTaskContext,
) context.Context {
	return context.WithValue(ctx, namespaceReplicationTaskContextKey{}, metadata)
}

// NamespaceReplicationTaskContextFromContext returns the stamped metadata when present.
func NamespaceReplicationTaskContextFromContext(
	ctx context.Context,
) (NamespaceReplicationTaskContext, bool) {
	metadata, ok := ctx.Value(namespaceReplicationTaskContextKey{}).(NamespaceReplicationTaskContext)
	return metadata, ok
}
