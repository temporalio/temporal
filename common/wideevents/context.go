package wideevents

import "context"

type replicationTaskOriginCtxKey struct{}
type namespaceReplicationTaskContextKey struct{}

// ReplicationTaskOrigin identifies the source task whose processing produced an event.
type ReplicationTaskOrigin struct {
	ClusterName         string
	ShardID             int32
	TaskID              int64
	ApplyArtifactSource ReplicationApplyArtifactSource
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
	EventData     NamespaceReplicationTaskEventData
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
