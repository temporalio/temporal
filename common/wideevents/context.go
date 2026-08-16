package wideevents

import "context"

type replicationTaskOriginCtxKey struct{}

// ReplicationTaskOrigin identifies the source task whose processing produced an event.
type ReplicationTaskOrigin struct {
	ClusterName    string
	ShardID        int32
	TaskID         int64
	ArtifactOrigin string
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
