package wideevents

import (
	"go.opentelemetry.io/otel/attribute"
)

// RemoteClusterLifecycleEventName aliases NamespaceLifecycleEventName for compatibility.
// TODO: Remove it after callers migrate to NamespaceLifecycleEventName.
const RemoteClusterLifecycleEventName = NamespaceLifecycleEventName

const (
	PhaseRemoteClusterUpsert = "remote_cluster_upsert"
	PhaseRemoteClusterRemove = "remote_cluster_remove"
)

// RemoteClusterLifecyclePayload preserves the namespace lifecycle event envelope with
// remote-cluster-specific details.
type RemoteClusterLifecyclePayload NamespaceLifecyclePayload

func (p RemoteClusterLifecyclePayload) EventName() string {
	return NamespaceLifecycleEventName
}

func (p RemoteClusterLifecyclePayload) Attributes() []attribute.KeyValue {
	return NamespaceLifecyclePayload(p).Attributes()
}
