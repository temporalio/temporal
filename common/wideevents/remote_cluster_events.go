package wideevents

import "go.opentelemetry.io/otel/log"

const RemoteClusterLifecycleEventName = "remote_cluster_lifecycle"

const (
	PhaseRemoteClusterUpsert = "remote_cluster_upsert"
	PhaseRemoteClusterRemove = "remote_cluster_remove"
)

// RemoteClusterLifecyclePayload preserves the namespace lifecycle event envelope
// while using a distinct event name and remote-cluster-specific details.
type RemoteClusterLifecyclePayload NamespaceLifecyclePayload

func (p RemoteClusterLifecyclePayload) EventName() string {
	return RemoteClusterLifecycleEventName
}

func (p RemoteClusterLifecyclePayload) Attributes() []log.KeyValue {
	return NamespaceLifecyclePayload(p).Attributes()
}
