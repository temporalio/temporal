package nsreplication

import (
	"context"

	replicationpb "go.temporal.io/api/replication/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
)

// NamespaceReplicationAdmitter decides whether to process a namespace
// replication task when the namespace does not yet exist locally.
type NamespaceReplicationAdmitter interface {
	Admit(ctx context.Context, currentCluster string, task *replicationspb.NamespaceTaskAttributes, options AdmissionOptions) bool
}

// AdmissionOptions carries delivery intent to the namespace admission policy.
// The zero value preserves legacy membership-based admission.
type AdmissionOptions struct {
	// RemovedPeer marks a validated authoritative UPDATE to a removed cluster.
	// The default policy retains the snapshot even if no namespace exists locally,
	// fencing delayed older additions. Custom policies retain control of admission.
	RemovedPeer bool
}

// DefaultAdmitter accepts member snapshots and explicit removal deliveries.
type DefaultAdmitter struct{}

// NewDefaultAdmitter creates the default NamespaceReplicationAdmitter.
func NewDefaultAdmitter() NamespaceReplicationAdmitter {
	return &DefaultAdmitter{}
}

// Admit accepts members and explicit removal deliveries. Legacy delivery does
// not set removal intent and retains the membership-only admission policy.
func (*DefaultAdmitter) Admit(
	_ context.Context,
	currentCluster string,
	task *replicationspb.NamespaceTaskAttributes,
	options AdmissionOptions,
) bool {
	clusters := task.GetReplicationConfig().GetClusters()
	return checkClusterIncludedInReplicationConfig(currentCluster, clusters) ||
		(currentCluster != "" && options.RemovedPeer &&
			task.GetNamespaceOperation() == enumsspb.NAMESPACE_OPERATION_UPDATE && len(clusters) > 0)
}

func checkClusterIncludedInReplicationConfig(clusterName string, repCfg []*replicationpb.ClusterReplicationConfig) bool {
	for _, cluster := range repCfg {
		if clusterName == cluster.GetClusterName() {
			return true
		}
	}
	return false
}
