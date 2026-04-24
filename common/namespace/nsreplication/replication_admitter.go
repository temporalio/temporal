package nsreplication

import (
	"context"

	replicationpb "go.temporal.io/api/replication/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
)

// NamespaceReplicationAdmitter decides whether to process a namespace
// replication task when the namespace does not yet exist locally.
type NamespaceReplicationAdmitter interface {
	Admit(ctx context.Context, currentCluster string, task *replicationspb.NamespaceTaskAttributes) bool
}

// DefaultAdmitter is the default implementation: admit the task iff the
// current cluster is listed in task.ReplicationConfig.Clusters.
type DefaultAdmitter struct{}

// NewDefaultAdmitter creates the default NamespaceReplicationAdmitter.
func NewDefaultAdmitter() NamespaceReplicationAdmitter {
	return &DefaultAdmitter{}
}

// Admit returns true iff currentCluster appears in the task's replication
// config cluster list.
func (*DefaultAdmitter) Admit(
	_ context.Context,
	currentCluster string,
	task *replicationspb.NamespaceTaskAttributes,
) bool {
	return checkClusterIncludedInReplicationConfig(currentCluster, task.GetReplicationConfig().GetClusters())
}

func checkClusterIncludedInReplicationConfig(clusterName string, repCfg []*replicationpb.ClusterReplicationConfig) bool {
	for _, cluster := range repCfg {
		if clusterName == cluster.GetClusterName() {
			return true
		}
	}
	return false
}
