package nsreplication

import (
	"context"

	replicationpb "go.temporal.io/api/replication/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/namespace"
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
	// Trusted marks a delivery addressed to this cluster whose source membership
	// decision the receiver trusts (see DeliveryPolicy). It is admitted without
	// consulting the admission policy. Legacy delivery never sets it.
	Trusted bool
}

// DeliveryPolicy decides how a receiver handles an authoritative CHASM delivery
// before applying it. trust reports whether the receiver follows the source's
// membership decision: it then skips its own removed-peer check and admits the
// snapshot because the source addressed it. Trust is honored only for a
// delivery addressed to this cluster. A non-nil error rejects the delivery
// before it can apply. The source treats InvalidArgument as a terminal peer
// failure and handles other codes according to its peer retry rules. The
// default leaves the receiver's membership checks in place.
// An untrusted removal is checked against ReplicationConfig.Clusters, so a
// deployment whose replication resolver decides membership differently must
// trust the deliveries that resolver addressed.
type DeliveryPolicy func(ctx context.Context, task *replicationspb.NamespaceTaskAttributes) (trust bool, err error)

// NewDefaultDeliveryPolicy creates the default DeliveryPolicy.
func NewDefaultDeliveryPolicy() DeliveryPolicy {
	return func(context.Context, *replicationspb.NamespaceTaskAttributes) (bool, error) { return false, nil }
}

// MutationDataValidator checks the final data of an authoritative namespace
// replication snapshot. A returned error rejects the mutation before it starts.
// Shadow mutations skip validation. Implementations treat the data map as read-only.
type MutationDataValidator func(ctx context.Context, namespaceID namespace.ID, namespaceName namespace.Name, data map[string]string) error

// NewDefaultMutationDataValidator creates a validator that accepts all data.
func NewDefaultMutationDataValidator() MutationDataValidator {
	return func(context.Context, namespace.ID, namespace.Name, map[string]string) error { return nil }
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
