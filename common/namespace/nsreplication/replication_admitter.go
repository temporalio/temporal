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
	// Addressed marks a delivery whose source membership decision the receiver
	// trusts (see DeliveryPolicy). It is admitted without consulting
	// the admission policy. Legacy delivery never sets it.
	Addressed bool
}

// DeliveryPolicy decides how a receiver handles an authoritative CHASM delivery
// before applying it. trust reports whether the receiver follows the source's
// membership decision: it then skips its own removed-peer check and admits the
// snapshot because the source addressed it. Trust is honored only for a
// delivery addressed to this cluster. A non-nil error is returned to the
// source, so a retryable one (prefer FailedPrecondition; Unavailable marks the
// destination down) defers a delivery the receiver cannot apply yet instead of
// letting it succeed partially. The default trusts nothing and defers nothing.
type DeliveryPolicy func(ctx context.Context, task *replicationspb.NamespaceTaskAttributes) (trust bool, err error)

// NewDefaultDeliveryPolicy creates the default DeliveryPolicy.
func NewDefaultDeliveryPolicy() DeliveryPolicy {
	return func(context.Context, *replicationspb.NamespaceTaskAttributes) (bool, error) { return false, nil }
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
