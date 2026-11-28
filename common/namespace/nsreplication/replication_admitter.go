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

// MembershipDecision selects whose membership decision governs removal
// validation and admission of an incoming snapshot.
type MembershipDecision int

const (
	// UseReceiverMembership applies the receiver's removal and admission checks.
	UseReceiverMembership MembershipDecision = iota
	// UseSourceMembership accepts the source's membership decision for a delivery
	// addressed to this receiver, bypassing the receiver's membership checks.
	UseSourceMembership
)

func (d MembershipDecision) String() string {
	switch d {
	case UseReceiverMembership:
		return "receiver"
	case UseSourceMembership:
		return "source"
	default:
		return "unknown"
	}
}

// AdmissionOptions carries delivery intent to the namespace admission policy.
// The zero value preserves legacy membership-based admission.
type AdmissionOptions struct {
	// RemovedPeer marks a validated authoritative UPDATE to a removed cluster.
	// The default policy retains the snapshot even if no namespace exists locally,
	// fencing delayed older additions.
	RemovedPeer bool
	// Membership is the effective membership decision for this delivery.
	Membership MembershipDecision
}

// DeliveryPolicy validates an authoritative CHASM delivery and selects the
// membership decision. UseSourceMembership is honored only for a delivery
// addressed to this cluster. A non-nil error rejects the delivery
// before it can apply. The source treats InvalidArgument as a terminal peer
// failure and handles other codes according to its peer retry rules.
// UseReceiverMembership checks removals against ReplicationConfig.Clusters.
// Deployments whose resolver derives membership differently should select
// UseSourceMembership for the deliveries their resolver addresses.
type DeliveryPolicy func(ctx context.Context, task *replicationspb.NamespaceTaskAttributes) (MembershipDecision, error)

// NewDefaultDeliveryPolicy creates the default DeliveryPolicy.
func NewDefaultDeliveryPolicy() DeliveryPolicy {
	return func(context.Context, *replicationspb.NamespaceTaskAttributes) (MembershipDecision, error) {
		return UseReceiverMembership, nil
	}
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
