package xdc

import (
	"context"
	"time"

	"github.com/google/uuid"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/testing/await"
	"google.golang.org/protobuf/types/known/durationpb"
)

func (s *namespaceReplicationCHASMTestSuite) TestOperatorRepairWithoutPeerTransport() {
	ctx, cancel := context.WithTimeout(s.T().Context(), testTimeout)
	defer cancel()
	active, standby := s.clusters[0], s.clusters[1]
	cleanup := active.OverrideDynamicConfig(s.T(), dynamicconfig.NamespaceReplicationTransportMode, dynamicconfig.NamespaceReplicationTransportModeCHASM)
	defer cleanup()
	s.failApplyRequests.Store(true)
	defer s.failApplyRequests.Store(false)
	name := "repair-" + uuid.NewString()
	_, err := active.FrontendClient().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace: name, IsGlobalNamespace: true, ActiveClusterName: active.ClusterName(),
		Clusters:                         []*replicationpb.ClusterReplicationConfig{{ClusterName: active.ClusterName()}, {ClusterName: standby.ClusterName()}},
		WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	})
	s.Require().NoError(err)
	failed := s.receiveObservedApply(ctx, name)
	s.Require().Error(failed.err)
	snapshot, err := active.AdminClient().GetNamespace(ctx, &adminservice.GetNamespaceRequest{
		Attributes: &adminservice.GetNamespaceRequest_Namespace{Namespace: name},
	})
	s.Require().NoError(err)
	s.Require().NotNil(snapshot.GetObservedAt())
	task := s.decodeNamespaceTask(failed.request)
	task.NamespaceOperation = enumsspb.NAMESPACE_OPERATION_UPDATE
	payload, err := nsreplication.MarshalNamespaceTask(task)
	s.Require().NoError(err)
	response, err := standby.AdminClient().ApplyNamespaceMutation(ctx, &adminservice.ApplyNamespaceMutationRequest{
		NamespaceTaskPayload: payload, SourceCluster: active.ClusterName(),
		Attribution: &adminservice.NamespaceMutationAttribution{
			Origin: adminservice.NamespaceMutationAttribution_ORIGIN_OPERATOR_REPAIR, RepairId: "xdc-repair", Reason: "peer transport unavailable",
		},
	})
	s.Require().NoError(err)
	s.Require().Equal(adminservice.ApplyNamespaceMutationResponse_OUTCOME_CREATED, response.GetOutcome())
	s.requirePersistedNamespace(ctx, standby, name)
	await.RequireTrue(s.T(), func() bool {
		result, reconcileErr := active.AdminClient().ReconcileNamespaceMutationComponents(ctx, &adminservice.ReconcileNamespaceMutationComponentsRequest{
			NamespaceId: snapshot.GetInfo().GetId(), RepairId: "xdc-repair", Reason: "peer transport unavailable",
			CutoffTime: snapshot.GetObservedAt(), AuthoritativePeerClusters: []string{standby.ClusterName()}, VerifiedPeerClusters: []string{standby.ClusterName()},
		})
		if reconcileErr != nil {
			return false
		}
		for _, component := range result.GetComponents() {
			if component.GetBusinessId() == failed.request.GetComponentBusinessId() && component.GetCompleted() {
				for _, peer := range component.GetPeers() {
					if peer.GetTargetCluster() == standby.ClusterName() && peer.GetOutcome() == adminservice.NamespaceMutationPeerReconciliation_OUTCOME_RECONCILED {
						return true
					}
				}
			}
		}
		return false
	}, replicationWaitTime, replicationCheckInterval)
}
