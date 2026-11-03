package frontend

import (
	"context"
	"fmt"
	"slices"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	chasmnsreplication "go.temporal.io/server/chasm/lib/namespacereplication"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"google.golang.org/protobuf/types/known/emptypb"
)

const namespaceMutationRepairVisibilityPageSize = 100

func (adh *AdminHandler) ReconcileNamespaceMutationComponents(
	ctx context.Context,
	request *adminservice.ReconcileNamespaceMutationComponentsRequest,
) (_ *adminservice.ReconcileNamespaceMutationComponentsResponse, retErr error) {
	defer log.CapturePanic(adh.logger, &retErr)
	if err := adh.validateNamespaceMutationRepair(ctx, request); err != nil {
		return nil, err
	}

	ctx = chasm.NewVisibilityManagerContext(ctx, adh.chasmVisibilityManager)
	query := fmt.Sprintf(
		`%s = %q AND ExecutionStatus = "Running"`,
		chasmnsreplication.NamespaceIDSearchAttributeName,
		request.GetNamespaceId(),
	)
	pageSize := int(request.GetPageSize())
	if pageSize <= 0 || pageSize > namespaceMutationRepairVisibilityPageSize {
		pageSize = namespaceMutationRepairVisibilityPageSize
	}
	page, err := chasm.ListExecutions[
		*chasmnsreplication.NamespaceMutationComponent,
		*emptypb.Empty,
	](ctx, &chasm.ListExecutionsRequest{
		NamespaceName: primitives.SystemLocalNamespace,
		Query:         query,
		PageSize:      pageSize,
		NextPageToken: request.GetNextPageToken(),
	})
	if err != nil {
		return nil, fmt.Errorf("list namespace mutation components: %w", err)
	}
	response := &adminservice.ReconcileNamespaceMutationComponentsResponse{
		NextPageToken: page.NextPageToken,
	}
	for _, execution := range page.Executions {
		response.Components = append(response.Components, adh.reconcileNamespaceMutationExecution(
			ctx,
			request,
			execution.BusinessID,
			execution.RunID,
		))
	}
	return response, nil
}

func (adh *AdminHandler) validateNamespaceMutationRepair(
	ctx context.Context,
	request *adminservice.ReconcileNamespaceMutationComponentsRequest,
) error {
	if request == nil {
		return serviceerror.NewInvalidArgument("request is required")
	}
	if request.GetNamespaceId() == "" || request.GetRepairId() == "" || request.GetReason() == "" {
		return serviceerror.NewInvalidArgument("namespace_id, repair_id, and reason are required")
	}
	if request.GetCutoffTime() == nil || request.GetCutoffTime().CheckValid() != nil {
		return serviceerror.NewInvalidArgument("a valid cutoff_time is required")
	}
	if request.GetCutoffTime().AsTime().After(adh.timeSource.Now()) {
		return serviceerror.NewInvalidArgument("cutoff_time cannot be in the server's future")
	}
	authoritativePeers, err := validateNamespaceRepairPeerSet(
		"authoritative_peer_clusters",
		request.GetAuthoritativePeerClusters(),
	)
	if err != nil {
		return err
	}
	verifiedPeers, err := validateNamespaceRepairPeerSet(
		"verified_peer_clusters",
		request.GetVerifiedPeerClusters(),
	)
	if err != nil {
		return err
	}
	currentCluster := adh.clusterMetadata.GetCurrentClusterName()
	if _, included := authoritativePeers[currentCluster]; included {
		return serviceerror.NewInvalidArgument("authoritative_peer_clusters must not include the source cluster")
	}
	for peer := range verifiedPeers {
		if _, configured := authoritativePeers[peer]; !configured {
			return serviceerror.NewInvalidArgument("verified_peer_clusters must be a subset of authoritative_peer_clusters")
		}
	}

	namespaceResponse, err := adh.persistenceMetadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{
		ID: request.GetNamespaceId(),
	})
	if err != nil {
		return err
	}
	detail := namespaceResponse.Namespace
	if !namespaceResponse.IsGlobalNamespace || detail.GetInfo().GetId() != request.GetNamespaceId() {
		return serviceerror.NewFailedPrecondition("repair namespace is not a matching global namespace")
	}
	if state := detail.GetInfo().GetState(); state != enumspb.NAMESPACE_STATE_REGISTERED && state != enumspb.NAMESPACE_STATE_DEPRECATED {
		return serviceerror.NewFailedPrecondition("repair namespace lifecycle state is not supported")
	}
	replicationConfig := detail.GetReplicationConfig()
	if replicationConfig.GetActiveClusterName() != currentCluster {
		return serviceerror.NewFailedPrecondition("repair must run on the active namespace cluster")
	}
	if state := replicationConfig.GetState(); state != enumspb.REPLICATION_STATE_NORMAL && state != enumspb.REPLICATION_STATE_UNSPECIFIED {
		return serviceerror.NewFailedPrecondition("namespace replication state must be NORMAL")
	}

	actualPeers := make([]string, 0, len(replicationConfig.GetClusters()))
	for _, cluster := range replicationConfig.GetClusters() {
		if cluster != currentCluster {
			actualPeers = append(actualPeers, cluster)
		}
	}
	slices.Sort(actualPeers)
	expectedPeers := slices.Clone(request.GetAuthoritativePeerClusters())
	slices.Sort(expectedPeers)
	if !slices.Equal(actualPeers, expectedPeers) {
		return serviceerror.NewFailedPrecondition("authoritative namespace replica set changed before component reconciliation")
	}
	return nil
}

func (adh *AdminHandler) reconcileNamespaceMutationExecution(
	ctx context.Context,
	request *adminservice.ReconcileNamespaceMutationComponentsRequest,
	businessID string,
	runID string,
) *adminservice.NamespaceMutationComponentReconciliation {
	result := &adminservice.NamespaceMutationComponentReconciliation{
		BusinessId: businessID,
		RunId:      runID,
	}
	response, err := adh.chasmNamespaceReplicationRepairClient.ReconcileNamespaceMutation(
		ctx,
		&namespacereplicationpb.ReconcileNamespaceMutationRequest{
			SystemNamespaceId:      primitives.SystemNamespaceID,
			BusinessId:             businessID,
			RunId:                  runID,
			NamespaceId:            request.GetNamespaceId(),
			RepairId:               request.GetRepairId(),
			Reason:                 request.GetReason(),
			Cutoff:                 request.GetCutoffTime(),
			AuthoritativePeerCells: request.GetAuthoritativePeerClusters(),
			VerifiedPeerCells:      request.GetVerifiedPeerClusters(),
		},
	)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	result.Eligible = response.GetComponentEligible()
	result.IneligibleReason = response.GetIneligibleReason()
	result.Completed = response.GetComponentCompleted()
	for _, peer := range response.GetPeers() {
		result.Peers = append(result.Peers, &adminservice.NamespaceMutationPeerReconciliation{
			TargetCluster: peer.GetTargetCell(),
			Outcome:       namespaceMutationPeerReconciliationOutcome(peer.GetOutcome()),
			Basis:         namespaceMutationPeerReconciliationBasis(peer.GetBasis()),
		})
	}
	return result
}

func namespaceMutationPeerReconciliationOutcome(
	outcome namespacereplicationpb.PeerReconciliation_Outcome,
) adminservice.NamespaceMutationPeerReconciliation_Outcome {
	switch outcome {
	case namespacereplicationpb.PeerReconciliation_OUTCOME_RECONCILED:
		return adminservice.NamespaceMutationPeerReconciliation_OUTCOME_RECONCILED
	case namespacereplicationpb.PeerReconciliation_OUTCOME_ALREADY_TERMINAL:
		return adminservice.NamespaceMutationPeerReconciliation_OUTCOME_ALREADY_TERMINAL
	case namespacereplicationpb.PeerReconciliation_OUTCOME_NOT_VERIFIED:
		return adminservice.NamespaceMutationPeerReconciliation_OUTCOME_NOT_VERIFIED
	default:
		return adminservice.NamespaceMutationPeerReconciliation_OUTCOME_UNSPECIFIED
	}
}

func namespaceMutationPeerReconciliationBasis(
	basis namespacereplicationpb.ManualReconciliationBasis,
) adminservice.NamespaceMutationPeerReconciliation_Basis {
	switch basis {
	case namespacereplicationpb.MANUAL_RECONCILIATION_BASIS_VERIFIED_TARGET:
		return adminservice.NamespaceMutationPeerReconciliation_BASIS_VERIFIED_TARGET
	case namespacereplicationpb.MANUAL_RECONCILIATION_BASIS_REMOVED_MEMBERSHIP:
		return adminservice.NamespaceMutationPeerReconciliation_BASIS_REMOVED_MEMBERSHIP
	default:
		return adminservice.NamespaceMutationPeerReconciliation_BASIS_UNSPECIFIED
	}
}

func validateNamespaceRepairPeerSet(field string, peers []string) (map[string]struct{}, error) {
	result := make(map[string]struct{}, len(peers))
	for _, peer := range peers {
		if peer == "" {
			return nil, serviceerror.NewInvalidArgument(field + " contains an empty cluster")
		}
		if _, duplicate := result[peer]; duplicate {
			return nil, serviceerror.NewInvalidArgument(field + " contains duplicate cluster " + peer)
		}
		result[peer] = struct{}{}
	}
	return result, nil
}
