package namespacereplication

import (
	"context"
	"maps"
	"slices"

	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.temporal.io/server/common/log"
	nsreplicationcommon "go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/primitives"
	"google.golang.org/protobuf/proto"
)

type serviceHandler struct {
	namespacereplicationpb.UnimplementedNamespaceReplicationServiceServer

	logger         log.Logger
	repairObserver *namespaceRepairObserver
}

func newServiceHandler(logger log.Logger, observer *namespaceRepairObserver) *serviceHandler {
	return &serviceHandler{logger: logger, repairObserver: observer}
}

func (h *serviceHandler) TriggerNamespaceMutation(
	ctx context.Context,
	req *namespacereplicationpb.TriggerNamespaceMutationRequest,
) (response *namespacereplicationpb.TriggerNamespaceMutationResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)

	if err := validateTriggerNamespaceMutationRequest(req); err != nil {
		return nil, err
	}

	key := executionKey(req)
	startResult, err := chasm.StartExecution[*NamespaceMutationComponent, *namespacereplicationpb.NamespaceMutation](
		ctx,
		key,
		func(mctx chasm.MutableContext, mutation *namespacereplicationpb.NamespaceMutation) (*NamespaceMutationComponent, error) {
			component := NewNamespaceMutationComponent(mutation)
			component.initializeVisibility(mctx)
			if err := TransitionScheduleLocal.Apply(component, mctx, EventScheduleLocal{}); err != nil {
				return nil, err
			}
			return component, nil
		},
		req.GetMutation(),
		chasm.WithRequestID(req.GetBusinessId()),
	)
	if err != nil {
		return nil, err
	}

	ref := chasm.NewComponentRef[*NamespaceMutationComponent](startResult.ExecutionKey)
	result, _, err := chasm.PollComponent(
		ctx,
		ref,
		func(component *NamespaceMutationComponent, _ chasm.Context, _ chasm.NoValue) (*namespacereplicationpb.TriggerNamespaceMutationResponse, bool, error) {
			local := component.GetLocalApply()
			switch local.GetOutcome() {
			case namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED,
				namespacereplicationpb.LOCAL_APPLY_OUTCOME_SKIPPED_SHADOW:
				return &namespacereplicationpb.TriggerNamespaceMutationResponse{}, true, nil
			case namespacereplicationpb.LOCAL_APPLY_OUTCOME_FAILED,
				namespacereplicationpb.LOCAL_APPLY_OUTCOME_INDETERMINATE:
				failure := local.GetFailure()
				return nil, true, localApplyError(failure.GetApplicationFailureInfo().GetType(), failure.GetMessage())
			default:
				return nil, false, nil
			}
		},
		nil,
	)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (h *serviceHandler) ReconcileNamespaceMutation(
	ctx context.Context,
	req *namespacereplicationpb.ReconcileNamespaceMutationRequest,
) (response *namespacereplicationpb.ReconcileNamespaceMutationResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)

	authoritativePeers, verifiedPeers, err := validateReconcileNamespaceMutationRequest(req)
	if err != nil {
		return nil, err
	}
	ref := chasm.NewComponentRef[*NamespaceMutationComponent](chasm.ExecutionKey{
		NamespaceID: req.GetSystemNamespaceId(),
		BusinessID:  req.GetBusinessId(),
		RunID:       req.GetRunId(),
	})
	if existing, closed, err := readReconciliationResult(ctx, ref, req); err != nil {
		return nil, err
	} else if closed {
		return existing, nil
	}

	result, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		func(component *NamespaceMutationComponent, mctx chasm.MutableContext, _ chasm.NoValue) (*namespaceMutationReconciliationUpdate, error) {
			return reconcileRunningNamespaceMutation(
				component,
				mctx,
				req,
				authoritativePeers,
				verifiedPeers,
			)
		},
		nil,
	)
	if err != nil {
		if existing, closed, readErr := readReconciliationResult(ctx, ref, req); readErr == nil && closed {
			return existing, nil
		}
		return nil, err
	}
	if h.repairObserver != nil {
		h.repairObserver.observe(
			ref,
			result.task,
			req,
			result.observations,
			result.terminal,
		)
	}
	return result.response, nil
}

type namespaceMutationReconciliationUpdate struct {
	response     *namespacereplicationpb.ReconcileNamespaceMutationResponse
	observations []manualPeerReconciliationObservation
	terminal     *componentTerminalObservation
	task         *replicationspb.NamespaceTaskAttributes
}

func namespaceReconciliationIneligibleReason(component *NamespaceMutationComponent, request *namespacereplicationpb.ReconcileNamespaceMutationRequest) string {
	local := component.GetLocalApply()
	if !localApplyAllowsPeerFanout(local.GetOutcome()) {
		return "local apply is not successful"
	}
	detail := component.GetMutation().GetNamespaceDetail()
	// Versions establish snapshot coverage; the cutoff separately limits operator scope.
	if detail.GetConfigVersion() > request.GetVerifiedConfigVersion().GetValue() || detail.GetFailoverVersion() > request.GetVerifiedFailoverVersion().GetValue() {
		return "mutation versions are newer than the verified source"
	}
	if local.GetResolvedAt() == nil || local.GetResolvedAt().AsTime().After(request.GetCutoff().AsTime()) {
		return "local apply resolved after repair cutoff"
	}
	return ""
}

func reconcileRunningNamespaceMutation(
	component *NamespaceMutationComponent,
	mctx chasm.MutableContext,
	request *namespacereplicationpb.ReconcileNamespaceMutationRequest,
	authoritativePeers map[string]struct{},
	verifiedPeers map[string]struct{},
) (*namespaceMutationReconciliationUpdate, error) {
	if component.GetMutation().GetNamespaceDetail().GetInfo().GetId() != request.GetNamespaceId() {
		return nil, serviceerror.NewFailedPrecondition("component namespace does not match repair namespace")
	}
	update := &namespaceMutationReconciliationUpdate{
		response: &namespacereplicationpb.ReconcileNamespaceMutationResponse{},
	}
	if reason := namespaceReconciliationIneligibleReason(component, request); reason != "" {
		update.response.IneligibleReason = reason
		return update, nil
	}

	update.response.ComponentEligible = true
	peers := slices.Sorted(maps.Keys(component.GetPeerApply()))
	for _, peer := range peers {
		peerResult, observation, err := reconcilePendingPeer(
			component,
			mctx,
			request,
			peer,
			authoritativePeers,
			verifiedPeers,
		)
		if err != nil {
			return nil, err
		}
		update.response.Peers = append(update.response.Peers, peerResult)
		if observation != nil {
			update.observations = append(update.observations, *observation)
		}
	}
	terminal, err := completeIfAllPeersTerminal(component, mctx)
	if err != nil {
		return nil, err
	}
	update.terminal = terminal
	update.response.ComponentCompleted = terminal != nil

	update.task = nsreplicationcommon.NamespaceDetailToTaskAttributes(
		component.GetMutation().GetOperation(),
		component.GetMutation().GetNamespaceDetail(),
	)
	return update, nil
}

func reconcilePendingPeer(
	component *NamespaceMutationComponent,
	mctx chasm.MutableContext,
	request *namespacereplicationpb.ReconcileNamespaceMutationRequest,
	peer string,
	authoritativePeers map[string]struct{},
	verifiedPeers map[string]struct{},
) (*namespacereplicationpb.PeerReconciliation, *manualPeerReconciliationObservation, error) {
	status := component.GetPeerApply()[peer]
	if !peerNeedsReconciliation(status) {
		return peerReconciliationResult(peer, status, enumsspb.NAMESPACE_RECONCILIATION_OUTCOME_ALREADY_TERMINAL), nil, nil
	}

	basis := enumsspb.MANUAL_RECONCILIATION_BASIS_REMOVED_MEMBERSHIP
	if _, configured := authoritativePeers[peer]; configured {
		if _, verified := verifiedPeers[peer]; !verified {
			return peerReconciliationResult(peer, status, enumsspb.NAMESPACE_RECONCILIATION_OUTCOME_NOT_VERIFIED), nil, nil
		}
		basis = enumsspb.MANUAL_RECONCILIATION_BASIS_VERIFIED_TARGET
	}
	err := TransitionPeerManuallyReconciled.Apply(component, mctx, EventPeerManuallyReconciled{
		Time:       mctx.Now(component),
		Cutoff:     request.GetCutoff().AsTime(),
		TargetCell: peer,
		RepairID:   request.GetRepairId(),
		Reason:     request.GetReason(),
		Basis:      basis,
	})
	if err != nil {
		return nil, nil, err
	}
	status = component.GetPeerApply()[peer]
	return peerReconciliationResult(peer, status, enumsspb.NAMESPACE_RECONCILIATION_OUTCOME_RECONCILED), &manualPeerReconciliationObservation{
		targetCell:   peer,
		basis:        basis,
		attemptCount: status.GetAttemptCount(),
		lastFailure:  status.GetLastFailure().GetMessage(),
	}, nil
}

func peerReconciliationResult(
	peer string,
	status *namespacereplicationpb.PeerApplyStatus,
	outcome enumsspb.NamespaceReconciliationOutcome,
) *namespacereplicationpb.PeerReconciliation {
	return &namespacereplicationpb.PeerReconciliation{
		TargetCell: peer,
		Outcome:    outcome,
		Basis:      status.GetManualReconciliation().GetBasis(),
	}
}

func readReconciliationResult(
	ctx context.Context,
	ref chasm.ComponentRef,
	req *namespacereplicationpb.ReconcileNamespaceMutationRequest,
) (*namespacereplicationpb.ReconcileNamespaceMutationResponse, bool, error) {
	type readResult struct {
		response *namespacereplicationpb.ReconcileNamespaceMutationResponse
		closed   bool
	}
	result, err := chasm.ReadComponent(
		chasm.NewContextWithOperationIntent(ctx, chasm.OperationIntentObserve),
		ref,
		func(component *NamespaceMutationComponent, _ chasm.Context, _ chasm.NoValue) (*readResult, error) {
			if component.GetMutation().GetNamespaceDetail().GetInfo().GetId() != req.GetNamespaceId() {
				return nil, serviceerror.NewFailedPrecondition("component namespace does not match repair namespace")
			}
			if component.GetStatus() == namespacereplicationpb.COMPONENT_STATUS_RUNNING {
				if reason := namespaceReconciliationIneligibleReason(component, req); reason != "" {
					return &readResult{closed: true, response: &namespacereplicationpb.ReconcileNamespaceMutationResponse{IneligibleReason: reason}}, nil
				}
				return &readResult{}, nil
			}
			response := &namespacereplicationpb.ReconcileNamespaceMutationResponse{
				ComponentEligible:  component.GetStatus() == namespacereplicationpb.COMPONENT_STATUS_COMPLETED,
				ComponentCompleted: component.GetStatus() == namespacereplicationpb.COMPONENT_STATUS_COMPLETED,
			}
			if component.GetStatus() != namespacereplicationpb.COMPONENT_STATUS_COMPLETED {
				response.IneligibleReason = "component is not running"
			}
			peers := slices.Sorted(maps.Keys(component.GetPeerApply()))
			for _, peer := range peers {
				response.Peers = append(response.Peers, peerReconciliationResult(
					peer, component.GetPeerApply()[peer], enumsspb.NAMESPACE_RECONCILIATION_OUTCOME_ALREADY_TERMINAL,
				))
			}
			return &readResult{response: response, closed: true}, nil
		},
		nil,
	)
	if err != nil {
		return nil, false, err
	}
	return result.response, result.closed, nil
}

func validateReconcileNamespaceMutationRequest(req *namespacereplicationpb.ReconcileNamespaceMutationRequest) (authoritativePeers, verifiedPeers map[string]struct{}, err error) {
	if req == nil {
		return nil, nil, serviceerror.NewInvalidArgument("request is required")
	}
	if req.GetSystemNamespaceId() != primitives.SystemNamespaceID {
		return nil, nil, serviceerror.NewInvalidArgument("system_namespace_id must be the Temporal system namespace ID")
	}
	if req.GetBusinessId() == "" || req.GetRunId() == "" || req.GetNamespaceId() == "" {
		return nil, nil, serviceerror.NewInvalidArgument("business_id, run_id, and namespace_id are required")
	}
	if req.GetRepairId() == "" || req.GetReason() == "" {
		return nil, nil, serviceerror.NewInvalidArgument("repair_id and reason are required")
	}
	if req.VerifiedConfigVersion == nil || req.VerifiedFailoverVersion == nil {
		return nil, nil, serviceerror.NewInvalidArgument("verified config and failover versions are required")
	}
	if req.GetCutoff() == nil || req.GetCutoff().CheckValid() != nil {
		return nil, nil, serviceerror.NewInvalidArgument("a valid cutoff is required")
	}
	return nsreplicationcommon.ValidatePeerSets(req.GetAuthoritativePeerCells(), req.GetVerifiedPeerCells())
}

func validateTriggerNamespaceMutationRequest(req *namespacereplicationpb.TriggerNamespaceMutationRequest) error {
	if req == nil || req.GetMutation() == nil {
		return serviceerror.NewInvalidArgument("mutation is required")
	}
	if !req.GetMutation().GetIsGlobalNamespace() {
		return serviceerror.NewInvalidArgument("mutation.is_global_namespace must be true")
	}
	if req.GetMutation().GetReplicateOnly() && req.GetMutation().GetShadow() {
		return serviceerror.NewInvalidArgument("mutation.replicate_only and mutation.shadow are mutually exclusive")
	}
	if req.GetMutation().GetReplicateOnly() && req.GetMutation().GetOperation() != enumsspb.NAMESPACE_OPERATION_UPDATE {
		return serviceerror.NewInvalidArgument("mutation.replicate_only requires an update operation")
	}
	if req.GetMutation().GetShadow() {
		if len(req.GetMutation().GetShadowLegacyTaskPayload()) == 0 {
			return serviceerror.NewInvalidArgument("mutation.shadow_legacy_task_payload is required")
		}
		if err := proto.Unmarshal(req.GetMutation().GetShadowLegacyTaskPayload(), &replicationspb.NamespaceTaskAttributes{}); err != nil {
			return serviceerror.NewInvalidArgumentf("decode mutation.shadow_legacy_task_payload: %v", err)
		}
	}
	if req.GetNamespaceId() == "" {
		return serviceerror.NewInvalidArgument("namespace_id is required")
	}
	if req.GetSystemNamespaceId() == "" {
		return serviceerror.NewInvalidArgument("system_namespace_id is required")
	}
	if req.GetSystemNamespaceId() != primitives.SystemNamespaceID {
		return serviceerror.NewInvalidArgument("system_namespace_id must be the Temporal system namespace ID")
	}
	if req.GetBusinessId() == "" {
		return serviceerror.NewInvalidArgument("business_id is required")
	}
	detail := req.GetMutation().GetNamespaceDetail()
	if detail == nil {
		return serviceerror.NewInvalidArgument("mutation.namespace_detail is required")
	}
	if detail.GetInfo() == nil {
		return serviceerror.NewInvalidArgument("mutation.namespace_detail.info is required")
	}
	if detail.GetConfig() == nil {
		return serviceerror.NewInvalidArgument("mutation.namespace_detail.config is required")
	}
	if detail.GetReplicationConfig() == nil {
		return serviceerror.NewInvalidArgument("mutation.namespace_detail.replication_config is required")
	}
	if detail.GetInfo().GetId() == "" {
		return serviceerror.NewInvalidArgument("mutation.namespace_detail.info.id is required")
	}
	if req.GetNamespaceId() != detail.GetInfo().GetId() {
		return serviceerror.NewInvalidArgument("namespace_id must match mutation.namespace_detail.info.id")
	}
	switch req.GetMutation().GetOperation() {
	case enumsspb.NAMESPACE_OPERATION_CREATE,
		enumsspb.NAMESPACE_OPERATION_UPDATE:
		return nil
	default:
		return serviceerror.NewInvalidArgument("mutation.operation must be create or update")
	}
}

func executionKey(req *namespacereplicationpb.TriggerNamespaceMutationRequest) chasm.ExecutionKey {
	return chasm.ExecutionKey{
		NamespaceID: req.GetSystemNamespaceId(),
		BusinessID:  req.GetBusinessId(),
	}
}

func localApplyError(errType, message string) error {
	if message == "" {
		message = "local apply failed"
	}
	switch errType {
	case localFailureUnavailable:
		return serviceerror.NewUnavailable(message)
	case localFailureInvalidArgument:
		return serviceerror.NewInvalidArgument(message)
	case localFailureAlreadyExists:
		return serviceerror.NewNamespaceAlreadyExists(message)
	default:
		return serviceerror.NewInternal(message)
	}
}
