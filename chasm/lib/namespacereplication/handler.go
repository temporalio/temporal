package namespacereplication

import (
	"context"
	"slices"
	"time"

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
	namespacereplicationpb.UnimplementedNamespaceReplicationRepairServiceServer

	logger         log.Logger
	repairObserver *namespaceRepairObserver
}

func newServiceHandler(logger log.Logger) *serviceHandler {
	return &serviceHandler{logger: logger}
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
		chasm.WithStartRequestID(req.GetBusinessId()),
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

	if err := validateReconcileNamespaceMutationRequest(req); err != nil {
		return nil, err
	}
	authoritativePeers := stringSet(req.GetAuthoritativePeerCells())
	verifiedPeers := stringSet(req.GetVerifiedPeerCells())
	cutoff := req.GetCutoff().AsTime()
	ref := chasm.NewComponentRef[*NamespaceMutationComponent](chasm.ExecutionKey{
		NamespaceID: req.GetSystemNamespaceId(),
		BusinessID:  req.GetBusinessId(),
		RunID:       req.GetRunId(),
	})
	if existing, closed, err := readClosedReconciliationResult(ctx, ref, req); err != nil {
		return nil, err
	} else if closed {
		return existing, nil
	}

	var observations []manualPeerReconciliationObservation
	var terminal *componentTerminalObservation
	var terminalTask *replicationspb.NamespaceTaskAttributes
	result, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		func(component *NamespaceMutationComponent, mctx chasm.MutableContext, _ chasm.NoValue) (*namespacereplicationpb.ReconcileNamespaceMutationResponse, error) {
			update, err := reconcileRunningNamespaceMutation(
				component,
				mctx,
				req,
				cutoff,
				authoritativePeers,
				verifiedPeers,
			)
			if update != nil {
				observations = update.observations
				terminal = update.terminal
				terminalTask = update.task
				return update.response, err
			}
			return nil, err
		},
		nil,
	)
	if err != nil {
		if existing, closed, readErr := readClosedReconciliationResult(ctx, ref, req); readErr == nil && closed {
			return existing, nil
		}
		return nil, err
	}
	if h.repairObserver != nil {
		h.repairObserver.observe(
			ref,
			terminalTask,
			req,
			observations,
			terminal,
		)
	}
	return result, nil
}

type namespaceMutationReconciliationUpdate struct {
	response     *namespacereplicationpb.ReconcileNamespaceMutationResponse
	observations []manualPeerReconciliationObservation
	terminal     *componentTerminalObservation
	task         *replicationspb.NamespaceTaskAttributes
}

func reconcileRunningNamespaceMutation(
	component *NamespaceMutationComponent,
	mctx chasm.MutableContext,
	request *namespacereplicationpb.ReconcileNamespaceMutationRequest,
	cutoff time.Time,
	authoritativePeers map[string]struct{},
	verifiedPeers map[string]struct{},
) (*namespaceMutationReconciliationUpdate, error) {
	if component.GetMutation().GetNamespaceDetail().GetInfo().GetId() != request.GetNamespaceId() {
		return nil, serviceerror.NewFailedPrecondition("component namespace does not match repair namespace")
	}
	update := &namespaceMutationReconciliationUpdate{
		response: &namespacereplicationpb.ReconcileNamespaceMutationResponse{},
	}
	local := component.GetLocalApply()
	if local.GetOutcome() != namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED &&
		local.GetOutcome() != namespacereplicationpb.LOCAL_APPLY_OUTCOME_SKIPPED_SHADOW {
		update.response.IneligibleReason = "local apply is not successful"
		return update, nil
	}
	if local.GetResolvedAt() == nil || local.GetResolvedAt().AsTime().After(cutoff) {
		update.response.IneligibleReason = "local apply resolved after repair cutoff"
		return update, nil
	}

	update.response.ComponentEligible = true
	peers := mapsKeys(component.GetPeerApply())
	slices.Sort(peers)
	for _, peer := range peers {
		peerResult, observation, err := reconcilePendingPeer(
			component,
			mctx,
			request,
			cutoff,
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
	if component.allPeersTerminal() {
		if err := TransitionAllPeersTerminal.Apply(component, mctx, EventAllPeersTerminal{}); err != nil {
			return nil, err
		}
		update.response.ComponentCompleted = true
		update.terminal = newComponentTerminalObservation(component, component.GetStatus())
	}
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
	cutoff time.Time,
	peer string,
	authoritativePeers map[string]struct{},
	verifiedPeers map[string]struct{},
) (*namespacereplicationpb.PeerReconciliation, *manualPeerReconciliationObservation, error) {
	result := &namespacereplicationpb.PeerReconciliation{TargetCell: peer}
	status := component.GetPeerApply()[peer]
	if status.GetOutcome() != namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING &&
		status.GetOutcome() != namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE {
		result.Outcome = namespacereplicationpb.PeerReconciliation_OUTCOME_ALREADY_TERMINAL
		return result, nil, nil
	}

	basis := namespacereplicationpb.MANUAL_RECONCILIATION_BASIS_REMOVED_MEMBERSHIP
	if _, configured := authoritativePeers[peer]; configured {
		if _, verified := verifiedPeers[peer]; !verified {
			result.Outcome = namespacereplicationpb.PeerReconciliation_OUTCOME_NOT_VERIFIED
			return result, nil, nil
		}
		basis = namespacereplicationpb.MANUAL_RECONCILIATION_BASIS_VERIFIED_TARGET
	}
	if err := TransitionPeerManuallyReconciled.Apply(component, mctx, EventPeerManuallyReconciled{
		Time:       mctx.Now(component),
		Cutoff:     cutoff,
		TargetCell: peer,
		RepairID:   request.GetRepairId(),
		Reason:     request.GetReason(),
		Basis:      basis,
	}); err != nil {
		return nil, nil, err
	}
	result.Outcome = namespacereplicationpb.PeerReconciliation_OUTCOME_RECONCILED
	result.Basis = basis
	status = component.GetPeerApply()[peer]
	return result, &manualPeerReconciliationObservation{
		targetCell:   peer,
		basis:        basis,
		attemptCount: status.GetAttemptCount(),
		lastFailure:  status.GetLastFailure().GetMessage(),
	}, nil
}

func readClosedReconciliationResult(
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
				return &readResult{}, nil
			}
			response := &namespacereplicationpb.ReconcileNamespaceMutationResponse{
				ComponentEligible:  component.GetStatus() == namespacereplicationpb.COMPONENT_STATUS_COMPLETED,
				ComponentCompleted: component.GetStatus() == namespacereplicationpb.COMPONENT_STATUS_COMPLETED,
			}
			if component.GetStatus() != namespacereplicationpb.COMPONENT_STATUS_COMPLETED {
				response.IneligibleReason = "component is not running"
			}
			peers := mapsKeys(component.GetPeerApply())
			slices.Sort(peers)
			for _, peer := range peers {
				response.Peers = append(response.Peers, &namespacereplicationpb.PeerReconciliation{
					TargetCell: peer,
					Outcome:    namespacereplicationpb.PeerReconciliation_OUTCOME_ALREADY_TERMINAL,
				})
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

func validateReconcileNamespaceMutationRequest(req *namespacereplicationpb.ReconcileNamespaceMutationRequest) error {
	if req == nil {
		return serviceerror.NewInvalidArgument("request is required")
	}
	if req.GetSystemNamespaceId() != primitives.SystemNamespaceID {
		return serviceerror.NewInvalidArgument("system_namespace_id must be the Temporal system namespace ID")
	}
	if req.GetBusinessId() == "" || req.GetRunId() == "" || req.GetNamespaceId() == "" {
		return serviceerror.NewInvalidArgument("business_id, run_id, and namespace_id are required")
	}
	if req.GetRepairId() == "" || req.GetReason() == "" {
		return serviceerror.NewInvalidArgument("repair_id and reason are required")
	}
	if req.GetCutoff() == nil || req.GetCutoff().CheckValid() != nil {
		return serviceerror.NewInvalidArgument("a valid cutoff is required")
	}
	authoritativePeers, err := validatePeerSet("authoritative_peer_cells", req.GetAuthoritativePeerCells())
	if err != nil {
		return err
	}
	verifiedPeers, err := validatePeerSet("verified_peer_cells", req.GetVerifiedPeerCells())
	if err != nil {
		return err
	}
	for peer := range verifiedPeers {
		if _, ok := authoritativePeers[peer]; !ok {
			return serviceerror.NewInvalidArgument("verified_peer_cells must be a subset of authoritative_peer_cells")
		}
	}
	return nil
}

func validatePeerSet(field string, peers []string) (map[string]struct{}, error) {
	result := make(map[string]struct{}, len(peers))
	for _, peer := range peers {
		if peer == "" {
			return nil, serviceerror.NewInvalidArgument(field + " contains an empty peer")
		}
		if _, duplicate := result[peer]; duplicate {
			return nil, serviceerror.NewInvalidArgument(field + " contains duplicate peer " + peer)
		}
		result[peer] = struct{}{}
	}
	return result, nil
}

func stringSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

func mapsKeys[V any](values map[string]V) []string {
	result := make([]string, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	return result
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
