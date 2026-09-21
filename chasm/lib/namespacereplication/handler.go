package namespacereplication

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives"
)

type handler struct {
	namespacereplicationpb.UnimplementedNamespaceReplicationServiceServer

	logger log.Logger
}

func newHandler(logger log.Logger) *handler {
	return &handler{logger: logger}
}

func (h *handler) TriggerNamespaceMutation(
	ctx context.Context,
	req *namespacereplicationpb.TriggerNamespaceMutationRequest,
) (response *namespacereplicationpb.TriggerNamespaceMutationResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)

	if err := validateTriggerNamespaceMutationRequest(req); err != nil {
		return nil, err
	}
	if req.GetMutation().GetReplicateOnly() && req.GetMutation().GetShadow() {
		return nil, serviceerror.NewInvalidArgument("mutation.replicate_only and mutation.shadow are mutually exclusive")
	}
	if req.GetMutation().GetReplicateOnly() && req.GetMutation().GetOperation() != namespacereplicationpb.NAMESPACE_OPERATION_UPDATE {
		return nil, serviceerror.NewInvalidArgument("mutation.replicate_only requires an update operation")
	}

	key := executionKey(req)
	startResult, err := chasm.StartExecution[*NamespaceMutationComponent, *namespacereplicationpb.NamespaceMutation](
		ctx,
		key,
		func(mctx chasm.MutableContext, mutation *namespacereplicationpb.NamespaceMutation) (*NamespaceMutationComponent, error) {
			component := NewNamespaceMutationComponent(mutation)
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
			case namespacereplicationpb.LOCAL_APPLY_OUTCOME_FAILED:
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

func validateTriggerNamespaceMutationRequest(req *namespacereplicationpb.TriggerNamespaceMutationRequest) error {
	if req == nil || req.GetMutation() == nil {
		return serviceerror.NewInvalidArgument("mutation is required")
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
	case namespacereplicationpb.NAMESPACE_OPERATION_CREATE,
		namespacereplicationpb.NAMESPACE_OPERATION_UPDATE:
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
