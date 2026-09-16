package namespacereplication

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.temporal.io/server/common/log"
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

	if req == nil || req.GetMutation() == nil {
		return nil, serviceerror.NewInvalidArgument("mutation is required")
	}
	if req.GetNamespaceId() == "" {
		return nil, serviceerror.NewInvalidArgument("namespace_id is required")
	}
	if req.GetSystemNamespaceId() == "" {
		return nil, serviceerror.NewInvalidArgument("system_namespace_id is required")
	}
	if req.GetBusinessId() == "" {
		return nil, serviceerror.NewInvalidArgument("business_id is required")
	}
	if req.GetMutation().GetNamespaceDetail() == nil {
		return nil, serviceerror.NewInvalidArgument("mutation.namespace_detail is required")
	}

	key := executionKey(req)
	if _, err := chasm.StartExecution[*NamespaceMutationComponent, *namespacereplicationpb.NamespaceMutation](
		ctx,
		key,
		func(mctx chasm.MutableContext, mutation *namespacereplicationpb.NamespaceMutation) (*NamespaceMutationComponent, error) {
			component := NewNamespaceMutationComponent(mutation)
			component.Visibility = chasm.NewComponentField(mctx, chasm.NewVisibility(mctx))
			if err := TransitionScheduleLocal.Apply(component, mctx, EventScheduleLocal{}); err != nil {
				return nil, err
			}
			return component, nil
		},
		req.GetMutation(),
	); err != nil {
		return nil, err
	}

	ref := chasm.NewComponentRef[*NamespaceMutationComponent](key)
	result, _, err := chasm.PollComponent(
		ctx,
		ref,
		func(component *NamespaceMutationComponent, _ chasm.Context, _ chasm.NoValue) (*namespacereplicationpb.TriggerNamespaceMutationResponse, bool, error) {
			local := component.GetLocalApply()
			switch local.GetOutcome() {
			case namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED:
				return &namespacereplicationpb.TriggerNamespaceMutationResponse{NewVersion: local.GetNewVersion()}, true, nil
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
