package activity

import (
	"context"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
)

var (
	businessIDReusePolicyMap = map[enumspb.ActivityIdReusePolicy]chasm.BusinessIDReusePolicy{
		enumspb.ACTIVITY_ID_REUSE_POLICY_ALLOW_DUPLICATE:             chasm.BusinessIDReusePolicyAllowDuplicate,
		enumspb.ACTIVITY_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY: chasm.BusinessIDReusePolicyAllowDuplicateFailedOnly,
		enumspb.ACTIVITY_ID_REUSE_POLICY_REJECT_DUPLICATE:            chasm.BusinessIDReusePolicyRejectDuplicate,
	}

	// TODO this will change once we rebase on main
	businessIDConflictPolicyMap = map[enumspb.ActivityIdConflictPolicy]chasm.BusinessIDConflictPolicy{
		enumspb.ACTIVITY_ID_CONFLICT_POLICY_FAIL: chasm.BusinessIDConflictPolicyFail,
	}
)

type handler struct {
	activitypb.UnimplementedActivityServiceServer
	config *Config
}

func newHandler(config *Config) *handler {
	return &handler{
		config: config,
	}
}

func (h *handler) StartActivityExecution(ctx context.Context, req *activitypb.StartActivityExecutionRequest) (*activitypb.StartActivityExecutionResponse, error) {
	frontendReq := req.GetFrontendRequest()

	reusePolicy, ok := businessIDReusePolicyMap[frontendReq.GetIdReusePolicy()]
	if !ok {
		return nil, serviceerror.NewFailedPrecondition(fmt.Sprintf("unsupported ID reuse policy: %v", frontendReq.GetIdReusePolicy()))
	}

	conflictPolicy, ok := businessIDConflictPolicyMap[frontendReq.GetIdConflictPolicy()]
	if !ok {
		return nil, serviceerror.NewFailedPrecondition(fmt.Sprintf("unsupported ID conflict policy: %v", frontendReq.GetIdConflictPolicy()))
	}

	response, key, _, err := chasm.NewExecution(
		ctx,
		chasm.ExecutionKey{
			NamespaceID: req.GetNamespaceId(),
			BusinessID:  req.GetFrontendRequest().GetActivityId(),
		},
		func(mutableContext chasm.MutableContext, request *workflowservice.StartActivityExecutionRequest) (*Activity, *workflowservice.StartActivityExecutionResponse, error) {
			newActivity, err := NewStandaloneActivity(mutableContext, request)
			if err != nil {
				return nil, nil, err
			}

			err = TransitionScheduled.Apply(newActivity, mutableContext, nil)
			if err != nil {
				return nil, nil, err
			}

			return newActivity, &workflowservice.StartActivityExecutionResponse{
				Started: true,
				// EagerTask: TODO when supported, need to call the same code that would handle the HandleStarted API
			}, nil
		},
		req.GetFrontendRequest(),
		chasm.WithRequestID(req.GetFrontendRequest().GetRequestId()),
		chasm.WithBusinessIDPolicy(reusePolicy, conflictPolicy),
	)

	if err != nil {
		return nil, err
	}

	response.RunId = key.RunID

	return &activitypb.StartActivityExecutionResponse{
		FrontendResponse: response,
	}, nil
}

// TerminateActivityExecution terminates a standalone activity execution
func (h *handler) TerminateActivityExecution(
	ctx context.Context,
	req *activitypb.TerminateActivityExecutionRequest,
) (response *activitypb.TerminateActivityExecutionResponse, err error) {
	frontendReq := req.GetFrontendRequest()

	ref := chasm.NewComponentRef[*Activity](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  frontendReq.GetActivityId(),
		RunID:       frontendReq.GetRunId(),
	})

	response, _, err = chasm.UpdateComponent(
		ctx,
		ref,
		(*Activity).handleTerminated,
		req,
	)

	if err != nil {
		return nil, err
	}

	return response, nil
}

// RequestCancelActivityExecution requests cancellation on a standalone activity execution
func (h *handler) RequestCancelActivityExecution(
	ctx context.Context,
	req *activitypb.RequestCancelActivityExecutionRequest,
) (response *activitypb.RequestCancelActivityExecutionResponse, err error) {
	frontendReq := req.GetFrontendRequest()

	ref := chasm.NewComponentRef[*Activity](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  frontendReq.GetActivityId(),
		RunID:       frontendReq.GetRunId(),
	})

	response, _, err = chasm.UpdateComponent(
		ctx,
		ref,
		(*Activity).handleCancellationRequested,
		req,
	)
	if err != nil {
		return nil, err
	}

	return response, nil
}
