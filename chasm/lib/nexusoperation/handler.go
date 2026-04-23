package nexusoperation

import (
	"context"
	"errors"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/log"
)

type handler struct {
	nexusoperationpb.UnimplementedNexusOperationServiceServer

	config *Config
	logger log.Logger
}

func newHandler(config *Config, logger log.Logger) *handler {
	return &handler{
		config: config,
		logger: logger,
	}
}

// StartNexusOperation creates a new standalone Nexus operation execution via CHASM.
func (h *handler) StartNexusOperation(
	ctx context.Context,
	req *nexusoperationpb.StartNexusOperationRequest,
) (response *nexusoperationpb.StartNexusOperationResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	frontendReq := req.GetFrontendRequest()

	result, err := chasm.StartExecution[*Operation](
		ctx,
		chasm.ExecutionKey{
			NamespaceID: req.GetNamespaceId(),
			BusinessID:  frontendReq.GetOperationId(),
		},
		newStandaloneOperation,
		req,
		chasm.WithRequestID(frontendReq.GetRequestId()),
		chasm.WithBusinessIDPolicy(
			idReusePolicyFromProto(frontendReq.GetIdReusePolicy()),
			idConflictPolicyFromProto(frontendReq.GetIdConflictPolicy()),
		),
	)
	if err != nil {
		if alreadyStartedErr, ok := errors.AsType[*chasm.ExecutionAlreadyStartedError](err); ok {
			return nil, serviceerror.NewNexusOperationExecutionAlreadyStartedf(
				alreadyStartedErr.CurrentRequestID,
				alreadyStartedErr.CurrentRunID,
				"nexus operation execution already started: request_id=%s, run_id=%s",
				alreadyStartedErr.CurrentRequestID,
				alreadyStartedErr.CurrentRunID,
			)
		}
		return nil, err
	}

	return &nexusoperationpb.StartNexusOperationResponse{
		FrontendResponse: &workflowservice.StartNexusOperationExecutionResponse{
			RunId:   result.ExecutionKey.RunID,
			Started: result.Created,
		},
	}, nil
}

// DescribeNexusOperation queries current operation state, optionally as a long-poll that waits
// for any state change.
//
// When used to long-poll, it returns an empty non-error response on context
// deadline expiry, to indicate that the state being waited for was not reached. Callers should
// interpret this as an invitation to resubmit their long-poll request. This response is sent before
// the caller's deadline (see nexusoperation.longPollBuffer) so that it is likely that the caller
// does indeed receive the non-error response.
func (h *handler) DescribeNexusOperation(
	ctx context.Context,
	req *nexusoperationpb.DescribeNexusOperationRequest,
) (response *nexusoperationpb.DescribeNexusOperationResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	ref := chasm.NewComponentRef[*Operation](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  req.GetFrontendRequest().GetOperationId(),
		RunID:       req.GetFrontendRequest().GetRunId(),
	})

	token := req.GetFrontendRequest().GetLongPollToken()
	if len(token) == 0 {
		// No long poll.
		return chasm.ReadComponent(ctx, ref, (*Operation).buildDescribeResponse, req)
	}

	// Determine the long poll timeout and buffer.
	ns := req.GetFrontendRequest().GetNamespace()
	ctx, cancel := contextutil.WithDeadlineBuffer(
		ctx,
		h.config.LongPollTimeout(ns),
		h.config.LongPollBuffer(ns),
	)
	defer cancel()

	// Poll for the operation state to change.
	response, _, err = chasm.PollComponent(ctx, ref, func(
		o *Operation,
		ctx chasm.Context,
		req *nexusoperationpb.DescribeNexusOperationRequest,
	) (*nexusoperationpb.DescribeNexusOperationResponse, bool, error) {
		changed, err := chasm.ExecutionStateChanged(o, ctx, token)
		if err != nil {
			if errors.Is(err, chasm.ErrMalformedComponentRef) {
				return nil, false, serviceerror.NewInvalidArgument("invalid long poll token")
			}
			if errors.Is(err, chasm.ErrInvalidComponentRef) {
				return nil, false, serviceerror.NewInvalidArgument("long poll token does not match execution")
			}
			return nil, false, err
		}
		if changed {
			response, err := o.buildDescribeResponse(ctx, req)
			return response, true, err
		}
		return nil, false, nil
	}, req)

	if err != nil && ctx.Err() != nil {
		// Send empty non-error response on deadline expiry: caller should continue long-polling.
		return &nexusoperationpb.DescribeNexusOperationResponse{
			FrontendResponse: &workflowservice.DescribeNexusOperationExecutionResponse{},
		}, nil
	}
	return response, err
}

// PollNexusOperation long-polls for a Nexus operation to reach a specific stage.
//
// It returns an empty non-error response on context deadline expiry, to indicate that the state
// being waited for was not reached. Callers should interpret this as an invitation to resubmit
// their long-poll request. This response is sent before the caller's
// deadline (see nexusoperation.longPollBuffer) so that it is likely that the caller
// does indeed receive the non-error response.
func (h *handler) PollNexusOperation(
	ctx context.Context,
	req *nexusoperationpb.PollNexusOperationRequest,
) (response *nexusoperationpb.PollNexusOperationResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	ref := chasm.NewComponentRef[*Operation](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  req.GetFrontendRequest().GetOperationId(),
		RunID:       req.GetFrontendRequest().GetRunId(),
	})

	// Determine the long poll timeout and buffer.
	ns := req.GetFrontendRequest().GetNamespace()
	ctx, cancel := contextutil.WithDeadlineBuffer(
		ctx,
		h.config.LongPollTimeout(ns),
		h.config.LongPollBuffer(ns),
	)
	defer cancel()

	// Poll for the wait stage to be reached.
	waitStage := req.GetFrontendRequest().GetWaitStage()
	response, _, err = chasm.PollComponent(ctx, ref, func(
		o *Operation,
		ctx chasm.Context,
		req *nexusoperationpb.PollNexusOperationRequest,
	) (*nexusoperationpb.PollNexusOperationResponse, bool, error) {
		if o.isWaitStageReached(ctx, waitStage) {
			response := o.buildPollResponse(ctx)
			return response, true, nil
		}
		return nil, false, nil
	}, req)

	if err != nil && ctx.Err() != nil {
		// Send an empty non-error response as an invitation to resubmit the long-poll.
		return &nexusoperationpb.PollNexusOperationResponse{
			FrontendResponse: &workflowservice.PollNexusOperationExecutionResponse{},
		}, nil
	}
	return response, err
}

// RequestCancelNexusOperation requests cancellation of a standalone Nexus operation via CHASM.
func (h *handler) RequestCancelNexusOperation(
	ctx context.Context,
	req *nexusoperationpb.RequestCancelNexusOperationRequest,
) (response *nexusoperationpb.RequestCancelNexusOperationResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	ref := chasm.NewComponentRef[*Operation](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  req.GetFrontendRequest().GetOperationId(),
		RunID:       req.GetFrontendRequest().GetRunId(),
	})

	resp, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		func(o *Operation, ctx chasm.MutableContext, req *nexusoperationpb.RequestCancelNexusOperationRequest) (*nexusoperationpb.RequestCancelNexusOperationResponse, error) {
			if err := o.RequestCancel(ctx, &nexusoperationpb.CancellationState{
				RequestId: req.GetFrontendRequest().GetRequestId(),
				Identity:  req.GetFrontendRequest().GetIdentity(),
				Reason:    req.GetFrontendRequest().GetReason(),
			}); err != nil {
				return nil, err
			}
			return &nexusoperationpb.RequestCancelNexusOperationResponse{}, nil
		}, req)

	return resp, err
}

// TerminateNexusOperation terminates a standalone Nexus operation via CHASM.
func (h *handler) TerminateNexusOperation(
	ctx context.Context,
	req *nexusoperationpb.TerminateNexusOperationRequest,
) (response *nexusoperationpb.TerminateNexusOperationResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	ref := chasm.NewComponentRef[*Operation](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  req.GetFrontendRequest().GetOperationId(),
		RunID:       req.GetFrontendRequest().GetRunId(),
	})

	resp, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		func(o *Operation, ctx chasm.MutableContext, req *nexusoperationpb.TerminateNexusOperationRequest) (*nexusoperationpb.TerminateNexusOperationResponse, error) {
			if _, err := o.Terminate(ctx, chasm.TerminateComponentRequest{
				RequestID: req.GetFrontendRequest().GetRequestId(),
				Identity:  req.GetFrontendRequest().GetIdentity(),
				Reason:    req.GetFrontendRequest().GetReason(),
			}); err != nil {
				return nil, err
			}
			return &nexusoperationpb.TerminateNexusOperationResponse{}, nil
		}, req)

	return resp, err
}

// DeleteNexusOperation terminates the nexus operation if running, then schedules it for deletion.
func (h *handler) DeleteNexusOperation(
	ctx context.Context,
	req *nexusoperationpb.DeleteNexusOperationRequest,
) (response *nexusoperationpb.DeleteNexusOperationResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	frontendReq := req.GetFrontendRequest()

	key := chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  frontendReq.GetOperationId(),
		RunID:       frontendReq.GetRunId(),
	}

	if err := chasm.DeleteExecution[*Operation](ctx, key, chasm.DeleteExecutionRequest{
		TerminateComponentRequest: chasm.TerminateComponentRequest{
			Reason: "Delete nexus operation execution",
		},
	}); err != nil {
		return nil, err
	}

	return &nexusoperationpb.DeleteNexusOperationResponse{}, nil
}
func idReusePolicyFromProto(p enumspb.NexusOperationIdReusePolicy) chasm.BusinessIDReusePolicy {
	switch p {
	case enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY:
		return chasm.BusinessIDReusePolicyAllowDuplicateFailedOnly
	case enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_REJECT_DUPLICATE:
		return chasm.BusinessIDReusePolicyRejectDuplicate
	default:
		return chasm.BusinessIDReusePolicyAllowDuplicate
	}
}

func idConflictPolicyFromProto(p enumspb.NexusOperationIdConflictPolicy) chasm.BusinessIDConflictPolicy {
	switch p {
	case enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING:
		return chasm.BusinessIDConflictPolicyUseExisting
	default:
		return chasm.BusinessIDConflictPolicyFail
	}
}
