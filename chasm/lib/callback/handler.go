package callback

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/api/errordetails/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type callbackExecutionHandler struct {
	callbackspb.UnimplementedCallbackExecutionServiceServer

	config *Config
	logger log.Logger
}

func newCallbackExecutionHandler(config *Config, logger log.Logger) *callbackExecutionHandler {
	return &callbackExecutionHandler{
		config: config,
		logger: logger,
	}
}

func (h *callbackExecutionHandler) StartCallbackExecution(
	ctx context.Context,
	req *callbackspb.StartCallbackExecutionRequest,
) (resp *callbackspb.StartCallbackExecutionResponse, err error) {
	frontendReq := req.FrontendRequest
	input := &StartCallbackExecutionInput{
		CallbackID:             frontendReq.GetCallbackId(),
		RequestID:              frontendReq.GetRequestId(),
		ScheduleToCloseTimeout: frontendReq.GetScheduleToCloseTimeout(),
		SearchAttributes:       frontendReq.GetSearchAttributes().GetIndexedFields(),
	}

	// Convert the API Callback to internal Callback proto.
	if nexusCb := frontendReq.GetCallback().GetNexus(); nexusCb != nil {
		input.Callback = &callbackspb.Callback{
			Variant: &callbackspb.Callback_Nexus_{
				Nexus: &callbackspb.Callback_Nexus{
					Url:    nexusCb.GetUrl(),
					Header: nexusCb.GetHeader(),
					Token:  nexusCb.GetToken(),
				},
			},
		}
	}

	// Extract completion payload.
	if completion := frontendReq.GetCompletion(); completion != nil {
		input.SuccessCompletion = completion.GetSuccess()
		input.FailureCompletion = completion.GetFailure()
	}

	result, err := chasm.StartExecution(
		ctx,
		chasm.ExecutionKey{
			NamespaceID: req.NamespaceId,
			BusinessID:  frontendReq.GetCallbackId(),
		},
		createCallbackExecution,
		input,
		chasm.WithRequestID(frontendReq.GetRequestId()),
		// Relying on these default policies, as no configuration knobs are
		// exposed to end users.
		chasm.WithBusinessIDPolicy(
			chasm.BusinessIDReusePolicyAllowDuplicate,
			chasm.BusinessIDConflictPolicyFail,
		),
	)

	var alreadyStartedErr *chasm.ExecutionAlreadyStartedError
	if errors.As(err, &alreadyStartedErr) {
		// TODO(chrsmith): You should define a serviceerror.CallbackExecutionAlreadyStarted if you haven't already and use it here.
		// TODO: WIP, err := serviceerror.NewCallbackExecutionAlreadyStarted("callback execution already started", frontendReq.GetCallbackId())
		st := status.New(codes.AlreadyExists, fmt.Sprintf("callback execution %q already exists", frontendReq.GetCallbackId()))
		st, _ = st.WithDetails(&errordetails.CallbackExecutionAlreadyStartedFailure{
			StartRequestId: alreadyStartedErr.CurrentRequestID,
			RunId:          alreadyStartedErr.CurrentRunID,
		})
		return nil, st.Err()
	}
	if err != nil {
		return nil, err
	}

	return &callbackspb.StartCallbackExecutionResponse{
		FrontendResponse: &workflowservice.StartCallbackExecutionResponse{
			RunId: result.ExecutionKey.RunID,
		},
	}, nil
}

func (h *callbackExecutionHandler) DescribeCallbackExecution(
	ctx context.Context,
	req *callbackspb.DescribeCallbackExecutionRequest,
) (resp *callbackspb.DescribeCallbackExecutionResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	// TODO(chrsmith): You need to also implement the long poll version of this.
	// 	See https://temporaltechnologies.slack.com/archives/D0AUGEPP7U1/p1777418231095779?thread_ts=1777417808.690789&cid=D0AUGEPP7U1
	resp, err = chasm.ReadComponent(
		ctx,
		chasm.NewComponentRef[*CallbackExecution](
			chasm.ExecutionKey{
				NamespaceID: req.NamespaceId,
				BusinessID:  req.FrontendRequest.GetCallbackId(),
				RunID:       req.FrontendRequest.GetRunId(),
			},
		),
		func(e *CallbackExecution, ctx chasm.Context, req *callbackspb.DescribeCallbackExecutionRequest) (*callbackspb.DescribeCallbackExecutionResponse, error) {
			info, err := e.Describe(ctx)
			if err != nil {
				return nil, err
			}
			resp := &workflowservice.DescribeCallbackExecutionResponse{
				Info: info,
			}
			// TODO(chrsmith): There should be an IncludeInput field on this request since you don't want the input populated on every describe response in the UI.
			// The field is on the incomming API, but we just aren't honoring it here.
			// if req.FrontendRequest.GetIncludeInput() { ... }
			if req.FrontendRequest.GetIncludeOutcome() {
				outcome, err := e.GetOutcome(ctx)
				if err != nil {
					return nil, err
				}
				resp.Outcome = outcome
			}
			return &callbackspb.DescribeCallbackExecutionResponse{
				FrontendResponse: resp,
			}, nil
		},
		req,
	)
	return resp, err
}

// PollCallbackExecution long-polls for callback execution outcome. It returns an empty non-error
// response on context deadline expiry, to indicate that the state being waited for was not reached.
// Callers should interpret this as an invitation to resubmit their long-poll request.
func (h *callbackExecutionHandler) PollCallbackExecution(
	ctx context.Context,
	req *callbackspb.PollCallbackExecutionRequest,
) (resp *callbackspb.PollCallbackExecutionResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	ref := chasm.NewComponentRef[*CallbackExecution](
		chasm.ExecutionKey{
			NamespaceID: req.NamespaceId,
			BusinessID:  req.FrontendRequest.GetCallbackId(),
			RunID:       req.FrontendRequest.GetRunId(),
		},
	)

	ns := req.FrontendRequest.GetNamespace()
	ctx, cancel := contextutil.WithDeadlineBuffer(
		ctx,
		h.config.LongPollTimeout(ns),
		h.config.LongPollBuffer(ns),
	)
	defer cancel()

	resp, _, err = chasm.PollComponent(ctx, ref, func(
		e *CallbackExecution,
		ctx chasm.Context,
		_ *callbackspb.PollCallbackExecutionRequest,
	) (*callbackspb.PollCallbackExecutionResponse, bool, error) {
		if !e.LifecycleState(ctx).IsClosed() {
			return nil, false, nil
		}
		outcome, err := e.GetOutcome(ctx)
		if err != nil {
			return nil, false, err
		}
		return &callbackspb.PollCallbackExecutionResponse{
			FrontendResponse: &workflowservice.PollCallbackExecutionResponse{
				RunId:   ctx.ExecutionKey().RunID,
				Outcome: outcome,
			},
		}, true, nil
	}, req)

	// TODO(chrsmith): Unresolved comment.
	/**
	Prefer checking the context error just in case the error that was returned by the code somehow did not propagate the context error back.

	From the SAA code:

		if err != nil && ctx.Err() != nil {
			// Send empty non-error response on deadline expiry: caller should continue long-polling.
			return &activitypb.DescribeActivityExecutionResponse{
				FrontendResponse: &workflowservice.DescribeActivityExecutionResponse{},
			}, nil
		}
	*/

	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		// Send an empty non-error response as an invitation to resubmit the long-poll.
		return &callbackspb.PollCallbackExecutionResponse{
			FrontendResponse: &workflowservice.PollCallbackExecutionResponse{},
		}, nil
	}
	return resp, err
}

func (h *callbackExecutionHandler) TerminateCallbackExecution(
	ctx context.Context,
	req *callbackspb.TerminateCallbackExecutionRequest,
) (resp *callbackspb.TerminateCallbackExecutionResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	resp, _, err = chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*CallbackExecution](
			chasm.ExecutionKey{
				NamespaceID: req.NamespaceId,
				BusinessID:  req.FrontendRequest.GetCallbackId(),
				RunID:       req.FrontendRequest.GetRunId(),
			},
		),
		func(e *CallbackExecution, ctx chasm.MutableContext, _ *callbackspb.TerminateCallbackExecutionRequest) (*callbackspb.TerminateCallbackExecutionResponse, error) {
			if _, err := e.Terminate(ctx, chasm.TerminateComponentRequest{
				Reason:    req.FrontendRequest.GetReason(),
				RequestID: req.FrontendRequest.GetRequestId(),
			}); err != nil {
				return nil, err
			}
			return &callbackspb.TerminateCallbackExecutionResponse{
				FrontendResponse: &workflowservice.TerminateCallbackExecutionResponse{},
			}, nil
		},
		req,
	)
	return resp, err
}

func (h *callbackExecutionHandler) DeleteCallbackExecution(
	ctx context.Context,
	req *callbackspb.DeleteCallbackExecutionRequest,
) (resp *callbackspb.DeleteCallbackExecutionResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	if err = chasm.DeleteExecution[*CallbackExecution](
		ctx,
		chasm.ExecutionKey{
			NamespaceID: req.NamespaceId,
			BusinessID:  req.FrontendRequest.GetCallbackId(),
			RunID:       req.FrontendRequest.GetRunId(),
		},
		chasm.DeleteExecutionRequest{
			TerminateComponentRequest: chasm.TerminateComponentRequest{
				Reason: "deleted",
			},
		},
	); err != nil {
		return nil, err
	}

	return &callbackspb.DeleteCallbackExecutionResponse{
		FrontendResponse: &workflowservice.DeleteCallbackExecutionResponse{},
	}, nil
}
