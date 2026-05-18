package callback

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/contextutil"
)

type callbackHandler struct {
	callbackspb.UnimplementedCallbackServiceServer

	config *Config
}

func newCallbackHandler(config *Config) *callbackHandler {
	return &callbackHandler{
		config: config,
	}
}

func (h *callbackHandler) StartCallbackExecution(
	ctx context.Context,
	req *callbackspb.StartCallbackExecutionRequest,
) (resp *callbackspb.StartCallbackExecutionResponse, err error) {
	frontendReq := req.GetFrontendRequest()

	// Gather all the data necessary to create the Callback component.
	input := &newStandaloneCallbackInput{
		RequestID:              frontendReq.GetRequestId(),
		ScheduleToCloseTimeout: frontendReq.GetScheduleToCloseTimeout(),
		Completion:             frontendReq.GetCompletion(),
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

	// Create the Callback in standalone mode, where it will be the root of the CHASM execution.
	result, err := chasm.StartExecution(
		ctx,
		chasm.ExecutionKey{
			NamespaceID: req.GetNamespaceId(),
			BusinessID:  frontendReq.GetCallbackId(),
		},
		newStandaloneCallback,
		input,
		chasm.WithRequestID(frontendReq.GetRequestId()),
		// Relying on these default policies. No configuration knobs are exposed to users.
		chasm.WithBusinessIDPolicy(
			chasm.BusinessIDReusePolicyAllowDuplicate,
			chasm.BusinessIDConflictPolicyFail,
		),
	)

	// Like Workflow IDs, the Callback ID can be reused. But only one Callback with a given Callback ID
	// can be executing at a given time.
	var alreadyStartedErr *chasm.ExecutionAlreadyStartedError
	if errors.As(err, &alreadyStartedErr) {
		svcErr := serviceerror.NewCallbackExecutionAlreadyStarted(
			"callback execution already started",
			alreadyStartedErr.CurrentRequestID,
			alreadyStartedErr.CurrentRunID,
		)
		return nil, svcErr
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

func (h *callbackHandler) DescribeCallbackExecution(
	ctx context.Context,
	req *callbackspb.DescribeCallbackExecutionRequest,
) (*callbackspb.DescribeCallbackExecutionResponse, error) {

	// Build the DescribeCallbackExecution proto. Closes over the req object.
	buildDescriptionProto := func(
		ctx chasm.Context,
		c *Callback,
	) (*callbackspb.DescribeCallbackExecutionResponse, error) {
		info, err := c.describe(ctx)
		if err != nil {
			return nil, err
		}
		resp := &workflowservice.DescribeCallbackExecutionResponse{
			Info: info,
		}

		if req.GetFrontendRequest().GetIncludeInput() {
			resp.Input = c.SuppliedCompletion.Get(ctx)
		}
		if req.GetFrontendRequest().GetIncludeOutcome() {
			resp.Outcome = c.outcome(ctx)
		}

		return &callbackspb.DescribeCallbackExecutionResponse{
			FrontendResponse: resp,
		}, nil
	}

	compRef := chasm.NewComponentRef[*Callback](
		chasm.ExecutionKey{
			NamespaceID: req.GetNamespaceId(),
			BusinessID:  req.GetFrontendRequest().GetCallbackId(),
			RunID:       req.GetFrontendRequest().GetRunId(),
		},
	)

	// Simple case. If no long-poll token is supplied, we just read and return
	// the persisted state.
	token := req.GetFrontendRequest().GetLongPollToken()
	if len(token) == 0 {
		return chasm.ReadComponent(
			ctx,
			compRef,
			func(
				c *Callback,
				ctx chasm.Context,
				_ *callbackspb.DescribeCallbackExecutionRequest) (*callbackspb.DescribeCallbackExecutionResponse, error) {
				return buildDescriptionProto(ctx, c)
			},
			req)
	}

	// Below, we send an empty non-error response on context deadline expiry. Here we compute a
	// deadline that causes us to send that response before the caller's own deadline.
	targetNamespace := req.GetFrontendRequest().GetNamespace()
	ctx, cancel := contextutil.WithDeadlineBuffer(
		ctx,
		h.config.LongPollTimeout(targetNamespace),
		h.config.LongPollBuffer(targetNamespace),
	)
	defer cancel()

	longpollReadFn := func(
		c *Callback,
		ctx chasm.Context,
		_ *callbackspb.DescribeCallbackExecutionRequest) (*callbackspb.DescribeCallbackExecutionResponse, bool, error) {
		changed, err := chasm.ExecutionStateChanged(c, ctx, token)
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
			response, err := buildDescriptionProto(ctx, c)
			return response, true, err
		}
		return nil, false, nil
	}

	// Now begin the polling, using our supplied reader.
	response, _, err := chasm.PollComponent(ctx, compRef, longpollReadFn, req)
	if err != nil && ctx.Err() != nil {
		// Send empty non-error response on deadline expiry: caller should continue long-polling.
		return &callbackspb.DescribeCallbackExecutionResponse{
			FrontendResponse: &workflowservice.DescribeCallbackExecutionResponse{},
		}, nil
	}
	return response, err
}

func (h *callbackHandler) PollCallbackExecution(
	ctx context.Context,
	req *callbackspb.PollCallbackExecutionRequest,
) (resp *callbackspb.PollCallbackExecutionResponse, err error) {

	ref := chasm.NewComponentRef[*Callback](
		chasm.ExecutionKey{
			NamespaceID: req.GetNamespaceId(),
			BusinessID:  req.GetFrontendRequest().GetCallbackId(),
			RunID:       req.GetFrontendRequest().GetRunId(),
		},
	)

	ns := req.GetFrontendRequest().GetNamespace()
	ctx, cancel := contextutil.WithDeadlineBuffer(
		ctx,
		h.config.LongPollTimeout(ns),
		h.config.LongPollBuffer(ns),
	)
	defer cancel()

	resp, _, err = chasm.PollComponent(ctx, ref, func(
		c *Callback,
		ctx chasm.Context,
		_ *callbackspb.PollCallbackExecutionRequest,
	) (*callbackspb.PollCallbackExecutionResponse, bool, error) {
		if !c.LifecycleState(ctx).IsClosed() {
			return nil, false, nil
		}
		return &callbackspb.PollCallbackExecutionResponse{
			FrontendResponse: &workflowservice.PollCallbackExecutionResponse{
				RunId:   ctx.ExecutionKey().RunID,
				Outcome: c.outcome(ctx),
			},
		}, true, nil
	}, req)

	if err != nil && ctx.Err() != nil {
		// Send an empty non-error response as an invitation to resubmit the long-poll.
		return &callbackspb.PollCallbackExecutionResponse{
			FrontendResponse: &workflowservice.PollCallbackExecutionResponse{},
		}, nil
	}
	return resp, err
}

func (h *callbackHandler) TerminateCallbackExecution(
	ctx context.Context,
	req *callbackspb.TerminateCallbackExecutionRequest,
) (resp *callbackspb.TerminateCallbackExecutionResponse, err error) {

	frontendReq := req.GetFrontendRequest()
	resp, _, err = chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*Callback](
			chasm.ExecutionKey{
				NamespaceID: req.GetNamespaceId(),
				BusinessID:  frontendReq.GetCallbackId(),
				RunID:       frontendReq.GetRunId(),
			},
		),
		func(c *Callback, ctx chasm.MutableContext, _ *callbackspb.TerminateCallbackExecutionRequest) (*callbackspb.TerminateCallbackExecutionResponse, error) {
			if _, err := c.Terminate(ctx, chasm.TerminateComponentRequest{
				Identity:  frontendReq.GetIdentity(),
				Reason:    frontendReq.GetReason(),
				RequestID: frontendReq.GetRequestId(),
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

func (h *callbackHandler) DeleteCallbackExecution(
	ctx context.Context,
	req *callbackspb.DeleteCallbackExecutionRequest,
) (resp *callbackspb.DeleteCallbackExecutionResponse, err error) {

	if err = chasm.DeleteExecution[*Callback](
		ctx,
		chasm.ExecutionKey{
			NamespaceID: req.GetNamespaceId(),
			BusinessID:  req.GetFrontendRequest().GetCallbackId(),
			RunID:       req.GetFrontendRequest().GetRunId(),
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
