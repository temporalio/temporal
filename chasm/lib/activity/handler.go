package activity

import (
	"context"
	"errors"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/resource"
)

var (
	businessIDReusePolicyMap = map[enumspb.ActivityIdReusePolicy]chasm.BusinessIDReusePolicy{
		enumspb.ACTIVITY_ID_REUSE_POLICY_ALLOW_DUPLICATE:             chasm.BusinessIDReusePolicyAllowDuplicate,
		enumspb.ACTIVITY_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY: chasm.BusinessIDReusePolicyAllowDuplicateFailedOnly,
		enumspb.ACTIVITY_ID_REUSE_POLICY_REJECT_DUPLICATE:            chasm.BusinessIDReusePolicyRejectDuplicate,
	}

	businessIDConflictPolicyMap = map[enumspb.ActivityIdConflictPolicy]chasm.BusinessIDConflictPolicy{
		enumspb.ACTIVITY_ID_CONFLICT_POLICY_FAIL:         chasm.BusinessIDConflictPolicyFail,
		enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING: chasm.BusinessIDConflictPolicyUseExisting,
	}
)

type handler struct {
	activitypb.UnimplementedActivityServiceServer
	config            *Config
	historyHandler    historyservice.HistoryServiceServer
	linkValidator     *linkValidator
	logger            log.Logger
	metricsHandler    metrics.Handler
	matchingClient    resource.MatchingClient
	namespaceRegistry namespace.Registry
}

func newHandler(
	config *Config,
	historyHandler historyservice.HistoryServiceServer,
	linkValidator *linkValidator,
	metricsHandler metrics.Handler,
	logger log.Logger,
	matchingClient resource.MatchingClient,
	namespaceRegistry namespace.Registry,
) *handler {
	return &handler{
		config:            config,
		historyHandler:    historyHandler,
		linkValidator:     linkValidator,
		logger:            logger,
		metricsHandler:    metricsHandler,
		matchingClient:    matchingClient,
		namespaceRegistry: namespaceRegistry,
	}
}

// StartActivityExecution schedules an activity execution. Note that while external callers refer to
// this as "start", the start transition in fact happens later, in response to the activity task in
// matching being delivered to a worker poll request.
func (h *handler) StartActivityExecution(ctx context.Context, req *activitypb.StartActivityExecutionRequest) (*activitypb.StartActivityExecutionResponse, error) {
	frontendReq := req.GetFrontendRequest()
	var eagerTaskData *eagerActivityTaskData

	reusePolicy, conflictPolicy, err := businessIDPolicies(frontendReq)
	if err != nil {
		return nil, err
	}

	maxCallbacks := h.config.MaxCallbacksPerExecution(frontendReq.GetNamespace())
	h.maybeDisableEagerActivityDispatch(ctx, req.GetNamespaceId(), frontendReq)

	result, err := chasm.StartExecution(
		ctx,
		chasm.ExecutionKey{
			NamespaceID: req.GetNamespaceId(),
			BusinessID:  frontendReq.GetActivityId(),
		},
		func(mutableContext chasm.MutableContext, request *workflowservice.StartActivityExecutionRequest) (*Activity, error) {
			newActivity, newEagerTaskData, err := h.newActivityForStartExecution(mutableContext, request, req.GetNamespaceId(), maxCallbacks)
			if err != nil {
				return nil, err
			}
			eagerTaskData = newEagerTaskData
			return newActivity, nil
		},
		frontendReq,
		chasm.WithRequestID(frontendReq.GetRequestId()),
		chasm.WithBusinessIDPolicy(reusePolicy, conflictPolicy),
	)

	if err != nil {
		return nil, startActivityExecutionError(err)
	}

	if result.Created {
		emitPayloadSizeMetric(
			h.metricsHandler.WithTags(
				metrics.NamespaceTag(frontendReq.GetNamespace()),
				metrics.OperationTag(metrics.HistoryRecordActivityTaskStartedScope),
			),
			frontendReq.GetInput().Size(),
		)
	}

	var eagerTask *workflowservice.PollActivityTaskQueueResponse
	if result.Created && eagerTaskData != nil {
		eagerTask, err = eagerTaskData.response(result.ExecutionRef)
		if err != nil {
			return nil, err
		}
		metrics.ActivityEagerExecutionCounter.With(eagerActivityMetricsHandler(h.metricsHandler, h.config, frontendReq)).Record(1)
	}

	if err := h.applyOnConflictOptions(ctx, frontendReq, result.Created, result.ExecutionKey, maxCallbacks); err != nil {
		return nil, err
	}

	return &activitypb.StartActivityExecutionResponse{
		FrontendResponse: &workflowservice.StartActivityExecutionResponse{
			RunId:             result.ExecutionKey.RunID,
			Started:           result.Created,
			EagerActivityTask: eagerTask,
			Link: &commonpb.Link{
				Variant: &commonpb.Link_Activity_{
					Activity: &commonpb.Link_Activity{
						Namespace:  frontendReq.GetNamespace(),
						ActivityId: frontendReq.GetActivityId(),
						RunId:      result.ExecutionKey.RunID,
					},
				},
			},
		},
	}, nil
}

func (h *handler) newActivityForStartExecution(
	mutableContext chasm.MutableContext,
	request *workflowservice.StartActivityExecutionRequest,
	namespaceID string,
	maxCallbacks int,
) (*Activity, *eagerActivityTaskData, error) {
	newActivity, err := NewStandaloneActivity(mutableContext, request)
	if err != nil {
		return nil, nil, err
	}

	if cbs := request.GetCompletionCallbacks(); len(cbs) > 0 {
		if err := newActivity.addCompletionCallbacks(mutableContext, request.GetRequestId(), cbs, maxCallbacks); err != nil {
			return nil, nil, err
		}
	}
	if len(request.GetLinks()) > 0 {
		if err := newActivity.attachLinks(mutableContext, request.GetLinks(), request.GetRequestId(), h.linkValidator, request.GetNamespace()); err != nil {
			return nil, nil, err
		}
	}

	if request.GetRequestEagerExecution() {
		err = TransitionEagerStarted.Apply(newActivity, mutableContext, eagerStartEvent{
			requestID: request.GetRequestId(),
			identity:  request.GetIdentity(),
		})
	} else {
		err = TransitionScheduled.Apply(newActivity, mutableContext, nil)
	}
	if err != nil {
		return nil, nil, err
	}
	if !request.GetRequestEagerExecution() {
		return newActivity, nil, nil
	}

	eagerTaskData, err := newActivity.eagerActivityTaskData(mutableContext, eagerActivityTaskRequest{
		namespaceID: namespaceID,
		namespace:   request.GetNamespace(),
		requestID:   request.GetRequestId(),
	})
	if err != nil {
		return nil, nil, err
	}
	return newActivity, eagerTaskData, nil
}

func businessIDPolicies(request *workflowservice.StartActivityExecutionRequest) (chasm.BusinessIDReusePolicy, chasm.BusinessIDConflictPolicy, error) {
	reusePolicy, ok := businessIDReusePolicyMap[request.GetIdReusePolicy()]
	if !ok {
		return 0, 0, serviceerror.NewInvalidArgumentf("unsupported ID reuse policy: %v", request.GetIdReusePolicy())
	}

	conflictPolicy, ok := businessIDConflictPolicyMap[request.GetIdConflictPolicy()]
	if !ok {
		return 0, 0, serviceerror.NewInvalidArgumentf("unsupported ID conflict policy: %v", request.GetIdConflictPolicy())
	}

	return reusePolicy, conflictPolicy, nil
}

func (h *handler) maybeDisableEagerActivityDispatch(
	ctx context.Context,
	namespaceID string,
	request *workflowservice.StartActivityExecutionRequest,
) {
	if request.GetRequestEagerExecution() && !h.eagerActivityDispatchAllowed(ctx, namespaceID, request) {
		// Matching grants are best-effort. On a denial or any Matching failure, schedule the
		// activity normally instead of failing the start request.
		request.RequestEagerExecution = false
	}
}

func startActivityExecutionError(err error) error {
	if alreadyStartedErr, ok := errors.AsType[*chasm.ExecutionAlreadyStartedError](err); ok {
		return serviceerror.NewActivityExecutionAlreadyStarted("activity execution already started", alreadyStartedErr.CurrentRequestID, alreadyStartedErr.CurrentRunID)
	}
	return err
}

// applyOnConflictOptions updates an existing activity after StartExecution returns it unchanged.
func (h *handler) applyOnConflictOptions(
	ctx context.Context,
	request *workflowservice.StartActivityExecutionRequest,
	created bool,
	executionKey chasm.ExecutionKey,
	maxCallbacks int,
) error {
	cbs := request.GetCompletionCallbacks()
	links := request.GetLinks()
	onConflict := request.GetOnConflictOptions()
	attachCallbacks := onConflict.GetAttachCompletionCallbacks() && len(cbs) > 0
	attachLinks := onConflict.GetAttachLinks() && len(links) > 0
	if created || (!attachCallbacks && !attachLinks) {
		return nil
	}

	// TODO: Use chasm.UpdateWithStartExecution to avoid a second transaction once the engine supports BusinessIDConflictPolicyFail in the updateFn path.
	requestID := request.GetRequestId()
	_, _, err := chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*Activity](executionKey),
		func(a *Activity, ctx chasm.MutableContext, _ any) (any, error) {
			if attachCallbacks {
				if err := a.addCompletionCallbacks(ctx, requestID, cbs, maxCallbacks); err != nil {
					return nil, err
				}
			}
			if attachLinks {
				if err := a.attachLinks(ctx, links, requestID, h.linkValidator, request.GetNamespace()); err != nil {
					return nil, err
				}
			}
			return nil, nil
		},
		nil,
		chasm.WithRequestID(requestID),
	)
	if err != nil && !errors.Is(err, chasm.ErrRequestIDAlreadyUsed) {
		return err
	}
	return nil
}

func (h *handler) eagerActivityDispatchAllowed(
	ctx context.Context,
	namespaceID string,
	request *workflowservice.StartActivityExecutionRequest,
) bool {
	return !h.config.EnableActivityEagerDispatchCheck(request.GetNamespace()) ||
		h.grantEagerActivityDispatch(ctx, namespaceID, request)
}

func (h *handler) grantEagerActivityDispatch(
	ctx context.Context,
	namespaceID string,
	request *workflowservice.StartActivityExecutionRequest,
) bool {
	if h.matchingClient == nil {
		return false
	}

	response, err := h.matchingClient.GrantEagerDispatch(ctx, &matchingservice.GrantEagerDispatchRequest{
		NamespaceId: namespaceID,
		TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
			TaskQueue:     request.GetTaskQueue().GetName(),
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		},
		Items: []*matchingservice.GrantEagerDispatchRequest_Item{{
			Count:    1,
			Priority: request.GetPriority(),
		}},
	})
	return err == nil && len(response.GetItems()) == 1 && response.GetItems()[0].GetGrantedCount() == 1
}

// DescribeActivityExecution queries current activity state, optionally as a long-poll that waits
// for any state change. When used to long-poll, it returns an empty non-error response on context
// deadline expiry, to indicate that the state being waited for was not reached. Callers should
// interpret this as an invitation to resubmit their long-poll request. This response is sent before
// the caller's deadline (see chasm.activity.longPollBuffer) so that it is likely that the caller
// does indeed receive the non-error response.
func (h *handler) DescribeActivityExecution(
	ctx context.Context,
	req *activitypb.DescribeActivityExecutionRequest,
) (response *activitypb.DescribeActivityExecutionResponse, err error) {
	ref := chasm.NewComponentRef[*Activity](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  req.GetFrontendRequest().GetActivityId(),
		RunID:       req.GetFrontendRequest().GetRunId(),
	})

	token := req.GetFrontendRequest().GetLongPollToken()
	if len(token) == 0 {
		return chasm.ReadComponent(ctx, ref, (*Activity).buildDescribeActivityExecutionResponse, req)
	}

	// Below, we send an empty non-error response on context deadline expiry. Here we compute a
	// deadline that causes us to send that response before the caller's own deadline (see
	// chasm.activity.longPollBuffer). We also cap the caller's deadline at
	// chasm.activity.longPollTimeout.
	ns := req.GetFrontendRequest().GetNamespace()
	ctx, cancel := contextutil.WithDeadlineBuffer(
		ctx,
		h.config.LongPollTimeout(ns),
		h.config.LongPollBuffer(ns),
	)
	defer cancel()

	response, _, err = chasm.PollComponent(ctx, ref, func(
		a *Activity,
		ctx chasm.Context,
		req *activitypb.DescribeActivityExecutionRequest,
	) (*activitypb.DescribeActivityExecutionResponse, bool, error) {
		changed, err := chasm.ExecutionStateChanged(a, ctx, token)
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
			response, err := a.buildDescribeActivityExecutionResponse(ctx, req)
			return response, true, err
		}
		return nil, false, nil
	}, req)

	if err != nil && ctx.Err() != nil {
		// Send empty non-error response on deadline expiry: caller should continue long-polling.
		return &activitypb.DescribeActivityExecutionResponse{
			FrontendResponse: &workflowservice.DescribeActivityExecutionResponse{},
		}, nil
	}
	return response, err
}

// PollActivityExecution long-polls for activity outcome. It returns an empty non-error response on
// context deadline expiry, to indicate that the state being waited for was not reached. Callers
// should interpret this as an invitation to resubmit their long-poll request. This response is sent
// before the caller's deadline (see chasm.activity.longPollBuffer) so that it is likely that the
// caller does indeed receive the non-error response.
func (h *handler) PollActivityExecution(
	ctx context.Context,
	req *activitypb.PollActivityExecutionRequest,
) (response *activitypb.PollActivityExecutionResponse, err error) {
	ref := chasm.NewComponentRef[*Activity](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  req.GetFrontendRequest().GetActivityId(),
		RunID:       req.GetFrontendRequest().GetRunId(),
	})

	// Below, we send an empty non-error response on context deadline expiry. Here we compute a
	// deadline that causes us to send that response before the caller's own deadline (see
	// chasm.activity.longPollBuffer). We also cap the caller's deadline at
	// chasm.activity.longPollTimeout.
	ns := req.GetFrontendRequest().GetNamespace()
	ctx, cancel := contextutil.WithDeadlineBuffer(
		ctx,
		h.config.LongPollTimeout(ns),
		h.config.LongPollBuffer(ns),
	)
	defer cancel()

	response, _, err = chasm.PollComponent(ctx, ref, func(
		a *Activity,
		ctx chasm.Context,
		req *activitypb.PollActivityExecutionRequest,
	) (*activitypb.PollActivityExecutionResponse, bool, error) {
		if a.LifecycleState(ctx) != chasm.LifecycleStateRunning {
			response := a.buildPollActivityExecutionResponse(ctx)
			return response, true, nil
		}
		return nil, false, nil
	}, req)

	if err != nil && ctx.Err() != nil {
		// Send an empty non-error response as an invitation to resubmit the long-poll.
		return &activitypb.PollActivityExecutionResponse{
			FrontendResponse: &workflowservice.PollActivityExecutionResponse{},
		}, nil
	}
	return response, err
}

// DeleteActivityExecution terminates the activity if running, then schedules it for deletion.
func (h *handler) DeleteActivityExecution(
	ctx context.Context,
	req *activitypb.DeleteActivityExecutionRequest,
) (*activitypb.DeleteActivityExecutionResponse, error) {
	frontendReq := req.GetFrontendRequest()

	key := chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  frontendReq.GetActivityId(),
		RunID:       frontendReq.GetRunId(),
	}

	if err := chasm.DeleteExecution[*Activity](ctx, key, chasm.DeleteExecutionRequest{
		TerminateComponentRequest: chasm.TerminateComponentRequest{
			Reason: "Delete activity execution",
		},
	}); err != nil {
		return nil, err
	}

	return &activitypb.DeleteActivityExecutionResponse{}, nil
}

// TerminateActivityExecution terminates an activity execution.
func (h *handler) TerminateActivityExecution(
	ctx context.Context,
	req *activitypb.TerminateActivityExecutionRequest,
) (*activitypb.TerminateActivityExecutionResponse, error) {
	frontendReq := req.GetFrontendRequest()

	ref := chasm.NewComponentRef[*Activity](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  frontendReq.GetActivityId(),
		RunID:       frontendReq.GetRunId(),
	})

	_, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		(*Activity).Terminate,
		chasm.TerminateComponentRequest{
			Reason:    frontendReq.GetReason(),
			Identity:  frontendReq.GetIdentity(),
			RequestID: frontendReq.GetRequestId(),
		},
	)

	if err != nil {
		return nil, err
	}

	return &activitypb.TerminateActivityExecutionResponse{}, nil
}

// RequestCancelActivityExecution requests cancellation of an activity execution.
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

func (h *handler) PauseActivityExecution(ctx context.Context, req *activitypb.PauseActivityExecutionRequest) (*activitypb.PauseActivityExecutionResponse, error) {
	frontendReq := req.GetFrontendRequest()
	if frontendReq.GetWorkflowId() != "" {
		_, err := h.historyHandler.PauseActivity(ctx, &historyservice.PauseActivityRequest{
			NamespaceId: req.GetNamespaceId(),
			FrontendRequest: &workflowservice.PauseActivityRequest{
				Namespace: frontendReq.GetNamespace(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: frontendReq.GetWorkflowId(),
					RunId:      frontendReq.GetRunId(),
				},
				Activity:  &workflowservice.PauseActivityRequest_Id{Id: frontendReq.GetActivityId()},
				Reason:    frontendReq.GetReason(),
				Identity:  frontendReq.GetIdentity(),
				RequestId: frontendReq.GetRequestId(),
			},
		})
		if err != nil {
			return nil, err
		}
		return &activitypb.PauseActivityExecutionResponse{}, nil
	}

	ref := chasm.NewComponentRef[*Activity](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  frontendReq.GetActivityId(),
		RunID:       frontendReq.GetRunId(),
	})

	_, _, err := chasm.UpdateComponent(ctx, ref, (*Activity).handlePauseRequested, req)
	if err != nil {
		return nil, err
	}
	return &activitypb.PauseActivityExecutionResponse{}, nil
}

func (h *handler) UnpauseActivityExecution(ctx context.Context, req *activitypb.UnpauseActivityExecutionRequest) (*activitypb.UnpauseActivityExecutionResponse, error) {
	frontendReq := req.GetFrontendRequest()
	if frontendReq.GetWorkflowId() != "" {
		// TODO: Forward the request ID once workflow activity unpause supports idempotency.
		_, err := h.historyHandler.UnpauseActivity(ctx, &historyservice.UnpauseActivityRequest{
			NamespaceId: req.GetNamespaceId(),
			FrontendRequest: &workflowservice.UnpauseActivityRequest{
				Namespace: frontendReq.GetNamespace(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: frontendReq.GetWorkflowId(),
					RunId:      frontendReq.GetRunId(),
				},
				Activity: &workflowservice.UnpauseActivityRequest_Id{Id: frontendReq.GetActivityId()},
				Jitter:   frontendReq.GetJitter(),
				Identity: frontendReq.GetIdentity(),
			},
		})
		if err != nil {
			return nil, err
		}
		return &activitypb.UnpauseActivityExecutionResponse{}, nil
	}

	ref := chasm.NewComponentRef[*Activity](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  frontendReq.GetActivityId(),
		RunID:       frontendReq.GetRunId(),
	})

	_, _, err := chasm.UpdateComponent(ctx, ref, (*Activity).handleUnpauseRequested, req)
	if err != nil {
		return nil, err
	}
	return &activitypb.UnpauseActivityExecutionResponse{}, nil
}

func (h *handler) ResetActivityExecution(ctx context.Context, req *activitypb.ResetActivityExecutionRequest) (*activitypb.ResetActivityExecutionResponse, error) {
	frontendReq := req.GetFrontendRequest()
	if frontendReq.GetWorkflowId() != "" {
		// TODO: Forward the request ID once workflow activity reset supports idempotency.
		_, err := h.historyHandler.ResetActivity(ctx, &historyservice.ResetActivityRequest{
			NamespaceId: req.GetNamespaceId(),
			FrontendRequest: &workflowservice.ResetActivityRequest{
				Namespace: frontendReq.GetNamespace(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: frontendReq.GetWorkflowId(),
					RunId:      frontendReq.GetRunId(),
				},
				Activity:               &workflowservice.ResetActivityRequest_Id{Id: frontendReq.GetActivityId()},
				ResetHeartbeat:         frontendReq.GetResetHeartbeat(),
				RestoreOriginalOptions: frontendReq.GetRestoreOriginalOptions(),
				KeepPaused:             frontendReq.GetKeepPaused(),
				Jitter:                 frontendReq.GetJitter(),
				Identity:               frontendReq.GetIdentity(),
			},
		})
		if err != nil {
			return nil, err
		}
		return &activitypb.ResetActivityExecutionResponse{}, nil
	}
	ref := chasm.NewComponentRef[*Activity](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  frontendReq.GetActivityId(),
		RunID:       frontendReq.GetRunId(),
	})

	_, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		(*Activity).handleReset,
		req,
	)
	if err != nil {
		return nil, err
	}
	return &activitypb.ResetActivityExecutionResponse{}, nil
}

func (h *handler) UpdateActivityExecutionOptions(ctx context.Context, req *activitypb.UpdateActivityExecutionOptionsRequest) (*activitypb.UpdateActivityExecutionOptionsResponse, error) {
	frontendReq := req.GetFrontendRequest()
	if frontendReq.GetWorkflowId() != "" {
		// TODO: Forward the request ID once workflow activity options updates support idempotency.
		resp, err := h.historyHandler.UpdateActivityOptions(ctx, &historyservice.UpdateActivityOptionsRequest{
			NamespaceId: req.GetNamespaceId(),
			UpdateRequest: &workflowservice.UpdateActivityOptionsRequest{
				Namespace: frontendReq.GetNamespace(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: frontendReq.GetWorkflowId(),
					RunId:      frontendReq.GetRunId(),
				},
				Activity:        &workflowservice.UpdateActivityOptionsRequest_Id{Id: frontendReq.GetActivityId()},
				ActivityOptions: frontendReq.GetActivityOptions(),
				UpdateMask:      frontendReq.GetUpdateMask(),
				RestoreOriginal: frontendReq.GetRestoreOriginal(),
				Identity:        frontendReq.GetIdentity(),
			},
		})
		if err != nil {
			return nil, err
		}
		return &activitypb.UpdateActivityExecutionOptionsResponse{
			FrontendResponse: &workflowservice.UpdateActivityExecutionOptionsResponse{
				ActivityOptions: resp.GetActivityOptions(),
			},
		}, nil
	}

	ref := chasm.NewComponentRef[*Activity](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  req.GetFrontendRequest().GetActivityId(),
		RunID:       req.GetFrontendRequest().GetRunId(),
	})
	response, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		(*Activity).UpdateActivityExecutionOptions,
		req,
	)
	if err != nil {
		return nil, err
	}
	return response, nil
}
