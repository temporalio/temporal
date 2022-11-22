// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package history

import (
	"context"
	"fmt"

	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

type (
	// workflow task business logic handler
	workflowTaskHandlerCallbacks interface {
		handleWorkflowTaskScheduled(context.Context, *historyservice.ScheduleWorkflowTaskRequest) error
		handleWorkflowTaskStarted(context.Context,
			*historyservice.RecordWorkflowTaskStartedRequest) (*historyservice.RecordWorkflowTaskStartedResponse, error)
		handleWorkflowTaskFailed(context.Context,
			*historyservice.RespondWorkflowTaskFailedRequest) error
		handleWorkflowTaskCompleted(context.Context,
			*historyservice.RespondWorkflowTaskCompletedRequest) (*historyservice.RespondWorkflowTaskCompletedResponse, error)
		verifyFirstWorkflowTaskScheduled(context.Context, *historyservice.VerifyFirstWorkflowTaskScheduledRequest) error
		// TODO also include the handle of workflow task timeout here
	}

	workflowTaskHandlerCallbacksImpl struct {
		currentClusterName         string
		config                     *configs.Config
		shard                      shard.Context
		workflowConsistencyChecker api.WorkflowConsistencyChecker
		timeSource                 clock.TimeSource
		namespaceRegistry          namespace.Registry
		tokenSerializer            common.TaskTokenSerializer
		metricsHandler             metrics.MetricsHandler
		logger                     log.Logger
		throttledLogger            log.Logger
		commandAttrValidator       *commandAttrValidator
		searchAttributesMapper     searchattribute.Mapper
		searchAttributesValidator  *searchattribute.Validator
	}
)

func newWorkflowTaskHandlerCallback(historyEngine *historyEngineImpl) *workflowTaskHandlerCallbacksImpl {
	return &workflowTaskHandlerCallbacksImpl{
		currentClusterName:         historyEngine.currentClusterName,
		config:                     historyEngine.config,
		shard:                      historyEngine.shard,
		workflowConsistencyChecker: historyEngine.workflowConsistencyChecker,
		timeSource:                 historyEngine.shard.GetTimeSource(),
		namespaceRegistry:          historyEngine.shard.GetNamespaceRegistry(),
		tokenSerializer:            historyEngine.tokenSerializer,
		metricsHandler:             historyEngine.metricsHandler,
		logger:                     historyEngine.logger,
		throttledLogger:            historyEngine.throttledLogger,
		commandAttrValidator: newCommandAttrValidator(
			historyEngine.shard.GetNamespaceRegistry(),
			historyEngine.config,
			historyEngine.searchAttributesValidator,
		),
		searchAttributesMapper:    historyEngine.shard.GetSearchAttributesMapper(),
		searchAttributesValidator: historyEngine.searchAttributesValidator,
	}
}

func (handler *workflowTaskHandlerCallbacksImpl) handleWorkflowTaskScheduled(
	ctx context.Context,
	req *historyservice.ScheduleWorkflowTaskRequest,
) error {

	_, err := api.GetActiveNamespace(handler.shard, namespace.ID(req.GetNamespaceId()))
	if err != nil {
		return err
	}

	return api.GetAndUpdateWorkflowWithNew(
		ctx,
		req.Clock,
		api.BypassMutableStateConsistencyPredicate,
		definition.NewWorkflowKey(
			req.NamespaceId,
			req.WorkflowExecution.WorkflowId,
			req.WorkflowExecution.RunId,
		),
		func(workflowContext api.WorkflowContext) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowContext.GetMutableState()
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, consts.ErrWorkflowCompleted
			}

			if req.IsFirstWorkflowTask && mutableState.HasProcessedOrPendingWorkflowTask() {
				return &api.UpdateWorkflowAction{
					Noop: true,
				}, nil
			}

			startEvent, err := mutableState.GetStartEvent(ctx)
			if err != nil {
				return nil, err
			}
			if err := mutableState.AddFirstWorkflowTaskScheduled(
				startEvent,
			); err != nil {
				return nil, err
			}

			return &api.UpdateWorkflowAction{}, nil
		},
		nil,
		handler.shard,
		handler.workflowConsistencyChecker,
	)
}

func (handler *workflowTaskHandlerCallbacksImpl) handleWorkflowTaskStarted(
	ctx context.Context,
	req *historyservice.RecordWorkflowTaskStartedRequest,
) (*historyservice.RecordWorkflowTaskStartedResponse, error) {

	namespaceEntry, err := api.GetActiveNamespace(handler.shard, namespace.ID(req.GetNamespaceId()))
	if err != nil {
		return nil, err
	}

	scheduledEventID := req.GetScheduledEventId()
	requestID := req.GetRequestId()

	var resp *historyservice.RecordWorkflowTaskStartedResponse
	err = api.GetAndUpdateWorkflowWithNew(
		ctx,
		req.Clock,
		api.BypassMutableStateConsistencyPredicate,
		definition.NewWorkflowKey(
			req.NamespaceId,
			req.WorkflowExecution.WorkflowId,
			req.WorkflowExecution.RunId,
		),
		func(workflowContext api.WorkflowContext) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowContext.GetMutableState()
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, consts.ErrWorkflowCompleted
			}

			workflowTask, isRunning := mutableState.GetWorkflowTaskInfo(scheduledEventID)
			metricsScope := handler.metricsHandler.WithTags(metrics.OperationTag(metrics.HistoryRecordWorkflowTaskStartedScope))

			// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
			// some extreme cassandra failure cases.
			if !isRunning && scheduledEventID >= mutableState.GetNextEventID() {
				metricsScope.Counter(metrics.StaleMutableStateCounter.GetMetricName()).Record(1)
				// Reload workflow execution history
				// ErrStaleState will trigger updateWorkflow function to reload the mutable state
				return nil, consts.ErrStaleState
			}

			// Check execution state to make sure task is in the list of outstanding tasks and it is not yet started.  If
			// task is not outstanding than it is most probably a duplicate and complete the task.
			if !isRunning {
				// Looks like WorkflowTask already completed as a result of another call.
				// It is OK to drop the task at this point.
				return nil, serviceerror.NewNotFound("Workflow task not found.")
			}

			updateAction := &api.UpdateWorkflowAction{}

			if workflowTask.StartedEventID != common.EmptyEventID {
				// If workflow task is started as part of the current request scope then return a positive response
				if workflowTask.RequestID == requestID {
					resp, err = handler.createRecordWorkflowTaskStartedResponse(mutableState, workflowTask, req.PollRequest.GetIdentity())
					if err != nil {
						return nil, err
					}
					updateAction.Noop = true
					return updateAction, nil
				}

				// Looks like WorkflowTask already started as a result of another call.
				// It is OK to drop the task at this point.
				return nil, serviceerrors.NewTaskAlreadyStarted("Workflow")
			}

			_, workflowTask, err = mutableState.AddWorkflowTaskStartedEvent(
				scheduledEventID,
				requestID,
				req.PollRequest.TaskQueue,
				req.PollRequest.Identity,
			)
			if err != nil {
				// Unable to add WorkflowTaskStarted event to history
				return nil, err
			}

			workflowScheduleToStartLatency := workflowTask.StartedTime.Sub(*workflowTask.ScheduledTime)
			namespaceName := namespaceEntry.Name()
			taskQueue := workflowTask.TaskQueue
			metrics.GetPerTaskQueueScope(
				metricsScope,
				namespaceName.String(),
				taskQueue.GetName(),
				taskQueue.GetKind(),
			).Timer(metrics.TaskScheduleToStartLatency.GetMetricName()).Record(
				workflowScheduleToStartLatency,
				metrics.TaskQueueTypeTag(enumspb.TASK_QUEUE_TYPE_WORKFLOW),
			)

			resp, err = handler.createRecordWorkflowTaskStartedResponse(mutableState, workflowTask, req.PollRequest.GetIdentity())
			if err != nil {
				return nil, err
			}
			return updateAction, nil
		},
		nil,
		handler.shard,
		handler.workflowConsistencyChecker,
	)

	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (handler *workflowTaskHandlerCallbacksImpl) handleWorkflowTaskFailed(
	ctx context.Context,
	req *historyservice.RespondWorkflowTaskFailedRequest,
) (retError error) {

	_, err := api.GetActiveNamespace(handler.shard, namespace.ID(req.GetNamespaceId()))
	if err != nil {
		return err
	}

	request := req.FailedRequest
	token, err := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return consts.ErrDeserializingToken
	}

	return api.GetAndUpdateWorkflowWithNew(
		ctx,
		token.Clock,
		api.BypassMutableStateConsistencyPredicate,
		definition.NewWorkflowKey(
			token.NamespaceId,
			token.WorkflowId,
			token.RunId,
		),
		func(workflowContext api.WorkflowContext) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowContext.GetMutableState()
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, consts.ErrWorkflowCompleted
			}

			scheduledEventID := token.GetScheduledEventId()
			workflowTask, isRunning := mutableState.GetWorkflowTaskInfo(scheduledEventID)
			if !isRunning || workflowTask.Attempt != token.Attempt || workflowTask.StartedEventID == common.EmptyEventID {
				return nil, serviceerror.NewNotFound("Workflow task not found.")
			}

			_, err := mutableState.AddWorkflowTaskFailedEvent(workflowTask.ScheduledEventID, workflowTask.StartedEventID, request.GetCause(), request.GetFailure(),
				request.GetIdentity(), request.GetBinaryChecksum(), "", "", 0)
			if err != nil {
				return nil, err
			}
			return &api.UpdateWorkflowAction{
				Noop:               false,
				CreateWorkflowTask: true,
			}, nil
		},
		nil,
		handler.shard,
		handler.workflowConsistencyChecker,
	)
}

func (handler *workflowTaskHandlerCallbacksImpl) handleWorkflowTaskCompleted(
	ctx context.Context,
	req *historyservice.RespondWorkflowTaskCompletedRequest,
) (resp *historyservice.RespondWorkflowTaskCompletedResponse, retError error) {

	namespaceEntry, err := api.GetActiveNamespace(handler.shard, namespace.ID(req.GetNamespaceId()))
	if err != nil {
		return nil, err
	}
	namespaceID := namespaceEntry.ID()

	request := req.CompleteRequest
	token, err0 := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err0 != nil {
		return nil, consts.ErrDeserializingToken
	}
	scheduledEventID := token.GetScheduledEventId()

	workflowContext, err := handler.workflowConsistencyChecker.GetWorkflowContext(
		ctx,
		token.Clock,
		func(mutableState workflow.MutableState) bool {
			_, ok := mutableState.GetWorkflowTaskInfo(scheduledEventID)
			if !ok && scheduledEventID >= mutableState.GetNextEventID() {
				handler.metricsHandler.Counter(metrics.StaleMutableStateCounter.GetMetricName()).Record(
					1,
					metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope))
				return false
			}
			return true
		},
		definition.NewWorkflowKey(
			namespaceID.String(),
			token.WorkflowId,
			token.RunId,
		),
	)
	if err != nil {
		return nil, err
	}
	defer func() { workflowContext.GetReleaseFn()(retError) }()

	weContext := workflowContext.GetContext()
	ms := workflowContext.GetMutableState()
	currentWorkflowTask, currentWorkflowTaskRunning := ms.GetWorkflowTaskInfo(scheduledEventID)

	executionInfo := ms.GetExecutionInfo()
	executionStats, err := weContext.LoadExecutionStats(ctx)
	if err != nil {
		return nil, err
	}

	if !ms.IsWorkflowExecutionRunning() || !currentWorkflowTaskRunning || currentWorkflowTask.Attempt != token.Attempt ||
		currentWorkflowTask.StartedEventID == common.EmptyEventID {
		return nil, serviceerror.NewNotFound("Workflow task not found.")
	}

	startedEventID := currentWorkflowTask.StartedEventID
	maxResetPoints := handler.config.MaxAutoResetPoints(namespaceEntry.Name().String())
	if ms.GetExecutionInfo().AutoResetPoints != nil && maxResetPoints == len(ms.GetExecutionInfo().AutoResetPoints.Points) {
		handler.metricsHandler.Counter(metrics.AutoResetPointsLimitExceededCounter.GetMetricName()).Record(
			1,
			metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope))
	}

	workflowTaskHeartbeating := request.GetForceCreateNewWorkflowTask() && len(request.Commands) == 0
	var workflowTaskHeartbeatTimeout bool
	var completedEvent *historypb.HistoryEvent
	if workflowTaskHeartbeating {
		namespace := namespaceEntry.Name()
		timeout := handler.config.WorkflowTaskHeartbeatTimeout(namespace.String())
		origSchedTime := timestamp.TimeValue(currentWorkflowTask.OriginalScheduledTime)
		if origSchedTime.UnixNano() > 0 && handler.timeSource.Now().After(origSchedTime.Add(timeout)) {
			workflowTaskHeartbeatTimeout = true

			scope := handler.metricsHandler.WithTags(
				metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope),
				metrics.NamespaceTag(namespace.String()),
			)
			scope.Counter(metrics.WorkflowTaskHeartbeatTimeoutCounter.GetMetricName()).Record(1)
			completedEvent, err = ms.AddWorkflowTaskTimedOutEvent(currentWorkflowTask.ScheduledEventID, currentWorkflowTask.StartedEventID)
			if err != nil {
				return nil, err
			}
			ms.ClearStickyness()
		} else {
			completedEvent, err = ms.AddWorkflowTaskCompletedEvent(scheduledEventID, startedEventID, request, maxResetPoints)
			if err != nil {
				return nil, err
			}
		}
	} else {
		completedEvent, err = ms.AddWorkflowTaskCompletedEvent(scheduledEventID, startedEventID, request, maxResetPoints)
		if err != nil {
			return nil, err
		}
	}

	var (
		wtFailedCause               *workflowTaskFailedCause
		activityNotStartedCancelled bool
		newMutableState             workflow.MutableState

		hasUnhandledEvents bool
		responseMutations  []workflowTaskResponseMutation
	)
	hasUnhandledEvents = ms.HasBufferedEvents()

	if request.StickyAttributes == nil || request.StickyAttributes.WorkerTaskQueue == nil {
		handler.metricsHandler.Counter(metrics.CompleteWorkflowTaskWithStickyDisabledCounter.GetMetricName()).Record(
			1,
			metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope))
		executionInfo.StickyTaskQueue = ""
		executionInfo.StickyScheduleToStartTimeout = timestamp.DurationFromSeconds(0)
	} else {
		handler.metricsHandler.Counter(metrics.CompleteWorkflowTaskWithStickyEnabledCounter.GetMetricName()).Record(
			1,
			metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope))
		executionInfo.StickyTaskQueue = request.StickyAttributes.WorkerTaskQueue.GetName()
		executionInfo.StickyScheduleToStartTimeout = request.StickyAttributes.GetScheduleToStartTimeout()
	}

	binChecksum := request.GetBinaryChecksum()
	if err := namespaceEntry.VerifyBinaryChecksum(binChecksum); err != nil {
		wtFailedCause = NewWorkflowTaskFailedCause(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_BINARY, serviceerror.NewInvalidArgument(fmt.Sprintf("binary %v is already marked as bad deployment", binChecksum)))
	} else {
		namespace := namespaceEntry.Name()
		workflowSizeChecker := newWorkflowSizeChecker(
			workflowSizeLimits{
				blobSizeLimitWarn:              handler.config.BlobSizeLimitWarn(namespace.String()),
				blobSizeLimitError:             handler.config.BlobSizeLimitError(namespace.String()),
				memoSizeLimitWarn:              handler.config.MemoSizeLimitWarn(namespace.String()),
				memoSizeLimitError:             handler.config.MemoSizeLimitError(namespace.String()),
				numPendingChildExecutionsLimit: handler.config.NumPendingChildExecutionsLimit(namespace.String()),
				numPendingActivitiesLimit:      handler.config.NumPendingActivitiesLimit(namespace.String()),
				numPendingSignalsLimit:         handler.config.NumPendingSignalsLimit(namespace.String()),
				numPendingCancelsRequestLimit:  handler.config.NumPendingCancelsRequestLimit(namespace.String()),
			},
			ms,
			handler.searchAttributesValidator,
			executionStats,
			handler.metricsHandler.WithTags(
				metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope),
				metrics.NamespaceTag(namespace.String()),
			),
			handler.throttledLogger,
		)

		workflowTaskHandler := newWorkflowTaskHandler(
			request.GetIdentity(),
			completedEvent.GetEventId(),
			ms,
			handler.commandAttrValidator,
			workflowSizeChecker,
			handler.logger,
			handler.namespaceRegistry,
			handler.metricsHandler,
			handler.config,
			handler.shard,
			handler.searchAttributesMapper,
		)

		if responseMutations, err = workflowTaskHandler.handleCommands(
			ctx,
			request.Commands,
		); err != nil {
			return nil, err
		}

		// set the vars used by following logic
		// further refactor should also clean up the vars used below
		wtFailedCause = workflowTaskHandler.workflowTaskFailedCause

		// failMessage is not used by workflowTaskHandlerCallbacks
		activityNotStartedCancelled = workflowTaskHandler.activityNotStartedCancelled
		// continueAsNewTimerTasks is not used by workflowTaskHandlerCallbacks

		newMutableState = workflowTaskHandler.newMutableState

		hasUnhandledEvents = workflowTaskHandler.hasBufferedEvents
	}

	if wtFailedCause != nil {
		handler.metricsHandler.Counter(metrics.FailedWorkflowTasksCounter.GetMetricName()).Record(
			1,
			metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope))
		handler.logger.Info("Failing the workflow task.",
			tag.Value(wtFailedCause.Message()),
			tag.WorkflowID(token.GetWorkflowId()),
			tag.WorkflowRunID(token.GetRunId()),
			tag.WorkflowNamespaceID(namespaceID.String()))
		if currentWorkflowTask.Attempt > 1 && wtFailedCause.failedCause != enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND {
			// drop this workflow task if it keeps failing. This will cause the workflow task to timeout and get retried after timeout.
			return nil, serviceerror.NewInvalidArgument(wtFailedCause.Message())
		}
		var nextEventBatchId int64
		ms, nextEventBatchId, err = failWorkflowTask(ctx, weContext, scheduledEventID, startedEventID, wtFailedCause, request)
		if err != nil {
			return nil, err
		}
		hasUnhandledEvents = true
		newMutableState = nil

		if wtFailedCause.workflowFailure != nil {
			attributes := &commandpb.FailWorkflowExecutionCommandAttributes{
				Failure: wtFailedCause.workflowFailure,
			}
			if _, err := ms.AddFailWorkflowEvent(nextEventBatchId, enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE, attributes, ""); err != nil {
				return nil, err
			}
			hasUnhandledEvents = false
		}
	}

	createNewWorkflowTask := ms.IsWorkflowExecutionRunning() && (hasUnhandledEvents || request.GetForceCreateNewWorkflowTask() || activityNotStartedCancelled)
	var newWorkflowTaskScheduledEventID int64
	if createNewWorkflowTask {
		bypassTaskGeneration := request.GetReturnNewWorkflowTask() && wtFailedCause == nil
		var newWorkflowTask *workflow.WorkflowTaskInfo
		var err error
		if workflowTaskHeartbeating && !workflowTaskHeartbeatTimeout {
			newWorkflowTask, err = ms.AddWorkflowTaskScheduledEventAsHeartbeat(
				bypassTaskGeneration,
				currentWorkflowTask.OriginalScheduledTime,
			)
		} else {
			newWorkflowTask, err = ms.AddWorkflowTaskScheduledEvent(bypassTaskGeneration)
		}
		if err != nil {
			return nil, err
		}

		newWorkflowTaskScheduledEventID = newWorkflowTask.ScheduledEventID
		// skip transfer task for workflow task if request asking to return new workflow task
		if bypassTaskGeneration {
			// start the new workflow task if request asked to do so
			// TODO: replace the poll request
			_, _, err := ms.AddWorkflowTaskStartedEvent(
				newWorkflowTask.ScheduledEventID,
				"request-from-RespondWorkflowTaskCompleted",
				newWorkflowTask.TaskQueue,
				request.Identity,
			)
			if err != nil {
				return nil, err
			}
		}
	}

	var updateErr error
	if newMutableState != nil {
		newWorkflowExecutionInfo := newMutableState.GetExecutionInfo()
		newWorkflowExecutionState := newMutableState.GetExecutionState()
		updateErr = weContext.UpdateWorkflowExecutionWithNewAsActive(
			ctx,
			handler.shard.GetTimeSource().Now(),
			workflow.NewContext(
				handler.shard,
				definition.NewWorkflowKey(
					newWorkflowExecutionInfo.NamespaceId,
					newWorkflowExecutionInfo.WorkflowId,
					newWorkflowExecutionState.RunId,
				),
				handler.logger,
			),
			newMutableState,
		)
	} else {
		updateErr = weContext.UpdateWorkflowExecutionAsActive(ctx, handler.shard.GetTimeSource().Now())
	}

	if updateErr != nil {
		if persistence.IsConflictErr(updateErr) {
			handler.metricsHandler.Counter(metrics.ConcurrencyUpdateFailureCounter.GetMetricName()).Record(
				1,
				metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope))
		}

		// if updateErr resulted in TransactionSizeLimitError then fail workflow
		switch updateErr.(type) {
		case *persistence.TransactionSizeLimitError:
			// must reload mutable state because the first call to updateWorkflowExecutionWithContext or continueAsNewWorkflowExecution
			// clears mutable state if error is returned
			ms, err = weContext.LoadMutableState(ctx)
			if err != nil {
				return nil, err
			}

			eventBatchFirstEventID := ms.GetNextEventID()
			if err := workflow.TerminateWorkflow(
				ms,
				eventBatchFirstEventID,
				common.FailureReasonTransactionSizeExceedsLimit,
				payloads.EncodeString(updateErr.Error()),
				consts.IdentityHistoryService,
				false,
			); err != nil {
				return nil, err
			}
			if err := weContext.UpdateWorkflowExecutionAsActive(
				ctx,
				handler.shard.GetTimeSource().Now(),
			); err != nil {
				return nil, err
			}
		}

		return nil, updateErr
	}

	handler.handleBufferedQueries(ms, req.GetCompleteRequest().GetQueryResults(), createNewWorkflowTask, namespaceEntry, workflowTaskHeartbeating)

	if workflowTaskHeartbeatTimeout {
		// at this point, update is successful, but we still return an error to client so that the worker will give up this workflow
		return nil, serviceerror.NewNotFound("workflow task heartbeat timeout")
	}

	if wtFailedCause != nil {
		return nil, serviceerror.NewInvalidArgument(wtFailedCause.Message())
	}

	resp = &historyservice.RespondWorkflowTaskCompletedResponse{}
	if request.GetReturnNewWorkflowTask() && createNewWorkflowTask {
		workflowTask, _ := ms.GetWorkflowTaskInfo(newWorkflowTaskScheduledEventID)
		resp.StartedResponse, err = handler.createRecordWorkflowTaskStartedResponse(ms, workflowTask, request.GetIdentity())
		if err != nil {
			return nil, err
		}
		// sticky is always enabled when worker request for new workflow task from RespondWorkflowTaskCompleted
		resp.StartedResponse.StickyExecutionEnabled = true
	}
	for _, mutation := range responseMutations {
		if err := mutation(resp); err != nil {
			return nil, err
		}
	}

	return resp, nil

}

func (handler *workflowTaskHandlerCallbacksImpl) verifyFirstWorkflowTaskScheduled(
	ctx context.Context,
	req *historyservice.VerifyFirstWorkflowTaskScheduledRequest,
) (retError error) {
	namespaceID := namespace.ID(req.GetNamespaceId())
	if err := api.ValidateNamespaceUUID(namespaceID); err != nil {
		return err
	}

	workflowContext, err := handler.workflowConsistencyChecker.GetWorkflowContext(
		ctx,
		req.Clock,
		api.BypassMutableStateConsistencyPredicate,
		definition.NewWorkflowKey(
			req.NamespaceId,
			req.WorkflowExecution.WorkflowId,
			req.WorkflowExecution.RunId,
		),
	)
	if err != nil {
		return err
	}
	defer func() { workflowContext.GetReleaseFn()(retError) }()

	mutableState := workflowContext.GetMutableState()
	if !mutableState.IsWorkflowExecutionRunning() &&
		mutableState.GetExecutionState().State != enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
		return nil
	}

	if !mutableState.HasProcessedOrPendingWorkflowTask() {
		return consts.ErrWorkflowNotReady
	}

	return nil
}

func (handler *workflowTaskHandlerCallbacksImpl) createRecordWorkflowTaskStartedResponse(
	ms workflow.MutableState,
	workflowTask *workflow.WorkflowTaskInfo,
	identity string,
) (*historyservice.RecordWorkflowTaskStartedResponse, error) {

	response := &historyservice.RecordWorkflowTaskStartedResponse{}
	response.WorkflowType = ms.GetWorkflowType()
	executionInfo := ms.GetExecutionInfo()
	if executionInfo.LastWorkflowTaskStartedEventId != common.EmptyEventID {
		response.PreviousStartedEventId = executionInfo.LastWorkflowTaskStartedEventId
	}

	// Starting workflowTask could result in different scheduledEventID if workflowTask was transient and new events came in
	// before it was started.
	response.ScheduledEventId = workflowTask.ScheduledEventID
	response.StartedEventId = workflowTask.StartedEventID
	response.StickyExecutionEnabled = ms.IsStickyTaskQueueEnabled()
	response.NextEventId = ms.GetNextEventID()
	response.Attempt = workflowTask.Attempt
	response.WorkflowExecutionTaskQueue = &taskqueuepb.TaskQueue{
		Name: executionInfo.TaskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	response.ScheduledTime = workflowTask.ScheduledTime
	response.StartedTime = workflowTask.StartedTime

	response.TransientWorkflowTask = ms.CreateTransientWorkflowTask(workflowTask, identity)

	currentBranchToken, err := ms.GetCurrentBranchToken()
	if err != nil {
		return nil, err
	}
	response.BranchToken = currentBranchToken

	qr := ms.GetQueryRegistry()
	bufferedQueryIDs := qr.GetBufferedIDs()
	if len(bufferedQueryIDs) > 0 {
		response.Queries = make(map[string]*querypb.WorkflowQuery, len(bufferedQueryIDs))
		for _, bufferedQueryID := range bufferedQueryIDs {
			input, err := qr.GetQueryInput(bufferedQueryID)
			if err != nil {
				continue
			}
			response.Queries[bufferedQueryID] = input
		}
	}

	return response, nil
}

func (handler *workflowTaskHandlerCallbacksImpl) handleBufferedQueries(ms workflow.MutableState, queryResults map[string]*querypb.WorkflowQueryResult, createNewWorkflowTask bool, namespaceEntry *namespace.Namespace, workflowTaskHeartbeating bool) {
	queryRegistry := ms.GetQueryRegistry()
	if !queryRegistry.HasBufferedQuery() {
		return
	}

	namespaceName := namespaceEntry.Name()
	workflowID := ms.GetExecutionInfo().WorkflowId
	runID := ms.GetExecutionState().GetRunId()

	scope := handler.metricsHandler.WithTags(
		metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope),
		metrics.NamespaceTag(namespaceEntry.Name().String()),
		metrics.CommandTypeTag("ConsistentQuery"))

	// if its a heartbeat workflow task it means local activities may still be running on the worker
	// which were started by an external event which happened before the query
	if workflowTaskHeartbeating {
		return
	}

	sizeLimitError := handler.config.BlobSizeLimitError(namespaceName.String())
	sizeLimitWarn := handler.config.BlobSizeLimitWarn(namespaceName.String())

	// Complete or fail all queries we have results for
	for id, result := range queryResults {
		if err := common.CheckEventBlobSizeLimit(
			result.GetAnswer().Size(),
			sizeLimitWarn,
			sizeLimitError,
			namespaceName.String(),
			workflowID,
			runID,
			scope,
			handler.throttledLogger,
			tag.BlobSizeViolationOperation("ConsistentQuery"),
		); err != nil {
			handler.logger.Info("failing query because query result size is too large",
				tag.WorkflowNamespace(namespaceName.String()),
				tag.WorkflowID(workflowID),
				tag.WorkflowRunID(runID),
				tag.QueryID(id),
				tag.Error(err))
			failedCompletionState := &workflow.QueryCompletionState{
				Type: workflow.QueryCompletionTypeFailed,
				Err:  err,
			}
			if err := queryRegistry.SetCompletionState(id, failedCompletionState); err != nil {
				handler.logger.Error(
					"failed to set query completion state to failed",
					tag.WorkflowNamespace(namespaceName.String()),
					tag.WorkflowID(workflowID),
					tag.WorkflowRunID(runID),
					tag.QueryID(id),
					tag.Error(err))
				scope.Counter(metrics.QueryRegistryInvalidStateCount.GetMetricName()).Record(1)
			}
		} else {
			succeededCompletionState := &workflow.QueryCompletionState{
				Type:   workflow.QueryCompletionTypeSucceeded,
				Result: result,
			}
			if err := queryRegistry.SetCompletionState(id, succeededCompletionState); err != nil {
				handler.logger.Error(
					"failed to set query completion state to succeeded",
					tag.WorkflowNamespace(namespaceName.String()),
					tag.WorkflowID(workflowID),
					tag.WorkflowRunID(runID),
					tag.QueryID(id),
					tag.Error(err))
				scope.Counter(metrics.QueryRegistryInvalidStateCount.GetMetricName()).Record(1)
			}
		}
	}

	// If no workflow task was created then it means no buffered events came in during this workflow task's handling.
	// This means all unanswered buffered queries can be dispatched directly through matching at this point.
	if !createNewWorkflowTask {
		buffered := queryRegistry.GetBufferedIDs()
		for _, id := range buffered {
			unblockCompletionState := &workflow.QueryCompletionState{
				Type: workflow.QueryCompletionTypeUnblocked,
			}
			if err := queryRegistry.SetCompletionState(id, unblockCompletionState); err != nil {
				handler.logger.Error(
					"failed to set query completion state to unblocked",
					tag.WorkflowNamespace(namespaceName.String()),
					tag.WorkflowID(workflowID),
					tag.WorkflowRunID(runID),
					tag.QueryID(id),
					tag.Error(err))
				scope.Counter(metrics.QueryRegistryInvalidStateCount.GetMetricName()).Record(1)
			}
		}
	}
}

func failWorkflowTask(
	ctx context.Context,
	wfContext workflow.Context,
	scheduledEventID int64,
	startedEventID int64,
	wtFailedCause *workflowTaskFailedCause,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
) (workflow.MutableState, int64, error) {

	// clear any updates we have accumulated so far
	wfContext.Clear()

	// Reload workflow execution so we can apply the workflow task failure event
	mutableState, err := wfContext.LoadMutableState(ctx)
	if err != nil {
		return nil, common.EmptyEventID, err
	}
	nextEventBatchId := mutableState.GetNextEventID()
	if _, err = mutableState.AddWorkflowTaskFailedEvent(
		scheduledEventID,
		startedEventID,
		wtFailedCause.failedCause,
		failure.NewServerFailure(wtFailedCause.Message(), true),
		request.GetIdentity(),
		request.GetBinaryChecksum(),
		"",
		"",
		0); err != nil {
		return nil, nextEventBatchId, err
	}

	// Return new mutable state back to the caller for further updates
	return mutableState, nextEventBatchId, nil
}
