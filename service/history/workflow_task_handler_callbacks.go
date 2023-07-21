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
	protocolpb "go.temporal.io/api/protocol/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/collection"
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
	"go.temporal.io/server/internal/effect"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/update"
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
		currentClusterName             string
		config                         *configs.Config
		shard                          shard.Context
		workflowConsistencyChecker     api.WorkflowConsistencyChecker
		timeSource                     clock.TimeSource
		namespaceRegistry              namespace.Registry
		tokenSerializer                common.TaskTokenSerializer
		metricsHandler                 metrics.Handler
		logger                         log.Logger
		throttledLogger                log.Logger
		commandAttrValidator           *commandAttrValidator
		searchAttributesMapperProvider searchattribute.MapperProvider
		searchAttributesValidator      *searchattribute.Validator
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
		searchAttributesMapperProvider: historyEngine.shard.GetSearchAttributesMapperProvider(),
		searchAttributesValidator:      historyEngine.searchAttributesValidator,
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
		req.ChildClock,
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

			if req.IsFirstWorkflowTask && mutableState.HadOrHasWorkflowTask() {
				return &api.UpdateWorkflowAction{
					Noop: true,
				}, nil
			}

			startEvent, err := mutableState.GetStartEvent(ctx)
			if err != nil {
				return nil, err
			}
			if _, err := mutableState.AddFirstWorkflowTaskScheduled(req.ParentClock, startEvent, false); err != nil {
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

			workflowTask := mutableState.GetWorkflowTaskByID(scheduledEventID)
			metricsScope := handler.metricsHandler.WithTags(metrics.OperationTag(metrics.HistoryRecordWorkflowTaskStartedScope))

			// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
			// some extreme cassandra failure cases.
			if workflowTask == nil && scheduledEventID >= mutableState.GetNextEventID() {
				metricsScope.Counter(metrics.StaleMutableStateCounter.GetMetricName()).Record(1)
				// Reload workflow execution history
				// ErrStaleState will trigger updateWorkflow function to reload the mutable state
				return nil, consts.ErrStaleState
			}

			// Check execution state to make sure task is in the list of outstanding tasks and it is not yet started.  If
			// task is not outstanding than it is most probably a duplicate and complete the task.
			if workflowTask == nil {
				// Looks like WorkflowTask already completed as a result of another call.
				// It is OK to drop the task at this point.
				return nil, serviceerror.NewNotFound("Workflow task not found.")
			}

			updateAction := &api.UpdateWorkflowAction{}

			if workflowTask.StartedEventID != common.EmptyEventID {
				// If workflow task is started as part of the current request scope then return a positive response
				if workflowTask.RequestID == requestID {
					resp, err = handler.createRecordWorkflowTaskStartedResponse(mutableState, workflowContext.GetUpdateRegistry(ctx), workflowTask, req.PollRequest.GetIdentity())
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

			// Assuming a workflow is running on a sticky task queue by a workerA.
			// After workerA is dead for more than 10s, matching will return StickyWorkerUnavailable error when history
			// tries to push a new workflow task. When history sees that error, it will fall back to push the task to
			// its original normal task queue without clear its stickiness to avoid an extra persistence write.
			// We will clear the stickiness here when that task is delivered to another worker polling from normal queue.
			// The stickiness info is used by frontend to decide if it should send down partial history or full history.
			// Sending down partial history will cost the worker an extra fetch to server for the full history.
			currentTaskQueue := mutableState.CurrentTaskQueue()
			if currentTaskQueue.Kind == enumspb.TASK_QUEUE_KIND_STICKY &&
				currentTaskQueue.GetName() != req.PollRequest.TaskQueue.GetName() {
				// req.PollRequest.TaskQueue.GetName() may include partition, but we only check when sticky is enabled,
				// and sticky queue never has partition, so it does not matter.
				mutableState.ClearStickyTaskQueue()
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

			if workflowTask.Type == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
				updateAction.Noop = true
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

			resp, err = handler.createRecordWorkflowTaskStartedResponse(mutableState, workflowContext.GetUpdateRegistry(ctx), workflowTask, req.PollRequest.GetIdentity())
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
			workflowTask := mutableState.GetWorkflowTaskByID(scheduledEventID)

			if workflowTask == nil ||
				workflowTask.StartedEventID == common.EmptyEventID ||
				(token.StartedEventId != common.EmptyEventID && token.StartedEventId != workflowTask.StartedEventID) ||
				(token.StartedTime != nil && workflowTask.StartedTime != nil && !token.StartedTime.Equal(*workflowTask.StartedTime)) ||
				workflowTask.Attempt != token.Attempt ||
				(workflowTask.Version != common.EmptyVersion && token.Version != workflowTask.Version) {
				// we have not alter mutable state yet, so release with it with nil to avoid clear MS.
				workflowContext.GetReleaseFn()(nil)
				return nil, serviceerror.NewNotFound("Workflow task not found.")
			}

			if _, err := mutableState.AddWorkflowTaskFailedEvent(
				workflowTask,
				request.GetCause(),
				request.GetFailure(),
				request.GetIdentity(),
				request.GetBinaryChecksum(),
				"",
				"",
				0); err != nil {
				return nil, err
			}

			// TODO (alex-update): if it was speculative WT that failed, and there is nothing but pending updates,
			//  new WT also should be create as speculative (or not?). Currently, it will be recreated as normal WT.
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
) (_ *historyservice.RespondWorkflowTaskCompletedResponse, retError error) {
	namespaceEntry, err := api.GetActiveNamespace(handler.shard, namespace.ID(req.GetNamespaceId()))
	if err != nil {
		return nil, err
	}

	request := req.CompleteRequest
	token, err0 := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err0 != nil {
		return nil, consts.ErrDeserializingToken
	}

	workflowContext, err := handler.workflowConsistencyChecker.GetWorkflowContext(
		ctx,
		token.Clock,
		func(mutableState workflow.MutableState) bool {
			workflowTask := mutableState.GetWorkflowTaskByID(token.GetScheduledEventId())
			if workflowTask == nil && token.GetScheduledEventId() >= mutableState.GetNextEventID() {
				handler.metricsHandler.Counter(metrics.StaleMutableStateCounter.GetMetricName()).Record(
					1,
					metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope))
				return false
			}
			return true
		},
		definition.NewWorkflowKey(
			namespaceEntry.ID().String(),
			token.WorkflowId,
			token.RunId,
		),
		workflow.LockPriorityHigh,
	)
	if err != nil {
		return nil, err
	}
	weContext := workflowContext.GetContext()
	ms := workflowContext.GetMutableState()

	currentWorkflowTask := ms.GetWorkflowTaskByID(token.GetScheduledEventId())
	if !ms.IsWorkflowExecutionRunning() ||
		currentWorkflowTask == nil ||
		currentWorkflowTask.StartedEventID == common.EmptyEventID ||
		(token.StartedEventId != common.EmptyEventID && token.StartedEventId != currentWorkflowTask.StartedEventID) ||
		(token.StartedTime != nil && currentWorkflowTask.StartedTime != nil && !token.StartedTime.Equal(*currentWorkflowTask.StartedTime)) ||
		currentWorkflowTask.Attempt != token.Attempt ||
		(token.Version != common.EmptyVersion && token.Version != currentWorkflowTask.Version) {
		// we have not alter mutable state yet, so release with it with nil to avoid clear MS.
		workflowContext.GetReleaseFn()(nil)
		return nil, serviceerror.NewNotFound("Workflow task not found.")
	}

	defer func() { workflowContext.GetReleaseFn()(retError) }()

	var effects effect.Buffer
	defer func() {
		// code in this file and workflowTaskHandler is inconsistent in the way
		// errors are returned - some functions which appear to return error
		// actually return nil in all cases and instead set a member variable
		// that should be observed by other collaborating code (e.g.
		// workflowtaskHandler.workflowTaskFailedCause). That made me paranoid
		// about the way this function exits so while we have this defer here
		// there is _also_ code to call effects.Cancel at key points.
		if retError != nil {
			effects.Cancel(ctx)
		}
		effects.Apply(ctx)
	}()

	// It's an error if the workflow has used versioning in the past but this task has no versioning info.
	if ms.GetWorkerVersionStamp().GetUseVersioning() && !request.GetWorkerVersionStamp().GetUseVersioning() {
		return nil, serviceerror.NewInvalidArgument("Workflow using versioning must continue to use versioning.")
	}

	nsName := namespaceEntry.Name().String()
	limits := workflow.WorkflowTaskCompletionLimits{
		MaxResetPoints:              handler.config.MaxAutoResetPoints(nsName),
		MaxSearchAttributeValueSize: handler.config.SearchAttributesSizeOfValueLimit(nsName),
	}
	// TODO: this metric is inaccurate, it should only be emitted if a new binary checksum (or build ID) is added in this completion.
	if ms.GetExecutionInfo().AutoResetPoints != nil && limits.MaxResetPoints == len(ms.GetExecutionInfo().AutoResetPoints.Points) {
		handler.metricsHandler.Counter(metrics.AutoResetPointsLimitExceededCounter.GetMetricName()).Record(
			1,
			metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope))
	}

	workflowTaskHeartbeating := request.GetForceCreateNewWorkflowTask() && len(request.Commands) == 0 && len(request.Messages) == 0
	var workflowTaskHeartbeatTimeout bool
	var completedEvent *historypb.HistoryEvent
	var responseMutations []workflowTaskResponseMutation

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
			completedEvent, err = ms.AddWorkflowTaskTimedOutEvent(currentWorkflowTask)
			if err != nil {
				return nil, err
			}
			ms.ClearStickyTaskQueue()
		} else {
			completedEvent, err = ms.AddWorkflowTaskCompletedEvent(currentWorkflowTask, request, limits)
			if err != nil {
				return nil, err
			}
		}
	} else {
		completedEvent, err = ms.AddWorkflowTaskCompletedEvent(currentWorkflowTask, request, limits)
		if err != nil {
			return nil, err
		}
	}
	// NOTE: completedEvent might be nil if WT was speculative and request has only `update.Rejection` messages.
	// See workflowTaskStateMachine.skipWorkflowTaskCompletedEvent for more details.

	if request.StickyAttributes == nil || request.StickyAttributes.WorkerTaskQueue == nil {
		handler.metricsHandler.Counter(metrics.CompleteWorkflowTaskWithStickyDisabledCounter.GetMetricName()).Record(
			1,
			metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope))
		ms.ClearStickyTaskQueue()
	} else {
		handler.metricsHandler.Counter(metrics.CompleteWorkflowTaskWithStickyEnabledCounter.GetMetricName()).Record(
			1,
			metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope))
		ms.SetStickyTaskQueue(request.StickyAttributes.WorkerTaskQueue.GetName(), request.StickyAttributes.GetScheduleToStartTimeout())
	}

	var (
		wtFailedCause               *workflowTaskFailedCause
		activityNotStartedCancelled bool
		newMutableState             workflow.MutableState
	)
	// hasBufferedEvents indicates if there are any buffered events which should generate a new workflow task
	hasBufferedEvents := ms.HasBufferedEvents()
	if err := namespaceEntry.VerifyBinaryChecksum(request.GetBinaryChecksum()); err != nil {
		wtFailedCause = newWorkflowTaskFailedCause(
			enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_BINARY,
			serviceerror.NewInvalidArgument(
				fmt.Sprintf(
					"binary %v is marked as bad deployment",
					request.GetBinaryChecksum())),
			nil)
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
			handler.metricsHandler.WithTags(
				metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope),
				metrics.NamespaceTag(namespace.String()),
			),
			handler.throttledLogger,
		)

		workflowTaskHandler := newWorkflowTaskHandler(
			request.GetIdentity(),
			completedEvent.GetEventId(), // If completedEvent is nil, then GetEventId() returns 0 and this value shouldn't be used in workflowTaskHandler.
			ms,
			weContext.UpdateRegistry(ctx),
			&effects,
			handler.commandAttrValidator,
			workflowSizeChecker,
			handler.logger,
			handler.namespaceRegistry,
			handler.metricsHandler,
			handler.config,
			handler.shard,
			handler.searchAttributesMapperProvider,
			hasBufferedEvents,
		)

		if responseMutations, err = workflowTaskHandler.handleCommands(
			ctx,
			request.Commands,
			collection.NewIndexedTakeList(
				request.Messages,
				func(msg *protocolpb.Message) string { return msg.Id },
			),
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

		hasBufferedEvents = workflowTaskHandler.hasBufferedEvents
	}

	wtFailedShouldCreateNewTask := false
	if wtFailedCause != nil {
		effects.Cancel(ctx)
		handler.metricsHandler.Counter(metrics.FailedWorkflowTasksCounter.GetMetricName()).Record(
			1,
			metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope))
		handler.logger.Info("Failing the workflow task.",
			tag.Value(wtFailedCause.Message()),
			tag.WorkflowID(token.GetWorkflowId()),
			tag.WorkflowRunID(token.GetRunId()),
			tag.WorkflowNamespaceID(namespaceEntry.ID().String()))
		if currentWorkflowTask.Attempt > 1 && wtFailedCause.failedCause != enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND {
			// drop this workflow task if it keeps failing. This will cause the workflow task to timeout and get retried after timeout.
			return nil, serviceerror.NewInvalidArgument(wtFailedCause.Message())
		}
		var wtFailedEventID int64
		ms, wtFailedEventID, err = failWorkflowTask(ctx, weContext, currentWorkflowTask, wtFailedCause, request)
		if err != nil {
			return nil, err
		}
		wtFailedShouldCreateNewTask = true
		newMutableState = nil

		if wtFailedCause.workflowFailure != nil {
			// Flush buffer event before failing the workflow
			ms.FlushBufferedEvents()

			attributes := &commandpb.FailWorkflowExecutionCommandAttributes{
				Failure: wtFailedCause.workflowFailure,
			}
			if _, err := ms.AddFailWorkflowEvent(wtFailedEventID, enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE, attributes, ""); err != nil {
				return nil, err
			}
			wtFailedShouldCreateNewTask = false
		}
	}

	bufferedEventShouldCreateNewTask := hasBufferedEvents && ms.HasAnyBufferedEvent(eventShouldGenerateNewTaskFilter)
	if hasBufferedEvents && !bufferedEventShouldCreateNewTask {
		// Make sure tasks that should not create a new event don't get stuck in ms forever
		ms.FlushBufferedEvents()
	}
	newWorkflowTaskType := enumsspb.WORKFLOW_TASK_TYPE_UNSPECIFIED
	if ms.IsWorkflowExecutionRunning() {
		if request.GetForceCreateNewWorkflowTask() || // Heartbeat WT is always of Normal type.
			wtFailedShouldCreateNewTask ||
			bufferedEventShouldCreateNewTask ||
			activityNotStartedCancelled {
			newWorkflowTaskType = enumsspb.WORKFLOW_TASK_TYPE_NORMAL
		} else if weContext.UpdateRegistry(ctx).HasOutgoing() {
			if completedEvent == nil || ms.GetNextEventID() == completedEvent.GetEventId()+1 {
				newWorkflowTaskType = enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE
			} else {
				newWorkflowTaskType = enumsspb.WORKFLOW_TASK_TYPE_NORMAL
			}
		}
	}

	bypassTaskGeneration := request.GetReturnNewWorkflowTask() && wtFailedCause == nil
	// TODO (alex-update): Need to support case when ReturnNewWorkflowTask=false and WT.Type=Speculative.
	// In this case WT needs to be added directly to matching.
	// Current implementation will create normal WT.
	if newWorkflowTaskType == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE && !bypassTaskGeneration {
		// If task generation can't be bypassed workflow task must be of Normal type because Speculative workflow task always skip task generation.
		newWorkflowTaskType = enumsspb.WORKFLOW_TASK_TYPE_NORMAL
	}

	var newWorkflowTask *workflow.WorkflowTaskInfo
	// Speculative workflow task will be created after mutable state is persisted.
	if newWorkflowTaskType == enumsspb.WORKFLOW_TASK_TYPE_NORMAL {
		var newWTErr error
		if workflowTaskHeartbeating && !workflowTaskHeartbeatTimeout {
			newWorkflowTask, newWTErr = ms.AddWorkflowTaskScheduledEventAsHeartbeat(
				bypassTaskGeneration,
				currentWorkflowTask.OriginalScheduledTime,
				enumsspb.WORKFLOW_TASK_TYPE_NORMAL, // Heartbeat workflow task is always of Normal type.
			)
		} else {
			newWorkflowTask, newWTErr = ms.AddWorkflowTaskScheduledEvent(bypassTaskGeneration, newWorkflowTaskType)
		}
		if newWTErr != nil {
			return nil, newWTErr
		}

		// skip transfer task for workflow task if request asking to return new workflow task
		if bypassTaskGeneration {
			// start the new workflow task if request asked to do so
			// TODO: replace the poll request
			_, newWorkflowTask, err = ms.AddWorkflowTaskStartedEvent(
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
		// If completedEvent is not nil (which it means that WT wasn't speculative)
		// OR new WT is normal, then mutable state is persisted.
		// Otherwise, (both old and new WT are speculative) mutable state is updated in memory only but not persisted.
		if completedEvent != nil || newWorkflowTaskType == enumsspb.WORKFLOW_TASK_TYPE_NORMAL {
			updateErr = weContext.UpdateWorkflowExecutionAsActive(ctx)
		}
	}

	if updateErr != nil {
		effects.Cancel(ctx)
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

			if err := workflow.TerminateWorkflow(
				ms,
				common.FailureReasonTransactionSizeExceedsLimit,
				payloads.EncodeString(updateErr.Error()),
				consts.IdentityHistoryService,
				false,
			); err != nil {
				return nil, err
			}
			if err := weContext.UpdateWorkflowExecutionAsActive(
				ctx,
			); err != nil {
				return nil, err
			}
		}

		return nil, updateErr
	}

	// Create speculative workflow task after mutable state is persisted.
	if newWorkflowTaskType == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		newWorkflowTask, err = ms.AddWorkflowTaskScheduledEvent(bypassTaskGeneration, newWorkflowTaskType)
		if err != nil {
			return nil, err
		}
		_, newWorkflowTask, err = ms.AddWorkflowTaskStartedEvent(
			newWorkflowTask.ScheduledEventID,
			"request-from-RespondWorkflowTaskCompleted",
			newWorkflowTask.TaskQueue,
			request.Identity,
		)
		if err != nil {
			return nil, err
		}
	}

	handler.handleBufferedQueries(ms, req.GetCompleteRequest().GetQueryResults(), newWorkflowTask != nil, namespaceEntry, workflowTaskHeartbeating)

	if workflowTaskHeartbeatTimeout {
		// at this point, update is successful, but we still return an error to client so that the worker will give up this workflow
		// release workflow lock with nil error to prevent mutable state from being cleared and reloaded
		workflowContext.GetReleaseFn()(nil)
		return nil, serviceerror.NewNotFound("workflow task heartbeat timeout")
	}

	if wtFailedCause != nil {
		// release workflow lock with nil error to prevent mutable state from being cleared and reloaded
		workflowContext.GetReleaseFn()(nil)
		return nil, serviceerror.NewInvalidArgument(wtFailedCause.Message())
	}

	resp := &historyservice.RespondWorkflowTaskCompletedResponse{}
	if request.GetReturnNewWorkflowTask() && newWorkflowTask != nil {
		resp.StartedResponse, err = handler.createRecordWorkflowTaskStartedResponse(ms, weContext.UpdateRegistry(ctx), newWorkflowTask, request.GetIdentity())
		if err != nil {
			return nil, err
		}
		// sticky is always enabled when worker request for new workflow task from RespondWorkflowTaskCompleted
		resp.StartedResponse.StickyExecutionEnabled = true
	}

	// If completedEvent is nil then it means that WT was speculative and
	// WT events (scheduled/started/completed) were not written to the history and were dropped.
	// SDK needs to know where to roll back its history event pointer, i.e. after what event all other events needs to be dropped.
	// SDK uses WorkflowTaskStartedEventID to do that.
	if completedEvent == nil {
		resp.ResetHistoryEventId = ms.GetExecutionInfo().LastWorkflowTaskStartedEventId
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
		workflow.LockPriorityLow,
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

	if !mutableState.HadOrHasWorkflowTask() {
		return consts.ErrWorkflowNotReady
	}

	return nil
}

func (handler *workflowTaskHandlerCallbacksImpl) createRecordWorkflowTaskStartedResponse(
	ms workflow.MutableState,
	updateRegistry update.Registry,
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
	response.StickyExecutionEnabled = ms.IsStickyTaskQueueSet()
	response.NextEventId = ms.GetNextEventID()
	response.Attempt = workflowTask.Attempt
	response.WorkflowExecutionTaskQueue = &taskqueuepb.TaskQueue{
		Name: executionInfo.TaskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	response.ScheduledTime = workflowTask.ScheduledTime
	response.StartedTime = workflowTask.StartedTime
	response.Version = workflowTask.Version

	// TODO (alex-update): Transient needs to be renamed to "TransientOrSpeculative"
	response.TransientWorkflowTask = ms.GetTransientWorkflowTaskInfo(workflowTask, identity)

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

	response.Messages = updateRegistry.ReadOutgoingMessages(workflowTask.StartedEventID)

	if workflowTask.Type == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE && len(response.GetMessages()) == 0 {
		return nil, serviceerror.NewNotFound("No messages for speculative workflow task.")
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
	workflowTask *workflow.WorkflowTaskInfo,
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
	wtFailedEvent, err := mutableState.AddWorkflowTaskFailedEvent(
		workflowTask,
		wtFailedCause.failedCause,
		failure.NewServerFailure(wtFailedCause.Message(), true),
		request.GetIdentity(),
		request.GetBinaryChecksum(),
		"",
		"",
		0)
	if err != nil {
		return nil, common.EmptyEventID, err
	}

	var wtFailedEventID int64
	if wtFailedEvent != nil {
		// If WTFailed event was added to the history then use its Id as wtFailedEventID.
		wtFailedEventID = wtFailedEvent.GetEventId()
	} else {
		// Otherwise, if it was transient WT, last event should be WTFailed event from the 1st attempt.
		wtFailedEventID = mutableState.GetNextEventID() - 1
	}

	// Return reloaded mutable state back to the caller for further updates.
	return mutableState, wtFailedEventID, nil
}

// Filter function to be passed to mutable_state.HasAnyBufferedEvent
// Returns true if the event should generate a new workflow task
// Currently only signal events with SkipGenerateWorkflowTask=true flag set do not generate tasks
func eventShouldGenerateNewTaskFilter(event *historypb.HistoryEvent) bool {
	if event.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
		return true
	}
	return !event.GetWorkflowExecutionSignaledEventAttributes().GetSkipGenerateWorkflowTask()
}
