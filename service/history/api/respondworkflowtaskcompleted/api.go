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

package respondworkflowtaskcompleted

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/service/history/api/recordworkflowtaskstarted"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
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
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/internal/effect"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/update"
)

type (
	WorkflowTaskCompletedHandler struct {
		config                         *configs.Config
		shardContext                   shard.Context
		workflowConsistencyChecker     api.WorkflowConsistencyChecker
		timeSource                     clock.TimeSource
		namespaceRegistry              namespace.Registry
		eventNotifier                  events.Notifier
		tokenSerializer                common.TaskTokenSerializer
		metricsHandler                 metrics.Handler
		logger                         log.Logger
		throttledLogger                log.Logger
		commandAttrValidator           *commandAttrValidator
		searchAttributesMapperProvider searchattribute.MapperProvider
		searchAttributesValidator      *searchattribute.Validator
		persistenceVisibilityMgr       manager.VisibilityManager
		commandHandlerRegistry         *workflow.CommandHandlerRegistry
	}
)

func NewWorkflowTaskCompletedHandler(
	shardContext shard.Context,
	tokenSerializer common.TaskTokenSerializer,
	eventNotifier events.Notifier,
	commandHandlerRegistry *workflow.CommandHandlerRegistry,
	searchAttributesValidator *searchattribute.Validator,
	visibilityManager manager.VisibilityManager,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) *WorkflowTaskCompletedHandler {
	return &WorkflowTaskCompletedHandler{
		config:                     shardContext.GetConfig(),
		shardContext:               shardContext,
		workflowConsistencyChecker: workflowConsistencyChecker,
		timeSource:                 shardContext.GetTimeSource(),
		namespaceRegistry:          shardContext.GetNamespaceRegistry(),
		eventNotifier:              eventNotifier,
		tokenSerializer:            tokenSerializer,
		metricsHandler:             shardContext.GetMetricsHandler(),
		logger:                     shardContext.GetLogger(),
		throttledLogger:            shardContext.GetThrottledLogger(),
		commandAttrValidator: newCommandAttrValidator(
			shardContext.GetNamespaceRegistry(),
			shardContext.GetConfig(),
			searchAttributesValidator,
		),
		searchAttributesMapperProvider: shardContext.GetSearchAttributesMapperProvider(),
		searchAttributesValidator:      searchAttributesValidator,
		persistenceVisibilityMgr:       visibilityManager,
		commandHandlerRegistry:         commandHandlerRegistry,
	}
}

//nolint:revive // cyclomatic complexity
func (handler *WorkflowTaskCompletedHandler) Invoke(
	ctx context.Context,
	req *historyservice.RespondWorkflowTaskCompletedRequest,
) (_ *historyservice.RespondWorkflowTaskCompletedResponse, retError error) {
	namespaceEntry, err := api.GetActiveNamespace(handler.shardContext, namespace.ID(req.GetNamespaceId()))
	if err != nil {
		return nil, err
	}

	request := req.CompleteRequest
	token, err0 := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err0 != nil {
		return nil, consts.ErrDeserializingToken
	}

	workflowLease, err := handler.workflowConsistencyChecker.GetWorkflowLease(
		ctx,
		token.Clock,
		func(mutableState workflow.MutableState) bool {
			workflowTask := mutableState.GetWorkflowTaskByID(token.GetScheduledEventId())
			if workflowTask == nil && token.GetScheduledEventId() >= mutableState.GetNextEventID() {
				metrics.StaleMutableStateCounter.With(handler.metricsHandler).Record(
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
	weContext := workflowLease.GetContext()
	ms := workflowLease.GetMutableState()

	currentWorkflowTask := ms.GetWorkflowTaskByID(token.GetScheduledEventId())
	if !ms.IsWorkflowExecutionRunning() ||
		currentWorkflowTask == nil ||
		currentWorkflowTask.StartedEventID == common.EmptyEventID ||
		(token.StartedEventId != common.EmptyEventID && token.StartedEventId != currentWorkflowTask.StartedEventID) ||
		(token.StartedTime != nil && !currentWorkflowTask.StartedTime.IsZero() && !token.StartedTime.AsTime().Equal(currentWorkflowTask.StartedTime)) ||
		currentWorkflowTask.Attempt != token.Attempt ||
		(token.Version != common.EmptyVersion && token.Version != currentWorkflowTask.Version) {
		// we have not alter mutable state yet, so release with it with nil to avoid clear MS.
		workflowLease.GetReleaseFn()(nil)
		return nil, serviceerror.NewNotFound("Workflow task not found.")
	}

	if assignedBuildId := ms.GetAssignedBuildId(); assignedBuildId != "" {
		// worker versioning is used, make sure the task was completed by the right build ID
		wftStartedBuildId := ms.GetExecutionInfo().GetWorkflowTaskBuildId()
		wftCompletedBuildId := request.GetWorkerVersionStamp().GetBuildId()
		if wftCompletedBuildId != wftStartedBuildId {
			workflowLease.GetReleaseFn()(nil)
			return nil, serviceerror.NewNotFound("this workflow task was not dispatched to this Build ID")
		}
	}

	defer func() { workflowLease.GetReleaseFn()(retError) }()

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
			handler.logger.Info("Cancel effects due to error.",
				tag.Error(retError),
				tag.WorkflowID(token.GetWorkflowId()),
				tag.WorkflowRunID(token.GetRunId()),
				tag.WorkflowNamespaceID(namespaceEntry.ID().String()))
			effects.Cancel(ctx)
		}
	}()

	// It's an error if the workflow has used versioning in the past but this task has no versioning info.
	if ms.GetMostRecentWorkerVersionStamp().GetUseVersioning() && !request.GetWorkerVersionStamp().GetUseVersioning() {
		return nil, serviceerror.NewInvalidArgument("Workflow using versioning must continue to use versioning.")
	}

	nsName := namespaceEntry.Name().String()
	limits := workflow.WorkflowTaskCompletionLimits{
		MaxResetPoints:              handler.config.MaxAutoResetPoints(nsName),
		MaxSearchAttributeValueSize: handler.config.SearchAttributesSizeOfValueLimit(nsName),
	}
	// TODO: this metric is inaccurate, it should only be emitted if a new binary checksum (or build ID) is added in this completion.
	if ms.GetExecutionInfo().AutoResetPoints != nil && limits.MaxResetPoints == len(ms.GetExecutionInfo().AutoResetPoints.Points) {
		metrics.AutoResetPointsLimitExceededCounter.With(handler.metricsHandler).Record(
			1,
			metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope))
	}

	var wtHeartbeatTimedOut bool
	var completedEvent *historypb.HistoryEvent

	// SDKs set ForceCreateNewWorkflowTask flag to true when they are doing WT heartbeats.
	// In a mean time, there might be pending commands and messages on the worker side.
	// If those commands/messages are sent on the heartbeat WT it means that WF is making progress.
	// WT heartbeat timeout is applicable only when WF doesn't make any progress and does heartbeats only.
	checkWTHeartbeatTimeout := request.GetForceCreateNewWorkflowTask() && len(request.Commands) == 0 && len(request.Messages) == 0

	if checkWTHeartbeatTimeout {
		// WorkflowTaskHeartbeatTimeout is a total duration for which workflow is allowed to send continuous heartbeats.
		// Default is 30 minutes.
		// Heartbeat duration is computed between now and OriginalScheduledTime, which is set when WT is scheduled and
		// carried over to the consequence WTs if they are heartbeat WTs.
		// After this timeout is expired, WT is timed out (although this specific WT doesn't)
		// and new WT will be scheduled on non-sticky task queue (see ClearStickyTaskQueue call bellow).
		wtHeartbeatTimeoutDuration := handler.config.WorkflowTaskHeartbeatTimeout(nsName)
		if currentWorkflowTask.OriginalScheduledTime.UnixNano() > 0 &&
			handler.timeSource.Now().After(currentWorkflowTask.OriginalScheduledTime.Add(wtHeartbeatTimeoutDuration)) {

			scope := handler.metricsHandler.WithTags(
				metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope),
				metrics.NamespaceTag(nsName),
			)
			metrics.WorkflowTaskHeartbeatTimeoutCounter.With(scope).Record(1)
			completedEvent, err = ms.AddWorkflowTaskTimedOutEvent(currentWorkflowTask)
			if err != nil {
				return nil, err
			}
			ms.ClearStickyTaskQueue()
			wtHeartbeatTimedOut = true
		}
	}
	// WT wasn't timed out (due to too many heartbeats), therefore WTCompleted event should be created.
	if !wtHeartbeatTimedOut {
		completedEvent, err = ms.AddWorkflowTaskCompletedEvent(currentWorkflowTask, request, limits)
		if err != nil {
			return nil, err
		}
	}
	// NOTE: completedEvent might be nil if WT was speculative and request has only `update.Rejection` messages.
	// See workflowTaskStateMachine.skipWorkflowTaskCompletedEvent for more details.

	if request.StickyAttributes == nil || request.StickyAttributes.WorkerTaskQueue == nil {
		metrics.CompleteWorkflowTaskWithStickyDisabledCounter.With(handler.metricsHandler).Record(
			1,
			metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope))
		ms.ClearStickyTaskQueue()
	} else {
		metrics.CompleteWorkflowTaskWithStickyEnabledCounter.With(handler.metricsHandler).Record(
			1,
			metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope))
		ms.SetStickyTaskQueue(request.StickyAttributes.WorkerTaskQueue.GetName(), request.StickyAttributes.GetScheduleToStartTimeout())
	}

	var (
		wtFailedCause               *workflowTaskFailedCause
		activityNotStartedCancelled bool
		newMutableState             workflow.MutableState
		responseMutations           []workflowTaskResponseMutation
	)
	updateRegistry := weContext.UpdateRegistry(ctx, nil)
	// hasBufferedEventsOrMessages indicates if there are any buffered events
	// or admitted updates which should generate a new workflow task.

	// TODO: HasOutgoingMessages call (=check for admitted updates) is comment out
	//   because non-durable admitted updates can't block WF from closing,
	//   because everytime WFT is failing, WF context is cleared together with update registry
	//   and admitted updates are lost. Uncomment this check when durable admitted is implemented
	//   or updates stay in the registry after WFT is failed.
	hasBufferedEventsOrMessages := ms.HasBufferedEvents() // || updateRegistry.HasOutgoingMessages(false)
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

		workflowTaskHandler := newWorkflowTaskCompletedHandler(
			request.GetIdentity(),
			completedEvent.GetEventId(), // If completedEvent is nil, then GetEventId() returns 0 and this value shouldn't be used in workflowTaskHandler.
			ms,
			updateRegistry,
			&effects,
			handler.commandAttrValidator,
			workflowSizeChecker,
			handler.logger,
			handler.namespaceRegistry,
			handler.metricsHandler,
			handler.config,
			handler.shardContext,
			handler.searchAttributesMapperProvider,
			hasBufferedEventsOrMessages,
			handler.commandHandlerRegistry,
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

		// Worker must respond with Update Accepted or Update Rejected message on every Update Requested
		// message that were delivered on specific WT, when completing this WT.
		// If worker ignored the update request (old SDK or SDK bug), then server rejects this update.
		// Otherwise, this update will be delivered (and new WT created) again and again.
		if err = workflowTaskHandler.rejectUnprocessedUpdates(
			ctx,
			currentWorkflowTask.ScheduledEventID,
			request.GetForceCreateNewWorkflowTask(),
			weContext.GetWorkflowKey(),
			request.GetIdentity(),
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

		hasBufferedEventsOrMessages = workflowTaskHandler.hasBufferedEventsOrMessages
	}

	wtFailedShouldCreateNewTask := false
	if wtFailedCause != nil {
		effects.Cancel(ctx)
		metrics.FailedWorkflowTasksCounter.With(handler.metricsHandler).Record(
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
		ms, wtFailedEventID, err = failWorkflowTask(ctx, handler.shardContext, weContext, currentWorkflowTask, wtFailedCause, request)
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

	bufferedEventShouldCreateNewTask := hasBufferedEventsOrMessages && ms.HasAnyBufferedEvent(eventShouldGenerateNewTaskFilter)
	if hasBufferedEventsOrMessages && !bufferedEventShouldCreateNewTask {
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

			// There shouldn't be any sent updates in the registry because
			// all sent but not processed updates were rejected by server.
			// Therefore, it doesn't matter if to includeAlreadySent or not.
		} else if updateRegistry.HasOutgoingMessages(true) {
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
		versioningStamp := request.WorkerVersionStamp
		if versioningStamp.GetUseVersioning() {
			if ms.GetAssignedBuildId() == "" {
				// old versioning is used. making sure the versioning stamp does not go through otherwise the
				// workflow will start using new versioning which may surprise users.
				// TODO: remove this block when deleting old wv [cleanup-old-wv]
				versioningStamp = nil
			} else {
				// new versioning is used. do not return new wft to worker if stamp build ID does not match wf build ID
				// let the task go through matching and get dispatched to the right worker
				if versioningStamp.GetBuildId() != ms.GetAssignedBuildId() {
					bypassTaskGeneration = false
				}
			}
		}

		var newWTErr error
		// If we checked WT heartbeat timeout before and WT wasn't timed out,
		// then OriginalScheduledTime needs to be carried over to the new WT.
		if checkWTHeartbeatTimeout && !wtHeartbeatTimedOut {
			newWorkflowTask, newWTErr = ms.AddWorkflowTaskScheduledEventAsHeartbeat(
				bypassTaskGeneration,
				timestamppb.New(currentWorkflowTask.OriginalScheduledTime),
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
				versioningStamp,
				nil,
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
			handler.shardContext,
			workflow.NewContext(
				handler.shardContext.GetConfig(),
				definition.NewWorkflowKey(
					newWorkflowExecutionInfo.NamespaceId,
					newWorkflowExecutionInfo.WorkflowId,
					newWorkflowExecutionState.RunId,
				),
				handler.logger,
				handler.shardContext.GetThrottledLogger(),
				handler.shardContext.GetMetricsHandler(),
			),
			newMutableState,
		)
	} else {
		// If completedEvent is not nil (which means that this WT wasn't speculative)
		// OR new WT is normal, then mutable state needs to be persisted.
		// Otherwise, (both current and new WT are speculative) mutable state is updated in memory only but not persisted.
		if completedEvent != nil || newWorkflowTaskType == enumsspb.WORKFLOW_TASK_TYPE_NORMAL {
			updateErr = weContext.UpdateWorkflowExecutionAsActive(ctx, handler.shardContext)
		}
	}

	if updateErr != nil {
		effects.Cancel(ctx)
		if persistence.IsConflictErr(updateErr) {
			metrics.ConcurrencyUpdateFailureCounter.With(handler.metricsHandler).Record(
				1,
				metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope))
		}

		// if updateErr resulted in TransactionSizeLimitError then fail workflow
		switch updateErr.(type) {
		case *persistence.TransactionSizeLimitError:
			// must reload mutable state because the first call to updateWorkflowExecutionWithContext or continueAsNewWorkflowExecution
			// clears mutable state if error is returned
			ms, err = weContext.LoadMutableState(ctx, handler.shardContext)
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
				handler.shardContext,
			); err != nil {
				return nil, err
			}
			updateRegistry.Abort(update.AbortReasonWorkflowCompleted)
		}

		return nil, updateErr
	}

	// If mutable state was persisted successfully (or persistence was skipped),
	// then effects needs to be applied immediately to keep registry and mutable state in sync.
	effects.Apply(ctx)

	if !ms.IsWorkflowExecutionRunning() {
		// If the workflow completed itself with one of the completion commands,
		// abort all waiters with "workflow completed" error.
		// Because all unprocessed updates were already rejected, incomplete updates in the registry are:
		// - updates that were received while this WFT was running,
		// - updates that were accepted but not completed by this WFT.
		// It is important to call this after applying effects to be sure there are no unprocessed effects.
		updateRegistry.Abort(update.AbortReasonWorkflowCompleted)
	}

	// Create speculative workflow task after mutable state is persisted.
	if newWorkflowTaskType == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		newWorkflowTask, err = ms.AddWorkflowTaskScheduledEvent(bypassTaskGeneration, newWorkflowTaskType)
		if err != nil {
			return nil, err
		}
		versioningStamp := request.WorkerVersionStamp
		if versioningStamp.GetUseVersioning() && ms.GetAssignedBuildId() == "" {
			// old versioning is used. making sure the versioning stamp does not go through otherwise the
			// workflow will start using new versioning which may surprise users.
			// TODO: remove this block when deleting old wv [cleanup-old-wv]
			versioningStamp = nil
		}
		_, newWorkflowTask, err = ms.AddWorkflowTaskStartedEvent(
			newWorkflowTask.ScheduledEventID,
			"request-from-RespondWorkflowTaskCompleted",
			newWorkflowTask.TaskQueue,
			request.Identity,
			versioningStamp,
			nil,
		)
		if err != nil {
			return nil, err
		}
	}

	handler.handleBufferedQueries(ms, req.GetCompleteRequest().GetQueryResults(), newWorkflowTask != nil, namespaceEntry)

	if wtHeartbeatTimedOut {
		// at this point, update is successful, but we still return an error to client so that the worker will give up this workflow
		// release workflow lock with nil error to prevent mutable state from being cleared and reloaded
		workflowLease.GetReleaseFn()(nil)
		return nil, serviceerror.NewNotFound("workflow task heartbeat timeout")
	}

	if wtFailedCause != nil {
		// release workflow lock with nil error to prevent mutable state from being cleared and reloaded
		workflowLease.GetReleaseFn()(nil)
		return nil, serviceerror.NewInvalidArgument(wtFailedCause.Message())
	}

	resp := &historyservice.RespondWorkflowTaskCompletedResponse{}
	//nolint:staticcheck
	if newWorkflowTask != nil && bypassTaskGeneration {
		resp.StartedResponse, err = recordworkflowtaskstarted.CreateRecordWorkflowTaskStartedResponse(
			ctx,
			ms,
			updateRegistry,
			newWorkflowTask,
			request.GetIdentity(),
			request.GetForceCreateNewWorkflowTask(),
		)
		if err != nil {
			return nil, err
		}
		// sticky is always enabled when worker request for new workflow task from RespondWorkflowTaskCompleted
		resp.StartedResponse.StickyExecutionEnabled = true

		resp.NewWorkflowTask, err = handler.withNewWorkflowTask(ctx, namespaceEntry.Name(), req, resp.StartedResponse)
		if err != nil {
			return nil, err
		}
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

func (handler *WorkflowTaskCompletedHandler) createPollWorkflowTaskQueueResponse(
	ctx context.Context,
	namespaceID namespace.ID,
	matchingResp *matchingservice.PollWorkflowTaskQueueResponse,
	branchToken []byte,
	maximumPageSize int32,
) (_ *workflowservice.PollWorkflowTaskQueueResponse, retError error) {

	if matchingResp.WorkflowExecution == nil {
		// this will happen if there is no workflow task to be send to worker / caller
		return &workflowservice.PollWorkflowTaskQueueResponse{}, nil
	}

	var history *historypb.History
	var continuation []byte
	var err error

	if matchingResp.GetStickyExecutionEnabled() && matchingResp.Query != nil {
		// meaning sticky query, we should not return any events to worker
		// since query task only check the current status
		history = &historypb.History{
			Events: []*historypb.HistoryEvent{},
		}
	} else {
		// here we have 3 cases:
		// 1. sticky && non query task
		// 2. non sticky &&  non query task
		// 3. non sticky && query task
		// for 1, partial history have to be send back
		// for 2 and 3, full history have to be send back

		var persistenceToken []byte

		firstEventID := common.FirstEventID
		nextEventID := matchingResp.GetNextEventId()
		if matchingResp.GetStickyExecutionEnabled() {
			firstEventID = matchingResp.GetPreviousStartedEventId() + 1
		}

		// TODO below is a temporal solution to guard against invalid event batch
		//  when data inconsistency occurs
		//  long term solution should check event batch pointing backwards within history store
		defer func() {
			if _, ok := retError.(*serviceerror.DataLoss); ok {
				api.TrimHistoryNode(
					ctx,
					handler.shardContext,
					handler.workflowConsistencyChecker,
					handler.eventNotifier,
					namespaceID.String(),
					matchingResp.WorkflowExecution.GetWorkflowId(),
					matchingResp.WorkflowExecution.GetRunId(),
				)
			}
		}()
		history, persistenceToken, err = api.GetHistory(
			ctx,
			handler.shardContext,
			namespaceID,
			matchingResp.GetWorkflowExecution(),
			firstEventID,
			nextEventID,
			maximumPageSize,
			nil,
			matchingResp.GetTransientWorkflowTask(),
			branchToken,
			handler.persistenceVisibilityMgr,
		)
		if err != nil {
			return nil, err
		}

		if len(persistenceToken) != 0 {
			continuation, err = api.SerializeHistoryToken(&tokenspb.HistoryContinuation{
				RunId:                 matchingResp.WorkflowExecution.GetRunId(),
				FirstEventId:          firstEventID,
				NextEventId:           nextEventID,
				PersistenceToken:      persistenceToken,
				TransientWorkflowTask: matchingResp.GetTransientWorkflowTask(),
				BranchToken:           branchToken,
			})
			if err != nil {
				return nil, err
			}
		}
	}

	resp := &workflowservice.PollWorkflowTaskQueueResponse{
		TaskToken:                  matchingResp.TaskToken,
		WorkflowExecution:          matchingResp.WorkflowExecution,
		WorkflowType:               matchingResp.WorkflowType,
		PreviousStartedEventId:     matchingResp.PreviousStartedEventId,
		StartedEventId:             matchingResp.StartedEventId,
		Query:                      matchingResp.Query,
		BacklogCountHint:           matchingResp.BacklogCountHint,
		Attempt:                    matchingResp.Attempt,
		History:                    history,
		NextPageToken:              continuation,
		WorkflowExecutionTaskQueue: matchingResp.WorkflowExecutionTaskQueue,
		ScheduledTime:              matchingResp.ScheduledTime,
		StartedTime:                matchingResp.StartedTime,
		Queries:                    matchingResp.Queries,
		Messages:                   matchingResp.Messages,
	}

	return resp, nil
}

func (handler *WorkflowTaskCompletedHandler) withNewWorkflowTask(
	ctx context.Context,
	namespaceName namespace.Name,
	request *historyservice.RespondWorkflowTaskCompletedRequest,
	response *historyservice.RecordWorkflowTaskStartedResponse,
) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
	taskToken, err := handler.tokenSerializer.Deserialize(request.CompleteRequest.TaskToken)
	if err != nil {
		return nil, err
	}

	taskToken = tasktoken.NewWorkflowTaskToken(
		taskToken.GetNamespaceId(),
		taskToken.GetWorkflowId(),
		taskToken.GetRunId(),
		response.GetScheduledEventId(),
		response.GetStartedEventId(),
		response.GetStartedTime(),
		response.GetAttempt(),
		response.GetClock(),
		response.GetVersion(),
	)
	token, err := handler.tokenSerializer.Serialize(taskToken)
	if err != nil {
		return nil, err
	}
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: taskToken.GetWorkflowId(),
		RunId:      taskToken.GetRunId(),
	}
	matchingResp := common.CreateMatchingPollWorkflowTaskQueueResponse(response, workflowExecution, token)

	return handler.createPollWorkflowTaskQueueResponse(
		ctx,
		namespace.ID(taskToken.NamespaceId),
		matchingResp,
		matchingResp.GetBranchToken(),
		int32(handler.config.HistoryMaxPageSize(namespaceName.String())),
	)
}

func (handler *WorkflowTaskCompletedHandler) handleBufferedQueries(
	ms workflow.MutableState,
	queryResults map[string]*querypb.WorkflowQueryResult,
	createNewWorkflowTask bool,
	namespaceEntry *namespace.Namespace,
) {
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
				metrics.QueryRegistryInvalidStateCount.With(scope).Record(1)
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
				metrics.QueryRegistryInvalidStateCount.With(scope).Record(1)
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
				metrics.QueryRegistryInvalidStateCount.With(scope).Record(1)
			}
		}
	}
}

func failWorkflowTask(
	ctx context.Context,
	shardContext shard.Context,
	wfContext workflow.Context,
	workflowTask *workflow.WorkflowTaskInfo,
	wtFailedCause *workflowTaskFailedCause,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
) (workflow.MutableState, int64, error) {

	// clear any updates we have accumulated so far
	wfContext.Clear()

	// Reload workflow execution so we can apply the workflow task failure event
	mutableState, err := wfContext.LoadMutableState(ctx, shardContext)
	if err != nil {
		return nil, common.EmptyEventID, err
	}
	wtFailedEvent, err := mutableState.AddWorkflowTaskFailedEvent(
		workflowTask,
		wtFailedCause.failedCause,
		failure.NewServerFailure(wtFailedCause.Message(), false),
		request.GetIdentity(),
		nil,
		request.GetBinaryChecksum(),
		"",
		"",
		0,
	)
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
