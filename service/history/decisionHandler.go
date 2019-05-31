// Copyright (c) 2017 Uber Technologies, Inc.
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
	ctx "context"
	"fmt"
	"time"

	"github.com/pborman/uuid"
	h "github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cron"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"go.uber.org/yarpc"
)

type (
	// decision business logic handler
	decisionHandler interface {
		handleDecisionTaskScheduled(ctx.Context, *h.ScheduleDecisionTaskRequest) error
		handleDecisionTaskStarted(ctx.Context,
			*h.RecordDecisionTaskStartedRequest) (*h.RecordDecisionTaskStartedResponse, error)
		handleDecisionTaskFailed(ctx.Context,
			*h.RespondDecisionTaskFailedRequest) error
		handleDecisionTaskCompleted(ctx.Context,
			*h.RespondDecisionTaskCompletedRequest) (*h.RespondDecisionTaskCompletedResponse, error)
		// TODO also include the handle of decision timeout here
	}

	decisionHandlerImpl struct {
		currentClusterName string
		config             *Config
		shard              ShardContext
		historyEngine      *historyEngineImpl
		historyCache       *historyCache
		txProcessor        transferQueueProcessor
		timerProcessor     timerQueueProcessor
		tokenSerializer    common.TaskTokenSerializer
		metricsClient      metrics.Client
		logger             log.Logger
		throttledLogger    log.Logger
	}
)

func newDecisionHandler(historyEngine *historyEngineImpl) *decisionHandlerImpl {
	return &decisionHandlerImpl{
		currentClusterName: historyEngine.currentClusterName,
		config:             historyEngine.config,
		shard:              historyEngine.shard,
		historyEngine:      historyEngine,
		historyCache:       historyEngine.historyCache,
		txProcessor:        historyEngine.txProcessor,
		timerProcessor:     historyEngine.timerProcessor,
		tokenSerializer:    historyEngine.tokenSerializer,
		metricsClient:      historyEngine.metricsClient,
		logger:             historyEngine.logger,
		throttledLogger:    historyEngine.throttledLogger,
	}
}

func (handler *decisionHandlerImpl) handleDecisionTaskScheduled(
	ctx ctx.Context,
	req *h.ScheduleDecisionTaskRequest,
) error {

	domainEntry, err := handler.historyEngine.getActiveDomainEntry(req.DomainUUID)
	if err != nil {
		return err
	}
	domainID := domainEntry.GetInfo().ID

	execution := workflow.WorkflowExecution{
		WorkflowId: req.WorkflowExecution.WorkflowId,
		RunId:      req.WorkflowExecution.RunId,
	}

	return handler.historyEngine.updateWorkflowExecutionWithAction(ctx, domainID, execution,
		func(msBuilder mutableState, tBuilder *timerBuilder) (*updateWorkflowAction, error) {
			if !msBuilder.IsWorkflowExecutionRunning() {
				return nil, ErrWorkflowCompleted
			}

			postActions := &updateWorkflowAction{
				createDecision: true,
				transferTasks:  []persistence.Task{&persistence.RecordWorkflowStartedTask{}},
			}

			startEvent, _ := msBuilder.GetStartEvent()
			executionTimestamp := getWorkflowExecutionTimestamp(msBuilder, startEvent)
			if req.GetIsFirstDecision() && executionTimestamp.After(time.Now()) {
				postActions.timerTasks = append(postActions.timerTasks, &persistence.WorkflowBackoffTimerTask{
					VisibilityTimestamp: executionTimestamp,
					TimeoutType:         persistence.WorkflowBackoffTimeoutTypeCron,
				})
				postActions.createDecision = false
			}

			return postActions, nil
		})
}

func (handler *decisionHandlerImpl) handleDecisionTaskStarted(
	ctx ctx.Context,
	req *h.RecordDecisionTaskStartedRequest,
) (resp *h.RecordDecisionTaskStartedResponse, retError error) {

	domainEntry, err := handler.historyEngine.getActiveDomainEntry(req.DomainUUID)
	if err != nil {
		return nil, err
	}
	domainID := domainEntry.GetInfo().ID

	context, release, err0 := handler.historyCache.getOrCreateWorkflowExecutionWithTimeout(ctx, domainID, *req.WorkflowExecution)
	if err0 != nil {
		return nil, err0
	}
	defer func() { release(retError) }()

	scheduleID := req.GetScheduleId()
	requestID := req.GetRequestId()

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		msBuilder, err0 := context.loadWorkflowExecution()
		if err0 != nil {
			return nil, err0
		}
		if !msBuilder.IsWorkflowExecutionRunning() {
			return nil, ErrWorkflowCompleted
		}

		tBuilder := handler.historyEngine.getTimerBuilder(context.getExecution())

		di, isRunning := msBuilder.GetPendingDecision(scheduleID)

		// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
		// some extreme cassandra failure cases.
		if !isRunning && scheduleID >= msBuilder.GetNextEventID() {
			handler.metricsClient.IncCounter(metrics.HistoryRecordDecisionTaskStartedScope, metrics.StaleMutableStateCounter)
			// Reload workflow execution history
			context.clear()
			continue Update_History_Loop
		}

		// Check execution state to make sure task is in the list of outstanding tasks and it is not yet started.  If
		// task is not outstanding than it is most probably a duplicate and complete the task.
		if !isRunning {
			// Looks like DecisionTask already completed as a result of another call.
			// It is OK to drop the task at this point.
			context.getLogger().Debug("Potentially duplicate task.", tag.TaskID(req.GetTaskId()), tag.WorkflowScheduleID(scheduleID), tag.TaskType(persistence.TransferTaskTypeDecisionTask))

			return nil, &workflow.EntityNotExistsError{Message: "Decision task not found."}
		}

		if di.StartedID != common.EmptyEventID {
			// If decision is started as part of the current request scope then return a positive response
			if di.RequestID == requestID {
				return handler.createRecordDecisionTaskStartedResponse(domainID, msBuilder, di, req.PollRequest.GetIdentity()), nil
			}

			// Looks like DecisionTask already started as a result of another call.
			// It is OK to drop the task at this point.
			context.getLogger().Debug("Potentially duplicate task.", tag.TaskID(req.GetTaskId()), tag.WorkflowScheduleID(scheduleID), tag.TaskType(persistence.TaskListTypeDecision))
			return nil, &h.EventAlreadyStartedError{Message: "Decision task already started."}
		}

		_, di = msBuilder.AddDecisionTaskStartedEvent(scheduleID, requestID, req.PollRequest)
		if di == nil {
			// Unable to add DecisionTaskStarted event to history
			return nil, &workflow.InternalServiceError{Message: "Unable to add DecisionTaskStarted event to history."}
		}

		// Start a timer for the decision task.
		timeOutTask := tBuilder.AddStartToCloseDecisionTimoutTask(di.ScheduleID, di.Attempt, di.DecisionTimeout)
		timerTasks := []persistence.Task{timeOutTask}
		defer handler.timerProcessor.NotifyNewTimers(handler.currentClusterName, handler.shard.GetCurrentTime(handler.currentClusterName), timerTasks)

		// Generate a transaction ID for appending events to history
		transactionID, err2 := handler.shard.GetNextTransferTaskID()
		if err2 != nil {
			return nil, err2
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
		// the history and try the operation again.
		if err3 := context.updateWorkflowExecution(nil, timerTasks, transactionID); err3 != nil {
			if err3 == ErrConflict {
				handler.metricsClient.IncCounter(metrics.HistoryRecordDecisionTaskStartedScope,
					metrics.ConcurrencyUpdateFailureCounter)
				continue Update_History_Loop
			}
			return nil, err3
		}

		return handler.createRecordDecisionTaskStartedResponse(domainID, msBuilder, di, req.PollRequest.GetIdentity()), nil
	}

	return nil, ErrMaxAttemptsExceeded
}

func (handler *decisionHandlerImpl) handleDecisionTaskFailed(
	ctx ctx.Context,
	req *h.RespondDecisionTaskFailedRequest,
) (retError error) {

	domainEntry, err := handler.historyEngine.getActiveDomainEntry(req.DomainUUID)
	if err != nil {
		return err
	}
	domainID := domainEntry.GetInfo().ID

	request := req.FailedRequest
	token, err0 := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err0 != nil {
		return ErrDeserializingToken
	}

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(token.WorkflowID),
		RunId:      common.StringPtr(token.RunID),
	}

	return handler.historyEngine.updateWorkflowExecution(ctx, domainID, workflowExecution, false, true,
		func(msBuilder mutableState, tBuilder *timerBuilder) ([]persistence.Task, error) {
			if !msBuilder.IsWorkflowExecutionRunning() {
				return nil, ErrWorkflowCompleted
			}

			scheduleID := token.ScheduleID
			di, isRunning := msBuilder.GetPendingDecision(scheduleID)
			if !isRunning || di.Attempt != token.ScheduleAttempt || di.StartedID == common.EmptyEventID {
				return nil, &workflow.EntityNotExistsError{Message: "Decision task not found."}
			}

			msBuilder.AddDecisionTaskFailedEvent(di.ScheduleID, di.StartedID, request.GetCause(), request.Details,
				request.GetIdentity(), "", "", "", 0)

			return nil, nil
		})
}

func (handler *decisionHandlerImpl) handleDecisionTaskCompleted(
	ctx ctx.Context,
	req *h.RespondDecisionTaskCompletedRequest,
) (resp *h.RespondDecisionTaskCompletedResponse, retError error) {

	domainEntry, err := handler.historyEngine.getActiveDomainEntry(req.DomainUUID)
	if err != nil {
		return nil, err
	}
	domainID := domainEntry.GetInfo().ID

	var eventStoreVersion int32
	if handler.config.EnableEventsV2(domainEntry.GetInfo().Name) {
		eventStoreVersion = persistence.EventStoreVersionV2
	}
	request := req.CompleteRequest
	token, err0 := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err0 != nil {
		return nil, ErrDeserializingToken
	}

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(token.WorkflowID),
		RunId:      common.StringPtr(token.RunID),
	}

	call := yarpc.CallFromContext(ctx)
	clientLibVersion := call.Header(common.LibraryVersionHeaderName)
	clientFeatureVersion := call.Header(common.FeatureVersionHeaderName)
	clientImpl := call.Header(common.ClientImplHeaderName)

	context, release, err0 := handler.historyCache.getOrCreateWorkflowExecutionWithTimeout(ctx, domainID, workflowExecution)
	if err0 != nil {
		return nil, err0
	}
	defer func() { release(retError) }()

	sizeLimitError := handler.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := handler.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)
	maxIDLengthLimit := handler.config.MaxIDLengthLimit()

	sizeChecker := &decisionBlobSizeChecker{
		sizeLimitWarn:  sizeLimitWarn,
		sizeLimitError: sizeLimitError,
		domainID:       domainID,
		workflowID:     token.WorkflowID,
		runID:          token.RunID,
		metricsClient:  handler.metricsClient,
		logger:         handler.throttledLogger,
	}

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		msBuilder, err1 := context.loadWorkflowExecution()
		if err1 != nil {
			return nil, err1
		}
		if !msBuilder.IsWorkflowExecutionRunning() {
			return nil, ErrWorkflowCompleted
		}

		executionInfo := msBuilder.GetExecutionInfo()
		tBuilder := handler.historyEngine.getTimerBuilder(context.getExecution())

		scheduleID := token.ScheduleID
		di, isRunning := msBuilder.GetPendingDecision(scheduleID)

		// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
		// some extreme cassandra failure cases.
		if !isRunning && scheduleID >= msBuilder.GetNextEventID() {
			handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope, metrics.StaleMutableStateCounter)
			// Reload workflow execution history
			context.clear()
			continue Update_History_Loop
		}

		if !msBuilder.IsWorkflowExecutionRunning() || !isRunning || di.Attempt != token.ScheduleAttempt ||
			di.StartedID == common.EmptyEventID {
			return nil, &workflow.EntityNotExistsError{Message: "Decision task not found."}
		}

		startedID := di.StartedID
		maxResetPoints := handler.config.MaxAutoResetPoints(domainEntry.GetInfo().Name)
		if msBuilder.GetExecutionInfo().AutoResetPoints != nil && maxResetPoints == len(msBuilder.GetExecutionInfo().AutoResetPoints.Points) {
			handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope, metrics.AutoResetPointsLimitExceededCounter)
			handler.throttledLogger.Warn("the number of auto-reset points is exceeding the limit, will do rotating.",
				tag.WorkflowDomainName(domainEntry.GetInfo().Name),
				tag.WorkflowDomainID(domainEntry.GetInfo().ID),
				tag.WorkflowID(workflowExecution.GetWorkflowId()),
				tag.WorkflowRunID(workflowExecution.GetRunId()),
				tag.Number(int64(maxResetPoints)))
		}
		completedEvent := msBuilder.AddDecisionTaskCompletedEvent(scheduleID, startedID, request, maxResetPoints)
		if completedEvent == nil {
			return nil, &workflow.InternalServiceError{Message: "Unable to add DecisionTaskCompleted event to history."}
		}

		var (
			failDecision                    bool
			failCause                       workflow.DecisionTaskFailedCause
			failMessage                     string
			isComplete                      bool
			activityNotStartedCancelled     bool
			transferTasks                   []persistence.Task
			timerTasks                      []persistence.Task
			continueAsNewBuilder            mutableState
			continueAsNewTimerTasks         []persistence.Task
			hasDecisionScheduleActivityTask bool
		)
		completedID := *completedEvent.EventId
		hasUnhandledEvents := msBuilder.HasBufferedEvents()

		if request.StickyAttributes == nil || request.StickyAttributes.WorkerTaskList == nil {
			handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope, metrics.CompleteDecisionWithStickyDisabledCounter)
			executionInfo.StickyTaskList = ""
			executionInfo.StickyScheduleToStartTimeout = 0
		} else {
			handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope, metrics.CompleteDecisionWithStickyEnabledCounter)
			executionInfo.StickyTaskList = request.StickyAttributes.WorkerTaskList.GetName()
			executionInfo.StickyScheduleToStartTimeout = request.StickyAttributes.GetScheduleToStartTimeoutSeconds()
		}
		executionInfo.ClientLibraryVersion = clientLibVersion
		executionInfo.ClientFeatureVersion = clientFeatureVersion
		executionInfo.ClientImpl = clientImpl

		sizeChecker.completedID = completedID
		sizeChecker.msBuilder = msBuilder

		binChecksum := request.GetBinaryChecksum()
		if _, ok := domainEntry.GetConfig().BadBinaries.Binaries[binChecksum]; ok {
			failDecision = true
			failCause = workflow.DecisionTaskFailedCauseBadBinary
			failMessage = fmt.Sprintf("binary %v is already marked as bad deployment", binChecksum)
		} else {
		Process_Decision_Loop:
			for _, d := range request.Decisions {
				switch *d.DecisionType {
				case workflow.DecisionTypeScheduleActivityTask:
					handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
						metrics.DecisionTypeScheduleActivityCounter)
					targetDomainID := domainID
					attributes := d.ScheduleActivityTaskDecisionAttributes
					// First check if we need to use a different target domain to schedule activity
					if attributes.Domain != nil {
						// TODO: Error handling for ActivitySchedule failed when domain lookup fails
						domainEntry, err := handler.shard.GetDomainCache().GetDomain(*attributes.Domain)
						if err != nil {
							return nil, &workflow.InternalServiceError{Message: "Unable to schedule activity across domain."}
						}
						targetDomainID = domainEntry.GetInfo().ID
					}

					if err := validateActivityScheduleAttributes(attributes, executionInfo.WorkflowTimeout, maxIDLengthLimit); err != nil {
						return nil, err
					}

					failWorkflow, err := sizeChecker.failWorkflowIfBlobSizeExceedsLimit(attributes.Input, "ScheduleActivityTaskDecisionAttributes.Input exceeds size limit.")
					if err != nil {
						return nil, err
					}
					if failWorkflow {
						isComplete = true
						break Process_Decision_Loop
					}

					scheduleEvent, _ := msBuilder.AddActivityTaskScheduledEvent(completedID, attributes)
					transferTasks = append(transferTasks, &persistence.ActivityTask{
						DomainID:   targetDomainID,
						TaskList:   *attributes.TaskList.Name,
						ScheduleID: *scheduleEvent.EventId,
					})
					hasDecisionScheduleActivityTask = true

				case workflow.DecisionTypeCompleteWorkflowExecution:
					handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
						metrics.DecisionTypeCompleteWorkflowCounter)
					if hasUnhandledEvents {
						failDecision = true
						failCause = workflow.DecisionTaskFailedCauseUnhandledDecision
						break Process_Decision_Loop
					}

					// If the decision has more than one completion event than just pick the first one
					if isComplete {
						handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
							metrics.MultipleCompletionDecisionsCounter)
						handler.logger.Warn("Multiple completion decisions", tag.WorkflowDecisionType(int64(*d.DecisionType)), tag.ErrorTypeMultipleCompletionDecisions)
						continue Process_Decision_Loop
					}
					attributes := d.CompleteWorkflowExecutionDecisionAttributes
					if err := validateCompleteWorkflowExecutionAttributes(attributes); err != nil {
						return nil, err
					}
					failWorkflow, err := sizeChecker.failWorkflowIfBlobSizeExceedsLimit(attributes.Result, "CompleteWorkflowExecutionDecisionAttributes.Result exceeds size limit.")
					if err != nil {
						return nil, err
					}
					if failWorkflow {
						isComplete = true
						break Process_Decision_Loop
					}

					// check if this is a cron workflow
					cronBackoff := msBuilder.GetCronBackoffDuration()
					if cronBackoff == cron.NoBackoff {
						// not cron, so complete this workflow execution
						if e := msBuilder.AddCompletedWorkflowEvent(completedID, attributes); e == nil {
							return nil, &workflow.InternalServiceError{Message: "Unable to add complete workflow event."}
						}
					} else {
						// this is a cron workflow
						startEvent, err := getWorkflowStartedEvent(
							handler.historyEngine.historyMgr,
							handler.historyEngine.historyV2Mgr,
							msBuilder.GetEventStoreVersion(),
							msBuilder.GetCurrentBranch(),
							handler.logger,
							domainID,
							workflowExecution.GetWorkflowId(),
							workflowExecution.GetRunId(),
							common.IntPtr(handler.shard.GetShardID()),
						)
						if err != nil {
							return nil, err
						}

						startAttributes := startEvent.WorkflowExecutionStartedEventAttributes
						continueAsNewAttributes := &workflow.ContinueAsNewWorkflowExecutionDecisionAttributes{
							WorkflowType:                        startAttributes.WorkflowType,
							TaskList:                            startAttributes.TaskList,
							RetryPolicy:                         startAttributes.RetryPolicy,
							Input:                               startAttributes.Input,
							ExecutionStartToCloseTimeoutSeconds: startAttributes.ExecutionStartToCloseTimeoutSeconds,
							TaskStartToCloseTimeoutSeconds:      startAttributes.TaskStartToCloseTimeoutSeconds,
							BackoffStartIntervalInSeconds:       common.Int32Ptr(int32(cronBackoff.Seconds())),
							Initiator:                           workflow.ContinueAsNewInitiatorCronSchedule.Ptr(),
							LastCompletionResult:                attributes.Result,
							CronSchedule:                        common.StringPtr(msBuilder.GetExecutionInfo().CronSchedule),
						}

						if _, continueAsNewBuilder, err = msBuilder.AddContinueAsNewEvent(completedID, completedID, domainEntry,
							startAttributes.GetParentWorkflowDomain(), continueAsNewAttributes, eventStoreVersion); err != nil {
							return nil, err
						}
					}

					isComplete = true
				case workflow.DecisionTypeFailWorkflowExecution:
					handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
						metrics.DecisionTypeFailWorkflowCounter)
					if hasUnhandledEvents {
						failDecision = true
						failCause = workflow.DecisionTaskFailedCauseUnhandledDecision
						break Process_Decision_Loop
					}

					// If the decision has more than one completion event than just pick the first one
					if isComplete {
						handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
							metrics.MultipleCompletionDecisionsCounter)
						handler.logger.Warn("Multiple completion decisions", tag.WorkflowDecisionType(int64(*d.DecisionType)), tag.ErrorTypeMultipleCompletionDecisions)
						continue Process_Decision_Loop
					}

					failedAttributes := d.FailWorkflowExecutionDecisionAttributes
					if err := validateFailWorkflowExecutionAttributes(failedAttributes); err != nil {
						return nil, err
					}

					failWorkflow, err := sizeChecker.failWorkflowIfBlobSizeExceedsLimit(failedAttributes.Details, "FailWorkflowExecutionDecisionAttributes.Details exceeds size limit.")
					if err != nil {
						return nil, err
					}
					if failWorkflow {
						isComplete = true
						break Process_Decision_Loop
					}

					backoffInterval := msBuilder.GetRetryBackoffDuration(failedAttributes.GetReason())
					continueAsNewInitiator := workflow.ContinueAsNewInitiatorRetryPolicy
					if backoffInterval == common.NoRetryBackoff {
						backoffInterval = msBuilder.GetCronBackoffDuration()
						continueAsNewInitiator = workflow.ContinueAsNewInitiatorCronSchedule
					}

					if backoffInterval == cron.NoBackoff {
						// no retry or cron
						if evt := msBuilder.AddFailWorkflowEvent(completedID, failedAttributes); evt == nil {
							return nil, &workflow.InternalServiceError{Message: "Unable to add fail workflow event."}
						}
					} else {
						// retry or cron with backoff
						startEvent, err := getWorkflowStartedEvent(
							handler.historyEngine.historyMgr,
							handler.historyEngine.historyV2Mgr,
							msBuilder.GetEventStoreVersion(),
							msBuilder.GetCurrentBranch(),
							handler.logger,
							domainID,
							workflowExecution.GetWorkflowId(),
							workflowExecution.GetRunId(),
							common.IntPtr(handler.shard.GetShardID()),
						)
						if err != nil {
							return nil, err
						}

						startAttributes := startEvent.WorkflowExecutionStartedEventAttributes
						continueAsNewAttributes := &workflow.ContinueAsNewWorkflowExecutionDecisionAttributes{
							WorkflowType:                        startAttributes.WorkflowType,
							TaskList:                            startAttributes.TaskList,
							RetryPolicy:                         startAttributes.RetryPolicy,
							Input:                               startAttributes.Input,
							ExecutionStartToCloseTimeoutSeconds: startAttributes.ExecutionStartToCloseTimeoutSeconds,
							TaskStartToCloseTimeoutSeconds:      startAttributes.TaskStartToCloseTimeoutSeconds,
							BackoffStartIntervalInSeconds:       common.Int32Ptr(int32(backoffInterval.Seconds())),
							Initiator:                           continueAsNewInitiator.Ptr(),
							FailureReason:                       failedAttributes.Reason,
							FailureDetails:                      failedAttributes.Details,
							LastCompletionResult:                startAttributes.LastCompletionResult,
							CronSchedule:                        common.StringPtr(msBuilder.GetExecutionInfo().CronSchedule),
						}

						if _, continueAsNewBuilder, err = msBuilder.AddContinueAsNewEvent(completedID, completedID, domainEntry,
							startAttributes.GetParentWorkflowDomain(), continueAsNewAttributes, eventStoreVersion); err != nil {
							return nil, err
						}
					}

					isComplete = true

				case workflow.DecisionTypeCancelWorkflowExecution:
					handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
						metrics.DecisionTypeCancelWorkflowCounter)
					// If new events came while we are processing the decision, we would fail this and give a chance to client
					// to process the new event.
					if hasUnhandledEvents {
						failDecision = true
						failCause = workflow.DecisionTaskFailedCauseUnhandledDecision
						break Process_Decision_Loop
					}

					// If the decision has more than one completion event than just pick the first one
					if isComplete {
						handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
							metrics.MultipleCompletionDecisionsCounter)
						handler.logger.Warn("Multiple completion decisions", tag.WorkflowDecisionType(int64(*d.DecisionType)), tag.ErrorTypeMultipleCompletionDecisions)
						continue Process_Decision_Loop
					}
					attributes := d.CancelWorkflowExecutionDecisionAttributes
					if err := validateCancelWorkflowExecutionAttributes(attributes); err != nil {
						return nil, err
					}
					msBuilder.AddWorkflowExecutionCanceledEvent(completedID, attributes)
					isComplete = true

				case workflow.DecisionTypeStartTimer:
					handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
						metrics.DecisionTypeStartTimerCounter)
					attributes := d.StartTimerDecisionAttributes
					if err := validateTimerScheduleAttributes(attributes, maxIDLengthLimit); err != nil {
						return nil, err
					}
					_, ti := msBuilder.AddTimerStartedEvent(completedID, attributes)
					if ti == nil {
						failDecision = true
						failCause = workflow.DecisionTaskFailedCauseStartTimerDuplicateID
						break Process_Decision_Loop
					}
					tBuilder.AddUserTimer(ti, msBuilder)

				case workflow.DecisionTypeRequestCancelActivityTask:
					handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
						metrics.DecisionTypeCancelActivityCounter)
					attributes := d.RequestCancelActivityTaskDecisionAttributes
					if err := validateActivityCancelAttributes(attributes, maxIDLengthLimit); err != nil {
						return nil, err
					}
					activityID := *attributes.ActivityId
					actCancelReqEvent, ai, isRunning := msBuilder.AddActivityTaskCancelRequestedEvent(completedID, activityID,
						common.StringDefault(request.Identity))
					if !isRunning {
						msBuilder.AddRequestCancelActivityTaskFailedEvent(completedID, activityID,
							activityCancellationMsgActivityIDUnknown)
						continue Process_Decision_Loop
					}

					if ai.StartedID == common.EmptyEventID {
						// We haven't started the activity yet, we can cancel the activity right away and
						// schedule a decision task to ensure the workflow makes progress.
						msBuilder.AddActivityTaskCanceledEvent(ai.ScheduleID, ai.StartedID, *actCancelReqEvent.EventId,
							[]byte(activityCancellationMsgActivityNotStarted), common.StringDefault(request.Identity))
						activityNotStartedCancelled = true
					}

				case workflow.DecisionTypeCancelTimer:
					handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
						metrics.DecisionTypeCancelTimerCounter)
					attributes := d.CancelTimerDecisionAttributes
					if err := validateTimerCancelAttributes(attributes, maxIDLengthLimit); err != nil {
						return nil, err
					}
					if msBuilder.AddTimerCanceledEvent(completedID, attributes, common.StringDefault(request.Identity)) == nil {
						msBuilder.AddCancelTimerFailedEvent(completedID, attributes, common.StringDefault(request.Identity))
					} else {
						// timer deletion is success. we need to rebuild the timer builder
						// since timer builder has a local cached version of timers
						tBuilder = handler.historyEngine.getTimerBuilder(context.getExecution())
						tBuilder.loadUserTimers(msBuilder)

						// timer deletion is a success, we may have deleted a fired timer in
						// which case we should reset hasBufferedEvents
						hasUnhandledEvents = msBuilder.HasBufferedEvents()
					}

				case workflow.DecisionTypeRecordMarker:
					handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
						metrics.DecisionTypeRecordMarkerCounter)
					attributes := d.RecordMarkerDecisionAttributes
					if err := validateRecordMarkerAttributes(attributes, maxIDLengthLimit); err != nil {
						return nil, err
					}
					failWorkflow, err := sizeChecker.failWorkflowIfBlobSizeExceedsLimit(attributes.Details, "RecordMarkerDecisionAttributes.Details exceeds size limit.")
					if err != nil {
						return nil, err
					}
					if failWorkflow {
						isComplete = true
						break Process_Decision_Loop
					}

					msBuilder.AddRecordMarkerEvent(completedID, attributes)

				case workflow.DecisionTypeRequestCancelExternalWorkflowExecution:
					handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
						metrics.DecisionTypeCancelExternalWorkflowCounter)
					attributes := d.RequestCancelExternalWorkflowExecutionDecisionAttributes
					if err := validateCancelExternalWorkflowExecutionAttributes(attributes, maxIDLengthLimit); err != nil {
						return nil, err
					}

					foreignDomainID := ""
					if attributes.GetDomain() == "" {
						foreignDomainID = executionInfo.DomainID
					} else {
						foreignDomainEntry, err := handler.shard.GetDomainCache().GetDomain(attributes.GetDomain())
						if err != nil {
							return nil, &workflow.InternalServiceError{
								Message: fmt.Sprintf("Unable to cancel workflow across domain: %v.", attributes.GetDomain())}
						}
						foreignDomainID = foreignDomainEntry.GetInfo().ID
					}

					cancelRequestID := uuid.New()
					wfCancelReqEvent, _ := msBuilder.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(completedID,
						cancelRequestID, attributes)
					if wfCancelReqEvent == nil {
						return nil, &workflow.InternalServiceError{Message: "Unable to add external cancel workflow request."}
					}

					transferTasks = append(transferTasks, &persistence.CancelExecutionTask{
						TargetDomainID:          foreignDomainID,
						TargetWorkflowID:        attributes.GetWorkflowId(),
						TargetRunID:             attributes.GetRunId(),
						TargetChildWorkflowOnly: attributes.GetChildWorkflowOnly(),
						InitiatedID:             wfCancelReqEvent.GetEventId(),
					})

				case workflow.DecisionTypeSignalExternalWorkflowExecution:
					handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
						metrics.DecisionTypeSignalExternalWorkflowCounter)

					attributes := d.SignalExternalWorkflowExecutionDecisionAttributes
					if err := validateSignalExternalWorkflowExecutionAttributes(attributes, maxIDLengthLimit); err != nil {
						return nil, err
					}
					failWorkflow, err := sizeChecker.failWorkflowIfBlobSizeExceedsLimit(attributes.Input, "SignalExternalWorkflowExecutionDecisionAttributes.Input exceeds size limit.")
					if err != nil {
						return nil, err
					}
					if failWorkflow {
						isComplete = true
						break Process_Decision_Loop
					}

					foreignDomainID := ""
					if attributes.GetDomain() == "" {
						foreignDomainID = executionInfo.DomainID
					} else {
						foreignDomainEntry, err := handler.shard.GetDomainCache().GetDomain(attributes.GetDomain())
						if err != nil {
							return nil, &workflow.InternalServiceError{
								Message: fmt.Sprintf("Unable to signal workflow across domain: %v.", attributes.GetDomain())}
						}
						foreignDomainID = foreignDomainEntry.GetInfo().ID
					}

					signalRequestID := uuid.New() // for deduplicate
					wfSignalReqEvent, _ := msBuilder.AddSignalExternalWorkflowExecutionInitiatedEvent(completedID,
						signalRequestID, attributes)
					if wfSignalReqEvent == nil {
						return nil, &workflow.InternalServiceError{Message: "Unable to add external signal workflow request."}
					}

					transferTasks = append(transferTasks, &persistence.SignalExecutionTask{
						TargetDomainID:          foreignDomainID,
						TargetWorkflowID:        attributes.Execution.GetWorkflowId(),
						TargetRunID:             attributes.Execution.GetRunId(),
						TargetChildWorkflowOnly: attributes.GetChildWorkflowOnly(),
						InitiatedID:             wfSignalReqEvent.GetEventId(),
					})

				case workflow.DecisionTypeContinueAsNewWorkflowExecution:
					handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
						metrics.DecisionTypeContinueAsNewCounter)
					if hasUnhandledEvents {
						failDecision = true
						failCause = workflow.DecisionTaskFailedCauseUnhandledDecision
						break Process_Decision_Loop
					}

					// If the decision has more than one completion event than just pick the first one
					if isComplete {
						handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
							metrics.MultipleCompletionDecisionsCounter)
						handler.logger.Warn("Multiple completion decisions", tag.WorkflowDecisionType(int64(*d.DecisionType)), tag.ErrorTypeMultipleCompletionDecisions)
						continue Process_Decision_Loop
					}
					attributes := d.ContinueAsNewWorkflowExecutionDecisionAttributes
					if err := validateContinueAsNewWorkflowExecutionAttributes(executionInfo, attributes, maxIDLengthLimit); err != nil {
						return nil, err
					}
					failWorkflow, err := sizeChecker.failWorkflowIfBlobSizeExceedsLimit(attributes.Input, "ContinueAsNewWorkflowExecutionDecisionAttributes.Input exceeds size limit.")
					if err != nil {
						return nil, err
					}
					if failWorkflow {
						isComplete = true
						break Process_Decision_Loop
					}

					// Extract parentDomainName so it can be passed down to next run of workflow execution
					var parentDomainName string
					if msBuilder.HasParentExecution() {
						parentDomainID := executionInfo.ParentDomainID
						parentDomainEntry, err := handler.shard.GetDomainCache().GetDomainByID(parentDomainID)
						if err != nil {
							return nil, err
						}
						parentDomainName = parentDomainEntry.GetInfo().Name
					}

					_, newStateBuilder, err := msBuilder.AddContinueAsNewEvent(completedID, completedID, domainEntry, parentDomainName, attributes, eventStoreVersion)
					if err != nil {
						return nil, err
					}

					isComplete = true
					continueAsNewBuilder = newStateBuilder

				case workflow.DecisionTypeStartChildWorkflowExecution:
					handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
						metrics.DecisionTypeChildWorkflowCounter)
					targetDomainID := domainID
					attributes := d.StartChildWorkflowExecutionDecisionAttributes
					if err := validateStartChildExecutionAttributes(executionInfo, attributes, maxIDLengthLimit); err != nil {
						return nil, err
					}
					failWorkflow, err := sizeChecker.failWorkflowIfBlobSizeExceedsLimit(attributes.Input, "StartChildWorkflowExecutionDecisionAttributes.Input exceeds size limit.")
					if err != nil {
						return nil, err
					}
					if failWorkflow {
						isComplete = true
						break Process_Decision_Loop
					}

					// First check if we need to use a different target domain to schedule child execution
					if attributes.Domain != nil {
						// TODO: Error handling for DecisionType_StartChildWorkflowExecution failed when domain lookup fails
						domainEntry, err := handler.shard.GetDomainCache().GetDomain(*attributes.Domain)
						if err != nil {
							return nil, &workflow.InternalServiceError{Message: "Unable to schedule child execution across domain."}
						}
						targetDomainID = domainEntry.GetInfo().ID
					}

					requestID := uuid.New()
					initiatedEvent, _ := msBuilder.AddStartChildWorkflowExecutionInitiatedEvent(completedID, requestID, attributes)
					transferTasks = append(transferTasks, &persistence.StartChildExecutionTask{
						TargetDomainID:   targetDomainID,
						TargetWorkflowID: *attributes.WorkflowId,
						InitiatedID:      *initiatedEvent.EventId,
					})

				default:
					return nil, &workflow.BadRequestError{Message: fmt.Sprintf("Unknown decision type: %v", *d.DecisionType)}
				}
			}
		}

		if failDecision {
			handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope, metrics.FailedDecisionsCounter)
			handler.logger.Info("Failing the decision.", tag.WorkflowDecisionFailCause(int64(failCause)),
				tag.WorkflowID(token.WorkflowID),
				tag.WorkflowRunID(token.RunID),
				tag.WorkflowDomainID(domainID))
			var err1 error
			msBuilder, err1 = handler.historyEngine.failDecision(context, scheduleID, startedID, failCause, []byte(failMessage), request)
			if err1 != nil {
				return nil, err1
			}
			tBuilder = handler.historyEngine.getTimerBuilder(context.getExecution())
			isComplete = false
			hasUnhandledEvents = true
			continueAsNewBuilder = nil
		}

		if tt := tBuilder.GetUserTimerTaskIfNeeded(msBuilder); tt != nil {
			timerTasks = append(timerTasks, tt)
		}
		if hasDecisionScheduleActivityTask {
			if tt := tBuilder.GetActivityTimerTaskIfNeeded(msBuilder); tt != nil {
				timerTasks = append(timerTasks, tt)
			}
		}

		// Schedule another decision task if new events came in during this decision or if request forced to
		createNewDecisionTask := !isComplete && (hasUnhandledEvents ||
			request.GetForceCreateNewDecisionTask() || activityNotStartedCancelled)
		var newDecisionTaskScheduledID int64
		if createNewDecisionTask {
			di := msBuilder.AddDecisionTaskScheduledEvent()
			if di == nil {
				return nil, &workflow.InternalServiceError{Message: "Failed to add decision scheduled event."}
			}
			newDecisionTaskScheduledID = di.ScheduleID
			// skip transfer task for decision if request asking to return new decision task
			if !request.GetReturnNewDecisionTask() {
				transferTasks = append(transferTasks, &persistence.DecisionTask{
					DomainID:   domainID,
					TaskList:   di.TaskList,
					ScheduleID: di.ScheduleID,
				})
				if msBuilder.IsStickyTaskListEnabled() {
					tBuilder := handler.historyEngine.getTimerBuilder(context.getExecution())
					stickyTaskTimeoutTimer := tBuilder.AddScheduleToStartDecisionTimoutTask(di.ScheduleID, di.Attempt,
						executionInfo.StickyScheduleToStartTimeout)
					timerTasks = append(timerTasks, stickyTaskTimeoutTimer)
				}
			} else {
				// start the new decision task if request asked to do so
				// TODO: replace the poll request
				msBuilder.AddDecisionTaskStartedEvent(di.ScheduleID, "request-from-RespondDecisionTaskCompleted", &workflow.PollForDecisionTaskRequest{
					TaskList: &workflow.TaskList{Name: common.StringPtr(di.TaskList)},
					Identity: request.Identity,
				})
				timeOutTask := tBuilder.AddStartToCloseDecisionTimoutTask(di.ScheduleID, di.Attempt, di.DecisionTimeout)
				timerTasks = append(timerTasks, timeOutTask)
			}
		}

		if isComplete {
			tranT, timerT, err := handler.historyEngine.getWorkflowHistoryCleanupTasks(
				domainID,
				workflowExecution.GetWorkflowId(),
				tBuilder)
			if err != nil {
				return nil, err
			}
			transferTasks = append(transferTasks, tranT)
			timerTasks = append(timerTasks, timerT)
		}

		// Generate a transaction ID for appending events to history
		transactionID, err3 := handler.shard.GetNextTransferTaskID()
		if err3 != nil {
			return nil, err3
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict then reload
		// the history and try the operation again.
		var updateErr error
		if continueAsNewBuilder != nil {
			continueAsNewTimerTasks = msBuilder.GetContinueAsNew().TimerTasks
			updateErr = context.continueAsNewWorkflowExecution(request.ExecutionContext, continueAsNewBuilder,
				transferTasks, timerTasks, transactionID)
		} else {
			updateErr = context.updateWorkflowExecutionWithContext(request.ExecutionContext, transferTasks, timerTasks,
				transactionID)
		}

		if updateErr != nil {
			if updateErr == ErrConflict {
				handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope, metrics.ConcurrencyUpdateFailureCounter)
				continue Update_History_Loop
			}

			// if updateErr resulted in TransactionSizeLimitError then fail workflow
			switch updateErr.(type) {
			case *persistence.TransactionSizeLimitError:
				// must reload mutable state because the first call to updateWorkflowExecutionWithContext or continueAsNewWorkflowExecution
				// clears mutable state if error is returned
				msBuilder, err = context.loadWorkflowExecution()
				if err != nil {
					return nil, err
				}

				msBuilder.AddWorkflowExecutionTerminatedEvent(&workflow.TerminateWorkflowExecutionRequest{
					Reason:   common.StringPtr(common.FailureReasonTransactionSizeExceedsLimit),
					Identity: common.StringPtr("cadence-history-server"),
					Details:  []byte(updateErr.Error()),
				})
				tranT, timerT, err := handler.historyEngine.getWorkflowHistoryCleanupTasks(domainID, workflowExecution.GetWorkflowId(), tBuilder)
				if err != nil {
					return nil, err
				}
				transferTasks = []persistence.Task{tranT}
				timerTasks = []persistence.Task{timerT}
				transactionID, err3 = handler.shard.GetNextTransferTaskID()
				if err3 != nil {
					return nil, err3
				}
				if err := context.updateWorkflowExecutionWithContext(request.ExecutionContext, transferTasks, timerTasks, transactionID); err != nil {
					return nil, err
				}
				handler.timerProcessor.NotifyNewTimers(handler.currentClusterName, handler.shard.GetCurrentTime(handler.currentClusterName), timerTasks)
			}
			return nil, updateErr
		}

		// add continueAsNewTimerTask
		timerTasks = append(timerTasks, continueAsNewTimerTasks...)
		// Inform timer about the new ones.
		handler.timerProcessor.NotifyNewTimers(handler.currentClusterName, handler.shard.GetCurrentTime(handler.currentClusterName), timerTasks)

		resp = &h.RespondDecisionTaskCompletedResponse{}
		if request.GetReturnNewDecisionTask() && createNewDecisionTask {
			di, _ := msBuilder.GetPendingDecision(newDecisionTaskScheduledID)
			resp.StartedResponse = handler.createRecordDecisionTaskStartedResponse(domainID, msBuilder, di, request.GetIdentity())
			// sticky is always enabled when worker request for new decision task from RespondDecisionTaskCompleted
			resp.StartedResponse.StickyExecutionEnabled = common.BoolPtr(true)
		}

		return resp, nil
	}

	return nil, ErrMaxAttemptsExceeded
}

func (handler *decisionHandlerImpl) createRecordDecisionTaskStartedResponse(
	domainID string,
	msBuilder mutableState,
	di *decisionInfo,
	identity string,
) *h.RecordDecisionTaskStartedResponse {

	response := &h.RecordDecisionTaskStartedResponse{}
	response.WorkflowType = msBuilder.GetWorkflowType()
	executionInfo := msBuilder.GetExecutionInfo()
	if executionInfo.LastProcessedEvent != common.EmptyEventID {
		response.PreviousStartedEventId = common.Int64Ptr(executionInfo.LastProcessedEvent)
	}

	// Starting decision could result in different scheduleID if decision was transient and new new events came in
	// before it was started.
	response.ScheduledEventId = common.Int64Ptr(di.ScheduleID)
	response.StartedEventId = common.Int64Ptr(di.StartedID)
	response.StickyExecutionEnabled = common.BoolPtr(msBuilder.IsStickyTaskListEnabled())
	response.NextEventId = common.Int64Ptr(msBuilder.GetNextEventID())
	response.Attempt = common.Int64Ptr(di.Attempt)
	response.WorkflowExecutionTaskList = common.TaskListPtr(workflow.TaskList{
		Name: &executionInfo.TaskList,
		Kind: common.TaskListKindPtr(workflow.TaskListKindNormal),
	})
	response.ScheduledTimestamp = common.Int64Ptr(di.ScheduledTimestamp)
	response.StartedTimestamp = common.Int64Ptr(di.StartedTimestamp)

	if di.Attempt > 0 {
		// This decision is retried from mutable state
		// Also return schedule and started which are not written to history yet
		scheduledEvent, startedEvent := msBuilder.CreateTransientDecisionEvents(di, identity)
		response.DecisionInfo = &workflow.TransientDecisionInfo{}
		response.DecisionInfo.ScheduledEvent = scheduledEvent
		response.DecisionInfo.StartedEvent = startedEvent
	}
	response.EventStoreVersion = common.Int32Ptr(msBuilder.GetEventStoreVersion())
	response.BranchToken = msBuilder.GetCurrentBranch()

	return response
}
