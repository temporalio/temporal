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
	"fmt"

	"github.com/pborman/uuid"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cron"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

type (
	workflowStartEventProvider func() (*workflow.HistoryEvent, error)

	timerBuilderProvider func() *timerBuilder

	decisionTaskHandlerImpl struct {
		identity                string
		decisionTaskCompletedID int64
		eventStoreVersion       int32
		domainEntry             *cache.DomainCacheEntry

		// internal state
		hasUnhandledEventsBeforeDecisions bool
		timerBuilder                      *timerBuilder
		transferTasks                     []persistence.Task
		timerTasks                        []persistence.Task
		failDecision                      bool
		failDecisionCause                 *workflow.DecisionTaskFailedCause
		activityNotStartedCancelled       bool
		// TODO since timer builder provide `GetActivityTimerTaskIfNeeded`,
		//  should probably delete hasDecisionScheduleActivityTask
		hasDecisionScheduleActivityTask bool
		continueAsNewBuilder            mutableState
		stopProcessing                  bool // should stop processing any more decisions
		mutableState                    mutableState

		// validation
		attrValidator    *decisionAttrValidator
		sizeLimitChecker *decisionBlobSizeChecker

		logger                     log.Logger
		timerBuilderProvider       timerBuilderProvider
		workflowStartEventProvider workflowStartEventProvider
		domainCache                cache.DomainCache
		metricsClient              metrics.Client
	}
)

func newDecisionTaskHandler(
	identity string,
	decisionTaskCompletedID int64,
	eventStoreVersion int32,
	domainEntry *cache.DomainCacheEntry,
	mutableState mutableState,
	attrValidator *decisionAttrValidator,
	sizeLimitChecker *decisionBlobSizeChecker,
	logger log.Logger,
	timerBuilderProvider timerBuilderProvider,
	workflowStartEventProvider workflowStartEventProvider,
	domainCache cache.DomainCache,
	metricsClient metrics.Client,
) *decisionTaskHandlerImpl {

	return &decisionTaskHandlerImpl{
		identity:                identity,
		decisionTaskCompletedID: decisionTaskCompletedID,
		eventStoreVersion:       eventStoreVersion,
		domainEntry:             domainEntry,

		// internal state
		hasUnhandledEventsBeforeDecisions: mutableState.HasBufferedEvents(),
		transferTasks:                     nil,
		timerTasks:                        nil,
		failDecision:                      false,
		failDecisionCause:                 nil,
		activityNotStartedCancelled:       false,
		hasDecisionScheduleActivityTask:   false,
		continueAsNewBuilder:              nil,
		stopProcessing:                    false,
		mutableState:                      mutableState,

		// validation
		attrValidator:    attrValidator,
		sizeLimitChecker: sizeLimitChecker,

		logger:                     logger,
		timerBuilder:               timerBuilderProvider(),
		timerBuilderProvider:       timerBuilderProvider,
		workflowStartEventProvider: workflowStartEventProvider,
		domainCache:                domainCache,
		metricsClient:              metricsClient,
	}
}

func (handler *decisionTaskHandlerImpl) handleDecisions(decisions []*workflow.Decision) error {
	var err error

	for _, decision := range decisions {

		err = handler.handleDecision(decision)
		if err != nil || handler.stopProcessing {
			return err
		}
	}

	return nil
}

func (handler *decisionTaskHandlerImpl) handleDecision(decision *workflow.Decision) error {
	switch decision.GetDecisionType() {
	case workflow.DecisionTypeScheduleActivityTask:
		return handler.handleDecisionScheduleActivity(decision.ScheduleActivityTaskDecisionAttributes)

	case workflow.DecisionTypeCompleteWorkflowExecution:
		return handler.handleDecisionCompleteWorkflow(decision.CompleteWorkflowExecutionDecisionAttributes)

	case workflow.DecisionTypeFailWorkflowExecution:
		return handler.handleDecisionFailWorkflow(decision.FailWorkflowExecutionDecisionAttributes)

	case workflow.DecisionTypeCancelWorkflowExecution:
		return handler.handleDecisionCancelWorkflow(decision.CancelWorkflowExecutionDecisionAttributes)

	case workflow.DecisionTypeStartTimer:
		return handler.handleDecisionStartTimer(decision.StartTimerDecisionAttributes)

	case workflow.DecisionTypeRequestCancelActivityTask:
		return handler.handleDecisionRequestCancelActivity(decision.RequestCancelActivityTaskDecisionAttributes)

	case workflow.DecisionTypeCancelTimer:
		return handler.handleDecisionCancelTimer(decision.CancelTimerDecisionAttributes)

	case workflow.DecisionTypeRecordMarker:
		return handler.handleDecisionRecordMarker(decision.RecordMarkerDecisionAttributes)

	case workflow.DecisionTypeRequestCancelExternalWorkflowExecution:
		return handler.handleDecisionRequestCancelExternalWorkflow(decision.RequestCancelExternalWorkflowExecutionDecisionAttributes)

	case workflow.DecisionTypeSignalExternalWorkflowExecution:
		return handler.handleDecisionSignalExternalWorkflow(decision.SignalExternalWorkflowExecutionDecisionAttributes)

	case workflow.DecisionTypeContinueAsNewWorkflowExecution:
		return handler.handleDecisionContinueAsNewWorkflow(decision.ContinueAsNewWorkflowExecutionDecisionAttributes)

	case workflow.DecisionTypeStartChildWorkflowExecution:
		return handler.handleDecisionStartChildWorkflow(decision.StartChildWorkflowExecutionDecisionAttributes)

	default:
		return &workflow.BadRequestError{Message: fmt.Sprintf("Unknown decision type: %v", decision.GetDecisionType())}
	}
}

func (handler *decisionTaskHandlerImpl) handleDecisionScheduleActivity(
	attr *workflow.ScheduleActivityTaskDecisionAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondDecisionTaskCompletedScope,
		metrics.DecisionTypeScheduleActivityCounter,
	)

	executionInfo := handler.mutableState.GetExecutionInfo()
	domainID := executionInfo.DomainID

	targetDomainID := domainID
	// First check if we need to use a different target domain to schedule activity
	if attr.Domain != nil {
		// TODO: Error handling for ActivitySchedule failed when domain lookup fails
		targetDomainEntry, err := handler.domainCache.GetDomain(attr.GetDomain())
		if err != nil {
			return &workflow.InternalServiceError{Message: "Unable to schedule activity across domain."}
		}
		targetDomainID = targetDomainEntry.GetInfo().ID
	}

	if err := handler.attrValidator.validateActivityScheduleAttributes(
		attr, executionInfo.WorkflowTimeout,
	); err != nil {
		return err
	}
	failWorkflow, err := handler.sizeLimitChecker.failWorkflowIfBlobSizeExceedsLimit(
		attr.Input,
		"ScheduleActivityTaskDecisionAttributes.Input exceeds size limit.",
	)
	if err != nil {
		return err
	}
	if failWorkflow {
		handler.stopProcessing = true
		return nil
	}

	scheduleEvent, _ := handler.mutableState.AddActivityTaskScheduledEvent(handler.decisionTaskCompletedID, attr)
	handler.transferTasks = append(handler.transferTasks, &persistence.ActivityTask{
		DomainID:   targetDomainID,
		TaskList:   attr.TaskList.GetName(),
		ScheduleID: scheduleEvent.GetEventId(),
	})
	handler.hasDecisionScheduleActivityTask = true
	return nil
}

func (handler *decisionTaskHandlerImpl) handleDecisionRequestCancelActivity(
	attr *workflow.RequestCancelActivityTaskDecisionAttributes,
) error {
	handler.metricsClient.IncCounter(
		metrics.HistoryRespondDecisionTaskCompletedScope,
		metrics.DecisionTypeCancelActivityCounter,
	)
	if err := handler.attrValidator.validateActivityCancelAttributes(attr); err != nil {
		return err
	}
	activityID := attr.GetActivityId()
	actCancelReqEvent, ai, isRunning := handler.mutableState.AddActivityTaskCancelRequestedEvent(
		handler.decisionTaskCompletedID,
		activityID,
		handler.identity,
	)
	if !isRunning {
		handler.mutableState.AddRequestCancelActivityTaskFailedEvent(
			handler.decisionTaskCompletedID,
			activityID,
			activityCancellationMsgActivityIDUnknown,
		)
		return nil
	}

	if ai.StartedID == common.EmptyEventID {
		// We haven't started the activity yet, we can cancel the activity right away and
		// schedule a decision task to ensure the workflow makes progress.
		handler.mutableState.AddActivityTaskCanceledEvent(
			ai.ScheduleID,
			ai.StartedID,
			actCancelReqEvent.GetEventId(),
			[]byte(activityCancellationMsgActivityNotStarted),
			handler.identity,
		)
		handler.activityNotStartedCancelled = true
	}

	return nil
}

func (handler *decisionTaskHandlerImpl) handleDecisionStartTimer(
	attr *workflow.StartTimerDecisionAttributes,
) error {
	handler.metricsClient.IncCounter(
		metrics.HistoryRespondDecisionTaskCompletedScope,
		metrics.DecisionTypeStartTimerCounter,
	)

	if err := handler.attrValidator.validateTimerScheduleAttributes(attr); err != nil {
		return err
	}
	_, ti := handler.mutableState.AddTimerStartedEvent(handler.decisionTaskCompletedID, attr)
	if ti == nil {
		handler.failDecision = true
		handler.failDecisionCause = workflow.DecisionTaskFailedCauseStartTimerDuplicateID.Ptr()
		handler.stopProcessing = true
		return nil
	}
	handler.timerBuilder.AddUserTimer(ti, handler.mutableState)
	return nil
}

func (handler *decisionTaskHandlerImpl) handleDecisionCompleteWorkflow(
	attr *workflow.CompleteWorkflowExecutionDecisionAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondDecisionTaskCompletedScope,
		metrics.DecisionTypeCompleteWorkflowCounter,
	)

	if handler.hasUnhandledEventsBeforeDecisions {
		handler.failDecision = true
		handler.failDecisionCause = workflow.DecisionTaskFailedCauseUnhandledDecision.Ptr()
		handler.stopProcessing = true
		return nil
	}

	// If the decision has more than one completion event than just pick the first one
	if !handler.mutableState.IsWorkflowExecutionRunning() {
		handler.metricsClient.IncCounter(
			metrics.HistoryRespondDecisionTaskCompletedScope,
			metrics.MultipleCompletionDecisionsCounter,
		)
		handler.logger.Warn(
			"Multiple completion decisions",
			tag.WorkflowDecisionType(int64(workflow.DecisionTypeCompleteWorkflowExecution)),
			tag.ErrorTypeMultipleCompletionDecisions,
		)
		return nil
	}

	if err := handler.attrValidator.validateCompleteWorkflowExecutionAttributes(attr); err != nil {
		return err
	}
	failWorkflow, err := handler.sizeLimitChecker.failWorkflowIfBlobSizeExceedsLimit(
		attr.Result,
		"CompleteWorkflowExecutionDecisionAttributes.Result exceeds size limit.",
	)
	if err != nil {
		return err
	}
	if failWorkflow {
		handler.stopProcessing = true
		return nil
	}

	// check if this is a cron workflow
	cronBackoff := handler.mutableState.GetCronBackoffDuration()
	if cronBackoff == cron.NoBackoff {
		// not cron, so complete this workflow execution
		if event := handler.mutableState.AddCompletedWorkflowEvent(handler.decisionTaskCompletedID, attr); event == nil {
			return &workflow.InternalServiceError{Message: "Unable to add complete workflow event."}
		}
	} else {
		// this is a cron workflow
		executionInfo := handler.mutableState.GetExecutionInfo()
		startEvent, err := handler.workflowStartEventProvider()
		if err != nil {
			return err
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
			LastCompletionResult:                attr.Result,
			CronSchedule:                        common.StringPtr(executionInfo.CronSchedule),
		}

		_, newStateBuilder, err := handler.mutableState.AddContinueAsNewEvent(
			handler.decisionTaskCompletedID,
			handler.decisionTaskCompletedID,
			handler.domainEntry,
			startAttributes.GetParentWorkflowDomain(),
			continueAsNewAttributes,
			handler.eventStoreVersion,
		)
		if err != nil {
			return err
		}

		handler.continueAsNewBuilder = newStateBuilder
	}

	return nil
}

func (handler *decisionTaskHandlerImpl) handleDecisionFailWorkflow(
	attr *workflow.FailWorkflowExecutionDecisionAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondDecisionTaskCompletedScope,
		metrics.DecisionTypeFailWorkflowCounter,
	)

	if handler.hasUnhandledEventsBeforeDecisions {
		handler.failDecision = true
		handler.failDecisionCause = workflow.DecisionTaskFailedCauseUnhandledDecision.Ptr()
		handler.stopProcessing = true
		return nil
	}

	// If the decision has more than one completion event than just pick the first one
	if !handler.mutableState.IsWorkflowExecutionRunning() {
		handler.metricsClient.IncCounter(
			metrics.HistoryRespondDecisionTaskCompletedScope,
			metrics.MultipleCompletionDecisionsCounter,
		)
		handler.logger.Warn(
			"Multiple completion decisions",
			tag.WorkflowDecisionType(int64(workflow.DecisionTypeFailWorkflowExecution)),
			tag.ErrorTypeMultipleCompletionDecisions,
		)
		return nil
	}

	if err := handler.attrValidator.validateFailWorkflowExecutionAttributes(attr); err != nil {
		return err
	}
	failWorkflow, err := handler.sizeLimitChecker.failWorkflowIfBlobSizeExceedsLimit(
		attr.Details,
		"FailWorkflowExecutionDecisionAttributes.Details exceeds size limit.",
	)
	if err != nil {
		return err
	}
	if failWorkflow {
		handler.stopProcessing = true
		return nil
	}

	backoffInterval := handler.mutableState.GetRetryBackoffDuration(attr.GetReason())
	continueAsNewInitiator := workflow.ContinueAsNewInitiatorRetryPolicy
	if backoffInterval == common.NoRetryBackoff {
		backoffInterval = handler.mutableState.GetCronBackoffDuration()
		continueAsNewInitiator = workflow.ContinueAsNewInitiatorCronSchedule
	}
	// TODO this line `backoffInterval == cron.NoBackoff` and the
	//  one above `backoffInterval == common.NoRetryBackoff` are essentially the same?
	if backoffInterval == cron.NoBackoff {
		// no retry or cron
		if event := handler.mutableState.AddFailWorkflowEvent(handler.decisionTaskCompletedID, attr); event == nil {
			return &workflow.InternalServiceError{Message: "Unable to add fail workflow event."}
		}
	} else {
		startEvent, err := handler.workflowStartEventProvider()
		if err != nil {
			return err
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
			FailureReason:                       attr.Reason,
			FailureDetails:                      attr.Details,
			LastCompletionResult:                startAttributes.LastCompletionResult,
			CronSchedule:                        common.StringPtr(handler.mutableState.GetExecutionInfo().CronSchedule),
		}

		_, newStateBuilder, err := handler.mutableState.AddContinueAsNewEvent(
			handler.decisionTaskCompletedID,
			handler.decisionTaskCompletedID,
			handler.domainEntry,
			startAttributes.GetParentWorkflowDomain(),
			continueAsNewAttributes,
			handler.eventStoreVersion)
		if err != nil {
			return err
		}

		handler.continueAsNewBuilder = newStateBuilder
	}

	return nil
}

func (handler *decisionTaskHandlerImpl) handleDecisionCancelTimer(
	attr *workflow.CancelTimerDecisionAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondDecisionTaskCompletedScope,
		metrics.DecisionTypeCancelTimerCounter,
	)

	if err := handler.attrValidator.validateTimerCancelAttributes(attr); err != nil {
		return err
	}
	if handler.mutableState.AddTimerCanceledEvent(
		handler.decisionTaskCompletedID,
		attr,
		handler.identity) == nil {
		handler.mutableState.AddCancelTimerFailedEvent(
			handler.decisionTaskCompletedID,
			attr,
			handler.identity,
		)
	} else {
		// timer deletion is success. we need to rebuild the timer builder
		// since timer builder has a local cached version of timers
		handler.timerBuilder = handler.timerBuilderProvider()
		handler.timerBuilder.loadUserTimers(handler.mutableState)

		// timer deletion is a success, we may have deleted a fired timer in
		// which case we should reset hasBufferedEvents
		// TODO deletion of timer fired event refreshing hasUnhandledEventsBeforeDecisions
		//  is not entirely correct, since during these decisions processing, new event may appear
		handler.hasUnhandledEventsBeforeDecisions = handler.mutableState.HasBufferedEvents()
	}
	return nil
}

func (handler *decisionTaskHandlerImpl) handleDecisionCancelWorkflow(
	attr *workflow.CancelWorkflowExecutionDecisionAttributes,
) error {

	handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
		metrics.DecisionTypeCancelWorkflowCounter)
	// If new events came while we are processing the decision, we would fail this and give a chance to client
	// to process the new event.
	if handler.hasUnhandledEventsBeforeDecisions {
		handler.failDecision = true
		handler.failDecisionCause = workflow.DecisionTaskFailedCauseUnhandledDecision.Ptr()
		handler.stopProcessing = true
		return nil
	}

	// If the decision has more than one completion event than just pick the first one
	if !handler.mutableState.IsWorkflowExecutionRunning() {
		handler.metricsClient.IncCounter(
			metrics.HistoryRespondDecisionTaskCompletedScope,
			metrics.MultipleCompletionDecisionsCounter,
		)
		handler.logger.Warn(
			"Multiple completion decisions",
			tag.WorkflowDecisionType(int64(workflow.DecisionTypeCancelWorkflowExecution)),
			tag.ErrorTypeMultipleCompletionDecisions,
		)
		return nil
	}

	if err := handler.attrValidator.validateCancelWorkflowExecutionAttributes(attr); err != nil {
		return err
	}
	handler.mutableState.AddWorkflowExecutionCanceledEvent(handler.decisionTaskCompletedID, attr)
	return nil
}

func (handler *decisionTaskHandlerImpl) handleDecisionRequestCancelExternalWorkflow(
	attr *workflow.RequestCancelExternalWorkflowExecutionDecisionAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondDecisionTaskCompletedScope,
		metrics.DecisionTypeCancelExternalWorkflowCounter,
	)

	executionInfo := handler.mutableState.GetExecutionInfo()
	if err := handler.attrValidator.validateCancelExternalWorkflowExecutionAttributes(attr); err != nil {
		return err
	}

	foreignDomainID := ""
	if attr.GetDomain() == "" {
		foreignDomainID = executionInfo.DomainID
	} else {
		foreignDomainEntry, err := handler.domainCache.GetDomain(attr.GetDomain())
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Unable to cancel workflow across domain: %v.", attr.GetDomain()),
			}
		}
		foreignDomainID = foreignDomainEntry.GetInfo().ID
	}

	cancelRequestID := uuid.New()
	wfCancelReqEvent, _ := handler.mutableState.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(
		handler.decisionTaskCompletedID, cancelRequestID, attr,
	)
	if wfCancelReqEvent == nil {
		return &workflow.InternalServiceError{Message: "Unable to add external cancel workflow request."}
	}

	handler.transferTasks = append(handler.transferTasks, &persistence.CancelExecutionTask{
		TargetDomainID:          foreignDomainID,
		TargetWorkflowID:        attr.GetWorkflowId(),
		TargetRunID:             attr.GetRunId(),
		TargetChildWorkflowOnly: attr.GetChildWorkflowOnly(),
		InitiatedID:             wfCancelReqEvent.GetEventId(),
	})
	return nil
}

func (handler *decisionTaskHandlerImpl) handleDecisionRecordMarker(
	attr *workflow.RecordMarkerDecisionAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondDecisionTaskCompletedScope,
		metrics.DecisionTypeRecordMarkerCounter,
	)
	if err := handler.attrValidator.validateRecordMarkerAttributes(attr); err != nil {
		return err
	}
	failWorkflow, err := handler.sizeLimitChecker.failWorkflowIfBlobSizeExceedsLimit(
		attr.Details,
		"RecordMarkerDecisionAttributes.Details exceeds size limit.",
	)
	if err != nil {
		return err
	}
	if failWorkflow {
		handler.stopProcessing = true
		return nil
	}

	handler.mutableState.AddRecordMarkerEvent(handler.decisionTaskCompletedID, attr)
	return nil
}

func (handler *decisionTaskHandlerImpl) handleDecisionContinueAsNewWorkflow(
	attr *workflow.ContinueAsNewWorkflowExecutionDecisionAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondDecisionTaskCompletedScope,
		metrics.DecisionTypeContinueAsNewCounter,
	)
	if handler.hasUnhandledEventsBeforeDecisions {
		handler.failDecision = true
		handler.failDecisionCause = workflow.DecisionTaskFailedCauseUnhandledDecision.Ptr()
		handler.stopProcessing = true
		return nil
	}

	executionInfo := handler.mutableState.GetExecutionInfo()
	// If the decision has more than one completion event than just pick the first one
	if !handler.mutableState.IsWorkflowExecutionRunning() {
		handler.metricsClient.IncCounter(
			metrics.HistoryRespondDecisionTaskCompletedScope,
			metrics.MultipleCompletionDecisionsCounter,
		)
		handler.logger.Warn(
			"Multiple completion decisions",
			tag.WorkflowDecisionType(int64(workflow.DecisionTypeContinueAsNewWorkflowExecution)),
			tag.ErrorTypeMultipleCompletionDecisions,
		)
		return nil
	}

	if err := handler.attrValidator.validateContinueAsNewWorkflowExecutionAttributes(executionInfo, attr); err != nil {
		return err
	}
	failWorkflow, err := handler.sizeLimitChecker.failWorkflowIfBlobSizeExceedsLimit(
		attr.Input,
		"ContinueAsNewWorkflowExecutionDecisionAttributes.Input exceeds size limit.",
	)
	if err != nil {
		return err
	}
	if failWorkflow {
		handler.stopProcessing = true
		return nil
	}

	// Extract parentDomainName so it can be passed down to next run of workflow execution
	var parentDomainName string
	if handler.mutableState.HasParentExecution() {
		parentDomainID := executionInfo.ParentDomainID
		parentDomainEntry, err := handler.domainCache.GetDomainByID(parentDomainID)
		if err != nil {
			return err
		}
		parentDomainName = parentDomainEntry.GetInfo().Name
	}

	_, newStateBuilder, err := handler.mutableState.AddContinueAsNewEvent(
		handler.decisionTaskCompletedID,
		handler.decisionTaskCompletedID,
		handler.domainEntry,
		parentDomainName,
		attr,
		handler.eventStoreVersion,
	)
	if err != nil {
		return err
	}

	handler.continueAsNewBuilder = newStateBuilder
	return nil
}

func (handler *decisionTaskHandlerImpl) handleDecisionStartChildWorkflow(
	attr *workflow.StartChildWorkflowExecutionDecisionAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondDecisionTaskCompletedScope,
		metrics.DecisionTypeChildWorkflowCounter,
	)

	executionInfo := handler.mutableState.GetExecutionInfo()
	if err := handler.attrValidator.validateStartChildExecutionAttributes(executionInfo, attr); err != nil {
		return err
	}
	failWorkflow, err := handler.sizeLimitChecker.failWorkflowIfBlobSizeExceedsLimit(
		attr.Input,
		"StartChildWorkflowExecutionDecisionAttributes.Input exceeds size limit.",
	)
	if err != nil {
		return err
	}
	if failWorkflow {
		handler.stopProcessing = true
		return nil
	}

	targetDomainID := executionInfo.DomainID
	// First check if we need to use a different target domain to schedule child execution
	if attr.Domain != nil {
		// TODO: Error handling for DecisionType_StartChildWorkflowExecution failed when domain lookup fails
		targetDomainEntry, err := handler.domainCache.GetDomain(attr.GetDomain())
		if err != nil {
			return &workflow.InternalServiceError{Message: "Unable to schedule child execution across domain."}
		}
		targetDomainID = targetDomainEntry.GetInfo().ID
	}

	requestID := uuid.New()
	initiatedEvent, _ := handler.mutableState.AddStartChildWorkflowExecutionInitiatedEvent(
		handler.decisionTaskCompletedID, requestID, attr,
	)
	handler.transferTasks = append(handler.transferTasks, &persistence.StartChildExecutionTask{
		TargetDomainID:   targetDomainID,
		TargetWorkflowID: attr.GetWorkflowId(),
		InitiatedID:      initiatedEvent.GetEventId(),
	})
	return nil
}

func (handler *decisionTaskHandlerImpl) handleDecisionSignalExternalWorkflow(
	attr *workflow.SignalExternalWorkflowExecutionDecisionAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondDecisionTaskCompletedScope,
		metrics.DecisionTypeSignalExternalWorkflowCounter,
	)

	executionInfo := handler.mutableState.GetExecutionInfo()
	if err := handler.attrValidator.validateSignalExternalWorkflowExecutionAttributes(attr); err != nil {
		return err
	}
	failWorkflow, err := handler.sizeLimitChecker.failWorkflowIfBlobSizeExceedsLimit(
		attr.Input,
		"SignalExternalWorkflowExecutionDecisionAttributes.Input exceeds size limit.",
	)
	if err != nil {
		return err
	}
	if failWorkflow {
		handler.stopProcessing = true
		return nil
	}

	foreignDomainID := ""
	if attr.GetDomain() == "" {
		foreignDomainID = executionInfo.DomainID
	} else {
		foreignDomainEntry, err := handler.domainCache.GetDomain(attr.GetDomain())
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Unable to signal workflow across domain: %v.", attr.GetDomain()),
			}
		}
		foreignDomainID = foreignDomainEntry.GetInfo().ID
	}

	signalRequestID := uuid.New() // for deduplicate
	wfSignalReqEvent, _ := handler.mutableState.AddSignalExternalWorkflowExecutionInitiatedEvent(
		handler.decisionTaskCompletedID, signalRequestID, attr,
	)
	if wfSignalReqEvent == nil {
		return &workflow.InternalServiceError{Message: "Unable to add external signal workflow request."}
	}

	handler.transferTasks = append(handler.transferTasks, &persistence.SignalExecutionTask{
		TargetDomainID:          foreignDomainID,
		TargetWorkflowID:        attr.Execution.GetWorkflowId(),
		TargetRunID:             attr.Execution.GetRunId(),
		TargetChildWorkflowOnly: attr.GetChildWorkflowOnly(),
		InitiatedID:             wfSignalReqEvent.GetEventId(),
	})
	return nil
}
