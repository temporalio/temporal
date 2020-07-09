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
	"fmt"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	decisionpb "go.temporal.io/api/decision/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/enums"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payloads"
)

type (
	decisionAttrValidationFn func() error

	decisionTaskHandlerImpl struct {
		identity                string
		decisionTaskCompletedID int64
		namespaceEntry          *cache.NamespaceCacheEntry

		// internal state
		hasUnhandledEventsBeforeDecisions bool
		failDecisionInfo                  *failDecisionInfo
		activityNotStartedCancelled       bool
		continueAsNewBuilder              mutableState
		stopProcessing                    bool // should stop processing any more decisions
		mutableState                      mutableState

		// validation
		attrValidator    *decisionAttrValidator
		sizeLimitChecker *workflowSizeChecker

		logger         log.Logger
		namespaceCache cache.NamespaceCache
		metricsClient  metrics.Client
		config         *Config
	}

	failDecisionInfo struct {
		cause   enumspb.DecisionTaskFailedCause
		message string
	}
)

func newDecisionTaskHandler(
	identity string,
	decisionTaskCompletedID int64,
	namespaceEntry *cache.NamespaceCacheEntry,
	mutableState mutableState,
	attrValidator *decisionAttrValidator,
	sizeLimitChecker *workflowSizeChecker,
	logger log.Logger,
	namespaceCache cache.NamespaceCache,
	metricsClient metrics.Client,
	config *Config,
) *decisionTaskHandlerImpl {

	return &decisionTaskHandlerImpl{
		identity:                identity,
		decisionTaskCompletedID: decisionTaskCompletedID,
		namespaceEntry:          namespaceEntry,

		// internal state
		hasUnhandledEventsBeforeDecisions: mutableState.HasBufferedEvents(),
		failDecisionInfo:                  nil,
		activityNotStartedCancelled:       false,
		continueAsNewBuilder:              nil,
		stopProcessing:                    false,
		mutableState:                      mutableState,

		// validation
		attrValidator:    attrValidator,
		sizeLimitChecker: sizeLimitChecker,

		logger:         logger,
		namespaceCache: namespaceCache,
		metricsClient:  metricsClient,
		config:         config,
	}
}

func (handler *decisionTaskHandlerImpl) handleDecisions(
	decisions []*decisionpb.Decision,
) error {

	// overall workflow size / count check
	failWorkflow, err := handler.sizeLimitChecker.failWorkflowSizeExceedsLimit()
	if err != nil || failWorkflow {
		return err
	}

	for _, decision := range decisions {

		err = handler.handleDecision(decision)
		if err != nil || handler.stopProcessing {
			return err
		}
	}

	return nil
}

func (handler *decisionTaskHandlerImpl) handleDecision(decision *decisionpb.Decision) error {
	switch decision.GetDecisionType() {
	case enumspb.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK:
		return handler.handleDecisionScheduleActivity(decision.GetScheduleActivityTaskDecisionAttributes())

	case enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION:
		return handler.handleDecisionCompleteWorkflow(decision.GetCompleteWorkflowExecutionDecisionAttributes())

	case enumspb.DECISION_TYPE_FAIL_WORKFLOW_EXECUTION:
		return handler.handleDecisionFailWorkflow(decision.GetFailWorkflowExecutionDecisionAttributes())

	case enumspb.DECISION_TYPE_CANCEL_WORKFLOW_EXECUTION:
		return handler.handleDecisionCancelWorkflow(decision.GetCancelWorkflowExecutionDecisionAttributes())

	case enumspb.DECISION_TYPE_START_TIMER:
		return handler.handleDecisionStartTimer(decision.GetStartTimerDecisionAttributes())

	case enumspb.DECISION_TYPE_REQUEST_CANCEL_ACTIVITY_TASK:
		return handler.handleDecisionRequestCancelActivity(decision.GetRequestCancelActivityTaskDecisionAttributes())

	case enumspb.DECISION_TYPE_CANCEL_TIMER:
		return handler.handleDecisionCancelTimer(decision.GetCancelTimerDecisionAttributes())

	case enumspb.DECISION_TYPE_RECORD_MARKER:
		return handler.handleDecisionRecordMarker(decision.GetRecordMarkerDecisionAttributes())

	case enumspb.DECISION_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION:
		return handler.handleDecisionRequestCancelExternalWorkflow(decision.GetRequestCancelExternalWorkflowExecutionDecisionAttributes())

	case enumspb.DECISION_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION:
		return handler.handleDecisionSignalExternalWorkflow(decision.GetSignalExternalWorkflowExecutionDecisionAttributes())

	case enumspb.DECISION_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION:
		return handler.handleDecisionContinueAsNewWorkflow(decision.GetContinueAsNewWorkflowExecutionDecisionAttributes())

	case enumspb.DECISION_TYPE_START_CHILD_WORKFLOW_EXECUTION:
		return handler.handleDecisionStartChildWorkflow(decision.GetStartChildWorkflowExecutionDecisionAttributes())

	case enumspb.DECISION_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
		return handler.handleDecisionUpsertWorkflowSearchAttributes(decision.GetUpsertWorkflowSearchAttributesDecisionAttributes())

	default:
		return serviceerror.NewInvalidArgument(fmt.Sprintf("Unknown decision type: %v", decision.GetDecisionType()))
	}
}

func (handler *decisionTaskHandlerImpl) handleDecisionScheduleActivity(
	attr *decisionpb.ScheduleActivityTaskDecisionAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondDecisionTaskCompletedScope,
		metrics.DecisionTypeScheduleActivityCounter,
	)

	executionInfo := handler.mutableState.GetExecutionInfo()
	namespaceID := executionInfo.NamespaceID
	targetNamespaceID := namespaceID
	if attr.GetNamespace() != "" {
		targetNamespaceEntry, err := handler.namespaceCache.GetNamespace(attr.GetNamespace())
		if err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("Unable to schedule activity across namespace %v.", attr.GetNamespace()))
		}
		targetNamespaceID = targetNamespaceEntry.GetInfo().Id
	}

	if err := handler.validateDecisionAttr(
		func() error {
			return handler.attrValidator.validateActivityScheduleAttributes(
				namespaceID,
				targetNamespaceID,
				attr,
				executionInfo.WorkflowRunTimeout,
			)
		},
		enumspb.DECISION_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	failWorkflow, err := handler.sizeLimitChecker.failWorkflowIfPayloadSizeExceedsLimit(
		metrics.DecisionTypeTag(enumspb.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK.String()),
		attr.GetInput().Size(),
		"ScheduleActivityTaskDecisionAttributes.Input exceeds size limit.",
	)
	if err != nil || failWorkflow {
		handler.stopProcessing = true
		return err
	}

	_, _, err = handler.mutableState.AddActivityTaskScheduledEvent(handler.decisionTaskCompletedID, attr)
	switch err.(type) {
	case nil:
		return nil
	case *serviceerror.InvalidArgument:
		return handler.handlerFailDecision(
			enumspb.DECISION_TASK_FAILED_CAUSE_SCHEDULE_ACTIVITY_DUPLICATE_ID, "",
		)
	default:
		return err
	}
}

func (handler *decisionTaskHandlerImpl) handleDecisionRequestCancelActivity(
	attr *decisionpb.RequestCancelActivityTaskDecisionAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondDecisionTaskCompletedScope,
		metrics.DecisionTypeCancelActivityCounter,
	)

	if err := handler.validateDecisionAttr(
		func() error {
			return handler.attrValidator.validateActivityCancelAttributes(attr)
		},
		enumspb.DECISION_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_ACTIVITY_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	scheduleID := attr.GetScheduledEventId()
	actCancelReqEvent, ai, err := handler.mutableState.AddActivityTaskCancelRequestedEvent(
		handler.decisionTaskCompletedID,
		scheduleID,
		handler.identity,
	)
	switch err.(type) {
	case nil:
		if ai.StartedID == common.EmptyEventID {
			// We haven't started the activity yet, we can cancel the activity right away and
			// schedule a decision task to ensure the workflow makes progress.
			_, err = handler.mutableState.AddActivityTaskCanceledEvent(
				ai.ScheduleID,
				ai.StartedID,
				actCancelReqEvent.GetEventId(),
				payloads.EncodeString(activityCancellationMsgActivityNotStarted),
				handler.identity,
			)
			if err != nil {
				return err
			}
			handler.activityNotStartedCancelled = true
		}
		return nil
	case *serviceerror.InvalidArgument:
		return handler.handlerFailDecision(
			enumspb.DECISION_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_ACTIVITY_ATTRIBUTES, "",
		)

	default:
		return err
	}
}

func (handler *decisionTaskHandlerImpl) handleDecisionStartTimer(
	attr *decisionpb.StartTimerDecisionAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondDecisionTaskCompletedScope,
		metrics.DecisionTypeStartTimerCounter,
	)

	if err := handler.validateDecisionAttr(
		func() error {
			return handler.attrValidator.validateTimerScheduleAttributes(attr)
		},
		enumspb.DECISION_TASK_FAILED_CAUSE_BAD_START_TIMER_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	_, _, err := handler.mutableState.AddTimerStartedEvent(handler.decisionTaskCompletedID, attr)
	switch err.(type) {
	case nil:
		return nil
	case *serviceerror.InvalidArgument:
		return handler.handlerFailDecision(
			enumspb.DECISION_TASK_FAILED_CAUSE_START_TIMER_DUPLICATE_ID, "",
		)
	default:
		return err
	}
}

func (handler *decisionTaskHandlerImpl) handleDecisionCompleteWorkflow(
	attr *decisionpb.CompleteWorkflowExecutionDecisionAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondDecisionTaskCompletedScope,
		metrics.DecisionTypeCompleteWorkflowCounter,
	)

	if handler.hasUnhandledEventsBeforeDecisions {
		return handler.handlerFailDecision(enumspb.DECISION_TASK_FAILED_CAUSE_UNHANDLED_DECISION, "")
	}

	if err := handler.validateDecisionAttr(
		func() error {
			return handler.attrValidator.validateCompleteWorkflowExecutionAttributes(attr)
		},
		enumspb.DECISION_TASK_FAILED_CAUSE_BAD_COMPLETE_WORKFLOW_EXECUTION_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	failWorkflow, err := handler.sizeLimitChecker.failWorkflowIfPayloadSizeExceedsLimit(
		metrics.DecisionTypeTag(enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION.String()),
		attr.GetResult().Size(),
		"CompleteWorkflowExecutionDecisionAttributes.Result exceeds size limit.",
	)
	if err != nil || failWorkflow {
		handler.stopProcessing = true
		return err
	}

	// If the decision has more than one completion event than just pick the first one
	if !handler.mutableState.IsWorkflowExecutionRunning() {
		handler.metricsClient.IncCounter(
			metrics.HistoryRespondDecisionTaskCompletedScope,
			metrics.MultipleCompletionDecisionsCounter,
		)
		handler.logger.Warn(
			"Multiple completion decisions",
			tag.WorkflowDecisionType(enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION),
			tag.ErrorTypeMultipleCompletionDecisions,
		)
		return nil
	}

	// check if this is a cron workflow
	cronBackoff, err := handler.mutableState.GetCronBackoffDuration()
	if err != nil {
		handler.stopProcessing = true
		return err
	}
	if cronBackoff == backoff.NoBackoff {
		// not cron, so complete this workflow execution
		if _, err := handler.mutableState.AddCompletedWorkflowEvent(handler.decisionTaskCompletedID, attr); err != nil {
			return serviceerror.NewInternal("Unable to add complete workflow event.")
		}
		return nil
	}

	// this is a cron workflow
	startEvent, err := handler.mutableState.GetStartEvent()
	if err != nil {
		return err
	}
	startAttributes := startEvent.GetWorkflowExecutionStartedEventAttributes()
	return handler.retryCronContinueAsNew(
		startAttributes,
		int32(cronBackoff.Seconds()),
		enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE,
		nil,
		attr.Result,
	)
}

func (handler *decisionTaskHandlerImpl) handleDecisionFailWorkflow(
	attr *decisionpb.FailWorkflowExecutionDecisionAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondDecisionTaskCompletedScope,
		metrics.DecisionTypeFailWorkflowCounter,
	)

	if handler.hasUnhandledEventsBeforeDecisions {
		return handler.handlerFailDecision(enumspb.DECISION_TASK_FAILED_CAUSE_UNHANDLED_DECISION, "")
	}

	if err := handler.validateDecisionAttr(
		func() error {
			return handler.attrValidator.validateFailWorkflowExecutionAttributes(attr)
		},
		enumspb.DECISION_TASK_FAILED_CAUSE_BAD_FAIL_WORKFLOW_EXECUTION_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	failWorkflow, err := handler.sizeLimitChecker.failWorkflowIfPayloadSizeExceedsLimit(
		metrics.DecisionTypeTag(enumspb.DECISION_TYPE_FAIL_WORKFLOW_EXECUTION.String()),
		attr.GetFailure().Size(),
		"FailWorkflowExecutionDecisionAttributes.Failure exceeds size limit.",
	)
	if err != nil || failWorkflow {
		handler.stopProcessing = true
		return err
	}

	// If the decision has more than one completion event than just pick the first one
	if !handler.mutableState.IsWorkflowExecutionRunning() {
		handler.metricsClient.IncCounter(
			metrics.HistoryRespondDecisionTaskCompletedScope,
			metrics.MultipleCompletionDecisionsCounter,
		)
		handler.logger.Warn(
			"Multiple completion decisions",
			tag.WorkflowDecisionType(enumspb.DECISION_TYPE_FAIL_WORKFLOW_EXECUTION),
			tag.ErrorTypeMultipleCompletionDecisions,
		)
		return nil
	}

	// below will check whether to do continue as new based on backoff & backoff or cron
	backoffInterval, retryStatus := handler.mutableState.GetRetryBackoffDuration(attr.GetFailure())
	continueAsNewInitiator := enumspb.CONTINUE_AS_NEW_INITIATOR_RETRY
	// first check the backoff retry
	if backoffInterval == backoff.NoBackoff {
		// if no backoff retry, set the backoffInterval using cron schedule
		backoffInterval, err = handler.mutableState.GetCronBackoffDuration()
		if err != nil {
			handler.stopProcessing = true
			return err
		}
		continueAsNewInitiator = enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE
	}
	// second check the backoff / cron schedule
	if backoffInterval == backoff.NoBackoff {
		// no retry or cron
		if _, err := handler.mutableState.AddFailWorkflowEvent(handler.decisionTaskCompletedID, retryStatus, attr); err != nil {
			return err
		}
		return nil
	}

	// this is a cron / backoff workflow
	startEvent, err := handler.mutableState.GetStartEvent()
	if err != nil {
		return err
	}
	startAttributes := startEvent.GetWorkflowExecutionStartedEventAttributes()
	return handler.retryCronContinueAsNew(
		startAttributes,
		int32(backoffInterval.Seconds()),
		continueAsNewInitiator,
		attr.GetFailure(),
		startAttributes.LastCompletionResult,
	)
}

func (handler *decisionTaskHandlerImpl) handleDecisionCancelTimer(
	attr *decisionpb.CancelTimerDecisionAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondDecisionTaskCompletedScope,
		metrics.DecisionTypeCancelTimerCounter,
	)

	if err := handler.validateDecisionAttr(
		func() error {
			return handler.attrValidator.validateTimerCancelAttributes(attr)
		},
		enumspb.DECISION_TASK_FAILED_CAUSE_BAD_CANCEL_TIMER_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	_, err := handler.mutableState.AddTimerCanceledEvent(
		handler.decisionTaskCompletedID,
		attr,
		handler.identity)
	switch err.(type) {
	case nil:
		// timer deletion is a success, we may have deleted a fired timer in
		// which case we should reset hasBufferedEvents
		// TODO deletion of timer fired event refreshing hasUnhandledEventsBeforeDecisions
		//  is not entirely correct, since during these decisions processing, new event may appear
		handler.hasUnhandledEventsBeforeDecisions = handler.mutableState.HasBufferedEvents()
		return nil
	case *serviceerror.InvalidArgument:
		_, err = handler.mutableState.AddCancelTimerFailedEvent(
			handler.decisionTaskCompletedID,
			attr,
			handler.identity,
		)
		return err
	default:
		return err
	}
}

func (handler *decisionTaskHandlerImpl) handleDecisionCancelWorkflow(
	attr *decisionpb.CancelWorkflowExecutionDecisionAttributes,
) error {

	handler.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
		metrics.DecisionTypeCancelWorkflowCounter)

	if handler.hasUnhandledEventsBeforeDecisions {
		return handler.handlerFailDecision(enumspb.DECISION_TASK_FAILED_CAUSE_UNHANDLED_DECISION, "")
	}

	if err := handler.validateDecisionAttr(
		func() error {
			return handler.attrValidator.validateCancelWorkflowExecutionAttributes(attr)
		},
		enumspb.DECISION_TASK_FAILED_CAUSE_BAD_CANCEL_WORKFLOW_EXECUTION_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	// If the decision has more than one completion event than just pick the first one
	if !handler.mutableState.IsWorkflowExecutionRunning() {
		handler.metricsClient.IncCounter(
			metrics.HistoryRespondDecisionTaskCompletedScope,
			metrics.MultipleCompletionDecisionsCounter,
		)
		handler.logger.Warn(
			"Multiple completion decisions",
			tag.WorkflowDecisionType(enumspb.DECISION_TYPE_CANCEL_WORKFLOW_EXECUTION),
			tag.ErrorTypeMultipleCompletionDecisions,
		)
		return nil
	}

	_, err := handler.mutableState.AddWorkflowExecutionCanceledEvent(handler.decisionTaskCompletedID, attr)
	return err
}

func (handler *decisionTaskHandlerImpl) handleDecisionRequestCancelExternalWorkflow(
	attr *decisionpb.RequestCancelExternalWorkflowExecutionDecisionAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondDecisionTaskCompletedScope,
		metrics.DecisionTypeCancelExternalWorkflowCounter,
	)

	executionInfo := handler.mutableState.GetExecutionInfo()
	namespaceID := executionInfo.NamespaceID
	targetNamespaceID := namespaceID
	if attr.GetNamespace() != "" {
		targetNamespaceEntry, err := handler.namespaceCache.GetNamespace(attr.GetNamespace())
		if err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("Unable to cancel workflow across namespace: %v.", attr.GetNamespace()))
		}
		targetNamespaceID = targetNamespaceEntry.GetInfo().Id
	}

	if err := handler.validateDecisionAttr(
		func() error {
			return handler.attrValidator.validateCancelExternalWorkflowExecutionAttributes(
				namespaceID,
				targetNamespaceID,
				attr,
			)
		},
		enumspb.DECISION_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	cancelRequestID := uuid.New()
	_, _, err := handler.mutableState.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(
		handler.decisionTaskCompletedID, cancelRequestID, attr,
	)
	return err
}

func (handler *decisionTaskHandlerImpl) handleDecisionRecordMarker(
	attr *decisionpb.RecordMarkerDecisionAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondDecisionTaskCompletedScope,
		metrics.DecisionTypeRecordMarkerCounter,
	)

	if err := handler.validateDecisionAttr(
		func() error {
			return handler.attrValidator.validateRecordMarkerAttributes(attr)
		},
		enumspb.DECISION_TASK_FAILED_CAUSE_BAD_RECORD_MARKER_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	failWorkflow, err := handler.sizeLimitChecker.failWorkflowIfPayloadSizeExceedsLimit(
		metrics.DecisionTypeTag(enumspb.DECISION_TYPE_RECORD_MARKER.String()),
		common.GetPayloadsMapSize(attr.GetDetails()),
		"RecordMarkerDecisionAttributes.Details exceeds size limit.",
	)
	if err != nil || failWorkflow {
		handler.stopProcessing = true
		return err
	}

	_, err = handler.mutableState.AddRecordMarkerEvent(handler.decisionTaskCompletedID, attr)
	return err
}

func (handler *decisionTaskHandlerImpl) handleDecisionContinueAsNewWorkflow(
	attr *decisionpb.ContinueAsNewWorkflowExecutionDecisionAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondDecisionTaskCompletedScope,
		metrics.DecisionTypeContinueAsNewCounter,
	)

	if handler.hasUnhandledEventsBeforeDecisions {
		return handler.handlerFailDecision(enumspb.DECISION_TASK_FAILED_CAUSE_UNHANDLED_DECISION, "")
	}

	executionInfo := handler.mutableState.GetExecutionInfo()

	if err := handler.validateDecisionAttr(
		func() error {
			return handler.attrValidator.validateContinueAsNewWorkflowExecutionAttributes(
				attr,
				executionInfo,
			)
		},
		enumspb.DECISION_TASK_FAILED_CAUSE_BAD_CONTINUE_AS_NEW_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	failWorkflow, err := handler.sizeLimitChecker.failWorkflowIfPayloadSizeExceedsLimit(
		metrics.DecisionTypeTag(enumspb.DECISION_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION.String()),
		attr.GetInput().Size(),
		"ContinueAsNewWorkflowExecutionDecisionAttributes. Input exceeds size limit.",
	)
	if err != nil || failWorkflow {
		handler.stopProcessing = true
		return err
	}

	if attr.WorkflowRunTimeoutSeconds <= 0 {
		// TODO(maxim): is decisionTaskCompletedID the correct id?
		// TODO(maxim): should we introduce new TimeoutTypes (Workflow, Run) for workflows?
		handler.stopProcessing = true
		_, err := handler.mutableState.AddTimeoutWorkflowEvent(handler.decisionTaskCompletedID, enumspb.RETRY_STATUS_TIMEOUT)
		return err
	}
	handler.logger.Debug("!!!! Continued as new without timeout",
		tag.WorkflowRunID(executionInfo.RunID))

	// If the decision has more than one completion event than just pick the first one
	if !handler.mutableState.IsWorkflowExecutionRunning() {
		handler.metricsClient.IncCounter(
			metrics.HistoryRespondDecisionTaskCompletedScope,
			metrics.MultipleCompletionDecisionsCounter,
		)
		handler.logger.Warn(
			"Multiple completion decisions",
			tag.WorkflowDecisionType(enumspb.DECISION_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION),
			tag.ErrorTypeMultipleCompletionDecisions,
		)
		return nil
	}

	// Extract parentNamespace so it can be passed down to next run of workflow execution
	var parentNamespace string
	if handler.mutableState.HasParentExecution() {
		parentNamespaceID := executionInfo.ParentNamespaceID
		parentNamespaceEntry, err := handler.namespaceCache.GetNamespaceByID(parentNamespaceID)
		if err != nil {
			return err
		}
		parentNamespace = parentNamespaceEntry.GetInfo().Name
	}

	_, newStateBuilder, err := handler.mutableState.AddContinueAsNewEvent(
		handler.decisionTaskCompletedID,
		handler.decisionTaskCompletedID,
		parentNamespace,
		attr,
	)
	if err != nil {
		return err
	}

	handler.continueAsNewBuilder = newStateBuilder
	return nil
}

func (handler *decisionTaskHandlerImpl) handleDecisionStartChildWorkflow(
	attr *decisionpb.StartChildWorkflowExecutionDecisionAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondDecisionTaskCompletedScope,
		metrics.DecisionTypeChildWorkflowCounter,
	)

	executionInfo := handler.mutableState.GetExecutionInfo()
	namespaceID := executionInfo.NamespaceID
	targetNamespaceID := namespaceID
	if attr.GetNamespace() != "" {
		targetNamespaceEntry, err := handler.namespaceCache.GetNamespace(attr.GetNamespace())
		if err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("Unable to schedule child execution across namespace %v.", attr.GetNamespace()))
		}
		targetNamespaceID = targetNamespaceEntry.GetInfo().Id
	}

	if err := handler.validateDecisionAttr(
		func() error {
			return handler.attrValidator.validateStartChildExecutionAttributes(
				namespaceID,
				targetNamespaceID,
				attr,
				executionInfo,
			)
		},
		enumspb.DECISION_TASK_FAILED_CAUSE_BAD_START_CHILD_EXECUTION_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	failWorkflow, err := handler.sizeLimitChecker.failWorkflowIfPayloadSizeExceedsLimit(
		metrics.DecisionTypeTag(enumspb.DECISION_TYPE_START_CHILD_WORKFLOW_EXECUTION.String()),
		attr.GetInput().Size(),
		"StartChildWorkflowExecutionDecisionAttributes.Input exceeds size limit.",
	)
	if err != nil || failWorkflow {
		handler.stopProcessing = true
		return err
	}

	enabled := handler.config.EnableParentClosePolicy(handler.namespaceEntry.GetInfo().Name)
	if enabled {
		enums.SetDefaultParentClosePolicy(&attr.ParentClosePolicy)
	} else {
		attr.ParentClosePolicy = enumspb.PARENT_CLOSE_POLICY_ABANDON
	}

	enums.SetDefaultWorkflowIdReusePolicy(&attr.WorkflowIdReusePolicy)

	requestID := uuid.New()
	_, _, err = handler.mutableState.AddStartChildWorkflowExecutionInitiatedEvent(
		handler.decisionTaskCompletedID, requestID, attr,
	)
	return err
}

func (handler *decisionTaskHandlerImpl) handleDecisionSignalExternalWorkflow(
	attr *decisionpb.SignalExternalWorkflowExecutionDecisionAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondDecisionTaskCompletedScope,
		metrics.DecisionTypeSignalExternalWorkflowCounter,
	)

	executionInfo := handler.mutableState.GetExecutionInfo()
	namespaceID := executionInfo.NamespaceID
	targetNamespaceID := namespaceID
	if attr.GetNamespace() != "" {
		targetNamespaceEntry, err := handler.namespaceCache.GetNamespace(attr.GetNamespace())
		if err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("Unable to signal workflow across namespace: %v.", attr.GetNamespace()))
		}
		targetNamespaceID = targetNamespaceEntry.GetInfo().Id
	}

	if err := handler.validateDecisionAttr(
		func() error {
			return handler.attrValidator.validateSignalExternalWorkflowExecutionAttributes(
				namespaceID,
				targetNamespaceID,
				attr,
			)
		},
		enumspb.DECISION_TASK_FAILED_CAUSE_BAD_SIGNAL_WORKFLOW_EXECUTION_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	failWorkflow, err := handler.sizeLimitChecker.failWorkflowIfPayloadSizeExceedsLimit(
		metrics.DecisionTypeTag(enumspb.DECISION_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION.String()),
		attr.GetInput().Size(),
		"SignalExternalWorkflowExecutionDecisionAttributes.Input exceeds size limit.",
	)
	if err != nil || failWorkflow {
		handler.stopProcessing = true
		return err
	}

	signalRequestID := uuid.New() // for deduplicate
	_, _, err = handler.mutableState.AddSignalExternalWorkflowExecutionInitiatedEvent(
		handler.decisionTaskCompletedID, signalRequestID, attr,
	)
	return err
}

func (handler *decisionTaskHandlerImpl) handleDecisionUpsertWorkflowSearchAttributes(
	attr *decisionpb.UpsertWorkflowSearchAttributesDecisionAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondDecisionTaskCompletedScope,
		metrics.DecisionTypeUpsertWorkflowSearchAttributesCounter,
	)

	// get namespace name
	executionInfo := handler.mutableState.GetExecutionInfo()
	namespaceID := executionInfo.NamespaceID
	namespaceEntry, err := handler.namespaceCache.GetNamespaceByID(namespaceID)
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("Unable to get namespace for namespaceID: %v.", namespaceID))
	}
	namespace := namespaceEntry.GetInfo().Name

	// valid search attributes for upsert
	if err := handler.validateDecisionAttr(
		func() error {
			return handler.attrValidator.validateUpsertWorkflowSearchAttributes(
				namespace,
				attr,
			)
		},
		enumspb.DECISION_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	// blob size limit check
	failWorkflow, err := handler.sizeLimitChecker.failWorkflowIfPayloadSizeExceedsLimit(
		metrics.DecisionTypeTag(enumspb.DECISION_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES.String()),
		searchAttributesSize(attr.GetSearchAttributes().GetIndexedFields()),
		"UpsertWorkflowSearchAttributesDecisionAttributes exceeds size limit.",
	)
	if err != nil || failWorkflow {
		handler.stopProcessing = true
		return err
	}

	_, err = handler.mutableState.AddUpsertWorkflowSearchAttributesEvent(
		handler.decisionTaskCompletedID, attr,
	)
	return err
}

func searchAttributesSize(fields map[string]*commonpb.Payload) int {
	result := 0

	for k, v := range fields {
		result += len(k)
		result += len(v.GetData())
	}
	return result
}

func (handler *decisionTaskHandlerImpl) retryCronContinueAsNew(
	attr *historypb.WorkflowExecutionStartedEventAttributes,
	backoffInterval int32,
	continueAsNewInitiator enumspb.ContinueAsNewInitiator,
	failure *failurepb.Failure,
	lastCompletionResult *commonpb.Payloads,
) error {

	continueAsNewAttributes := &decisionpb.ContinueAsNewWorkflowExecutionDecisionAttributes{
		WorkflowType:                  attr.WorkflowType,
		TaskQueue:                     attr.TaskQueue,
		RetryPolicy:                   attr.RetryPolicy,
		Input:                         attr.Input,
		WorkflowRunTimeoutSeconds:     attr.WorkflowRunTimeoutSeconds,
		WorkflowTaskTimeoutSeconds:    attr.WorkflowTaskTimeoutSeconds,
		CronSchedule:                  attr.CronSchedule,
		BackoffStartIntervalInSeconds: backoffInterval,
		Initiator:                     continueAsNewInitiator,
		Failure:                       failure,
		LastCompletionResult:          lastCompletionResult,
		Header:                        attr.Header,
		Memo:                          attr.Memo,
		SearchAttributes:              attr.SearchAttributes,
	}

	_, newStateBuilder, err := handler.mutableState.AddContinueAsNewEvent(
		handler.decisionTaskCompletedID,
		handler.decisionTaskCompletedID,
		attr.GetParentWorkflowNamespace(),
		continueAsNewAttributes,
	)
	if err != nil {
		return err
	}

	handler.continueAsNewBuilder = newStateBuilder
	return nil
}

func (handler *decisionTaskHandlerImpl) validateDecisionAttr(
	validationFn decisionAttrValidationFn,
	failedCause enumspb.DecisionTaskFailedCause,
) error {

	if err := validationFn(); err != nil {
		if _, ok := err.(*serviceerror.InvalidArgument); ok {
			return handler.handlerFailDecision(failedCause, err.Error())
		}
		return err
	}

	return nil
}

func (handler *decisionTaskHandlerImpl) handlerFailDecision(
	failedCause enumspb.DecisionTaskFailedCause,
	failMessage string,
) error {
	handler.failDecisionInfo = &failDecisionInfo{
		cause:   failedCause,
		message: failMessage,
	}
	handler.stopProcessing = true
	return nil
}
