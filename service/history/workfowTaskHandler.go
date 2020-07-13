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
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
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

	workflowTaskHandlerImpl struct {
		identity                string
		workflowTaskCompletedID int64
		namespaceEntry          *cache.NamespaceCacheEntry

		// internal state
		hasUnhandledEventsBeforeDecisions bool
		failWorkflowTaskInfo              *failWorkflowTaskInfo
		activityNotStartedCancelled       bool
		continueAsNewBuilder              mutableState
		stopProcessing                    bool // should stop processing any more decisions
		mutableState                      mutableState

		// validation
		attrValidator    *commandAttrValidator
		sizeLimitChecker *workflowSizeChecker

		logger         log.Logger
		namespaceCache cache.NamespaceCache
		metricsClient  metrics.Client
		config         *Config
	}

	failWorkflowTaskInfo struct {
		cause   enumspb.WorkflowTaskFailedCause
		message string
	}
)

func newWorkflowTaskHandler(
	identity string,
	workflowTaskCompletedID int64,
	namespaceEntry *cache.NamespaceCacheEntry,
	mutableState mutableState,
	attrValidator *commandAttrValidator,
	sizeLimitChecker *workflowSizeChecker,
	logger log.Logger,
	namespaceCache cache.NamespaceCache,
	metricsClient metrics.Client,
	config *Config,
) *workflowTaskHandlerImpl {

	return &workflowTaskHandlerImpl{
		identity:                identity,
		workflowTaskCompletedID: workflowTaskCompletedID,
		namespaceEntry:          namespaceEntry,

		// internal state
		hasUnhandledEventsBeforeDecisions: mutableState.HasBufferedEvents(),
		failWorkflowTaskInfo:              nil,
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

func (handler *workflowTaskHandlerImpl) handleCommands(
	commands []*commandpb.Command,
) error {

	// overall workflow size / count check
	failWorkflow, err := handler.sizeLimitChecker.failWorkflowSizeExceedsLimit()
	if err != nil || failWorkflow {
		return err
	}

	for _, command := range commands {

		err = handler.handleCommand(command)
		if err != nil || handler.stopProcessing {
			return err
		}
	}

	return nil
}

func (handler *workflowTaskHandlerImpl) handleCommand(command *commandpb.Command) error {
	switch command.GetCommandType() {
	case enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK:
		return handler.handleDecisionScheduleActivity(command.GetScheduleActivityTaskCommandAttributes())

	case enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION:
		return handler.handleDecisionCompleteWorkflow(command.GetCompleteWorkflowExecutionCommandAttributes())

	case enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION:
		return handler.handleDecisionFailWorkflow(command.GetFailWorkflowExecutionCommandAttributes())

	case enumspb.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION:
		return handler.handleDecisionCancelWorkflow(command.GetCancelWorkflowExecutionCommandAttributes())

	case enumspb.COMMAND_TYPE_START_TIMER:
		return handler.handleDecisionStartTimer(command.GetStartTimerCommandAttributes())

	case enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK:
		return handler.handleDecisionRequestCancelActivity(command.GetRequestCancelActivityTaskCommandAttributes())

	case enumspb.COMMAND_TYPE_CANCEL_TIMER:
		return handler.handleDecisionCancelTimer(command.GetCancelTimerCommandAttributes())

	case enumspb.COMMAND_TYPE_RECORD_MARKER:
		return handler.handleDecisionRecordMarker(command.GetRecordMarkerCommandAttributes())

	case enumspb.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION:
		return handler.handleDecisionRequestCancelExternalWorkflow(command.GetRequestCancelExternalWorkflowExecutionCommandAttributes())

	case enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION:
		return handler.handleDecisionSignalExternalWorkflow(command.GetSignalExternalWorkflowExecutionCommandAttributes())

	case enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION:
		return handler.handleCommandContinueAsNewWorkflow(command.GetContinueAsNewWorkflowExecutionCommandAttributes())

	case enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION:
		return handler.handleDecisionStartChildWorkflow(command.GetStartChildWorkflowExecutionCommandAttributes())

	case enumspb.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
		return handler.handleDecisionUpsertWorkflowSearchAttributes(command.GetUpsertWorkflowSearchAttributesCommandAttributes())

	default:
		return serviceerror.NewInvalidArgument(fmt.Sprintf("Unknown command type: %v", command.GetCommandType()))
	}
}

func (handler *workflowTaskHandlerImpl) handleDecisionScheduleActivity(
	attr *commandpb.ScheduleActivityTaskCommandAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondWorkflowTaskCompletedScope,
		metrics.CommandTypeScheduleActivityCounter,
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
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	failWorkflow, err := handler.sizeLimitChecker.failWorkflowIfPayloadSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK.String()),
		attr.GetInput().Size(),
		"ScheduleActivityTaskCommandAttributes.Input exceeds size limit.",
	)
	if err != nil || failWorkflow {
		handler.stopProcessing = true
		return err
	}

	_, _, err = handler.mutableState.AddActivityTaskScheduledEvent(handler.workflowTaskCompletedID, attr)
	switch err.(type) {
	case nil:
		return nil
	case *serviceerror.InvalidArgument:
		return handler.handlerFailDecision(
			enumspb.WORKFLOW_TASK_FAILED_CAUSE_SCHEDULE_ACTIVITY_DUPLICATE_ID, "",
		)
	default:
		return err
	}
}

func (handler *workflowTaskHandlerImpl) handleDecisionRequestCancelActivity(
	attr *commandpb.RequestCancelActivityTaskCommandAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondWorkflowTaskCompletedScope,
		metrics.CommandTypeCancelActivityCounter,
	)

	if err := handler.validateDecisionAttr(
		func() error {
			return handler.attrValidator.validateActivityCancelAttributes(attr)
		},
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_ACTIVITY_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	scheduleID := attr.GetScheduledEventId()
	actCancelReqEvent, ai, err := handler.mutableState.AddActivityTaskCancelRequestedEvent(
		handler.workflowTaskCompletedID,
		scheduleID,
		handler.identity,
	)
	switch err.(type) {
	case nil:
		if ai.StartedID == common.EmptyEventID {
			// We haven't started the activity yet, we can cancel the activity right away and
			// schedule a workflow task to ensure the workflow makes progress.
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
			enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_ACTIVITY_ATTRIBUTES, "",
		)

	default:
		return err
	}
}

func (handler *workflowTaskHandlerImpl) handleDecisionStartTimer(
	attr *commandpb.StartTimerCommandAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondWorkflowTaskCompletedScope,
		metrics.CommandTypeStartTimerCounter,
	)

	if err := handler.validateDecisionAttr(
		func() error {
			return handler.attrValidator.validateTimerScheduleAttributes(attr)
		},
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_START_TIMER_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	_, _, err := handler.mutableState.AddTimerStartedEvent(handler.workflowTaskCompletedID, attr)
	switch err.(type) {
	case nil:
		return nil
	case *serviceerror.InvalidArgument:
		return handler.handlerFailDecision(
			enumspb.WORKFLOW_TASK_FAILED_CAUSE_START_TIMER_DUPLICATE_ID, "",
		)
	default:
		return err
	}
}

func (handler *workflowTaskHandlerImpl) handleDecisionCompleteWorkflow(
	attr *commandpb.CompleteWorkflowExecutionCommandAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondWorkflowTaskCompletedScope,
		metrics.CommandTypeCompleteWorkflowCounter,
	)

	if handler.hasUnhandledEventsBeforeDecisions {
		return handler.handlerFailDecision(enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND, "")
	}

	if err := handler.validateDecisionAttr(
		func() error {
			return handler.attrValidator.validateCompleteWorkflowExecutionAttributes(attr)
		},
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_COMPLETE_WORKFLOW_EXECUTION_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	failWorkflow, err := handler.sizeLimitChecker.failWorkflowIfPayloadSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION.String()),
		attr.GetResult().Size(),
		"CompleteWorkflowExecutionCommandAttributes.Result exceeds size limit.",
	)
	if err != nil || failWorkflow {
		handler.stopProcessing = true
		return err
	}

	// If the decision has more than one completion event than just pick the first one
	if !handler.mutableState.IsWorkflowExecutionRunning() {
		handler.metricsClient.IncCounter(
			metrics.HistoryRespondWorkflowTaskCompletedScope,
			metrics.MultipleCompletionDecisionsCounter,
		)
		handler.logger.Warn(
			"Multiple completion decisions",
			tag.WorkflowCommandType(enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION),
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
		if _, err := handler.mutableState.AddCompletedWorkflowEvent(handler.workflowTaskCompletedID, attr); err != nil {
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

func (handler *workflowTaskHandlerImpl) handleDecisionFailWorkflow(
	attr *commandpb.FailWorkflowExecutionCommandAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondWorkflowTaskCompletedScope,
		metrics.CommandTypeFailWorkflowCounter,
	)

	if handler.hasUnhandledEventsBeforeDecisions {
		return handler.handlerFailDecision(enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND, "")
	}

	if err := handler.validateDecisionAttr(
		func() error {
			return handler.attrValidator.validateFailWorkflowExecutionAttributes(attr)
		},
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_FAIL_WORKFLOW_EXECUTION_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	failWorkflow, err := handler.sizeLimitChecker.failWorkflowIfPayloadSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION.String()),
		attr.GetFailure().Size(),
		"FailWorkflowExecutionCommandAttributes.Failure exceeds size limit.",
	)
	if err != nil || failWorkflow {
		handler.stopProcessing = true
		return err
	}

	// If the decision has more than one completion event than just pick the first one
	if !handler.mutableState.IsWorkflowExecutionRunning() {
		handler.metricsClient.IncCounter(
			metrics.HistoryRespondWorkflowTaskCompletedScope,
			metrics.MultipleCompletionDecisionsCounter,
		)
		handler.logger.Warn(
			"Multiple completion decisions",
			tag.WorkflowCommandType(enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION),
			tag.ErrorTypeMultipleCompletionDecisions,
		)
		return nil
	}

	// below will check whether to do continue as new based on backoff & backoff or cron
	backoffInterval, retryState := handler.mutableState.GetRetryBackoffDuration(attr.GetFailure())
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
		if _, err := handler.mutableState.AddFailWorkflowEvent(handler.workflowTaskCompletedID, retryState, attr); err != nil {
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

func (handler *workflowTaskHandlerImpl) handleDecisionCancelTimer(
	attr *commandpb.CancelTimerCommandAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondWorkflowTaskCompletedScope,
		metrics.CommandTypeCancelTimerCounter,
	)

	if err := handler.validateDecisionAttr(
		func() error {
			return handler.attrValidator.validateTimerCancelAttributes(attr)
		},
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_CANCEL_TIMER_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	_, err := handler.mutableState.AddTimerCanceledEvent(
		handler.workflowTaskCompletedID,
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
			handler.workflowTaskCompletedID,
			attr,
			handler.identity,
		)
		return err
	default:
		return err
	}
}

func (handler *workflowTaskHandlerImpl) handleDecisionCancelWorkflow(
	attr *commandpb.CancelWorkflowExecutionCommandAttributes,
) error {

	handler.metricsClient.IncCounter(metrics.HistoryRespondWorkflowTaskCompletedScope,
		metrics.CommandTypeCancelWorkflowCounter)

	if handler.hasUnhandledEventsBeforeDecisions {
		return handler.handlerFailDecision(enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND, "")
	}

	if err := handler.validateDecisionAttr(
		func() error {
			return handler.attrValidator.validateCancelWorkflowExecutionAttributes(attr)
		},
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_CANCEL_WORKFLOW_EXECUTION_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	// If the decision has more than one completion event than just pick the first one
	if !handler.mutableState.IsWorkflowExecutionRunning() {
		handler.metricsClient.IncCounter(
			metrics.HistoryRespondWorkflowTaskCompletedScope,
			metrics.MultipleCompletionDecisionsCounter,
		)
		handler.logger.Warn(
			"Multiple completion decisions",
			tag.WorkflowCommandType(enumspb.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION),
			tag.ErrorTypeMultipleCompletionDecisions,
		)
		return nil
	}

	_, err := handler.mutableState.AddWorkflowExecutionCanceledEvent(handler.workflowTaskCompletedID, attr)
	return err
}

func (handler *workflowTaskHandlerImpl) handleDecisionRequestCancelExternalWorkflow(
	attr *commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondWorkflowTaskCompletedScope,
		metrics.CommandTypeCancelExternalWorkflowCounter,
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
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	cancelRequestID := uuid.New()
	_, _, err := handler.mutableState.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(
		handler.workflowTaskCompletedID, cancelRequestID, attr,
	)
	return err
}

func (handler *workflowTaskHandlerImpl) handleDecisionRecordMarker(
	attr *commandpb.RecordMarkerCommandAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondWorkflowTaskCompletedScope,
		metrics.CommandTypeRecordMarkerCounter,
	)

	if err := handler.validateDecisionAttr(
		func() error {
			return handler.attrValidator.validateRecordMarkerAttributes(attr)
		},
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_RECORD_MARKER_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	failWorkflow, err := handler.sizeLimitChecker.failWorkflowIfPayloadSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_RECORD_MARKER.String()),
		common.GetPayloadsMapSize(attr.GetDetails()),
		"RecordMarkerCommandAttributes.Details exceeds size limit.",
	)
	if err != nil || failWorkflow {
		handler.stopProcessing = true
		return err
	}

	_, err = handler.mutableState.AddRecordMarkerEvent(handler.workflowTaskCompletedID, attr)
	return err
}

func (handler *workflowTaskHandlerImpl) handleCommandContinueAsNewWorkflow(
	attr *commandpb.ContinueAsNewWorkflowExecutionCommandAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondWorkflowTaskCompletedScope,
		metrics.CommandTypeContinueAsNewCounter,
	)

	if handler.hasUnhandledEventsBeforeDecisions {
		return handler.handlerFailDecision(enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND, "")
	}

	executionInfo := handler.mutableState.GetExecutionInfo()

	if err := handler.validateDecisionAttr(
		func() error {
			return handler.attrValidator.validateContinueAsNewWorkflowExecutionAttributes(
				attr,
				executionInfo,
			)
		},
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_CONTINUE_AS_NEW_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	failWorkflow, err := handler.sizeLimitChecker.failWorkflowIfPayloadSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION.String()),
		attr.GetInput().Size(),
		"ContinueAsNewWorkflowExecutionCommandAttributes. Input exceeds size limit.",
	)
	if err != nil || failWorkflow {
		handler.stopProcessing = true
		return err
	}

	if attr.WorkflowRunTimeoutSeconds <= 0 {
		// TODO(maxim): is workflowTaskCompletedID the correct id?
		// TODO(maxim): should we introduce new TimeoutTypes (Workflow, Run) for workflows?
		handler.stopProcessing = true
		_, err := handler.mutableState.AddTimeoutWorkflowEvent(handler.workflowTaskCompletedID, enumspb.RETRY_STATE_TIMEOUT)
		return err
	}
	handler.logger.Debug("!!!! Continued as new without timeout",
		tag.WorkflowRunID(executionInfo.RunID))

	// If the decision has more than one completion event than just pick the first one
	if !handler.mutableState.IsWorkflowExecutionRunning() {
		handler.metricsClient.IncCounter(
			metrics.HistoryRespondWorkflowTaskCompletedScope,
			metrics.MultipleCompletionDecisionsCounter,
		)
		handler.logger.Warn(
			"Multiple completion decisions",
			tag.WorkflowCommandType(enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION),
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
		handler.workflowTaskCompletedID,
		handler.workflowTaskCompletedID,
		parentNamespace,
		attr,
	)
	if err != nil {
		return err
	}

	handler.continueAsNewBuilder = newStateBuilder
	return nil
}

func (handler *workflowTaskHandlerImpl) handleDecisionStartChildWorkflow(
	attr *commandpb.StartChildWorkflowExecutionCommandAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondWorkflowTaskCompletedScope,
		metrics.CommandTypeChildWorkflowCounter,
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
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_START_CHILD_EXECUTION_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	failWorkflow, err := handler.sizeLimitChecker.failWorkflowIfPayloadSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION.String()),
		attr.GetInput().Size(),
		"StartChildWorkflowExecutionCommandAttributes.Input exceeds size limit.",
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
		handler.workflowTaskCompletedID, requestID, attr,
	)
	return err
}

func (handler *workflowTaskHandlerImpl) handleDecisionSignalExternalWorkflow(
	attr *commandpb.SignalExternalWorkflowExecutionCommandAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondWorkflowTaskCompletedScope,
		metrics.CommandTypeSignalExternalWorkflowCounter,
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
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SIGNAL_WORKFLOW_EXECUTION_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	failWorkflow, err := handler.sizeLimitChecker.failWorkflowIfPayloadSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION.String()),
		attr.GetInput().Size(),
		"SignalExternalWorkflowExecutionCommandAttributes.Input exceeds size limit.",
	)
	if err != nil || failWorkflow {
		handler.stopProcessing = true
		return err
	}

	signalRequestID := uuid.New() // for deduplicate
	_, _, err = handler.mutableState.AddSignalExternalWorkflowExecutionInitiatedEvent(
		handler.workflowTaskCompletedID, signalRequestID, attr,
	)
	return err
}

func (handler *workflowTaskHandlerImpl) handleDecisionUpsertWorkflowSearchAttributes(
	attr *commandpb.UpsertWorkflowSearchAttributesCommandAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondWorkflowTaskCompletedScope,
		metrics.CommandTypeUpsertWorkflowSearchAttributesCounter,
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
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	// blob size limit check
	failWorkflow, err := handler.sizeLimitChecker.failWorkflowIfPayloadSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES.String()),
		searchAttributesSize(attr.GetSearchAttributes().GetIndexedFields()),
		"UpsertWorkflowSearchAttributesCommandAttributes exceeds size limit.",
	)
	if err != nil || failWorkflow {
		handler.stopProcessing = true
		return err
	}

	_, err = handler.mutableState.AddUpsertWorkflowSearchAttributesEvent(
		handler.workflowTaskCompletedID, attr,
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

func (handler *workflowTaskHandlerImpl) retryCronContinueAsNew(
	attr *historypb.WorkflowExecutionStartedEventAttributes,
	backoffInterval int32,
	continueAsNewInitiator enumspb.ContinueAsNewInitiator,
	failure *failurepb.Failure,
	lastCompletionResult *commonpb.Payloads,
) error {

	continueAsNewAttributes := &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
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
		handler.workflowTaskCompletedID,
		handler.workflowTaskCompletedID,
		attr.GetParentWorkflowNamespace(),
		continueAsNewAttributes,
	)
	if err != nil {
		return err
	}

	handler.continueAsNewBuilder = newStateBuilder
	return nil
}

func (handler *workflowTaskHandlerImpl) validateDecisionAttr(
	validationFn decisionAttrValidationFn,
	failedCause enumspb.WorkflowTaskFailedCause,
) error {

	if err := validationFn(); err != nil {
		if _, ok := err.(*serviceerror.InvalidArgument); ok {
			return handler.handlerFailDecision(failedCause, err.Error())
		}
		return err
	}

	return nil
}

func (handler *workflowTaskHandlerImpl) handlerFailDecision(
	failedCause enumspb.WorkflowTaskFailedCause,
	failMessage string,
) error {
	handler.failWorkflowTaskInfo = &failWorkflowTaskInfo{
		cause:   failedCause,
		message: failMessage,
	}
	handler.stopProcessing = true
	return nil
}
