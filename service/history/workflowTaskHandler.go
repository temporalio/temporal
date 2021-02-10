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
	"time"

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
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/history/configs"
)

type (
	commandAttrValidationFn func() error

	workflowTaskHandlerImpl struct {
		identity                string
		workflowTaskCompletedID int64
		namespaceEntry          *cache.NamespaceCacheEntry

		// internal state
		hasBufferedEvents               bool
		workflowTaskFailedErr           *workflowTaskFailedError
		activityNotStartedCancelled     bool
		continueAsNewBuilder            mutableState
		stopProcessing                  bool // should stop processing any more commands
		mutableState                    mutableState
		initiatedChildExecutionsInBatch map[string]struct{} // Set of initiated child executions in the workflow task

		// validation
		attrValidator    *commandAttrValidator
		sizeLimitChecker *workflowSizeChecker

		logger         log.Logger
		namespaceCache cache.NamespaceCache
		metricsClient  metrics.Client
		config         *configs.Config
	}

	workflowTaskFailedError struct {
		failedCause enumspb.WorkflowTaskFailedCause
		// Use concrete type (not `error` interface) here to indicate that cause error is always InvalidArgument error
		// and it is ok to return InvalidArgument to the caller in case of workflowTaskFailedError.
		causeErr *serviceerror.InvalidArgument
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
	config *configs.Config,
) *workflowTaskHandlerImpl {

	return &workflowTaskHandlerImpl{
		identity:                identity,
		workflowTaskCompletedID: workflowTaskCompletedID,
		namespaceEntry:          namespaceEntry,

		// internal state
		hasBufferedEvents:               mutableState.HasBufferedEvents(),
		workflowTaskFailedErr:           nil,
		activityNotStartedCancelled:     false,
		continueAsNewBuilder:            nil,
		stopProcessing:                  false,
		mutableState:                    mutableState,
		initiatedChildExecutionsInBatch: make(map[string]struct{}),

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

	for _, command := range commands {

		err := handler.handleCommand(command)
		if err != nil || handler.stopProcessing {
			return err
		}
	}

	return nil
}

func (handler *workflowTaskHandlerImpl) handleCommand(command *commandpb.Command) error {
	switch command.GetCommandType() {
	case enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK:
		return handler.handleCommandScheduleActivity(command.GetScheduleActivityTaskCommandAttributes())

	case enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION:
		return handler.handleCommandCompleteWorkflow(command.GetCompleteWorkflowExecutionCommandAttributes())

	case enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION:
		return handler.handleCommandFailWorkflow(command.GetFailWorkflowExecutionCommandAttributes())

	case enumspb.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION:
		return handler.handleCommandCancelWorkflow(command.GetCancelWorkflowExecutionCommandAttributes())

	case enumspb.COMMAND_TYPE_START_TIMER:
		return handler.handleCommandStartTimer(command.GetStartTimerCommandAttributes())

	case enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK:
		return handler.handleCommandRequestCancelActivity(command.GetRequestCancelActivityTaskCommandAttributes())

	case enumspb.COMMAND_TYPE_CANCEL_TIMER:
		return handler.handleCommandCancelTimer(command.GetCancelTimerCommandAttributes())

	case enumspb.COMMAND_TYPE_RECORD_MARKER:
		return handler.handleCommandRecordMarker(command.GetRecordMarkerCommandAttributes())

	case enumspb.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION:
		return handler.handleCommandRequestCancelExternalWorkflow(command.GetRequestCancelExternalWorkflowExecutionCommandAttributes())

	case enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION:
		return handler.handleCommandSignalExternalWorkflow(command.GetSignalExternalWorkflowExecutionCommandAttributes())

	case enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION:
		return handler.handleCommandContinueAsNewWorkflow(command.GetContinueAsNewWorkflowExecutionCommandAttributes())

	case enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION:
		return handler.handleCommandStartChildWorkflow(command.GetStartChildWorkflowExecutionCommandAttributes())

	case enumspb.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
		return handler.handleCommandUpsertWorkflowSearchAttributes(command.GetUpsertWorkflowSearchAttributesCommandAttributes())

	default:
		return serviceerror.NewInvalidArgument(fmt.Sprintf("Unknown command type: %v", command.GetCommandType()))
	}
}

func (handler *workflowTaskHandlerImpl) handleCommandScheduleActivity(
	attr *commandpb.ScheduleActivityTaskCommandAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondWorkflowTaskCompletedScope,
		metrics.CommandTypeScheduleActivityCounter,
	)

	executionInfo := handler.mutableState.GetExecutionInfo()
	namespaceID := executionInfo.NamespaceId
	targetNamespaceID := namespaceID
	if attr.GetNamespace() != "" {
		targetNamespaceEntry, err := handler.namespaceCache.GetNamespace(attr.GetNamespace())
		if err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("Unable to schedule activity across namespace %v.", attr.GetNamespace()))
		}
		targetNamespaceID = targetNamespaceEntry.GetInfo().Id
	}

	if err := handler.validateCommandAttr(
		func() error {
			return handler.attrValidator.validateActivityScheduleAttributes(
				namespaceID,
				targetNamespaceID,
				attr,
				timestamp.DurationValue(executionInfo.WorkflowRunTimeout),
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

	enums.SetDefaultTaskQueueKind(&attr.GetTaskQueue().Kind)

	_, _, err = handler.mutableState.AddActivityTaskScheduledEvent(handler.workflowTaskCompletedID, attr)
	switch err := err.(type) {
	case nil:
		return nil
	case *serviceerror.InvalidArgument:
		return handler.failCommand(enumspb.WORKFLOW_TASK_FAILED_CAUSE_SCHEDULE_ACTIVITY_DUPLICATE_ID, err)
	default:
		return err
	}
}

func (handler *workflowTaskHandlerImpl) handleCommandRequestCancelActivity(
	attr *commandpb.RequestCancelActivityTaskCommandAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondWorkflowTaskCompletedScope,
		metrics.CommandTypeCancelActivityCounter,
	)

	if err := handler.validateCommandAttr(
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
	switch err := err.(type) {
	case nil:
		if ai != nil {
			// If ai is nil, the activity has already been canceled/completed/timedout. The cancel request
			// will be recorded in the history, but no further action will be taken.

			if ai.StartedId == common.EmptyEventID {
				// We haven't started the activity yet, we can cancel the activity right away and
				// schedule a workflow task to ensure the workflow makes progress.
				_, err = handler.mutableState.AddActivityTaskCanceledEvent(
					ai.ScheduleId,
					ai.StartedId,
					actCancelReqEvent.GetEventId(),
					payloads.EncodeString(activityCancellationMsgActivityNotStarted),
					handler.identity,
				)
				if err != nil {
					return err
				}
				handler.activityNotStartedCancelled = true
			}
		}
		return nil
	case *serviceerror.InvalidArgument:
		return handler.failCommand(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_ACTIVITY_ATTRIBUTES, err)
	default:
		return err
	}
}

func (handler *workflowTaskHandlerImpl) handleCommandStartTimer(
	attr *commandpb.StartTimerCommandAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondWorkflowTaskCompletedScope,
		metrics.CommandTypeStartTimerCounter,
	)

	if err := handler.validateCommandAttr(
		func() error {
			return handler.attrValidator.validateTimerScheduleAttributes(attr)
		},
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_START_TIMER_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	_, _, err := handler.mutableState.AddTimerStartedEvent(handler.workflowTaskCompletedID, attr)
	switch err := err.(type) {
	case nil:
		return nil
	case *serviceerror.InvalidArgument:
		return handler.failCommand(enumspb.WORKFLOW_TASK_FAILED_CAUSE_START_TIMER_DUPLICATE_ID, err)
	default:
		return err
	}
}

func (handler *workflowTaskHandlerImpl) handleCommandCompleteWorkflow(
	attr *commandpb.CompleteWorkflowExecutionCommandAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondWorkflowTaskCompletedScope,
		metrics.CommandTypeCompleteWorkflowCounter,
	)

	if handler.hasBufferedEvents {
		return handler.failCommand(enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND, nil)
	}

	if err := handler.validateCommandAttr(
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

	// If the workflow task has more than one completion event than just pick the first one
	if !handler.mutableState.IsWorkflowExecutionRunning() {
		handler.metricsClient.IncCounter(
			metrics.HistoryRespondWorkflowTaskCompletedScope,
			metrics.MultipleCompletionCommandsCounter,
		)
		handler.logger.Warn(
			"Multiple completion commands",
			tag.WorkflowCommandType(enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION),
			tag.ErrorTypeMultipleCompletionCommands,
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
		cronBackoff,
		enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE,
		nil,
		attr.Result,
	)
}

func (handler *workflowTaskHandlerImpl) handleCommandFailWorkflow(
	attr *commandpb.FailWorkflowExecutionCommandAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondWorkflowTaskCompletedScope,
		metrics.CommandTypeFailWorkflowCounter,
	)

	if handler.hasBufferedEvents {
		return handler.failCommand(enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND, nil)
	}

	if err := handler.validateCommandAttr(
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

	// If the workflow task has more than one completion event than just pick the first one
	if !handler.mutableState.IsWorkflowExecutionRunning() {
		handler.metricsClient.IncCounter(
			metrics.HistoryRespondWorkflowTaskCompletedScope,
			metrics.MultipleCompletionCommandsCounter,
		)
		handler.logger.Warn(
			"Multiple completion commands",
			tag.WorkflowCommandType(enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION),
			tag.ErrorTypeMultipleCompletionCommands,
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
		backoffInterval,
		continueAsNewInitiator,
		attr.GetFailure(),
		startAttributes.LastCompletionResult,
	)
}

func (handler *workflowTaskHandlerImpl) handleCommandCancelTimer(
	attr *commandpb.CancelTimerCommandAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondWorkflowTaskCompletedScope,
		metrics.CommandTypeCancelTimerCounter,
	)

	if err := handler.validateCommandAttr(
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
	switch err := err.(type) {
	case nil:
		// timer deletion is a success, we may have deleted a fired timer in
		// which case we should reset hasBufferedEvents
		// TODO deletion of timer fired event refreshing hasBufferedEvents
		//  is not entirely correct, since during these commands processing, new event may appear
		handler.hasBufferedEvents = handler.mutableState.HasBufferedEvents()
		return nil
	case *serviceerror.InvalidArgument:
		return handler.failCommand(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_CANCEL_TIMER_ATTRIBUTES, err)
	default:
		return err
	}
}

func (handler *workflowTaskHandlerImpl) handleCommandCancelWorkflow(
	attr *commandpb.CancelWorkflowExecutionCommandAttributes,
) error {

	handler.metricsClient.IncCounter(metrics.HistoryRespondWorkflowTaskCompletedScope,
		metrics.CommandTypeCancelWorkflowCounter)

	if handler.hasBufferedEvents {
		return handler.failCommand(enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND, nil)
	}

	if err := handler.validateCommandAttr(
		func() error {
			return handler.attrValidator.validateCancelWorkflowExecutionAttributes(attr)
		},
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_CANCEL_WORKFLOW_EXECUTION_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	// If the workflow task has more than one completion event than just pick the first one
	if !handler.mutableState.IsWorkflowExecutionRunning() {
		handler.metricsClient.IncCounter(
			metrics.HistoryRespondWorkflowTaskCompletedScope,
			metrics.MultipleCompletionCommandsCounter,
		)
		handler.logger.Warn(
			"Multiple completion commands",
			tag.WorkflowCommandType(enumspb.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION),
			tag.ErrorTypeMultipleCompletionCommands,
		)
		return nil
	}

	_, err := handler.mutableState.AddWorkflowExecutionCanceledEvent(handler.workflowTaskCompletedID, attr)
	return err
}

func (handler *workflowTaskHandlerImpl) handleCommandRequestCancelExternalWorkflow(
	attr *commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondWorkflowTaskCompletedScope,
		metrics.CommandTypeCancelExternalWorkflowCounter,
	)

	executionInfo := handler.mutableState.GetExecutionInfo()
	namespaceID := executionInfo.NamespaceId
	targetNamespaceID := namespaceID
	if attr.GetNamespace() != "" {
		targetNamespaceEntry, err := handler.namespaceCache.GetNamespace(attr.GetNamespace())
		if err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("Unable to cancel workflow across namespace: %v.", attr.GetNamespace()))
		}
		targetNamespaceID = targetNamespaceEntry.GetInfo().Id
	}

	if err := handler.validateCommandAttr(
		func() error {
			return handler.attrValidator.validateCancelExternalWorkflowExecutionAttributes(
				namespaceID,
				targetNamespaceID,
				handler.initiatedChildExecutionsInBatch,
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

func (handler *workflowTaskHandlerImpl) handleCommandRecordMarker(
	attr *commandpb.RecordMarkerCommandAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondWorkflowTaskCompletedScope,
		metrics.CommandTypeRecordMarkerCounter,
	)

	if err := handler.validateCommandAttr(
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

	if handler.hasBufferedEvents {
		return handler.failCommand(enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND, nil)
	}

	searchattribute.SetTypes(
		attr.SearchAttributes,
		searchattribute.ConvertDynamicConfigToIndexedValueTypes(handler.config.ValidSearchAttributes))

	if err := handler.validateCommandAttr(
		func() error {
			return handler.attrValidator.validateContinueAsNewWorkflowExecutionAttributes(
				attr,
				handler.mutableState.GetExecutionInfo(),
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

	// If the workflow task has more than one completion event than just pick the first one
	if !handler.mutableState.IsWorkflowExecutionRunning() {
		handler.metricsClient.IncCounter(
			metrics.HistoryRespondWorkflowTaskCompletedScope,
			metrics.MultipleCompletionCommandsCounter,
		)
		handler.logger.Warn(
			"Multiple completion commands",
			tag.WorkflowCommandType(enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION),
			tag.ErrorTypeMultipleCompletionCommands,
		)
		return nil
	}

	// Extract parentNamespace so it can be passed down to next run of workflow execution
	var parentNamespace string
	if handler.mutableState.HasParentExecution() {
		parentNamespaceID := handler.mutableState.GetExecutionInfo().ParentNamespaceId
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

func (handler *workflowTaskHandlerImpl) handleCommandStartChildWorkflow(
	attr *commandpb.StartChildWorkflowExecutionCommandAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondWorkflowTaskCompletedScope,
		metrics.CommandTypeChildWorkflowCounter,
	)

	namespaceID := handler.mutableState.GetExecutionInfo().NamespaceId
	parentNamespace := handler.namespaceEntry.GetInfo().GetName()
	targetNamespaceID := namespaceID
	targetNamespace := parentNamespace
	if attr.GetNamespace() != "" {
		targetNamespaceEntry, err := handler.namespaceCache.GetNamespace(attr.GetNamespace())
		if err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("Unable to schedule child execution across namespace %v.", attr.GetNamespace()))
		}
		targetNamespace = targetNamespaceEntry.GetInfo().Name
		targetNamespaceID = targetNamespaceEntry.GetInfo().Id
	}

	searchattribute.SetTypes(
		attr.SearchAttributes,
		searchattribute.ConvertDynamicConfigToIndexedValueTypes(handler.config.ValidSearchAttributes))

	if err := handler.validateCommandAttr(
		func() error {
			return handler.attrValidator.validateStartChildExecutionAttributes(
				namespaceID,
				targetNamespaceID,
				targetNamespace,
				attr,
				handler.mutableState.GetExecutionInfo(),
				handler.config.DefaultWorkflowTaskTimeout,
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
	if err == nil {
		// Keep track of all child initiated commands in this workflow task to validate request cancel commands
		handler.initiatedChildExecutionsInBatch[attr.GetWorkflowId()] = struct{}{}
	}
	return err
}

func (handler *workflowTaskHandlerImpl) handleCommandSignalExternalWorkflow(
	attr *commandpb.SignalExternalWorkflowExecutionCommandAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondWorkflowTaskCompletedScope,
		metrics.CommandTypeSignalExternalWorkflowCounter,
	)

	executionInfo := handler.mutableState.GetExecutionInfo()
	namespaceID := executionInfo.NamespaceId
	targetNamespaceID := namespaceID
	if attr.GetNamespace() != "" {
		targetNamespaceEntry, err := handler.namespaceCache.GetNamespace(attr.GetNamespace())
		if err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("Unable to signal workflow across namespace: %v.", attr.GetNamespace()))
		}
		targetNamespaceID = targetNamespaceEntry.GetInfo().Id
	}

	if err := handler.validateCommandAttr(
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

func (handler *workflowTaskHandlerImpl) handleCommandUpsertWorkflowSearchAttributes(
	attr *commandpb.UpsertWorkflowSearchAttributesCommandAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondWorkflowTaskCompletedScope,
		metrics.CommandTypeUpsertWorkflowSearchAttributesCounter,
	)

	// get namespace name
	executionInfo := handler.mutableState.GetExecutionInfo()
	namespaceID := executionInfo.NamespaceId
	namespaceEntry, err := handler.namespaceCache.GetNamespaceByID(namespaceID)
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("Unable to get namespace for namespaceID: %v.", namespaceID))
	}
	namespace := namespaceEntry.GetInfo().Name

	searchattribute.SetTypes(
		attr.SearchAttributes,
		searchattribute.ConvertDynamicConfigToIndexedValueTypes(handler.config.ValidSearchAttributes))

	// valid search attributes for upsert
	if err := handler.validateCommandAttr(
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
	backoffInterval time.Duration,
	continueAsNewInitiator enumspb.ContinueAsNewInitiator,
	failure *failurepb.Failure,
	lastCompletionResult *commonpb.Payloads,
) error {

	continueAsNewAttributes := &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
		WorkflowType:         attr.WorkflowType,
		TaskQueue:            attr.TaskQueue,
		RetryPolicy:          attr.RetryPolicy,
		Input:                attr.Input,
		WorkflowRunTimeout:   attr.WorkflowRunTimeout,
		WorkflowTaskTimeout:  attr.WorkflowTaskTimeout,
		CronSchedule:         attr.CronSchedule,
		BackoffStartInterval: &backoffInterval,
		Initiator:            continueAsNewInitiator,
		Failure:              failure,
		LastCompletionResult: lastCompletionResult,
		Header:               attr.Header,
		Memo:                 attr.Memo,
		SearchAttributes:     attr.SearchAttributes,
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

func (handler *workflowTaskHandlerImpl) validateCommandAttr(
	validationFn commandAttrValidationFn,
	failedCause enumspb.WorkflowTaskFailedCause,
) error {

	if err := validationFn(); err != nil {
		if err, ok := err.(*serviceerror.InvalidArgument); ok {
			return handler.failCommand(failedCause, err)
		}
		return err
	}

	return nil
}

func (handler *workflowTaskHandlerImpl) failCommand(
	failedCause enumspb.WorkflowTaskFailedCause,
	causeErr *serviceerror.InvalidArgument,
) error {
	handler.workflowTaskFailedErr = NewWorkflowTaskFailedError(failedCause, causeErr)
	handler.stopProcessing = true
	return nil
}

func NewWorkflowTaskFailedError(failedCause enumspb.WorkflowTaskFailedCause, causeErr *serviceerror.InvalidArgument) *workflowTaskFailedError {
	return &workflowTaskFailedError{
		failedCause: failedCause,
		causeErr:    causeErr,
	}
}

func (fwti *workflowTaskFailedError) Error() string {
	if fwti.causeErr == nil {
		return fwti.failedCause.String()
	}

	return fmt.Sprintf("%v: %v", fwti.failedCause, fwti.causeErr.Error())
}

func (fwti *workflowTaskFailedError) ServiceError() *serviceerror.InvalidArgument {
	return serviceerror.NewInvalidArgument(fwti.Error())
}
