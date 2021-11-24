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
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/searchattribute"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/enums"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

type (
	commandAttrValidationFn func() error

	workflowTaskHandlerImpl struct {
		identity                string
		workflowTaskCompletedID int64

		// internal state
		hasBufferedEvents               bool
		workflowTaskFailedCause         *workflowTaskFailedCause
		activityNotStartedCancelled     bool
		newStateBuilder                 workflow.MutableState
		stopProcessing                  bool // should stop processing any more commands
		mutableState                    workflow.MutableState
		initiatedChildExecutionsInBatch map[string]struct{} // Set of initiated child executions in the workflow task

		// validation
		attrValidator          *commandAttrValidator
		sizeLimitChecker       *workflowSizeChecker
		searchAttributesMapper searchattribute.Mapper

		logger            log.Logger
		namespaceRegistry namespace.Registry
		metricsClient     metrics.Client
		config            *configs.Config
		shard             shard.Context
	}

	workflowTaskFailedCause struct {
		failedCause enumspb.WorkflowTaskFailedCause
		causeErr    error
	}
)

func newWorkflowTaskHandler(
	identity string,
	workflowTaskCompletedID int64,
	mutableState workflow.MutableState,
	attrValidator *commandAttrValidator,
	sizeLimitChecker *workflowSizeChecker,
	logger log.Logger,
	namespaceRegistry namespace.Registry,
	metricsClient metrics.Client,
	config *configs.Config,
	shard shard.Context,
	searchAttributesMapper searchattribute.Mapper,
) *workflowTaskHandlerImpl {

	return &workflowTaskHandlerImpl{
		identity:                identity,
		workflowTaskCompletedID: workflowTaskCompletedID,

		// internal state
		hasBufferedEvents:               mutableState.HasBufferedEvents(),
		workflowTaskFailedCause:         nil,
		activityNotStartedCancelled:     false,
		newStateBuilder:                 nil,
		stopProcessing:                  false,
		mutableState:                    mutableState,
		initiatedChildExecutionsInBatch: make(map[string]struct{}),

		// validation
		attrValidator:          attrValidator,
		sizeLimitChecker:       sizeLimitChecker,
		searchAttributesMapper: searchAttributesMapper,

		logger:            logger,
		namespaceRegistry: namespaceRegistry,
		metricsClient:     metricsClient,
		config:            config,
		shard:             shard,
	}
}

func (handler *workflowTaskHandlerImpl) handleCommands(
	commands []*commandpb.Command,
) error {
	if err := handler.attrValidator.validateCommandSequence(
		commands,
	); err != nil {
		return err
	}
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
	namespaceID := namespace.ID(executionInfo.NamespaceId)
	targetNamespaceID := namespaceID
	if attr.GetNamespace() != "" {
		targetNamespaceEntry, err := handler.namespaceRegistry.GetNamespace(namespace.Name(attr.GetNamespace()))
		if err != nil {
			return serviceerror.NewUnavailable(fmt.Sprintf("Unable to schedule activity across namespace %v.", attr.GetNamespace()))
		}
		targetNamespaceID = targetNamespaceEntry.ID()
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
	if err != nil {
		if _, ok := err.(*serviceerror.InvalidArgument); ok {
			return handler.failCommand(enumspb.WORKFLOW_TASK_FAILED_CAUSE_SCHEDULE_ACTIVITY_DUPLICATE_ID, err)
		}
		return err
	}
	return nil
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
	if err != nil {
		if _, ok := err.(*serviceerror.InvalidArgument); ok {
			return handler.failCommand(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_ACTIVITY_ATTRIBUTES, err)
		}
		return err
	}
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
	if err != nil {
		if _, ok := err.(*serviceerror.InvalidArgument); ok {
			return handler.failCommand(enumspb.WORKFLOW_TASK_FAILED_CAUSE_START_TIMER_DUPLICATE_ID, err)
		}
		return err
	}
	return nil
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

	cronBackoff := handler.mutableState.GetCronBackoffDuration()
	var newExecutionRunID string
	if cronBackoff != backoff.NoBackoff {
		newExecutionRunID = uuid.New()
	}

	// Always add workflow completed event to this one
	_, err = handler.mutableState.AddCompletedWorkflowEvent(handler.workflowTaskCompletedID, attr, newExecutionRunID)
	if err != nil {
		return err
	}

	// Check if this workflow has a cron schedule
	if cronBackoff != backoff.NoBackoff {
		return handler.handleCron(cronBackoff, attr.GetResult(), nil, newExecutionRunID)
	}

	return nil
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

	// First check retry policy to do a retry.
	retryBackoff, retryState := handler.mutableState.GetRetryBackoffDuration(attr.GetFailure())
	cronBackoff := backoff.NoBackoff
	if retryBackoff == backoff.NoBackoff {
		// If no retry, check cron.
		cronBackoff = handler.mutableState.GetCronBackoffDuration()
	}

	var newExecutionRunID string
	if retryBackoff != backoff.NoBackoff || cronBackoff != backoff.NoBackoff {
		newExecutionRunID = uuid.New()
	}

	// Always add workflow failed event
	if _, err = handler.mutableState.AddFailWorkflowEvent(
		handler.workflowTaskCompletedID,
		retryState,
		attr,
		newExecutionRunID,
	); err != nil {
		return err
	}

	// Handle retry or cron
	if retryBackoff != backoff.NoBackoff {
		return handler.handleRetry(retryBackoff, retryState, attr.GetFailure(), newExecutionRunID)
	} else if cronBackoff != backoff.NoBackoff {
		return handler.handleCron(cronBackoff, nil, attr.GetFailure(), newExecutionRunID)
	}

	// No retry or cron
	return nil
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
	if err != nil {
		if _, ok := err.(*serviceerror.InvalidArgument); ok {
			return handler.failCommand(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_CANCEL_TIMER_ATTRIBUTES, err)
		}
		return err
	}

	// timer deletion is a success, we may have deleted a fired timer in
	// which case we should reset hasBufferedEvents
	// TODO deletion of timer fired event refreshing hasBufferedEvents
	//  is not entirely correct, since during these commands processing, new event may appear
	handler.hasBufferedEvents = handler.mutableState.HasBufferedEvents()
	return nil
}

func (handler *workflowTaskHandlerImpl) handleCommandCancelWorkflow(
	attr *commandpb.CancelWorkflowExecutionCommandAttributes,
) error {

	handler.metricsClient.IncCounter(
		metrics.HistoryRespondWorkflowTaskCompletedScope,
		metrics.CommandTypeCancelWorkflowCounter,
	)

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
	namespaceID := namespace.ID(executionInfo.NamespaceId)
	targetNamespaceID := namespaceID
	if attr.GetNamespace() != "" {
		targetNamespaceEntry, err := handler.namespaceRegistry.GetNamespace(namespace.Name(attr.GetNamespace()))
		if err != nil {
			return serviceerror.NewUnavailable(fmt.Sprintf("Unable to cancel workflow across namespace: %v.", attr.GetNamespace()))
		}
		targetNamespaceID = targetNamespaceEntry.ID()
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

	namespaceName := handler.mutableState.GetNamespaceEntry().Name()

	if err := handler.validateCommandAttr(
		func() error {
			return handler.attrValidator.validateContinueAsNewWorkflowExecutionAttributes(
				namespaceName,
				attr,
				handler.mutableState.GetExecutionInfo(),
				handler.config.DefaultVisibilityIndexName,
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

	failWorkflow, err = handler.sizeLimitChecker.failWorkflowIfMemoSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION.String()),
		attr.GetMemo().Size(),
		"ContinueAsNewWorkflowExecutionCommandAttributes. Memo exceeds size limit.",
	)
	if err != nil || failWorkflow {
		handler.stopProcessing = true
		return err
	}

	failWorkflow, err = handler.sizeLimitChecker.failWorkflowIfSearchAttributesSizeExceedsLimit(
		attr.GetSearchAttributes(),
		namespaceName,
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION.String()),
	)
	if err != nil || failWorkflow {
		handler.stopProcessing = true
		return err
	}

	if err := searchattribute.SubstituteAliases(handler.searchAttributesMapper, attr.GetSearchAttributes(), namespaceName.String()); err != nil {
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

	// Extract parentNamespace, so it can be passed down to next run of workflow execution
	var parentNamespace namespace.Name
	if handler.mutableState.HasParentExecution() {
		parentNamespaceID := namespace.ID(handler.mutableState.GetExecutionInfo().ParentNamespaceId)
		parentNamespaceEntry, err := handler.namespaceRegistry.GetNamespaceByID(parentNamespaceID)
		if err != nil {
			return err
		}
		parentNamespace = parentNamespaceEntry.Name()
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

	handler.newStateBuilder = newStateBuilder
	return nil
}

func (handler *workflowTaskHandlerImpl) handleCommandStartChildWorkflow(
	attr *commandpb.StartChildWorkflowExecutionCommandAttributes,
) error {
	handler.metricsClient.IncCounter(
		metrics.HistoryRespondWorkflowTaskCompletedScope,
		metrics.CommandTypeChildWorkflowCounter,
	)

	parentNamespaceEntry := handler.mutableState.GetNamespaceEntry()
	parentNamespaceID := parentNamespaceEntry.ID()
	parentNamespace := parentNamespaceEntry.Name()
	targetNamespaceID := parentNamespaceID
	targetNamespace := parentNamespace
	if attr.GetNamespace() != "" {
		targetNamespaceEntry, err := handler.namespaceRegistry.GetNamespace(namespace.Name(attr.GetNamespace()))
		if err != nil {
			return serviceerror.NewUnavailable(fmt.Sprintf("Unable to schedule child execution across namespace %v.", attr.GetNamespace()))
		}
		targetNamespace = targetNamespaceEntry.Name()
		targetNamespaceID = targetNamespaceEntry.ID()
	} else {
		attr.Namespace = parentNamespace.String()
	}

	if err := handler.validateCommandAttr(
		func() error {
			return handler.attrValidator.validateStartChildExecutionAttributes(
				parentNamespaceID,
				targetNamespaceID,
				targetNamespace,
				attr,
				handler.mutableState.GetExecutionInfo(),
				handler.config.DefaultWorkflowTaskTimeout,
				handler.config.DefaultVisibilityIndexName,
			)
		},
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_START_CHILD_EXECUTION_ATTRIBUTES,
	); err != nil || handler.stopProcessing {
		return err
	}

	failWorkflow, err := handler.sizeLimitChecker.failWorkflowIfPayloadSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION.String()),
		attr.GetInput().Size(),
		"StartChildWorkflowExecutionCommandAttributes. Input exceeds size limit.",
	)
	if err != nil || failWorkflow {
		handler.stopProcessing = true
		return err
	}

	failWorkflow, err = handler.sizeLimitChecker.failWorkflowIfMemoSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION.String()),
		attr.GetMemo().Size(),
		"StartChildWorkflowExecutionCommandAttributes. Memo exceeds size limit.",
	)
	if err != nil || failWorkflow {
		handler.stopProcessing = true
		return err
	}

	failWorkflow, err = handler.sizeLimitChecker.failWorkflowIfSearchAttributesSizeExceedsLimit(
		attr.GetSearchAttributes(),
		targetNamespace,
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION.String()),
	)
	if err != nil || failWorkflow {
		handler.stopProcessing = true
		return err
	}

	if err := searchattribute.SubstituteAliases(handler.searchAttributesMapper, attr.GetSearchAttributes(), targetNamespace.String()); err != nil {
		handler.stopProcessing = true
		return err
	}

	enabled := handler.config.EnableParentClosePolicy(parentNamespace.String())
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
	namespaceID := namespace.ID(executionInfo.NamespaceId)
	targetNamespaceID := namespaceID
	if attr.GetNamespace() != "" {
		targetNamespaceEntry, err := handler.namespaceRegistry.GetNamespace(namespace.Name(attr.GetNamespace()))
		if err != nil {
			return serviceerror.NewUnavailable(fmt.Sprintf("Unable to signal workflow across namespace: %v.", attr.GetNamespace()))
		}
		targetNamespaceID = targetNamespaceEntry.ID()
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
	namespaceID := namespace.ID(executionInfo.NamespaceId)
	namespaceEntry, err := handler.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("Unable to get namespace for namespaceID: %v.", namespaceID))
	}
	namespace := namespaceEntry.Name()

	// valid search attributes for upsert
	if err := handler.validateCommandAttr(
		func() error {
			return handler.attrValidator.validateUpsertWorkflowSearchAttributes(
				namespace,
				attr,
				handler.config.DefaultVisibilityIndexName,
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

	failWorkflow, err = handler.sizeLimitChecker.failWorkflowIfSearchAttributesSizeExceedsLimit(
		attr.GetSearchAttributes(),
		namespace,
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES.String()),
	)
	if err != nil || failWorkflow {
		handler.stopProcessing = true
		return err
	}

	if err := searchattribute.SubstituteAliases(handler.searchAttributesMapper, attr.GetSearchAttributes(), namespace.String()); err != nil {
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

func (handler *workflowTaskHandlerImpl) handleRetry(
	backoffInterval time.Duration,
	retryState enumspb.RetryState,
	failure *failurepb.Failure,
	newRunID string,
) error {
	startEvent, err := handler.mutableState.GetStartEvent()
	if err != nil {
		return err
	}
	startAttr := startEvent.GetWorkflowExecutionStartedEventAttributes()

	newStateBuilder, err := createMutableState(
		handler.shard,
		handler.mutableState.GetNamespaceEntry(),
		newRunID,
	)
	if err != nil {
		return err
	}
	err = workflow.SetupNewWorkflowForRetryOrCron(
		handler.mutableState,
		newStateBuilder,
		newRunID,
		startAttr,
		nil,
		failure,
		backoffInterval,
		enumspb.CONTINUE_AS_NEW_INITIATOR_RETRY,
	)
	if err != nil {
		return err
	}

	handler.newStateBuilder = newStateBuilder
	return nil
}

func (handler *workflowTaskHandlerImpl) handleCron(
	backoffInterval time.Duration,
	lastCompletionResult *commonpb.Payloads,
	failure *failurepb.Failure,
	newRunID string,
) error {
	startEvent, err := handler.mutableState.GetStartEvent()
	if err != nil {
		return err
	}
	startAttr := startEvent.GetWorkflowExecutionStartedEventAttributes()

	if failure != nil {
		lastCompletionResult = startAttr.LastCompletionResult
	}

	newStateBuilder, err := createMutableState(
		handler.shard,
		handler.mutableState.GetNamespaceEntry(),
		newRunID,
	)
	if err != nil {
		return err
	}
	err = workflow.SetupNewWorkflowForRetryOrCron(
		handler.mutableState,
		newStateBuilder,
		newRunID,
		startAttr,
		lastCompletionResult,
		failure,
		backoffInterval,
		enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE,
	)
	if err != nil {
		return err
	}

	handler.newStateBuilder = newStateBuilder
	return nil
}

func (handler *workflowTaskHandlerImpl) validateCommandAttr(
	validationFn commandAttrValidationFn,
	failedCause enumspb.WorkflowTaskFailedCause,
) error {

	if err := validationFn(); err != nil {
		if _, ok := err.(*serviceerror.InvalidArgument); ok {
			return handler.failCommand(failedCause, err)
		}
		return err
	}

	return nil
}

func (handler *workflowTaskHandlerImpl) failCommand(
	failedCause enumspb.WorkflowTaskFailedCause,
	causeErr error,
) error {
	handler.workflowTaskFailedCause = NewWorkflowTaskFailedCause(failedCause, causeErr)
	handler.stopProcessing = true
	return nil
}

func NewWorkflowTaskFailedCause(failedCause enumspb.WorkflowTaskFailedCause, causeErr error) *workflowTaskFailedCause {
	return &workflowTaskFailedCause{
		failedCause: failedCause,
		causeErr:    causeErr,
	}
}

func (wtfc *workflowTaskFailedCause) Message() string {
	if wtfc.causeErr == nil {
		return wtfc.failedCause.String()
	}

	return fmt.Sprintf("%v: %v", wtfc.failedCause, wtfc.causeErr.Error())
}
