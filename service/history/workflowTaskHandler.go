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
	"time"

	"github.com/pborman/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/historyservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/enums"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

type (
	commandAttrValidationFn func() (enumspb.WorkflowTaskFailedCause, error)

	workflowTaskHandlerImpl struct {
		identity                string
		workflowTaskCompletedID int64

		// internal state
		hasBufferedEvents               bool
		workflowTaskFailedCause         *workflowTaskFailedCause
		activityNotStartedCancelled     bool
		newMutableState                 workflow.MutableState
		stopProcessing                  bool // should stop processing any more commands
		mutableState                    workflow.MutableState
		initiatedChildExecutionsInBatch map[string]struct{} // Set of initiated child executions in the workflow task

		// validation
		attrValidator          *commandAttrValidator
		sizeLimitChecker       *workflowSizeChecker
		searchAttributesMapper searchattribute.Mapper

		logger            log.Logger
		namespaceRegistry namespace.Registry
		metricsHandler    metrics.MetricsHandler
		config            *configs.Config
		shard             shard.Context
		tokenSerializer   common.TaskTokenSerializer
	}

	workflowTaskFailedCause struct {
		failedCause     enumspb.WorkflowTaskFailedCause
		causeErr        error
		workflowFailure *failurepb.Failure
	}

	workflowTaskResponseMutation func(
		resp *historyservice.RespondWorkflowTaskCompletedResponse,
	) error

	commandPostAction func(
		ctx context.Context,
	) (workflowTaskResponseMutation, error)

	handleCommandResponse struct {
		workflowTaskResponseMutation workflowTaskResponseMutation
		commandPostAction            commandPostAction
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
	metricsHandler metrics.MetricsHandler,
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
		newMutableState:                 nil,
		stopProcessing:                  false,
		mutableState:                    mutableState,
		initiatedChildExecutionsInBatch: make(map[string]struct{}),

		// validation
		attrValidator:          attrValidator,
		sizeLimitChecker:       sizeLimitChecker,
		searchAttributesMapper: searchAttributesMapper,

		logger:            logger,
		namespaceRegistry: namespaceRegistry,
		metricsHandler:    metricsHandler.WithTags(metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope)),
		config:            config,
		shard:             shard,
		tokenSerializer:   common.NewProtoTaskTokenSerializer(),
	}
}

func (handler *workflowTaskHandlerImpl) handleCommands(
	ctx context.Context,
	commands []*commandpb.Command,
) ([]workflowTaskResponseMutation, error) {
	if err := handler.attrValidator.validateCommandSequence(
		commands,
	); err != nil {
		return nil, err
	}

	var mutations []workflowTaskResponseMutation
	var postActions []commandPostAction
	for _, command := range commands {
		response, err := handler.handleCommand(ctx, command)
		if err != nil || handler.stopProcessing {
			return nil, err
		}
		if response != nil {
			if response.workflowTaskResponseMutation != nil {
				mutations = append(mutations, response.workflowTaskResponseMutation)
			}
			if response.commandPostAction != nil {
				postActions = append(postActions, response.commandPostAction)
			}
		}
	}

	for _, postAction := range postActions {
		mutation, err := postAction(ctx)
		if err != nil || handler.stopProcessing {
			return nil, err
		}
		if mutation != nil {
			mutations = append(mutations, mutation)
		}
	}

	return mutations, nil
}

func (handler *workflowTaskHandlerImpl) handleCommand(ctx context.Context, command *commandpb.Command) (*handleCommandResponse, error) {
	switch command.GetCommandType() {
	case enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK:
		return handler.handleCommandScheduleActivity(ctx, command.GetScheduleActivityTaskCommandAttributes())

	case enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION:
		return nil, handler.handleCommandCompleteWorkflow(ctx, command.GetCompleteWorkflowExecutionCommandAttributes())

	case enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION:
		return nil, handler.handleCommandFailWorkflow(ctx, command.GetFailWorkflowExecutionCommandAttributes())

	case enumspb.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION:
		return nil, handler.handleCommandCancelWorkflow(ctx, command.GetCancelWorkflowExecutionCommandAttributes())

	case enumspb.COMMAND_TYPE_START_TIMER:
		return nil, handler.handleCommandStartTimer(ctx, command.GetStartTimerCommandAttributes())

	case enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK:
		return nil, handler.handleCommandRequestCancelActivity(ctx, command.GetRequestCancelActivityTaskCommandAttributes())

	case enumspb.COMMAND_TYPE_CANCEL_TIMER:
		return nil, handler.handleCommandCancelTimer(ctx, command.GetCancelTimerCommandAttributes())

	case enumspb.COMMAND_TYPE_RECORD_MARKER:
		return nil, handler.handleCommandRecordMarker(ctx, command.GetRecordMarkerCommandAttributes())

	case enumspb.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION:
		return nil, handler.handleCommandRequestCancelExternalWorkflow(ctx, command.GetRequestCancelExternalWorkflowExecutionCommandAttributes())

	case enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION:
		return nil, handler.handleCommandSignalExternalWorkflow(ctx, command.GetSignalExternalWorkflowExecutionCommandAttributes())

	case enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION:
		return nil, handler.handleCommandContinueAsNewWorkflow(ctx, command.GetContinueAsNewWorkflowExecutionCommandAttributes())

	case enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION:
		return nil, handler.handleCommandStartChildWorkflow(ctx, command.GetStartChildWorkflowExecutionCommandAttributes())

	case enumspb.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
		return nil, handler.handleCommandUpsertWorkflowSearchAttributes(ctx, command.GetUpsertWorkflowSearchAttributesCommandAttributes())

	case enumspb.COMMAND_TYPE_MODIFY_WORKFLOW_PROPERTIES:
		return nil, handler.handleCommandModifyWorkflowProperties(ctx, command.GetModifyWorkflowPropertiesCommandAttributes())

	default:
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("Unknown command type: %v", command.GetCommandType()))
	}
}

func (handler *workflowTaskHandlerImpl) handleCommandScheduleActivity(
	_ context.Context,
	attr *commandpb.ScheduleActivityTaskCommandAttributes,
) (*handleCommandResponse, error) {

	handler.metricsHandler.Counter(metrics.CommandTypeScheduleActivityCounter.GetMetricName()).Record(1)

	executionInfo := handler.mutableState.GetExecutionInfo()
	namespaceID := namespace.ID(executionInfo.NamespaceId)

	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.validateActivityScheduleAttributes(
				namespaceID,
				attr,
				timestamp.DurationValue(executionInfo.WorkflowRunTimeout),
			)
		},
	); err != nil || handler.stopProcessing {
		return nil, err
	}

	if err := handler.sizeLimitChecker.checkIfPayloadSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK.String()),
		attr.GetInput().Size(),
		"ScheduleActivityTaskCommandAttributes.Input exceeds size limit.",
	); err != nil {
		return nil, handler.failWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES, err)
	}
	if err := handler.sizeLimitChecker.checkIfNumPendingActivitiesExceedsLimit(); err != nil {
		return nil, handler.failCommand(enumspb.WORKFLOW_TASK_FAILED_CAUSE_PENDING_ACTIVITIES_LIMIT_EXCEEDED, err)
	}

	enums.SetDefaultTaskQueueKind(&attr.GetTaskQueue().Kind)

	eagerStartActivity := false
	namespace := handler.mutableState.GetNamespaceEntry().Name().String()
	if attr.RequestEagerExecution && handler.config.EnableActivityEagerExecution(namespace) {
		eagerStartActivity = true
	}

	_, _, err := handler.mutableState.AddActivityTaskScheduledEvent(
		handler.workflowTaskCompletedID,
		attr,
		eagerStartActivity,
	)
	if err != nil {
		if _, ok := err.(*serviceerror.InvalidArgument); ok {
			return nil, handler.failCommand(enumspb.WORKFLOW_TASK_FAILED_CAUSE_SCHEDULE_ACTIVITY_DUPLICATE_ID, err)
		}
		return nil, err
	}

	if !eagerStartActivity {
		return &handleCommandResponse{}, nil
	}

	return &handleCommandResponse{
		commandPostAction: func(ctx context.Context) (workflowTaskResponseMutation, error) {
			return handler.handlePostCommandEagerExecuteActivity(ctx, attr)
		},
	}, nil
}

func (handler *workflowTaskHandlerImpl) handlePostCommandEagerExecuteActivity(
	_ context.Context,
	attr *commandpb.ScheduleActivityTaskCommandAttributes,
) (workflowTaskResponseMutation, error) {
	ai, ok := handler.mutableState.GetActivityByActivityID(attr.ActivityId)
	if !ok {
		// activity cancelled in the same worflow task
		return nil, nil
	}

	if _, err := handler.mutableState.AddActivityTaskStartedEvent(
		ai,
		ai.GetScheduledEventId(),
		uuid.New(),
		handler.identity,
	); err != nil {
		return nil, err
	}

	executionInfo := handler.mutableState.GetExecutionInfo()
	namespaceID := namespace.ID(executionInfo.NamespaceId)
	runID := handler.mutableState.GetExecutionState().RunId

	shardClock, err := handler.shard.NewVectorClock()
	if err != nil {
		return nil, err
	}

	taskToken := &tokenspb.Task{
		NamespaceId:      namespaceID.String(),
		WorkflowId:       executionInfo.WorkflowId,
		RunId:            runID,
		ScheduledEventId: ai.GetScheduledEventId(),
		Attempt:          ai.Attempt,
		ActivityId:       attr.ActivityId,
		ActivityType:     attr.ActivityType.GetName(),
		Clock:            shardClock,
	}
	serializedToken, err := handler.tokenSerializer.Serialize(taskToken)
	if err != nil {
		return nil, err
	}

	activityTask := &workflowservice.PollActivityTaskQueueResponse{
		ActivityId:   attr.ActivityId,
		ActivityType: attr.ActivityType,
		Header:       attr.Header,
		Input:        attr.Input,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: executionInfo.WorkflowId,
			RunId:      runID,
		},
		CurrentAttemptScheduledTime: ai.ScheduledTime,
		ScheduledTime:               ai.ScheduledTime,
		ScheduleToCloseTimeout:      attr.ScheduleToCloseTimeout,
		StartedTime:                 ai.StartedTime,
		StartToCloseTimeout:         attr.StartToCloseTimeout,
		HeartbeatTimeout:            attr.HeartbeatTimeout,
		TaskToken:                   serializedToken,
		Attempt:                     ai.Attempt,
		HeartbeatDetails:            ai.LastHeartbeatDetails,
		WorkflowType:                handler.mutableState.GetWorkflowType(),
		WorkflowNamespace:           handler.mutableState.GetNamespaceEntry().Name().String(),
	}
	handler.metricsHandler.Counter(
		metrics.ActivityEagerExecutionCounter.GetMetricName(),
	).Record(
		1,
		metrics.NamespaceTag(string(handler.mutableState.GetNamespaceEntry().Name())),
		metrics.TaskQueueTag(ai.TaskQueue),
	)

	return func(resp *historyservice.RespondWorkflowTaskCompletedResponse) error {
		resp.ActivityTasks = append(resp.ActivityTasks, activityTask)
		return nil
	}, nil
}

func (handler *workflowTaskHandlerImpl) handleCommandRequestCancelActivity(
	_ context.Context,
	attr *commandpb.RequestCancelActivityTaskCommandAttributes,
) error {

	handler.metricsHandler.Counter(metrics.CommandTypeCancelActivityCounter.GetMetricName()).Record(1)

	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.validateActivityCancelAttributes(attr)
		},
	); err != nil || handler.stopProcessing {
		return err
	}

	scheduledEventID := attr.GetScheduledEventId()
	actCancelReqEvent, ai, err := handler.mutableState.AddActivityTaskCancelRequestedEvent(
		handler.workflowTaskCompletedID,
		scheduledEventID,
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

		if ai.StartedEventId == common.EmptyEventID {
			// We haven't started the activity yet, we can cancel the activity right away and
			// schedule a workflow task to ensure the workflow makes progress.
			_, err = handler.mutableState.AddActivityTaskCanceledEvent(
				ai.ScheduledEventId,
				ai.StartedEventId,
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
	_ context.Context,
	attr *commandpb.StartTimerCommandAttributes,
) error {

	handler.metricsHandler.Counter(metrics.CommandTypeStartTimerCounter.GetMetricName()).Record(1)

	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.validateTimerScheduleAttributes(attr)
		},
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
	ctx context.Context,
	attr *commandpb.CompleteWorkflowExecutionCommandAttributes,
) error {

	handler.metricsHandler.Counter(metrics.CommandTypeCompleteWorkflowCounter.GetMetricName()).Record(1)

	if handler.hasBufferedEvents {
		return handler.failCommand(enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND, nil)
	}

	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.validateCompleteWorkflowExecutionAttributes(attr)
		},
	); err != nil || handler.stopProcessing {
		return err
	}

	if err := handler.sizeLimitChecker.checkIfPayloadSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION.String()),
		attr.GetResult().Size(),
		"CompleteWorkflowExecutionCommandAttributes.Result exceeds size limit.",
	); err != nil {
		return handler.failWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES, err)
	}

	// If the workflow task has more than one completion event than just pick the first one
	if !handler.mutableState.IsWorkflowExecutionRunning() {
		handler.metricsHandler.Counter(metrics.MultipleCompletionCommandsCounter.GetMetricName()).Record(1)
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
	_, err := handler.mutableState.AddCompletedWorkflowEvent(handler.workflowTaskCompletedID, attr, newExecutionRunID)
	if err != nil {
		return err
	}

	// Check if this workflow has a cron schedule
	if cronBackoff != backoff.NoBackoff {
		return handler.handleCron(ctx, cronBackoff, attr.GetResult(), nil, newExecutionRunID)
	}

	return nil
}

func (handler *workflowTaskHandlerImpl) handleCommandFailWorkflow(
	ctx context.Context,
	attr *commandpb.FailWorkflowExecutionCommandAttributes,
) error {

	handler.metricsHandler.Counter(metrics.CommandTypeFailWorkflowCounter.GetMetricName()).Record(1)

	if handler.hasBufferedEvents {
		return handler.failCommand(enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND, nil)
	}

	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.validateFailWorkflowExecutionAttributes(attr)
		},
	); err != nil || handler.stopProcessing {
		return err
	}

	err := handler.sizeLimitChecker.checkIfPayloadSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION.String()),
		attr.GetFailure().Size(),
		"FailWorkflowExecutionCommandAttributes.Failure exceeds size limit.",
	)
	if err != nil {
		return handler.failWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_FAIL_WORKFLOW_EXECUTION_ATTRIBUTES, err)
	}

	// If the workflow task has more than one completion event than just pick the first one
	if !handler.mutableState.IsWorkflowExecutionRunning() {
		handler.metricsHandler.Counter(metrics.MultipleCompletionCommandsCounter.GetMetricName()).Record(1)
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
		return handler.handleRetry(ctx, retryBackoff, retryState, attr.GetFailure(), newExecutionRunID)
	} else if cronBackoff != backoff.NoBackoff {
		return handler.handleCron(ctx, cronBackoff, nil, attr.GetFailure(), newExecutionRunID)
	}

	// No retry or cron
	return nil
}

func (handler *workflowTaskHandlerImpl) handleCommandCancelTimer(
	_ context.Context,
	attr *commandpb.CancelTimerCommandAttributes,
) error {

	handler.metricsHandler.Counter(metrics.CommandTypeCancelTimerCounter.GetMetricName()).Record(1)

	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.validateTimerCancelAttributes(attr)
		},
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
	_ context.Context,
	attr *commandpb.CancelWorkflowExecutionCommandAttributes,
) error {

	handler.metricsHandler.Counter(metrics.CommandTypeCancelWorkflowCounter.GetMetricName()).Record(1)

	if handler.hasBufferedEvents {
		return handler.failCommand(enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND, nil)
	}

	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.validateCancelWorkflowExecutionAttributes(attr)
		},
	); err != nil || handler.stopProcessing {
		return err
	}

	// If the workflow task has more than one completion event than just pick the first one
	if !handler.mutableState.IsWorkflowExecutionRunning() {
		handler.metricsHandler.Counter(metrics.MultipleCompletionCommandsCounter.GetMetricName()).Record(1)
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
	_ context.Context,
	attr *commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes,
) error {

	handler.metricsHandler.Counter(metrics.CommandTypeCancelExternalWorkflowCounter.GetMetricName()).Record(1)

	executionInfo := handler.mutableState.GetExecutionInfo()
	namespaceID := namespace.ID(executionInfo.NamespaceId)
	targetNamespaceID := namespaceID
	if attr.GetNamespace() != "" {
		targetNamespaceEntry, err := handler.namespaceRegistry.GetNamespace(namespace.Name(attr.GetNamespace()))
		if err != nil {
			return err
		}
		targetNamespaceID = targetNamespaceEntry.ID()
	}

	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.validateCancelExternalWorkflowExecutionAttributes(
				namespaceID,
				targetNamespaceID,
				handler.initiatedChildExecutionsInBatch,
				attr,
			)
		},
	); err != nil || handler.stopProcessing {
		return err
	}
	if err := handler.sizeLimitChecker.checkIfNumPendingCancelRequestsExceedsLimit(); err != nil {
		return handler.failCommand(enumspb.WORKFLOW_TASK_FAILED_CAUSE_PENDING_REQUEST_CANCEL_LIMIT_EXCEEDED, err)
	}

	cancelRequestID := uuid.New()
	_, _, err := handler.mutableState.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(
		handler.workflowTaskCompletedID, cancelRequestID, attr, targetNamespaceID,
	)

	return err
}

func (handler *workflowTaskHandlerImpl) handleCommandRecordMarker(
	_ context.Context,
	attr *commandpb.RecordMarkerCommandAttributes,
) error {

	handler.metricsHandler.Counter(metrics.CommandTypeRecordMarkerCounter.GetMetricName()).Record(1)

	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.validateRecordMarkerAttributes(attr)
		},
	); err != nil || handler.stopProcessing {
		return err
	}

	if err := handler.sizeLimitChecker.checkIfPayloadSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_RECORD_MARKER.String()),
		common.GetPayloadsMapSize(attr.GetDetails()),
		"RecordMarkerCommandAttributes.Details exceeds size limit.",
	); err != nil {
		return handler.failWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_RECORD_MARKER_ATTRIBUTES, err)
	}

	_, err := handler.mutableState.AddRecordMarkerEvent(handler.workflowTaskCompletedID, attr)
	return err
}

func (handler *workflowTaskHandlerImpl) handleCommandContinueAsNewWorkflow(
	ctx context.Context,
	attr *commandpb.ContinueAsNewWorkflowExecutionCommandAttributes,
) error {

	handler.metricsHandler.Counter(metrics.CommandTypeContinueAsNewCounter.GetMetricName()).Record(1)

	if handler.hasBufferedEvents {
		return handler.failCommand(enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND, nil)
	}

	namespaceName := handler.mutableState.GetNamespaceEntry().Name()

	unaliasedSas, err := searchattribute.UnaliasFields(
		handler.searchAttributesMapper,
		attr.GetSearchAttributes(),
		namespaceName.String(),
	)
	if err != nil {
		handler.stopProcessing = true
		return err
	}
	if unaliasedSas != nil {
		newAttr := *attr
		newAttr.SearchAttributes = unaliasedSas
		attr = &newAttr
	}

	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.validateContinueAsNewWorkflowExecutionAttributes(
				namespaceName,
				attr,
				handler.mutableState.GetExecutionInfo(),
				handler.config.DefaultVisibilityIndexName,
			)
		},
	); err != nil || handler.stopProcessing {
		return err
	}

	if err := handler.sizeLimitChecker.checkIfPayloadSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION.String()),
		attr.GetInput().Size(),
		"ContinueAsNewWorkflowExecutionCommandAttributes. Input exceeds size limit.",
	); err != nil {
		return handler.failWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_CONTINUE_AS_NEW_ATTRIBUTES, err)
	}

	if err := handler.sizeLimitChecker.checkIfMemoSizeExceedsLimit(
		attr.GetMemo(),
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION.String()),
		"ContinueAsNewWorkflowExecutionCommandAttributes. Memo exceeds size limit.",
	); err != nil {
		return handler.failWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_CONTINUE_AS_NEW_ATTRIBUTES, err)
	}

	// search attribute validation must be done after unaliasing keys
	if err := handler.sizeLimitChecker.checkIfSearchAttributesSizeExceedsLimit(
		attr.GetSearchAttributes(),
		namespaceName,
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION.String()),
	); err != nil {
		return handler.failWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_CONTINUE_AS_NEW_ATTRIBUTES, err)
	}

	// If the workflow task has more than one completion event than just pick the first one
	if !handler.mutableState.IsWorkflowExecutionRunning() {
		handler.metricsHandler.Counter(metrics.MultipleCompletionCommandsCounter.GetMetricName()).Record(1)
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
		if err == nil {
			parentNamespace = parentNamespaceEntry.Name()
		}
	}

	_, newMutableState, err := handler.mutableState.AddContinueAsNewEvent(
		ctx,
		handler.workflowTaskCompletedID,
		handler.workflowTaskCompletedID,
		parentNamespace,
		attr,
	)
	if err != nil {
		return err
	}

	handler.newMutableState = newMutableState
	return nil
}

func (handler *workflowTaskHandlerImpl) handleCommandStartChildWorkflow(
	_ context.Context,
	attr *commandpb.StartChildWorkflowExecutionCommandAttributes,
) error {

	handler.metricsHandler.Counter(metrics.CommandTypeChildWorkflowCounter.GetMetricName()).Record(1)

	parentNamespaceEntry := handler.mutableState.GetNamespaceEntry()
	parentNamespaceID := parentNamespaceEntry.ID()
	parentNamespace := parentNamespaceEntry.Name()
	targetNamespaceID := parentNamespaceID
	targetNamespace := parentNamespace
	if attr.GetNamespace() != "" {
		targetNamespaceEntry, err := handler.namespaceRegistry.GetNamespace(namespace.Name(attr.GetNamespace()))
		if err != nil {
			return err
		}
		targetNamespace = targetNamespaceEntry.Name()
		targetNamespaceID = targetNamespaceEntry.ID()
	} else {
		attr.Namespace = parentNamespace.String()
	}

	unaliasedSas, err := searchattribute.UnaliasFields(
		handler.searchAttributesMapper,
		attr.GetSearchAttributes(),
		targetNamespace.String(),
	)
	if err != nil {
		handler.stopProcessing = true
		return err
	}
	if unaliasedSas != nil {
		newAttr := *attr
		newAttr.SearchAttributes = unaliasedSas
		attr = &newAttr
	}

	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
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
	); err != nil || handler.stopProcessing {
		return err
	}

	if err := handler.sizeLimitChecker.checkIfPayloadSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION.String()),
		attr.GetInput().Size(),
		"StartChildWorkflowExecutionCommandAttributes. Input exceeds size limit.",
	); err != nil {
		return handler.failWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_START_CHILD_EXECUTION_ATTRIBUTES, err)
	}

	if err := handler.sizeLimitChecker.checkIfMemoSizeExceedsLimit(
		attr.GetMemo(),
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION.String()),
		"StartChildWorkflowExecutionCommandAttributes.Memo exceeds size limit.",
	); err != nil {
		return handler.failWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_START_CHILD_EXECUTION_ATTRIBUTES, err)
	}

	// search attribute validation must be done after unaliasing keys
	if err := handler.sizeLimitChecker.checkIfSearchAttributesSizeExceedsLimit(
		attr.GetSearchAttributes(),
		targetNamespace,
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION.String()),
	); err != nil {
		return handler.failWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_START_CHILD_EXECUTION_ATTRIBUTES, err)
	}

	// child workflow limit
	if err := handler.sizeLimitChecker.checkIfNumChildWorkflowsExceedsLimit(); err != nil {
		return handler.failCommand(enumspb.WORKFLOW_TASK_FAILED_CAUSE_PENDING_CHILD_WORKFLOWS_LIMIT_EXCEEDED, err)
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
		handler.workflowTaskCompletedID, requestID, attr, targetNamespaceID,
	)
	if err == nil {
		// Keep track of all child initiated commands in this workflow task to validate request cancel commands
		handler.initiatedChildExecutionsInBatch[attr.GetWorkflowId()] = struct{}{}
	}
	return err
}

func (handler *workflowTaskHandlerImpl) handleCommandSignalExternalWorkflow(
	_ context.Context,
	attr *commandpb.SignalExternalWorkflowExecutionCommandAttributes,
) error {

	handler.metricsHandler.Counter(metrics.CommandTypeSignalExternalWorkflowCounter.GetMetricName()).Record(1)

	executionInfo := handler.mutableState.GetExecutionInfo()
	namespaceID := namespace.ID(executionInfo.NamespaceId)
	targetNamespaceID := namespaceID
	if attr.GetNamespace() != "" {
		targetNamespaceEntry, err := handler.namespaceRegistry.GetNamespace(namespace.Name(attr.GetNamespace()))
		if err != nil {
			return err
		}
		targetNamespaceID = targetNamespaceEntry.ID()
	}

	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.validateSignalExternalWorkflowExecutionAttributes(
				namespaceID,
				targetNamespaceID,
				attr,
			)
		},
	); err != nil || handler.stopProcessing {
		return err
	}
	if err := handler.sizeLimitChecker.checkIfNumPendingSignalsExceedsLimit(); err != nil {
		return handler.failCommand(enumspb.WORKFLOW_TASK_FAILED_CAUSE_PENDING_SIGNALS_LIMIT_EXCEEDED, err)
	}

	if err := handler.sizeLimitChecker.checkIfPayloadSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION.String()),
		attr.GetInput().Size(),
		"SignalExternalWorkflowExecutionCommandAttributes.Input exceeds size limit.",
	); err != nil {
		return handler.failWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SIGNAL_WORKFLOW_EXECUTION_ATTRIBUTES, err)
	}

	signalRequestID := uuid.New() // for deduplicate
	_, _, err := handler.mutableState.AddSignalExternalWorkflowExecutionInitiatedEvent(
		handler.workflowTaskCompletedID, signalRequestID, attr, targetNamespaceID,
	)
	return err
}

func (handler *workflowTaskHandlerImpl) handleCommandUpsertWorkflowSearchAttributes(
	_ context.Context,
	attr *commandpb.UpsertWorkflowSearchAttributesCommandAttributes,
) error {

	handler.metricsHandler.Counter(metrics.CommandTypeUpsertWorkflowSearchAttributesCounter.GetMetricName()).Record(1)

	// get namespace name
	executionInfo := handler.mutableState.GetExecutionInfo()
	namespaceID := namespace.ID(executionInfo.NamespaceId)
	namespaceEntry, err := handler.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("Unable to get namespace for namespaceID: %v.", namespaceID))
	}
	namespace := namespaceEntry.Name()

	unaliasedSas, err := searchattribute.UnaliasFields(
		handler.searchAttributesMapper,
		attr.GetSearchAttributes(),
		namespace.String(),
	)
	if err != nil {
		handler.stopProcessing = true
		return err
	}
	if unaliasedSas != nil {
		newAttr := *attr
		newAttr.SearchAttributes = unaliasedSas
		attr = &newAttr
	}

	// valid search attributes for upsert
	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.validateUpsertWorkflowSearchAttributes(
				namespace,
				attr,
				handler.config.DefaultVisibilityIndexName,
			)
		},
	); err != nil || handler.stopProcessing {
		return err
	}

	// blob size limit check
	if err := handler.sizeLimitChecker.checkIfPayloadSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES.String()),
		payloadsMapSize(attr.GetSearchAttributes().GetIndexedFields()),
		"UpsertWorkflowSearchAttributesCommandAttributes exceeds size limit.",
	); err != nil {
		return handler.failWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES, err)
	}

	// new search attributes size limit check
	// search attribute validation must be done after unaliasing keys
	err = handler.sizeLimitChecker.checkIfSearchAttributesSizeExceedsLimit(
		&commonpb.SearchAttributes{
			IndexedFields: payload.MergeMapOfPayload(
				executionInfo.SearchAttributes,
				attr.GetSearchAttributes().GetIndexedFields(),
			),
		},
		namespace,
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES.String()),
	)
	if err != nil {
		return handler.failWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES, err)
	}

	_, err = handler.mutableState.AddUpsertWorkflowSearchAttributesEvent(
		handler.workflowTaskCompletedID, attr,
	)
	return err
}

func (handler *workflowTaskHandlerImpl) handleCommandModifyWorkflowProperties(
	_ context.Context,
	attr *commandpb.ModifyWorkflowPropertiesCommandAttributes,
) error {

	handler.metricsHandler.Counter(metrics.CommandTypeModifyWorkflowPropertiesCounter.GetMetricName()).Record(1)

	// get namespace name
	executionInfo := handler.mutableState.GetExecutionInfo()
	namespaceID := namespace.ID(executionInfo.NamespaceId)
	namespaceEntry, err := handler.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("Unable to get namespace for namespaceID: %v.", namespaceID))
	}
	namespace := namespaceEntry.Name()

	// valid properties
	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.validateModifyWorkflowProperties(namespace, attr)
		},
	); err != nil || handler.stopProcessing {
		return err
	}

	// blob size limit check
	if err := handler.sizeLimitChecker.checkIfPayloadSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_MODIFY_WORKFLOW_PROPERTIES.String()),
		payloadsMapSize(attr.GetUpsertedMemo().GetFields()),
		"ModifyWorkflowPropertiesCommandAttributes exceeds size limit.",
	); err != nil {
		return handler.failWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_MODIFY_WORKFLOW_PROPERTIES_ATTRIBUTES, err)
	}

	// new memo size limit check
	err = handler.sizeLimitChecker.checkIfMemoSizeExceedsLimit(
		&commonpb.Memo{
			Fields: payload.MergeMapOfPayload(executionInfo.Memo, attr.GetUpsertedMemo().GetFields()),
		},
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_MODIFY_WORKFLOW_PROPERTIES.String()),
		"ModifyWorkflowPropertiesCommandAttributes. Memo exceeds size limit.",
	)
	if err != nil {
		return handler.failWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_MODIFY_WORKFLOW_PROPERTIES_ATTRIBUTES, err)
	}

	_, err = handler.mutableState.AddWorkflowPropertiesModifiedEvent(
		handler.workflowTaskCompletedID, attr,
	)
	return err
}

func payloadsMapSize(fields map[string]*commonpb.Payload) int {
	result := 0

	for k, v := range fields {
		result += len(k)
		result += len(v.GetData())
	}
	return result
}

func (handler *workflowTaskHandlerImpl) handleRetry(
	ctx context.Context,
	backoffInterval time.Duration,
	retryState enumspb.RetryState,
	failure *failurepb.Failure,
	newRunID string,
) error {
	startEvent, err := handler.mutableState.GetStartEvent(ctx)
	if err != nil {
		return err
	}
	startAttr := startEvent.GetWorkflowExecutionStartedEventAttributes()

	newMutableState := workflow.NewMutableState(
		handler.shard,
		handler.shard.GetEventsCache(),
		handler.shard.GetLogger(),
		handler.mutableState.GetNamespaceEntry(),
		handler.shard.GetTimeSource().Now(),
	)

	err = workflow.SetupNewWorkflowForRetryOrCron(
		ctx,
		handler.mutableState,
		newMutableState,
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

	err = newMutableState.SetHistoryTree(
		ctx,
		newMutableState.GetExecutionInfo().WorkflowExecutionTimeout,
		newMutableState.GetExecutionInfo().WorkflowRunTimeout,
		newRunID,
	)
	if err != nil {
		return err
	}

	handler.newMutableState = newMutableState
	return nil
}

func (handler *workflowTaskHandlerImpl) handleCron(
	ctx context.Context,
	backoffInterval time.Duration,
	lastCompletionResult *commonpb.Payloads,
	failure *failurepb.Failure,
	newRunID string,
) error {
	startEvent, err := handler.mutableState.GetStartEvent(ctx)
	if err != nil {
		return err
	}
	startAttr := startEvent.GetWorkflowExecutionStartedEventAttributes()

	if failure != nil {
		lastCompletionResult = startAttr.LastCompletionResult
	}

	newMutableState := workflow.NewMutableState(
		handler.shard,
		handler.shard.GetEventsCache(),
		handler.shard.GetLogger(),
		handler.mutableState.GetNamespaceEntry(),
		handler.shard.GetTimeSource().Now(),
	)

	err = workflow.SetupNewWorkflowForRetryOrCron(
		ctx,
		handler.mutableState,
		newMutableState,
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

	err = newMutableState.SetHistoryTree(
		ctx,
		newMutableState.GetExecutionInfo().WorkflowExecutionTimeout,
		newMutableState.GetExecutionInfo().WorkflowRunTimeout,
		newRunID,
	)
	if err != nil {
		return err
	}

	handler.newMutableState = newMutableState
	return nil
}

func (handler *workflowTaskHandlerImpl) validateCommandAttr(
	validationFn commandAttrValidationFn,
) error {

	if failedCause, err := validationFn(); err != nil {
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

func (handler *workflowTaskHandlerImpl) failWorkflow(
	failedCause enumspb.WorkflowTaskFailedCause,
	causeErr error,
) error {
	handler.workflowTaskFailedCause = &workflowTaskFailedCause{
		failedCause:     failedCause,
		causeErr:        causeErr,
		workflowFailure: failure.NewServerFailure(causeErr.Error(), true),
	}
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
