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
	"errors"
	"fmt"
	"time"

	"github.com/pborman/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"

	"go.temporal.io/server/common/definition"

	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/internal/effect"
	"go.temporal.io/server/internal/protocol"
	"go.temporal.io/server/service/history/workflow/update"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/enums"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

const (
	activityCancellationMsgActivityNotStarted = "ACTIVITY_ID_NOT_STARTED"
)

type (
	commandAttrValidationFn func() (enumspb.WorkflowTaskFailedCause, error)

	workflowTaskCompletedHandler struct {
		identity                string
		workflowTaskCompletedID int64

		// internal state
		hasBufferedEventsOrMessages     bool
		workflowTaskFailedCause         *workflowTaskFailedCause
		activityNotStartedCancelled     bool
		newMutableState                 workflow.MutableState
		stopProcessing                  bool // should stop processing any more commands
		mutableState                    workflow.MutableState
		effects                         effect.Controller
		initiatedChildExecutionsInBatch map[string]struct{} // Set of initiated child executions in the workflow task
		updateRegistry                  update.Registry

		// validation
		attrValidator                  *commandAttrValidator
		sizeLimitChecker               *workflowSizeChecker
		searchAttributesMapperProvider searchattribute.MapperProvider

		logger                 log.Logger
		namespaceRegistry      namespace.Registry
		metricsHandler         metrics.Handler
		config                 *configs.Config
		shard                  shard.Context
		tokenSerializer        common.TaskTokenSerializer
		commandHandlerRegistry *workflow.CommandHandlerRegistry
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

func newWorkflowTaskCompletedHandler(
	identity string,
	workflowTaskCompletedID int64,
	mutableState workflow.MutableState,
	updateRegistry update.Registry,
	effects effect.Controller,
	attrValidator *commandAttrValidator,
	sizeLimitChecker *workflowSizeChecker,
	logger log.Logger,
	namespaceRegistry namespace.Registry,
	metricsHandler metrics.Handler,
	config *configs.Config,
	shard shard.Context,
	searchAttributesMapperProvider searchattribute.MapperProvider,
	hasBufferedEventsOrMessages bool,
	commandHandlerRegistry *workflow.CommandHandlerRegistry,
) *workflowTaskCompletedHandler {
	return &workflowTaskCompletedHandler{
		identity:                identity,
		workflowTaskCompletedID: workflowTaskCompletedID,

		// internal state
		hasBufferedEventsOrMessages:     hasBufferedEventsOrMessages,
		workflowTaskFailedCause:         nil,
		activityNotStartedCancelled:     false,
		newMutableState:                 nil,
		stopProcessing:                  false,
		mutableState:                    mutableState,
		effects:                         effects,
		initiatedChildExecutionsInBatch: make(map[string]struct{}),
		updateRegistry:                  updateRegistry,

		// validation
		attrValidator:                  attrValidator,
		sizeLimitChecker:               sizeLimitChecker,
		searchAttributesMapperProvider: searchAttributesMapperProvider,

		logger:            logger,
		namespaceRegistry: namespaceRegistry,
		metricsHandler: metricsHandler.WithTags(
			metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope),
			metrics.NamespaceTag(mutableState.GetNamespaceEntry().Name().String()),
		),
		config:                 config,
		shard:                  shard,
		tokenSerializer:        common.NewProtoTaskTokenSerializer(),
		commandHandlerRegistry: commandHandlerRegistry,
	}
}

func (handler *workflowTaskCompletedHandler) handleCommands(
	ctx context.Context,
	commands []*commandpb.Command,
	msgs *collection.IndexedTakeList[string, *protocolpb.Message],
) ([]workflowTaskResponseMutation, error) {
	if err := handler.attrValidator.validateCommandSequence(
		commands,
	); err != nil {
		return nil, err
	}

	var mutations []workflowTaskResponseMutation
	var postActions []commandPostAction
	for _, command := range commands {
		response, err := handler.handleCommand(ctx, command, msgs)
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

	// For every update.Acceptance and update.Respond (i.e. update completion) messages
	// there should be a corresponding PROTOCOL_MESSAGE command. These messages are processed together
	// with this command and, at this point, should be processed already.
	// Therefore, remaining messages should be only update.Rejection.
	// However, PROTOCOL_MESSAGE command is not required by server. If it is not present,
	// update.Acceptance and update.Respond messages will be processed after all commands in order they are in request.
	for _, msg := range msgs.TakeRemaining() {
		err := handler.handleMessage(ctx, msg)
		if err != nil || handler.stopProcessing {
			return nil, err
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

func (handler *workflowTaskCompletedHandler) rejectUnprocessedUpdates(
	ctx context.Context,
	workflowTaskScheduledEventID int64,
	wtHeartbeat bool,
	wfKey definition.WorkflowKey,
	workerIdentity string,
) error {

	// If server decided to fail WT (instead of completing), don't reject updates.
	// New WT will be created, and it will deliver these updates again to the worker.
	// Worker will do full history replay, and updates should be delivered again.
	if handler.workflowTaskFailedCause != nil {
		return nil
	}

	// If WT is a heartbeat WT, then it doesn't have to have messages.
	if wtHeartbeat {
		return nil
	}

	// If worker has just completed workflow with one of the WF completion command,
	// then it might skip processing some updates. In this case, it doesn't indicate old SDK or bug.
	// All unprocessed updates will be rejected with "workflow is closing" reason though.
	if !handler.mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	rejectedUpdateIDs, err := handler.updateRegistry.RejectUnprocessed(
		ctx,
		handler.effects)

	if err != nil {
		return err
	}

	if len(rejectedUpdateIDs) > 0 {
		handler.logger.Warn(
			"Workflow task completed w/o processing updates.",
			tag.WorkflowNamespaceID(wfKey.NamespaceID),
			tag.WorkflowID(wfKey.WorkflowID),
			tag.WorkflowRunID(wfKey.RunID),
			tag.WorkflowEventID(workflowTaskScheduledEventID),
			tag.NewStringTag("worker-identity", workerIdentity),
			tag.NewStringsTag("update-ids", rejectedUpdateIDs),
		)
	}

	// At this point there must not be any updates in a Sent state.
	// All updates which were sent on this WT are processed by worker or rejected by server.
	return nil
}

//revive:disable:cyclomatic grandfathered
func (handler *workflowTaskCompletedHandler) handleCommand(
	ctx context.Context,
	command *commandpb.Command,
	msgs *collection.IndexedTakeList[string, *protocolpb.Message],
) (*handleCommandResponse, error) {

	metrics.CommandCounter.With(handler.metricsHandler).
		Record(1, metrics.CommandTypeTag(command.GetCommandType().String()))

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

	case enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE:
		return nil, handler.handleCommandProtocolMessage(ctx, command.GetProtocolMessageCommandAttributes(), msgs)

	default:
		ch, ok := handler.commandHandlerRegistry.Handler(command.GetCommandType())
		if !ok {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("Unknown command type: %v", command.GetCommandType()))
		}
		validator := commandValidator{sizeChecker: handler.sizeLimitChecker, commandType: command.GetCommandType()}
		err := ch(ctx, handler.mutableState, validator, handler.workflowTaskCompletedID, command)
		var failWFTErr workflow.FailWorkflowTaskError
		if errors.As(err, &failWFTErr) {
			if failWFTErr.FailWorkflow {
				return nil, handler.failWorkflow(failWFTErr.Cause, failWFTErr)
			}
			return nil, handler.failWorkflowTask(failWFTErr.Cause, failWFTErr)
		}
		return nil, err
	}
}

func (handler *workflowTaskCompletedHandler) handleMessage(
	ctx context.Context,
	message *protocolpb.Message,
) error {
	protocolType, msgType, err := protocol.Identify(message)
	if err != nil {
		return serviceerror.NewInvalidArgument(err.Error())
	}
	if err := handler.sizeLimitChecker.checkIfPayloadSizeExceedsLimit(
		// TODO (alex-update): Should use MessageTypeTag here but then it needs to be another metric name too.
		metrics.CommandTypeTag(msgType.String()),
		proto.Size(message.Body),
		fmt.Sprintf("Message type %v exceeds size limit.", msgType),
	); err != nil {
		return handler.failWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_UPDATE_WORKFLOW_EXECUTION_MESSAGE, err)
	}

	switch protocolType {
	case update.ProtocolV1:
		upd := handler.updateRegistry.Find(ctx, message.ProtocolInstanceId)
		if upd == nil {
			upd, err = handler.updateRegistry.TryResurrect(ctx, message)
			if err != nil {
				return handler.failWorkflowTaskOnInvalidArgument(
					enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_UPDATE_WORKFLOW_EXECUTION_MESSAGE, err)
			}
		}
		if upd == nil {
			// Update was not found in the registry and can't be resurrected.
			return handler.failWorkflowTask(
				enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_UPDATE_WORKFLOW_EXECUTION_MESSAGE,
				serviceerror.NewNotFound(fmt.Sprintf("update %s wasn't found on the server. This is most likely a transient error which will be resolved automatically by retries", message.ProtocolInstanceId)))
		}

		if err := upd.OnProtocolMessage(
			ctx,
			message,
			workflow.WithEffects(handler.effects, handler.mutableState)); err != nil {
			return handler.failWorkflowTaskOnInvalidArgument(
				enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_UPDATE_WORKFLOW_EXECUTION_MESSAGE, err)
		}
	default:
		return handler.failWorkflowTask(
			enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_UPDATE_WORKFLOW_EXECUTION_MESSAGE,
			serviceerror.NewInvalidArgument(fmt.Sprintf("unsupported protocol type %s", protocolType)))
	}

	return nil
}

func (handler *workflowTaskCompletedHandler) handleCommandProtocolMessage(
	ctx context.Context,
	attr *commandpb.ProtocolMessageCommandAttributes,
	msgs *collection.IndexedTakeList[string, *protocolpb.Message],
) error {
	metrics.CommandTypeProtocolMessage.With(handler.metricsHandler).Record(1)

	executionInfo := handler.mutableState.GetExecutionInfo()
	namespaceID := namespace.ID(executionInfo.NamespaceId)

	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.validateProtocolMessageAttributes(
				namespaceID,
				attr,
				executionInfo.WorkflowRunTimeout,
			)
		},
	); err != nil || handler.stopProcessing {
		return err
	}

	if msg, ok := msgs.Take(attr.MessageId); ok {
		return handler.handleMessage(ctx, msg)
	}
	return handler.failWorkflowTask(
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_UPDATE_WORKFLOW_EXECUTION_MESSAGE,
		serviceerror.NewInvalidArgument(fmt.Sprintf("ProtocolMessageCommand referenced absent message ID %s", attr.MessageId)),
	)
}

func (handler *workflowTaskCompletedHandler) handleCommandScheduleActivity(
	_ context.Context,
	attr *commandpb.ScheduleActivityTaskCommandAttributes,
) (*handleCommandResponse, error) {
	metrics.CommandTypeScheduleActivityCounter.With(handler.metricsHandler).Record(1)

	executionInfo := handler.mutableState.GetExecutionInfo()
	namespaceID := namespace.ID(executionInfo.NamespaceId)

	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.validateActivityScheduleAttributes(
				namespaceID,
				attr,
				executionInfo.WorkflowRunTimeout,
			)
		},
	); err != nil || handler.stopProcessing {
		return nil, err
	}

	if handler.mutableState.GetAssignedBuildId() == "" {
		// TODO: this is supported in new versioning [cleanup-old-wv]
		if attr.UseWorkflowBuildId && attr.TaskQueue.GetName() != "" && attr.TaskQueue.Name != handler.mutableState.GetExecutionInfo().TaskQueue {
			err := serviceerror.NewInvalidArgument("Activity with UseCompatibleVersion cannot run on different task queue.")
			return nil, handler.failWorkflowTask(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES, err)
		}
	}

	if err := handler.sizeLimitChecker.checkIfPayloadSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK.String()),
		attr.GetInput().Size(),
		"ScheduleActivityTaskCommandAttributes.Input exceeds size limit.",
	); err != nil {
		return nil, handler.failWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES, err)
	}
	if err := handler.sizeLimitChecker.checkIfNumPendingActivitiesExceedsLimit(); err != nil {
		return nil, handler.failWorkflowTask(enumspb.WORKFLOW_TASK_FAILED_CAUSE_PENDING_ACTIVITIES_LIMIT_EXCEEDED, err)
	}

	enums.SetDefaultTaskQueueKind(&attr.GetTaskQueue().Kind)

	namespace := handler.mutableState.GetNamespaceEntry().Name().String()

	oldVersioningUsed := handler.mutableState.GetMostRecentWorkerVersionStamp().GetUseVersioning()
	newVersioningUsed := handler.mutableState.GetExecutionInfo().GetAssignedBuildId() != ""
	versioningUsed := oldVersioningUsed || newVersioningUsed

	// Enable eager activity start if dynamic config enables it and either 1. workflow doesn't use versioning,
	// or 2. workflow uses versioning and activity intends to use a compatible version (since a
	// worker is obviously compatible with itself and we are okay dispatching an eager task knowing that there may be a
	// newer "default" compatible version).
	// Note that if `UseWorkflowBuildId` is false, it implies that the activity should run on the "default" version
	// for the task queue.
	eagerStartActivity := attr.RequestEagerExecution && handler.config.EnableActivityEagerExecution(namespace) &&
		(!versioningUsed || attr.UseWorkflowBuildId)

	_, _, err := handler.mutableState.AddActivityTaskScheduledEvent(
		handler.workflowTaskCompletedID,
		attr,
		eagerStartActivity,
	)
	if err != nil {
		return nil, handler.failWorkflowTaskOnInvalidArgument(enumspb.WORKFLOW_TASK_FAILED_CAUSE_SCHEDULE_ACTIVITY_DUPLICATE_ID, err)
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

func (handler *workflowTaskCompletedHandler) handlePostCommandEagerExecuteActivity(
	_ context.Context,
	attr *commandpb.ScheduleActivityTaskCommandAttributes,
) (workflowTaskResponseMutation, error) {
	if !handler.mutableState.IsWorkflowExecutionRunning() {
		// workflow closed in the same workflow task
		// this function is executed as a callback after all workflow commands
		// are handled, so need to check for workflow completion case.
		return nil, nil
	}

	ai, ok := handler.mutableState.GetActivityByActivityID(attr.ActivityId)
	if !ok {
		// activity cancelled in the same worflow task
		return nil, nil
	}

	var stamp *commonpb.WorkerVersionStamp
	// eager activity always uses workflow's build ID
	buildId := handler.mutableState.GetAssignedBuildId()
	stamp = &commonpb.WorkerVersionStamp{UseVersioning: buildId != "", BuildId: buildId}

	if _, err := handler.mutableState.AddActivityTaskStartedEvent(
		ai,
		ai.GetScheduledEventId(),
		uuid.New(),
		handler.identity,
		stamp,
		nil,
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

	taskToken := tasktoken.NewActivityTaskToken(
		namespaceID.String(),
		executionInfo.WorkflowId,
		runID,
		ai.GetScheduledEventId(),
		attr.ActivityId,
		attr.ActivityType.GetName(),
		ai.Attempt,
		shardClock,
		ai.Version,
	)
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
	metrics.ActivityEagerExecutionCounter.With(handler.metricsHandler).Record(
		1,
		metrics.NamespaceTag(string(handler.mutableState.GetNamespaceEntry().Name())),
		metrics.TaskQueueTag(ai.TaskQueue),
	)

	return func(resp *historyservice.RespondWorkflowTaskCompletedResponse) error {
		resp.ActivityTasks = append(resp.ActivityTasks, activityTask)
		return nil
	}, nil
}

func (handler *workflowTaskCompletedHandler) handleCommandRequestCancelActivity(
	_ context.Context,
	attr *commandpb.RequestCancelActivityTaskCommandAttributes,
) error {
	metrics.CommandTypeCancelActivityCounter.With(handler.metricsHandler).Record(1)

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
		return handler.failWorkflowTaskOnInvalidArgument(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_ACTIVITY_ATTRIBUTES, err)
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

func (handler *workflowTaskCompletedHandler) handleCommandStartTimer(
	_ context.Context,
	attr *commandpb.StartTimerCommandAttributes,
) error {
	metrics.CommandTypeStartTimerCounter.With(handler.metricsHandler).Record(1)

	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.validateTimerScheduleAttributes(attr)
		},
	); err != nil || handler.stopProcessing {
		return err
	}

	_, _, err := handler.mutableState.AddTimerStartedEvent(handler.workflowTaskCompletedID, attr)
	if err != nil {
		return handler.failWorkflowTaskOnInvalidArgument(enumspb.WORKFLOW_TASK_FAILED_CAUSE_START_TIMER_DUPLICATE_ID, err)
	}
	return nil
}

func (handler *workflowTaskCompletedHandler) handleCommandCompleteWorkflow(
	ctx context.Context,
	attr *commandpb.CompleteWorkflowExecutionCommandAttributes,
) error {
	metrics.CommandTypeCompleteWorkflowCounter.With(handler.metricsHandler).Record(1)

	if handler.hasBufferedEventsOrMessages {
		return handler.failWorkflowTask(enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND, nil)
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
		metrics.MultipleCompletionCommandsCounter.With(handler.metricsHandler).Record(1)
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

func (handler *workflowTaskCompletedHandler) handleCommandFailWorkflow(
	ctx context.Context,
	attr *commandpb.FailWorkflowExecutionCommandAttributes,
) error {
	metrics.CommandTypeFailWorkflowCounter.With(handler.metricsHandler).Record(1)

	if handler.hasBufferedEventsOrMessages {
		return handler.failWorkflowTask(enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND, nil)
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
		metrics.MultipleCompletionCommandsCounter.With(handler.metricsHandler).Record(1)
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
		return handler.handleRetry(ctx, retryBackoff, attr.GetFailure(), newExecutionRunID)
	} else if cronBackoff != backoff.NoBackoff {
		return handler.handleCron(ctx, cronBackoff, nil, attr.GetFailure(), newExecutionRunID)
	}

	// No retry or cron
	return nil
}

func (handler *workflowTaskCompletedHandler) handleCommandCancelTimer(
	_ context.Context,
	attr *commandpb.CancelTimerCommandAttributes,
) error {
	metrics.CommandTypeCancelTimerCounter.With(handler.metricsHandler).Record(1)

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
		return handler.failWorkflowTaskOnInvalidArgument(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_CANCEL_TIMER_ATTRIBUTES, err)
	}

	// In case the timer was cancelled and its TimerFired event was deleted from buffered events, attempt
	// to unset hasBufferedEvents to allow the workflow to complete.
	handler.hasBufferedEventsOrMessages = handler.hasBufferedEventsOrMessages && handler.mutableState.HasBufferedEvents()
	return nil
}

func (handler *workflowTaskCompletedHandler) handleCommandCancelWorkflow(
	ctx context.Context,
	attr *commandpb.CancelWorkflowExecutionCommandAttributes,
) error {
	metrics.CommandTypeCancelWorkflowCounter.With(handler.metricsHandler).Record(1)

	if handler.hasBufferedEventsOrMessages {
		return handler.failWorkflowTask(enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND, nil)
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
		metrics.MultipleCompletionCommandsCounter.With(handler.metricsHandler).Record(1)
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

func (handler *workflowTaskCompletedHandler) handleCommandRequestCancelExternalWorkflow(
	_ context.Context,
	attr *commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes,
) error {
	metrics.CommandTypeCancelExternalWorkflowCounter.With(handler.metricsHandler).Record(1)

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
		return handler.failWorkflowTask(enumspb.WORKFLOW_TASK_FAILED_CAUSE_PENDING_REQUEST_CANCEL_LIMIT_EXCEEDED, err)
	}

	cancelRequestID := uuid.New()
	_, _, err := handler.mutableState.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(
		handler.workflowTaskCompletedID, cancelRequestID, attr, targetNamespaceID,
	)

	return err
}

func (handler *workflowTaskCompletedHandler) handleCommandRecordMarker(
	_ context.Context,
	attr *commandpb.RecordMarkerCommandAttributes,
) error {
	metrics.CommandTypeRecordMarkerCounter.With(handler.metricsHandler).Record(1)

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

func (handler *workflowTaskCompletedHandler) handleCommandContinueAsNewWorkflow(
	ctx context.Context,
	attr *commandpb.ContinueAsNewWorkflowExecutionCommandAttributes,
) error {
	metrics.CommandTypeContinueAsNewCounter.With(handler.metricsHandler).Record(1)

	if handler.hasBufferedEventsOrMessages {
		return handler.failWorkflowTask(enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND, nil)
	}

	namespaceName := handler.mutableState.GetNamespaceEntry().Name()

	unaliasedSas, err := searchattribute.UnaliasFields(
		handler.searchAttributesMapperProvider,
		attr.GetSearchAttributes(),
		namespaceName.String(),
	)
	if err != nil {
		return handler.failWorkflowTaskOnInvalidArgument(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES, err)
	}
	if unaliasedSas != attr.GetSearchAttributes() {
		// Create a copy of the `attr` to avoid modification of original `attr`,
		// which can be needed again in case of retry.
		newAttr := common.CloneProto(attr)
		newAttr.SearchAttributes = unaliasedSas
		attr = newAttr
	}

	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.validateContinueAsNewWorkflowExecutionAttributes(
				namespaceName,
				attr,
				handler.mutableState.GetExecutionInfo(),
			)
		},
	); err != nil || handler.stopProcessing {
		return err
	}

	if handler.mutableState.GetAssignedBuildId() == "" {
		// TODO: this is supported in new versioning [cleanup-old-wv]
		if attr.InheritBuildId && attr.TaskQueue.GetName() != "" && attr.TaskQueue.Name != handler.mutableState.GetExecutionInfo().TaskQueue {
			err := serviceerror.NewInvalidArgument("ContinueAsNew with UseCompatibleVersion cannot run on different task queue.")
			return handler.failWorkflowTask(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_CONTINUE_AS_NEW_ATTRIBUTES, err)
		}
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
		metrics.MultipleCompletionCommandsCounter.With(handler.metricsHandler).Record(1)
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

func (handler *workflowTaskCompletedHandler) handleCommandStartChildWorkflow(
	_ context.Context,
	attr *commandpb.StartChildWorkflowExecutionCommandAttributes,
) error {
	metrics.CommandTypeChildWorkflowCounter.With(handler.metricsHandler).Record(1)

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
		handler.searchAttributesMapperProvider,
		attr.GetSearchAttributes(),
		targetNamespace.String(),
	)
	if err != nil {
		return handler.failWorkflowTaskOnInvalidArgument(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES, err)
	}
	if unaliasedSas != attr.GetSearchAttributes() {
		// Create a copy of the `attr` to avoid modification of original `attr`,
		// which can be needed again in case of retry.
		newAttr := common.CloneProto(attr)
		newAttr.SearchAttributes = unaliasedSas
		attr = newAttr
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
			)
		},
	); err != nil || handler.stopProcessing {
		return err
	}

	if handler.mutableState.GetAssignedBuildId() == "" {
		// TODO: this is supported in new versioning [cleanup-old-wv]
		if attr.InheritBuildId && attr.TaskQueue.GetName() != "" && attr.TaskQueue.Name != handler.mutableState.GetExecutionInfo().TaskQueue {
			err := serviceerror.NewInvalidArgument("StartChildWorkflowExecution with UseCompatibleVersion cannot run on different task queue.")
			return handler.failWorkflowTask(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_START_CHILD_EXECUTION_ATTRIBUTES, err)
		}
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
		return handler.failWorkflowTask(enumspb.WORKFLOW_TASK_FAILED_CAUSE_PENDING_CHILD_WORKFLOWS_LIMIT_EXCEEDED, err)
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

func (handler *workflowTaskCompletedHandler) handleCommandSignalExternalWorkflow(
	_ context.Context,
	attr *commandpb.SignalExternalWorkflowExecutionCommandAttributes,
) error {
	metrics.CommandTypeSignalExternalWorkflowCounter.With(handler.metricsHandler).Record(1)

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
		return handler.failWorkflowTask(enumspb.WORKFLOW_TASK_FAILED_CAUSE_PENDING_SIGNALS_LIMIT_EXCEEDED, err)
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

func (handler *workflowTaskCompletedHandler) handleCommandUpsertWorkflowSearchAttributes(
	_ context.Context,
	attr *commandpb.UpsertWorkflowSearchAttributesCommandAttributes,
) error {
	metrics.CommandTypeUpsertWorkflowSearchAttributesCounter.With(handler.metricsHandler).Record(1)

	// get namespace name
	executionInfo := handler.mutableState.GetExecutionInfo()
	namespaceID := namespace.ID(executionInfo.NamespaceId)
	namespaceEntry, err := handler.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("Unable to get namespace for namespaceID: %v.", namespaceID))
	}
	namespace := namespaceEntry.Name()

	unaliasedSas, err := searchattribute.UnaliasFields(
		handler.searchAttributesMapperProvider,
		attr.GetSearchAttributes(),
		namespace.String(),
	)
	if err != nil {
		return handler.failWorkflowTaskOnInvalidArgument(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES, err)
	}
	if unaliasedSas != attr.GetSearchAttributes() {
		// Create a copy of the `attr` to avoid modification of original `attr`,
		// which can be needed again in case of retry.
		newAttr := common.CloneProto(attr)
		newAttr.SearchAttributes = unaliasedSas
		attr = newAttr
	}

	// valid search attributes for upsert
	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.validateUpsertWorkflowSearchAttributes(namespace, attr)
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

func (handler *workflowTaskCompletedHandler) handleCommandModifyWorkflowProperties(
	_ context.Context,
	attr *commandpb.ModifyWorkflowPropertiesCommandAttributes,
) error {
	metrics.CommandTypeModifyWorkflowPropertiesCounter.With(handler.metricsHandler).Record(1)

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

func (handler *workflowTaskCompletedHandler) handleRetry(
	ctx context.Context,
	backoffInterval time.Duration,
	failure *failurepb.Failure,
	newRunID string,
) error {
	startEvent, err := handler.mutableState.GetStartEvent(ctx)
	if err != nil {
		return err
	}
	startAttr := startEvent.GetWorkflowExecutionStartedEventAttributes()

	newMutableState := workflow.NewMutableStateInChain(
		handler.shard,
		handler.shard.GetEventsCache(),
		handler.shard.GetLogger(),
		handler.mutableState.GetNamespaceEntry(),
		handler.mutableState.GetWorkflowKey().WorkflowID,
		newRunID,
		handler.shard.GetTimeSource().Now(),
		handler.mutableState,
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

func (handler *workflowTaskCompletedHandler) handleCron(
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

	newMutableState := workflow.NewMutableStateInChain(
		handler.shard,
		handler.shard.GetEventsCache(),
		handler.shard.GetLogger(),
		handler.mutableState.GetNamespaceEntry(),
		handler.mutableState.GetWorkflowKey().WorkflowID,
		newRunID,
		handler.shard.GetTimeSource().Now(),
		handler.mutableState,
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

func (handler *workflowTaskCompletedHandler) validateCommandAttr(
	validationFn commandAttrValidationFn,
) error {

	return handler.failWorkflowTaskOnInvalidArgument(validationFn())
}

func (handler *workflowTaskCompletedHandler) failWorkflowTaskOnInvalidArgument(
	wtFailedCause enumspb.WorkflowTaskFailedCause,
	err error,
) error {

	switch err.(type) {
	case *serviceerror.InvalidArgument:
		return handler.failWorkflowTask(wtFailedCause, err)
	default:
		return err
	}
}

func (handler *workflowTaskCompletedHandler) failWorkflowTask(
	failedCause enumspb.WorkflowTaskFailedCause,
	causeErr error,
) error {

	handler.workflowTaskFailedCause = newWorkflowTaskFailedCause(
		failedCause,
		causeErr,
		nil)
	handler.stopProcessing = true
	// NOTE: failWorkflowTask always return nil.
	//  It is important to clear returned error if WT needs to be failed to properly add WTFailed event.
	//  Handler will rely on stopProcessing flag and workflowTaskFailedCause field.
	return nil
}

func (handler *workflowTaskCompletedHandler) failWorkflow(
	failedCause enumspb.WorkflowTaskFailedCause,
	causeErr error,
) error {

	handler.workflowTaskFailedCause = newWorkflowTaskFailedCause(
		failedCause,
		causeErr,
		failure.NewServerFailure(causeErr.Error(), true))
	handler.stopProcessing = true
	// NOTE: failWorkflow always return nil.
	//  It is important to clear returned error if WT needs to be failed to properly add WTFailed and FailWorkflow events.
	//  Handler will rely on stopProcessing flag and workflowTaskFailedCause field.
	return nil
}

func newWorkflowTaskFailedCause(failedCause enumspb.WorkflowTaskFailedCause, causeErr error, workflowFailure *failurepb.Failure) *workflowTaskFailedCause {

	return &workflowTaskFailedCause{
		failedCause:     failedCause,
		causeErr:        causeErr,
		workflowFailure: workflowFailure,
	}
}

func (c *workflowTaskFailedCause) Message() string {

	if c.causeErr == nil {
		return c.failedCause.String()
	}

	return fmt.Sprintf("%v: %v", c.failedCause, c.causeErr.Error())
}

// commandValidator implements [workflow.CommandValidator] for use in registered command handlers.
type commandValidator struct {
	sizeChecker *workflowSizeChecker
	commandType enumspb.CommandType
}

func (v commandValidator) IsValidPayloadSize(size int) bool {
	err := v.sizeChecker.checkIfPayloadSizeExceedsLimit(metrics.CommandTypeTag(v.commandType.String()), size, "")
	return err == nil
}
