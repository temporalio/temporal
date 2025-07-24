package respondworkflowtaskcompleted

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pborman/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/effect"
	"go.temporal.io/server/common/enums"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/protocol"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/update"
	"google.golang.org/protobuf/proto"
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
		newMutableState                 historyi.MutableState
		stopProcessing                  bool // should stop processing any more commands
		mutableState                    historyi.MutableState
		effects                         effect.Controller
		initiatedChildExecutionsInBatch map[string]struct{} // Set of initiated child executions in the workflow task
		updateRegistry                  update.Registry

		// validation
		attrValidator                  *api.CommandAttrValidator
		sizeLimitChecker               *workflowSizeChecker
		searchAttributesMapperProvider searchattribute.MapperProvider

		logger                 log.Logger
		namespaceRegistry      namespace.Registry
		metricsHandler         metrics.Handler
		config                 *configs.Config
		shard                  historyi.ShardContext
		tokenSerializer        *tasktoken.Serializer
		commandHandlerRegistry *workflow.CommandHandlerRegistry
		matchingClient         matchingservice.MatchingServiceClient
	}

	workflowTaskFailedCause struct {
		failedCause       enumspb.WorkflowTaskFailedCause
		causeErr          error
		terminateWorkflow bool // when true, this task failure should be considered terminal to its workflow
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
	mutableState historyi.MutableState,
	updateRegistry update.Registry,
	effects effect.Controller,
	attrValidator *api.CommandAttrValidator,
	sizeLimitChecker *workflowSizeChecker,
	logger log.Logger,
	namespaceRegistry namespace.Registry,
	metricsHandler metrics.Handler,
	config *configs.Config,
	shard historyi.ShardContext,
	searchAttributesMapperProvider searchattribute.MapperProvider,
	hasBufferedEventsOrMessages bool,
	commandHandlerRegistry *workflow.CommandHandlerRegistry,
	matchingClient matchingservice.MatchingServiceClient,
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
		tokenSerializer:        tasktoken.NewSerializer(),
		commandHandlerRegistry: commandHandlerRegistry,
		matchingClient:         matchingClient,
	}
}

func (handler *workflowTaskCompletedHandler) handleCommands(
	ctx context.Context,
	commands []*commandpb.Command,
	msgs *collection.IndexedTakeList[string, *protocolpb.Message],
) ([]workflowTaskResponseMutation, error) {
	if err := handler.attrValidator.ValidateCommandSequence(
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
) {

	// If server decided to fail WT (instead of completing), don't reject updates.
	// New WT will be created, and it will deliver these updates again to the worker.
	// Worker will do full history replay, and updates should be delivered again.
	if handler.workflowTaskFailedCause != nil {
		return
	}

	// If WT is a heartbeat WT, then it doesn't have to have messages.
	if wtHeartbeat {
		return
	}

	// If worker has just completed workflow with one of the WF completion command,
	// then it might skip processing some updates. In this case, it doesn't indicate old SDK or bug.
	// All unprocessed updates will be aborted later though.
	if !handler.mutableState.IsWorkflowExecutionRunning() {
		return
	}

	rejectedUpdateIDs := handler.updateRegistry.RejectUnprocessed(
		ctx,
		handler.effects)

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
}

//revive:disable:cyclomatic grandfathered
func (handler *workflowTaskCompletedHandler) handleCommand(
	ctx context.Context,
	command *commandpb.Command,
	msgs *collection.IndexedTakeList[string, *protocolpb.Message],
) (*handleCommandResponse, error) {

	metrics.CommandCounter.With(handler.metricsHandler).
		Record(1, metrics.CommandTypeTag(command.GetCommandType().String()))
	var response *handleCommandResponse
	var historyEvent *historypb.HistoryEvent
	var err error

	// TODO: ideally history events should not be exposed here. We should be passing the command
	// all the way down but it requires a bigger refactor of the mutable state interface.
	switch command.GetCommandType() {
	case enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK:
		historyEvent, response, err = handler.handleCommandScheduleActivity(ctx, command.GetScheduleActivityTaskCommandAttributes())

	case enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION:
		historyEvent, err = handler.handleCommandCompleteWorkflow(ctx, command.GetCompleteWorkflowExecutionCommandAttributes())

	case enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION:
		historyEvent, err = handler.handleCommandFailWorkflow(ctx, command.GetFailWorkflowExecutionCommandAttributes())

	case enumspb.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION:
		historyEvent, err = handler.handleCommandCancelWorkflow(ctx, command.GetCancelWorkflowExecutionCommandAttributes())

	case enumspb.COMMAND_TYPE_START_TIMER:
		historyEvent, err = handler.handleCommandStartTimer(ctx, command.GetStartTimerCommandAttributes())

	case enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK:
		historyEvent, err = handler.handleCommandRequestCancelActivity(ctx, command.GetRequestCancelActivityTaskCommandAttributes())

	case enumspb.COMMAND_TYPE_CANCEL_TIMER:
		historyEvent, err = handler.handleCommandCancelTimer(ctx, command.GetCancelTimerCommandAttributes())

	case enumspb.COMMAND_TYPE_RECORD_MARKER:
		historyEvent, err = handler.handleCommandRecordMarker(ctx, command.GetRecordMarkerCommandAttributes())

	case enumspb.COMMAND_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION:
		historyEvent, err = handler.handleCommandRequestCancelExternalWorkflow(ctx, command.GetRequestCancelExternalWorkflowExecutionCommandAttributes())

	case enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION:
		historyEvent, err = handler.handleCommandSignalExternalWorkflow(ctx, command.GetSignalExternalWorkflowExecutionCommandAttributes())

	case enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION:
		historyEvent, err = handler.handleCommandContinueAsNewWorkflow(ctx, command.GetContinueAsNewWorkflowExecutionCommandAttributes())

	case enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION:
		historyEvent, err = handler.handleCommandStartChildWorkflow(ctx, command.GetStartChildWorkflowExecutionCommandAttributes())

	case enumspb.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
		historyEvent, err = handler.handleCommandUpsertWorkflowSearchAttributes(ctx, command.GetUpsertWorkflowSearchAttributesCommandAttributes())

	case enumspb.COMMAND_TYPE_MODIFY_WORKFLOW_PROPERTIES:
		historyEvent, err = handler.handleCommandModifyWorkflowProperties(ctx, command.GetModifyWorkflowPropertiesCommandAttributes())

	case enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE:
		return nil, handler.handleCommandProtocolMessage(ctx, command.GetProtocolMessageCommandAttributes(), msgs)

	default:
		// Nexus command handlers are registered in /components/nexusoperations/workflow/commands.go
		ch, ok := handler.commandHandlerRegistry.Handler(command.GetCommandType())
		if !ok {
			return nil, serviceerror.NewInvalidArgumentf("Unknown command type: %v", command.GetCommandType())
		}
		validator := commandValidator{sizeChecker: handler.sizeLimitChecker, commandType: command.GetCommandType()}
		err := ch(ctx, handler.mutableState, validator, handler.workflowTaskCompletedID, command)
		var failWFTErr workflow.FailWorkflowTaskError
		if errors.As(err, &failWFTErr) {
			if failWFTErr.TerminateWorkflow {
				return nil, handler.terminateWorkflow(failWFTErr.Cause, failWFTErr)
			}
			return nil, handler.failWorkflowTask(failWFTErr.Cause, failWFTErr)
		}
		return nil, err
	}

	if err != nil {
		return nil, err
	}

	if historyEvent != nil && command.UserMetadata != nil {
		historyEvent.UserMetadata = command.UserMetadata
	}
	return response, nil
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
		return handler.terminateWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_UPDATE_WORKFLOW_EXECUTION_MESSAGE, err)
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
				serviceerror.NewNotFoundf("update %s wasn't found on the server. This is most likely a transient error which will be resolved automatically by retries", message.ProtocolInstanceId))
		}

		if err := upd.OnProtocolMessage(message, workflow.WithEffects(handler.effects, handler.mutableState)); err != nil {
			return handler.failWorkflowTaskOnInvalidArgument(
				enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_UPDATE_WORKFLOW_EXECUTION_MESSAGE, err)
		}
	default:
		return handler.failWorkflowTask(
			enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_UPDATE_WORKFLOW_EXECUTION_MESSAGE,
			serviceerror.NewInvalidArgumentf("unsupported protocol type %s", protocolType))
	}

	return nil
}

func (handler *workflowTaskCompletedHandler) handleCommandProtocolMessage(
	ctx context.Context,
	attr *commandpb.ProtocolMessageCommandAttributes,
	msgs *collection.IndexedTakeList[string, *protocolpb.Message],
) error {
	executionInfo := handler.mutableState.GetExecutionInfo()
	namespaceID := namespace.ID(executionInfo.NamespaceId)

	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.ValidateProtocolMessageAttributes(
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
		serviceerror.NewInvalidArgumentf("ProtocolMessageCommand referenced absent message ID %s", attr.MessageId),
	)
}

func (handler *workflowTaskCompletedHandler) handleCommandScheduleActivity(
	_ context.Context,
	attr *commandpb.ScheduleActivityTaskCommandAttributes,
) (*historypb.HistoryEvent, *handleCommandResponse, error) {
	executionInfo := handler.mutableState.GetExecutionInfo()
	namespaceID := namespace.ID(executionInfo.NamespaceId)

	// TODO(fairness): remove this again once the SDK allows setting the fairness key
	const fairnessKeyPrefix = "x-temporal-internal-fairness-key["
	if after, ok := strings.CutPrefix(attr.GetActivityId(), fairnessKeyPrefix); ok {
		if endIndex := strings.Index(after, "]"); endIndex != -1 {
			keyAndWeight := after[:endIndex]
			if colonIndex := strings.Index(keyAndWeight, ":"); colonIndex != -1 {
				key := keyAndWeight[:colonIndex]
				if weight, err := strconv.ParseFloat(keyAndWeight[colonIndex+1:], 32); err == nil {
					attr.Priority = cmp.Or(attr.Priority, &commonpb.Priority{})
					attr.Priority.FairnessKey = key
					attr.Priority.FairnessWeight = float32(weight)
				}
			}
		}
	}

	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.ValidateActivityScheduleAttributes(
				namespaceID,
				attr,
				executionInfo.WorkflowRunTimeout,
			)
		},
	); err != nil || handler.stopProcessing {
		return nil, nil, err
	}

	if handler.mutableState.GetAssignedBuildId() == "" {
		// TODO: this is supported in new versioning [cleanup-old-wv]
		if attr.UseWorkflowBuildId && attr.TaskQueue.GetName() != "" && attr.TaskQueue.Name != handler.mutableState.GetExecutionInfo().TaskQueue {
			err := serviceerror.NewInvalidArgument("Activity with UseCompatibleVersion cannot run on different task queue.")
			return nil, nil, handler.failWorkflowTask(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES, err)
		}
	}

	if err := handler.sizeLimitChecker.checkIfPayloadSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK.String()),
		attr.GetInput().Size(),
		"ScheduleActivityTaskCommandAttributes.Input exceeds size limit.",
	); err != nil {
		return nil, nil, handler.terminateWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES, err)
	}
	if err := handler.sizeLimitChecker.checkIfNumPendingActivitiesExceedsLimit(); err != nil {
		return nil, nil, handler.failWorkflowTask(enumspb.WORKFLOW_TASK_FAILED_CAUSE_PENDING_ACTIVITIES_LIMIT_EXCEEDED, err)
	}

	enums.SetDefaultTaskQueueKind(&attr.GetTaskQueue().Kind)

	namespace := handler.mutableState.GetNamespaceEntry().Name().String()

	// TODO: versioning 3 allows eager activity dispatch for both pinned and unpinned workflows, no
	// special consideration is need. Remove the versioning logic from here. [cleanup-old-wv]
	oldVersioningUsed := handler.mutableState.GetMostRecentWorkerVersionStamp().GetUseVersioning() &&
		// for V3 versioning it's ok to dispatch eager activities
		handler.mutableState.GetEffectiveVersioningBehavior() == enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED
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

	event, _, err := handler.mutableState.AddActivityTaskScheduledEvent(
		handler.workflowTaskCompletedID,
		attr,
		eagerStartActivity,
	)
	if err != nil {
		return nil, nil, handler.failWorkflowTaskOnInvalidArgument(enumspb.WORKFLOW_TASK_FAILED_CAUSE_SCHEDULE_ACTIVITY_DUPLICATE_ID, err)
	}

	if !eagerStartActivity {
		return event, &handleCommandResponse{}, nil
	}

	return event,
		&handleCommandResponse{
			commandPostAction: func(ctx context.Context) (workflowTaskResponseMutation, error) {
				return handler.handlePostCommandEagerExecuteActivity(ctx, attr)
			},
		},
		nil
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
	metrics.ActivityEagerExecutionCounter.With(
		workflow.GetPerTaskQueueFamilyScope(handler.metricsHandler, handler.mutableState.GetNamespaceEntry().Name(), ai.TaskQueue, handler.config),
	).Record(1)

	return func(resp *historyservice.RespondWorkflowTaskCompletedResponse) error {
		resp.ActivityTasks = append(resp.ActivityTasks, activityTask)
		return nil
	}, nil
}

func (handler *workflowTaskCompletedHandler) handleCommandRequestCancelActivity(
	_ context.Context,
	attr *commandpb.RequestCancelActivityTaskCommandAttributes,
) (*historypb.HistoryEvent, error) {
	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.ValidateActivityCancelAttributes(attr)
		},
	); err != nil || handler.stopProcessing {
		return nil, err
	}

	scheduledEventID := attr.GetScheduledEventId()
	actCancelReqEvent, ai, err := handler.mutableState.AddActivityTaskCancelRequestedEvent(
		handler.workflowTaskCompletedID,
		scheduledEventID,
		handler.identity,
	)
	if err != nil {
		return nil, handler.failWorkflowTaskOnInvalidArgument(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_ACTIVITY_ATTRIBUTES, err)
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
				return nil, err
			}
			handler.activityNotStartedCancelled = true
		}
	}
	return actCancelReqEvent, nil
}

func (handler *workflowTaskCompletedHandler) handleCommandStartTimer(
	_ context.Context,
	attr *commandpb.StartTimerCommandAttributes,
) (*historypb.HistoryEvent, error) {
	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.ValidateTimerScheduleAttributes(attr)
		},
	); err != nil || handler.stopProcessing {
		return nil, err
	}

	event, _, err := handler.mutableState.AddTimerStartedEvent(handler.workflowTaskCompletedID, attr)
	if err != nil {
		return nil, handler.failWorkflowTaskOnInvalidArgument(enumspb.WORKFLOW_TASK_FAILED_CAUSE_START_TIMER_DUPLICATE_ID, err)
	}
	return event, nil
}

func (handler *workflowTaskCompletedHandler) handleCommandCompleteWorkflow(
	ctx context.Context,
	attr *commandpb.CompleteWorkflowExecutionCommandAttributes,
) (*historypb.HistoryEvent, error) {
	if handler.hasBufferedEventsOrMessages {
		return nil, handler.failWorkflowTask(enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND, nil)
	}

	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.ValidateCompleteWorkflowExecutionAttributes(attr)
		},
	); err != nil || handler.stopProcessing {
		return nil, err
	}

	if err := handler.sizeLimitChecker.checkIfPayloadSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION.String()),
		attr.GetResult().Size(),
		"CompleteWorkflowExecutionCommandAttributes.Result exceeds size limit.",
	); err != nil {
		return nil, handler.terminateWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES, err)
	}

	// If the workflow task has more than one completion event then just pick the first one
	if !handler.mutableState.IsWorkflowExecutionRunning() {
		metrics.MultipleCompletionCommandsCounter.With(handler.metricsHandler).Record(1)
		handler.logger.Warn(
			"Multiple completion commands",
			tag.WorkflowCommandType(enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION),
			tag.ErrorTypeMultipleCompletionCommands,
		)
		return nil, nil
	}

	cronBackoff := handler.mutableState.GetCronBackoffDuration()
	var newExecutionRunID string
	if cronBackoff != backoff.NoBackoff {
		newExecutionRunID = uuid.New()
	}

	// Always add workflow completed event to this one
	event, err := handler.mutableState.AddCompletedWorkflowEvent(handler.workflowTaskCompletedID, attr, newExecutionRunID)
	if err != nil {
		return nil, err
	}

	// Check if this workflow has a cron schedule
	if cronBackoff != backoff.NoBackoff {
		return event, handler.handleCron(ctx, cronBackoff, attr.GetResult(), nil, newExecutionRunID)
	}

	return event, nil
}

func (handler *workflowTaskCompletedHandler) handleCommandFailWorkflow(
	ctx context.Context,
	attr *commandpb.FailWorkflowExecutionCommandAttributes,
) (*historypb.HistoryEvent, error) {
	if handler.hasBufferedEventsOrMessages {
		return nil, handler.failWorkflowTask(enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND, nil)
	}

	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.ValidateFailWorkflowExecutionAttributes(attr)
		},
	); err != nil || handler.stopProcessing {
		return nil, err
	}

	if err := handler.sizeLimitChecker.checkIfPayloadSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION.String()),
		attr.GetFailure().Size(),
		"FailWorkflowExecutionCommandAttributes.Failure exceeds size limit.",
	); err != nil {
		return nil, handler.terminateWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_FAIL_WORKFLOW_EXECUTION_ATTRIBUTES, err)
	}

	// If the workflow task has more than one completion event then just pick the first one
	if !handler.mutableState.IsWorkflowExecutionRunning() {
		metrics.MultipleCompletionCommandsCounter.With(handler.metricsHandler).Record(1)
		handler.logger.Warn(
			"Multiple completion commands",
			tag.WorkflowCommandType(enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION),
			tag.ErrorTypeMultipleCompletionCommands,
		)
		return nil, nil
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
	event, err := handler.mutableState.AddFailWorkflowEvent(
		handler.workflowTaskCompletedID,
		retryState,
		attr,
		newExecutionRunID,
	)
	if err != nil {
		return nil, err
	}

	// Handle retry or cron
	if retryBackoff != backoff.NoBackoff {
		return event, handler.handleRetry(ctx, retryBackoff, attr.GetFailure(), newExecutionRunID)
	} else if cronBackoff != backoff.NoBackoff {
		return event, handler.handleCron(ctx, cronBackoff, nil, attr.GetFailure(), newExecutionRunID)
	}

	// No retry or cron
	return event, nil
}

func (handler *workflowTaskCompletedHandler) handleCommandCancelTimer(
	_ context.Context,
	attr *commandpb.CancelTimerCommandAttributes,
) (*historypb.HistoryEvent, error) {
	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.ValidateTimerCancelAttributes(attr)
		},
	); err != nil || handler.stopProcessing {
		return nil, err
	}

	event, err := handler.mutableState.AddTimerCanceledEvent(
		handler.workflowTaskCompletedID,
		attr,
		handler.identity)
	if err != nil {
		return nil, handler.failWorkflowTaskOnInvalidArgument(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_CANCEL_TIMER_ATTRIBUTES, err)
	}

	// In case the timer was cancelled and its TimerFired event was deleted from buffered events, attempt
	// to unset hasBufferedEvents to allow the workflow to complete.
	handler.hasBufferedEventsOrMessages = handler.hasBufferedEventsOrMessages && handler.mutableState.HasBufferedEvents()
	return event, nil
}

func (handler *workflowTaskCompletedHandler) handleCommandCancelWorkflow(
	ctx context.Context,
	attr *commandpb.CancelWorkflowExecutionCommandAttributes,
) (*historypb.HistoryEvent, error) {
	if handler.hasBufferedEventsOrMessages {
		return nil, handler.failWorkflowTask(enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND, nil)
	}

	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.ValidateCancelWorkflowExecutionAttributes(attr)
		},
	); err != nil || handler.stopProcessing {
		return nil, err
	}

	// If the workflow task has more than one completion event than just pick the first one
	if !handler.mutableState.IsWorkflowExecutionRunning() {
		metrics.MultipleCompletionCommandsCounter.With(handler.metricsHandler).Record(1)
		handler.logger.Warn(
			"Multiple completion commands",
			tag.WorkflowCommandType(enumspb.COMMAND_TYPE_CANCEL_WORKFLOW_EXECUTION),
			tag.ErrorTypeMultipleCompletionCommands,
		)
		return nil, nil
	}

	return handler.mutableState.AddWorkflowExecutionCanceledEvent(handler.workflowTaskCompletedID, attr)
}

func (handler *workflowTaskCompletedHandler) handleCommandRequestCancelExternalWorkflow(
	_ context.Context,
	attr *commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes,
) (*historypb.HistoryEvent, error) {
	executionInfo := handler.mutableState.GetExecutionInfo()
	namespaceID := namespace.ID(executionInfo.NamespaceId)
	targetNamespaceID := namespaceID
	if attr.GetNamespace() != "" {
		targetNamespaceEntry, err := handler.namespaceRegistry.GetNamespace(namespace.Name(attr.GetNamespace()))
		if err != nil {
			return nil, err
		}
		targetNamespaceID = targetNamespaceEntry.ID()
	}

	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.ValidateCancelExternalWorkflowExecutionAttributes(
				namespaceID,
				targetNamespaceID,
				handler.initiatedChildExecutionsInBatch,
				attr,
			)
		},
	); err != nil || handler.stopProcessing {
		return nil, err
	}
	if err := handler.sizeLimitChecker.checkIfNumPendingCancelRequestsExceedsLimit(); err != nil {
		return nil, handler.failWorkflowTask(enumspb.WORKFLOW_TASK_FAILED_CAUSE_PENDING_REQUEST_CANCEL_LIMIT_EXCEEDED, err)
	}

	cancelRequestID := uuid.New()
	event, _, err := handler.mutableState.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(
		handler.workflowTaskCompletedID, cancelRequestID, attr, targetNamespaceID,
	)

	return event, err
}

func (handler *workflowTaskCompletedHandler) handleCommandRecordMarker(
	_ context.Context,
	attr *commandpb.RecordMarkerCommandAttributes,
) (*historypb.HistoryEvent, error) {
	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.ValidateRecordMarkerAttributes(attr)
		},
	); err != nil || handler.stopProcessing {
		return nil, err
	}

	if err := handler.sizeLimitChecker.checkIfPayloadSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_RECORD_MARKER.String()),
		common.GetPayloadsMapSize(attr.GetDetails()),
		"RecordMarkerCommandAttributes.Details exceeds size limit.",
	); err != nil {
		return nil, handler.terminateWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_RECORD_MARKER_ATTRIBUTES, err)
	}

	return handler.mutableState.AddRecordMarkerEvent(handler.workflowTaskCompletedID, attr)
}

func (handler *workflowTaskCompletedHandler) handleCommandContinueAsNewWorkflow(
	ctx context.Context,
	attr *commandpb.ContinueAsNewWorkflowExecutionCommandAttributes,
) (*historypb.HistoryEvent, error) {
	if handler.hasBufferedEventsOrMessages {
		return nil, handler.failWorkflowTask(enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND, nil)
	}

	namespaceName := handler.mutableState.GetNamespaceEntry().Name()

	unaliasedSas, err := searchattribute.UnaliasFields(
		handler.searchAttributesMapperProvider,
		attr.GetSearchAttributes(),
		namespaceName.String(),
	)
	if err != nil {
		return nil, handler.failWorkflowTaskOnInvalidArgument(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES, err)
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
			return handler.attrValidator.ValidateContinueAsNewWorkflowExecutionAttributes(
				namespaceName,
				attr,
				handler.mutableState.GetExecutionInfo(),
			)
		},
	); err != nil || handler.stopProcessing {
		return nil, err
	}

	if handler.mutableState.GetAssignedBuildId() == "" {
		// TODO(carlydf): this is supported in new versioning [cleanup-old-wv]
		if attr.InheritBuildId && attr.TaskQueue.GetName() != "" && attr.TaskQueue.Name != handler.mutableState.GetExecutionInfo().TaskQueue {
			err := serviceerror.NewInvalidArgument("ContinueAsNew with UseCompatibleVersion cannot run on different task queue.")
			return nil, handler.failWorkflowTask(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_CONTINUE_AS_NEW_ATTRIBUTES, err)
		}
	}

	if err := handler.sizeLimitChecker.checkIfPayloadSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION.String()),
		attr.GetInput().Size(),
		"ContinueAsNewWorkflowExecutionCommandAttributes. Input exceeds size limit.",
	); err != nil {
		return nil, handler.terminateWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_CONTINUE_AS_NEW_ATTRIBUTES, err)
	}

	if err := handler.sizeLimitChecker.checkIfMemoSizeExceedsLimit(
		attr.GetMemo(),
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION.String()),
		"ContinueAsNewWorkflowExecutionCommandAttributes. Memo exceeds size limit.",
	); err != nil {
		return nil, handler.terminateWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_CONTINUE_AS_NEW_ATTRIBUTES, err)
	}

	// search attribute validation must be done after unaliasing keys
	if err := handler.sizeLimitChecker.checkIfSearchAttributesSizeExceedsLimit(
		attr.GetSearchAttributes(),
		namespaceName,
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION.String()),
	); err != nil {
		return nil, handler.terminateWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_CONTINUE_AS_NEW_ATTRIBUTES, err)
	}

	// If the workflow task has more than one completion event than just pick the first one
	if !handler.mutableState.IsWorkflowExecutionRunning() {
		metrics.MultipleCompletionCommandsCounter.With(handler.metricsHandler).Record(1)
		handler.logger.Warn(
			"Multiple completion commands",
			tag.WorkflowCommandType(enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION),
			tag.ErrorTypeMultipleCompletionCommands,
		)
		return nil, nil
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

	event, newMutableState, err := handler.mutableState.AddContinueAsNewEvent(
		ctx,
		handler.workflowTaskCompletedID,
		handler.workflowTaskCompletedID,
		parentNamespace,
		attr,
		worker_versioning.GetIsWFTaskQueueInVersionDetector(handler.matchingClient),
	)
	if err != nil {
		return nil, err
	}

	handler.newMutableState = newMutableState
	return event, nil
}

func (handler *workflowTaskCompletedHandler) handleCommandStartChildWorkflow(
	_ context.Context,
	attr *commandpb.StartChildWorkflowExecutionCommandAttributes,
) (*historypb.HistoryEvent, error) {
	parentNamespaceEntry := handler.mutableState.GetNamespaceEntry()
	parentNamespaceID := parentNamespaceEntry.ID()
	parentNamespace := parentNamespaceEntry.Name()
	targetNamespaceID := parentNamespaceID
	targetNamespace := parentNamespace
	if attr.GetNamespace() != "" {
		targetNamespaceEntry, err := handler.namespaceRegistry.GetNamespace(namespace.Name(attr.GetNamespace()))
		if err != nil {
			return nil, err
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
		return nil, handler.failWorkflowTaskOnInvalidArgument(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES, err)
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
			return handler.attrValidator.ValidateStartChildExecutionAttributes(
				parentNamespaceID,
				targetNamespaceID,
				targetNamespace,
				attr,
				handler.mutableState.GetExecutionInfo(),
				handler.config.DefaultWorkflowTaskTimeout,
			)
		},
	); err != nil || handler.stopProcessing {
		return nil, err
	}

	if handler.mutableState.GetAssignedBuildId() == "" {
		// TODO: this is supported in new versioning [cleanup-old-wv]
		if attr.InheritBuildId && attr.TaskQueue.GetName() != "" && attr.TaskQueue.Name != handler.mutableState.GetExecutionInfo().TaskQueue {
			err := serviceerror.NewInvalidArgument("StartChildWorkflowExecution with UseCompatibleVersion cannot run on different task queue.")
			return nil, handler.failWorkflowTask(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_START_CHILD_EXECUTION_ATTRIBUTES, err)
		}
	}

	if err := handler.sizeLimitChecker.checkIfPayloadSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION.String()),
		attr.GetInput().Size(),
		"StartChildWorkflowExecutionCommandAttributes. Input exceeds size limit.",
	); err != nil {
		return nil, handler.terminateWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_START_CHILD_EXECUTION_ATTRIBUTES, err)
	}

	if err := handler.sizeLimitChecker.checkIfMemoSizeExceedsLimit(
		attr.GetMemo(),
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION.String()),
		"StartChildWorkflowExecutionCommandAttributes.Memo exceeds size limit.",
	); err != nil {
		return nil, handler.terminateWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_START_CHILD_EXECUTION_ATTRIBUTES, err)
	}

	// search attribute validation must be done after unaliasing keys
	if err := handler.sizeLimitChecker.checkIfSearchAttributesSizeExceedsLimit(
		attr.GetSearchAttributes(),
		targetNamespace,
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION.String()),
	); err != nil {
		return nil, handler.terminateWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_START_CHILD_EXECUTION_ATTRIBUTES, err)
	}

	// child workflow limit
	if err := handler.sizeLimitChecker.checkIfNumChildWorkflowsExceedsLimit(); err != nil {
		return nil, handler.failWorkflowTask(enumspb.WORKFLOW_TASK_FAILED_CAUSE_PENDING_CHILD_WORKFLOWS_LIMIT_EXCEEDED, err)
	}

	enabled := handler.config.EnableParentClosePolicy(parentNamespace.String())
	if enabled {
		enums.SetDefaultParentClosePolicy(&attr.ParentClosePolicy)
	} else {
		attr.ParentClosePolicy = enumspb.PARENT_CLOSE_POLICY_ABANDON
	}

	enums.SetDefaultWorkflowIdReusePolicy(&attr.WorkflowIdReusePolicy)

	event, _, err := handler.mutableState.AddStartChildWorkflowExecutionInitiatedEvent(
		handler.workflowTaskCompletedID, attr, targetNamespaceID,
	)
	if err == nil {
		// Keep track of all child initiated commands in this workflow task to validate request cancel commands
		handler.initiatedChildExecutionsInBatch[attr.GetWorkflowId()] = struct{}{}
	}
	return event, err
}

func (handler *workflowTaskCompletedHandler) handleCommandSignalExternalWorkflow(
	_ context.Context,
	attr *commandpb.SignalExternalWorkflowExecutionCommandAttributes,
) (*historypb.HistoryEvent, error) {
	executionInfo := handler.mutableState.GetExecutionInfo()
	namespaceID := namespace.ID(executionInfo.NamespaceId)
	targetNamespaceID := namespaceID
	if attr.GetNamespace() != "" {
		targetNamespaceEntry, err := handler.namespaceRegistry.GetNamespace(namespace.Name(attr.GetNamespace()))
		if err != nil {
			return nil, err
		}
		targetNamespaceID = targetNamespaceEntry.ID()
	}

	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.ValidateSignalExternalWorkflowExecutionAttributes(
				namespaceID,
				targetNamespaceID,
				attr,
			)
		},
	); err != nil || handler.stopProcessing {
		return nil, err
	}
	if err := handler.sizeLimitChecker.checkIfNumPendingSignalsExceedsLimit(); err != nil {
		return nil, handler.failWorkflowTask(enumspb.WORKFLOW_TASK_FAILED_CAUSE_PENDING_SIGNALS_LIMIT_EXCEEDED, err)
	}

	if err := handler.sizeLimitChecker.checkIfPayloadSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION.String()),
		attr.GetInput().Size(),
		"SignalExternalWorkflowExecutionCommandAttributes.Input exceeds size limit.",
	); err != nil {
		return nil, handler.terminateWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SIGNAL_WORKFLOW_EXECUTION_ATTRIBUTES, err)
	}

	signalRequestID := uuid.New() // for deduplicate
	event, _, err := handler.mutableState.AddSignalExternalWorkflowExecutionInitiatedEvent(
		handler.workflowTaskCompletedID, signalRequestID, attr, targetNamespaceID,
	)
	return event, err
}

func (handler *workflowTaskCompletedHandler) handleCommandUpsertWorkflowSearchAttributes(
	_ context.Context,
	attr *commandpb.UpsertWorkflowSearchAttributesCommandAttributes,
) (*historypb.HistoryEvent, error) {
	// get namespace name
	executionInfo := handler.mutableState.GetExecutionInfo()
	namespaceID := namespace.ID(executionInfo.NamespaceId)
	namespaceEntry, err := handler.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("Unable to get namespace for namespaceID: %v.", namespaceID)
	}
	namespace := namespaceEntry.Name()

	unaliasedSas, err := searchattribute.UnaliasFields(
		handler.searchAttributesMapperProvider,
		attr.GetSearchAttributes(),
		namespace.String(),
	)
	if err != nil {
		return nil, handler.failWorkflowTaskOnInvalidArgument(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES, err)
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
			return handler.attrValidator.ValidateUpsertWorkflowSearchAttributes(namespace, attr)
		},
	); err != nil || handler.stopProcessing {
		return nil, err
	}

	// blob size limit check
	if err := handler.sizeLimitChecker.checkIfPayloadSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES.String()),
		payloadsMapSize(attr.GetSearchAttributes().GetIndexedFields()),
		"UpsertWorkflowSearchAttributesCommandAttributes exceeds size limit.",
	); err != nil {
		return nil, handler.terminateWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES, err)
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
		return nil, handler.terminateWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SEARCH_ATTRIBUTES, err)
	}

	return handler.mutableState.AddUpsertWorkflowSearchAttributesEvent(
		handler.workflowTaskCompletedID, attr,
	)
}

func (handler *workflowTaskCompletedHandler) handleCommandModifyWorkflowProperties(
	_ context.Context,
	attr *commandpb.ModifyWorkflowPropertiesCommandAttributes,
) (*historypb.HistoryEvent, error) {
	// get namespace name
	executionInfo := handler.mutableState.GetExecutionInfo()
	namespaceID := namespace.ID(executionInfo.NamespaceId)
	_, err := handler.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("Unable to get namespace for namespaceID: %v.", namespaceID)
	}

	// valid properties
	if err := handler.validateCommandAttr(
		func() (enumspb.WorkflowTaskFailedCause, error) {
			return handler.attrValidator.ValidateModifyWorkflowProperties(attr)
		},
	); err != nil || handler.stopProcessing {
		return nil, err
	}

	// blob size limit check
	if err := handler.sizeLimitChecker.checkIfPayloadSizeExceedsLimit(
		metrics.CommandTypeTag(enumspb.COMMAND_TYPE_MODIFY_WORKFLOW_PROPERTIES.String()),
		payloadsMapSize(attr.GetUpsertedMemo().GetFields()),
		"ModifyWorkflowPropertiesCommandAttributes exceeds size limit.",
	); err != nil {
		return nil, handler.terminateWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_MODIFY_WORKFLOW_PROPERTIES_ATTRIBUTES, err)
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
		return nil, handler.terminateWorkflow(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_MODIFY_WORKFLOW_PROPERTIES_ATTRIBUTES, err)
	}

	return handler.mutableState.AddWorkflowPropertiesModifiedEvent(
		handler.workflowTaskCompletedID, attr,
	)
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

	newMutableState, err := workflow.NewMutableStateInChain(
		handler.shard,
		handler.shard.GetEventsCache(),
		handler.shard.GetLogger(),
		handler.mutableState.GetNamespaceEntry(),
		handler.mutableState.GetWorkflowKey().WorkflowID,
		newRunID,
		handler.shard.GetTimeSource().Now(),
		handler.mutableState,
	)
	if err != nil {
		return err
	}

	err = workflow.SetupNewWorkflowForRetryOrCron(
		ctx,
		handler.mutableState,
		newMutableState,
		newRunID,
		startAttr,
		startEvent.Links,
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

	newMutableState, err := workflow.NewMutableStateInChain(
		handler.shard,
		handler.shard.GetEventsCache(),
		handler.shard.GetLogger(),
		handler.mutableState.GetNamespaceEntry(),
		handler.mutableState.GetWorkflowKey().WorkflowID,
		newRunID,
		handler.shard.GetTimeSource().Now(),
		handler.mutableState,
	)
	if err != nil {
		return err
	}

	err = workflow.SetupNewWorkflowForRetryOrCron(
		ctx,
		handler.mutableState,
		newMutableState,
		newRunID,
		startAttr,
		startEvent.Links,
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
		false)
	handler.stopProcessing = true
	// NOTE: failWorkflowTask always returns nil.
	//  It is important to clear returned error if WT needs to be failed to properly add WTFailed event.
	//  Handler will rely on stopProcessing flag and workflowTaskFailedCause field.
	return nil
}

func (handler *workflowTaskCompletedHandler) terminateWorkflow(
	failedCause enumspb.WorkflowTaskFailedCause,
	causeErr error,
) error {

	handler.workflowTaskFailedCause = newWorkflowTaskFailedCause(
		failedCause,
		causeErr,
		true)
	handler.stopProcessing = true
	// NOTE: terminateWorkflow always returns nil.
	//  It is important to clear returned error if WT needs to be failed to properly add WTFailed and FailWorkflow events.
	//  Handler will rely on stopProcessing flag and workflowTaskFailedCause field.
	return nil
}

func newWorkflowTaskFailedCause(failedCause enumspb.WorkflowTaskFailedCause, causeErr error, terminatesWorkflow bool) *workflowTaskFailedCause {

	return &workflowTaskFailedCause{
		failedCause:       failedCause,
		causeErr:          causeErr,
		terminateWorkflow: terminatesWorkflow,
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
