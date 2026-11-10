package workflow

import (
	"context"
	"slices"
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	workerservicepb "go.temporal.io/api/nexusservices/workerservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/workercommands"
	historyi "go.temporal.io/server/service/history/interfaces"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// WorkflowHeartbeatNotificationUpdateAdmittedEventType identifies an admitted Update before it has a history event.
const WorkflowHeartbeatNotificationUpdateAdmittedEventType = "UPDATE_ADMITTED"

const (
	workflowHeartbeatNotificationSignal              = "signal"
	workflowHeartbeatNotificationCancellation        = "cancellation"
	workflowHeartbeatNotificationTimer               = "timer"
	workflowHeartbeatNotificationActivityResult      = "activity_result"
	workflowHeartbeatNotificationChildWorkflowResult = "child_workflow_result"
	workflowHeartbeatNotificationNexusResult         = "nexus_result"
	workflowHeartbeatNotificationUpdate              = "update"
)

// workflowHeartbeatNotificationInputTypeForEvent maps an eligible event to its configuration category.
func workflowHeartbeatNotificationInputTypeForEvent(eventType enumspb.EventType) string {
	switch eventType {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED,
		enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED:
		return workflowHeartbeatNotificationSignal
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED:
		return workflowHeartbeatNotificationCancellation
	case enumspb.EVENT_TYPE_TIMER_FIRED:
		return workflowHeartbeatNotificationTimer
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
		return workflowHeartbeatNotificationActivityResult
	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED:
		return workflowHeartbeatNotificationChildWorkflowResult
	case enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_FAILED:
		return workflowHeartbeatNotificationNexusResult
	default:
		return ""
	}
}

func workflowHeartbeatNotificationInputTypeForName(eventType string) string {
	if eventType == WorkflowHeartbeatNotificationUpdateAdmittedEventType {
		return workflowHeartbeatNotificationUpdate
	}
	value, err := enumspb.EventTypeFromString(eventType)
	if err != nil {
		return ""
	}
	return workflowHeartbeatNotificationInputTypeForEvent(value)
}

// isWorkflowHeartbeatNotificationInputEnabled checks the namespace rollout and input type filter.
func isWorkflowHeartbeatNotificationInputEnabled(shardCtx historyi.ShardContext, namespaceName, eventType string) bool {
	config := shardCtx.GetConfig()
	if !config.EnableWorkflowHeartbeatNotification(namespaceName) {
		return false
	}
	category := workflowHeartbeatNotificationInputTypeForName(eventType)
	if category == "" {
		return false
	}
	allowed := config.WorkflowHeartbeatNotificationInputTypes(namespaceName)
	return slices.ContainsFunc(allowed, func(value string) bool {
		return strings.EqualFold(value, category)
	})
}

// IsWorkflowHeartbeatNotificationInputType reports whether an event type is eligible for a Workflow Task heartbeat notification.
// Buffering alone does not make an event eligible; for example, ActivityTaskStarted records progress but does not normally require a new Workflow Task.
func IsWorkflowHeartbeatNotificationInputType(eventType enumspb.EventType) bool {
	return workflowHeartbeatNotificationInputTypeForEvent(eventType) != ""
}

func (c *ContextImpl) handleBufferedInputsForWorkflowHeartbeatNotification(
	ctx context.Context,
	shardCtx historyi.ShardContext,
	newBufferedEvents []*historypb.HistoryEvent,
) {
	if len(newBufferedEvents) == 0 || !c.MutableState.HasStartedWorkflowTask() {
		return
	}

	filteredEventTypes := make([]string, 0, len(newBufferedEvents))
	for _, event := range newBufferedEvents {
		if IsWorkflowHeartbeatNotificationInputType(event.GetEventType()) {
			filteredEventTypes = append(filteredEventTypes, event.GetEventType().String())
		}
	}
	if len(filteredEventTypes) == 0 {
		return
	}

	// TODO: Relaxed accuracy for the estimate to avoid iterating and comparing buffered events:
	// any prior buffered event counts, not only workflow-heartbeat-notification-eligible ones.
	hadBufferedInput := c.MutableState.NumBufferedEvents() > len(newBufferedEvents)
	firstBufferedInput := !hadBufferedInput && !c.hasPendingUpdateForWorkflowHeartbeatNotification()
	namespaceEntry := c.MutableState.GetNamespaceEntry()
	if namespaceEntry == nil {
		return
	}
	namespaceName := namespaceEntry.Name().String()
	handler := c.metricsHandler.WithTags(metrics.NamespaceTag(namespaceName))

	// Rollout settings control WHN dispatch, while these metrics count all eligible buffered inputs.
	for i, eventType := range filteredEventTypes {
		inputHandler := handler.WithTags(metrics.StringTag("event_type", eventType))
		metrics.WorkflowHeartbeatNotificationBufferedInput.With(inputHandler).Record(1)
		if firstBufferedInput && i == 0 {
			metrics.WorkflowHeartbeatNotificationEstimatedFirstBufferedInput.With(inputHandler).Record(1)
		}
	}
	if firstBufferedInput {
		notification, err := PrepareWorkflowHeartbeatNotification(c.MutableState, filteredEventTypes[0])
		if err != nil {
			c.logger.Warn("Unable to prepare workflow heartbeat notification", tag.Error(err))
		} else {
			SendWorkflowHeartbeatNotification(ctx, shardCtx, notification)
		}
	}
}

type WorkflowHeartbeatNotification struct {
	workflowKey   definition.WorkflowKey
	namespaceName string
	controlQueue  string
	taskToken     []byte
	// inputEventTypeName is a history event name or UPDATE_ADMITTED for an admitted Update.
	inputEventTypeName string
}

// PrepareWorkflowHeartbeatNotification snapshots the started task while the workflow lease is held.
func PrepareWorkflowHeartbeatNotification(ms historyi.MutableState, inputEventTypeName string) (*WorkflowHeartbeatNotification, error) {
	task := ms.GetStartedWorkflowTask()
	if task == nil || task.WorkerControlTaskQueue == "" {
		return nil, nil
	}
	key := ms.GetWorkflowKey()
	token, err := tasktoken.NewSerializer().Serialize(tasktoken.NewWorkflowTaskToken(
		key.NamespaceID,
		key.WorkflowID,
		key.RunID,
		task.ScheduledEventID,
		task.StartedEventID,
		timestamppb.New(task.StartedTime),
		task.Attempt,
		task.StartedClock,
		task.Version,
	))
	if err != nil {
		return nil, err
	}
	return &WorkflowHeartbeatNotification{
		workflowKey:        key,
		namespaceName:      ms.GetNamespaceEntry().Name().String(),
		controlQueue:       task.WorkerControlTaskQueue,
		taskToken:          token,
		inputEventTypeName: inputEventTypeName,
	}, nil
}

// SendWorkflowHeartbeatNotification attempts an in-memory dispatch after the input transaction succeeds.
func SendWorkflowHeartbeatNotification(
	ctx context.Context,
	shardCtx historyi.ShardContext,
	notification *WorkflowHeartbeatNotification,
) {
	if notification == nil || !isWorkflowHeartbeatNotificationInputEnabled(shardCtx, notification.namespaceName, notification.inputEventTypeName) {
		return
	}
	go func() {
		start := time.Now()
		outcome := "unspecified"
		defer func() {
			handler := shardCtx.GetMetricsHandler().WithTags(
				metrics.NamespaceTag(notification.namespaceName),
				metrics.StringTag("event_type", notification.inputEventTypeName),
			)
			metrics.WorkflowHeartbeatNotificationDeliveryLatency.With(handler).Record(time.Since(start))
			metrics.WorkflowHeartbeatNotificationResults.With(handler).Record(1, metrics.OutcomeTag(outcome))
			shardCtx.GetLogger().Debug("Workflow heartbeat notification dispatch finished",
				tag.WorkflowNamespace(notification.namespaceName),
				tag.WorkflowID(notification.workflowKey.WorkflowID),
				tag.WorkflowRunID(notification.workflowKey.RunID),
				tag.NewStringTag("event_type", notification.inputEventTypeName),
				tag.NewStringTag("outcome", outcome),
			)
		}()
		matchingClient, err := shardCtx.GetMatchingClient()
		if err != nil {
			outcome = "matching_client_error"
			shardCtx.GetLogger().Warn("Unable to get Matching client for workflow heartbeat notification", tag.Error(err))
			return
		}
		command := &workerpb.WorkerCommand{Type: &workerpb.WorkerCommand_WorkflowHeartbeatNotification{
			WorkflowHeartbeatNotification: &workerpb.WorkflowHeartbeatNotificationCommand{
				TaskToken: notification.taskToken,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: notification.workflowKey.WorkflowID,
					RunId:      notification.workflowKey.RunID,
				},
			},
		}}
		requestData, err := proto.Marshal(&workerservicepb.ExecuteCommandsRequest{Commands: []*workerpb.WorkerCommand{command}})
		if err != nil {
			outcome = "encode_error"
			shardCtx.GetLogger().Warn("Unable to encode workflow heartbeat notification", tag.Error(err))
			return
		}
		matchCtx, matchCancel := context.WithTimeout(
			context.WithoutCancel(ctx),
			shardCtx.GetConfig().WorkflowHeartbeatNotificationDispatchTimeout(notification.namespaceName),
		)
		defer matchCancel()
		response, err := matchingClient.DispatchNexusTask(matchCtx, &matchingservice.DispatchNexusTaskRequest{
			NamespaceId: notification.workflowKey.NamespaceID,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: notification.controlQueue,
				Kind: enumspb.TASK_QUEUE_KIND_WORKER_COMMANDS,
			},
			Request: &nexuspb.Request{
				Header: map[string]string{},
				Variant: &nexuspb.Request_StartOperation{StartOperation: &nexuspb.StartOperationRequest{
					Service:   workercommands.ServiceName,
					Operation: workercommands.OperationName,
					Payload: &commonpb.Payload{
						Metadata: map[string][]byte{"encoding": []byte("binary/protobuf")},
						Data:     requestData,
					},
				}},
			},
		})
		if err != nil {
			outcome = "rpc_error"
			shardCtx.GetLogger().Warn("Unable to dispatch workflow heartbeat notification",
				tag.WorkflowNamespace(notification.namespaceName),
				tag.WorkflowID(notification.workflowKey.WorkflowID),
				tag.WorkflowRunID(notification.workflowKey.RunID),
				tag.Error(err))
			return
		}
		outcome = classifyWorkflowHeartbeatNotificationResponse(response)
	}()
}

func classifyWorkflowHeartbeatNotificationResponse(response *matchingservice.DispatchNexusTaskResponse) string {
	if response.GetRequestTimeout() != nil {
		return "request_timeout"
	}
	if response.GetFailure() != nil || (response.GetOutcome() != nil && response.GetResponse() == nil) {
		return "worker_error"
	}
	payload := response.GetResponse().GetStartOperation().GetSyncSuccess().GetPayload()
	if payload == nil {
		return "invalid_response"
	}
	var commandResponse workerservicepb.ExecuteCommandsResponse
	if err := proto.Unmarshal(payload.GetData(), &commandResponse); err != nil || len(commandResponse.GetResults()) != 1 {
		return "invalid_response"
	}
	result := commandResponse.GetResults()[0].GetWorkflowHeartbeatNotification()
	if result == nil {
		return "invalid_response"
	}
	outcome, ok := workerpb.WorkflowHeartbeatNotificationResult_Outcome_name[int32(result.GetOutcome())]
	if !ok {
		return "invalid_response"
	}
	return outcome
}

// hasPendingUpdateForWorkflowHeartbeatNotification reads pending Updates from the current Registry.
func (c *ContextImpl) hasPendingUpdateForWorkflowHeartbeatNotification() bool {
	return c.updateRegistry != nil &&
		c.updateRegistry.FailoverVersion() == c.MutableState.GetCurrentVersion() &&
		c.updateRegistry.HasOutgoingMessages(false)
}
