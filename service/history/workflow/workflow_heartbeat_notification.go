package workflow

import (
	"context"
	"encoding/base64"
	"slices"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	workerservicepb "go.temporal.io/api/nexusservices/workerservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/workercommands"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/proto"
)

// WorkflowHeartbeatNotificationUpdateAdmittedMetricTag is a dummy event type: admitting an Update writes no history
// event (WorkflowExecutionUpdateAdmitted is only written when reapplying on reset).
const WorkflowHeartbeatNotificationUpdateAdmittedMetricTag = "UpdateAdmitted"

// isWorkflowHeartbeatNotificationEnabled checks rollout.
func isWorkflowHeartbeatNotificationEnabled(
	shardCtx historyi.ShardContext, namespaceName, workflowID string) bool {
	config := shardCtx.GetConfig()
	rolloutPercent := config.WorkflowHeartbeatNotificationRolloutPercent(namespaceName)
	return dynamicconfig.RolloutAccepts(
		[]byte(namespaceName+"\x00"+workflowID),
		rolloutPercent,
	)
}

// IsWorkflowHeartbeatNotificationInputType reports whether an event type is eligible for a Workflow Task heartbeat notification.
// An event type that is never buffered does not need WHN. Buffering alone does not make an event eligible;
// for example, ActivityTaskStarted records progress but does not normally require a new Workflow Task.
func IsWorkflowHeartbeatNotificationInputType(eventType enumspb.EventType) bool {
	switch eventType {
	case
		// signal
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED,
		enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		// cancel
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		// timer
		enumspb.EVENT_TYPE_TIMER_FIRED,
		// child workflow
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED,
		// nexus operation
		enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_FAILED,
		// todo: activity results may need a different handling mechanism for potential high unnecessary traffic it may trigger
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
		return true
	default:
		return false
	}
}

// HasBufferedWorkflowHeartbeatNotificationInput checks at most 5 prior buffered events.
// Events in newBufferedEvents are skipped without consuming the limit.
// Of the 31 implemented bufferable event types, 25 are WHN-eligible. Assuming independent, uniformly
// distributed prior event types, five checks find an eligible type with probability 1-(6/31)^5 = 99.97%,
// exceeding the 99.9% target. This is an estimate, not a guarantee for actual event ordering.
// If no eligible input is found within the limit, another best-effort notification is allowed.
func HasBufferedWorkflowHeartbeatNotificationInput(ms historyi.MutableState, newBufferedEvents []*historypb.HistoryEvent) bool {
	checked := 0
	found := false
	ms.HasAnyBufferedEvent(func(event *historypb.HistoryEvent) bool {
		if slices.Contains(newBufferedEvents, event) {
			return false
		}
		checked++
		found = IsWorkflowHeartbeatNotificationInputType(event.GetEventType())
		return found || checked == 5
	})
	return found
}

// handleWorkflowHeartbeatNotification handles buffered events waiting for the next Workflow Task to reach the worker.
// Admitted Updates can also trigger WHN while waiting for that task, through the Update path.
func (c *ContextImpl) handleWorkflowHeartbeatNotification(
	ctx context.Context,
	shardCtx historyi.ShardContext,
	newBufferedEvents []*historypb.HistoryEvent,
) {
	if len(newBufferedEvents) == 0 || !c.MutableState.HasStartedWorkflowTask() {
		return
	}

	filteredNewBufferedEventTypes := make([]string, 0, len(newBufferedEvents))
	for _, event := range newBufferedEvents {
		if IsWorkflowHeartbeatNotificationInputType(event.GetEventType()) {
			filteredNewBufferedEventTypes = append(filteredNewBufferedEventTypes, event.GetEventType().String())
		}
	}
	if len(filteredNewBufferedEventTypes) == 0 {
		return
	}

	hadBufferedInput := HasBufferedWorkflowHeartbeatNotificationInput(c.MutableState, newBufferedEvents)
	firstBufferedInput := !hadBufferedInput && !c.hasPendingUpdateForWorkflowHeartbeatNotification()
	namespaceEntry := c.MutableState.GetNamespaceEntry()
	if namespaceEntry == nil {
		return
	}
	namespaceName := namespaceEntry.Name().String()
	handler := c.metricsHandler.WithTags(metrics.NamespaceTag(namespaceName))

	// The rollout setting controls WHN dispatch, while these metrics count all eligible buffered inputs.
	for i, eventType := range filteredNewBufferedEventTypes {
		inputHandler := handler.WithTags(metrics.EventTypeTag(eventType))
		metrics.WorkflowHeartbeatNotificationBufferedInput.With(inputHandler).Record(1)
		if firstBufferedInput && i == 0 {
			metrics.WorkflowHeartbeatNotificationEstimatedFirstBufferedInput.With(inputHandler).Record(1)
		}
	}
	if firstBufferedInput {
		notification, err := NewWorkflowHeartbeatNotification(c.MutableState, filteredNewBufferedEventTypes[0])
		if err != nil {
			c.logger.Error("WHN: Unable to prepare workflow heartbeat notification", tag.Error(err))
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

// NewWorkflowHeartbeatNotification snapshots the started task while the workflow lease is held.
// It returns nil if the worker didn't register a control queue or the task token can't be rebuilt.
func NewWorkflowHeartbeatNotification(ms historyi.MutableState, inputEventTypeName string) (*WorkflowHeartbeatNotification, error) {
	info := ms.GetExecutionInfo()
	controlQueue := info.GetWorkflowTaskWorkerControlQueue()
	if controlQueue == "" {
		return nil, nil
	}
	key := ms.GetWorkflowKey()
	taskToken := RebuildCurrentWorkflowTaskToken(key, info)
	if taskToken == nil {
		return nil, nil
	}
	token, err := tasktoken.NewSerializer().Serialize(taskToken)
	if err != nil {
		return nil, err
	}
	return &WorkflowHeartbeatNotification{
		workflowKey:        key,
		namespaceName:      ms.GetNamespaceEntry().Name().String(),
		controlQueue:       controlQueue,
		taskToken:          token,
		inputEventTypeName: inputEventTypeName,
	}, nil
}

// SendWorkflowHeartbeatNotification submits a best-effort in-memory task after the input transaction succeeds.
// Admission never waits for a worker. Notifications are dropped when the buffer is full or their queue timeout expires.
func SendWorkflowHeartbeatNotification(
	ctx context.Context,
	shardCtx historyi.ShardContext,
	notification *WorkflowHeartbeatNotification,
) {
	if notification == nil || !isWorkflowHeartbeatNotificationEnabled(shardCtx, notification.namespaceName, notification.workflowKey.WorkflowID) {
		return
	}
	// The engine is already running. Avoid waiting during shard shutdown or initialization.
	lookupCtx, cancel := context.WithCancel(ctx)
	cancel()
	engine, err := shardCtx.GetEngine(lookupCtx)
	if err != nil {
		shardCtx.GetLogger().Error("WHN: Unable to get History engine for workflow heartbeat notification", tag.Error(err))
		return
	}
	// Preserve request metadata without retaining its cancellation or deadline after the RPC returns.
	requestCtx := context.WithoutCancel(ctx)
	task := &tasks.MemoryImmediateTask{
		WorkflowKey:  notification.workflowKey,
		QueueTimeout: shardCtx.GetConfig().WorkflowHeartbeatNotificationDispatchTimeout(notification.namespaceName),
		Callback: func(queueCtx context.Context) {
			dispatchCtx, cancel := context.WithCancel(requestCtx)
			stop := context.AfterFunc(queueCtx, cancel)
			defer stop()
			defer cancel()
			if queueCtx.Err() != nil {
				return
			}
			dispatchWorkflowHeartbeatNotification(dispatchCtx, shardCtx, notification)
		},
	}
	engine.NotifyNewTasks(map[tasks.Category][]tasks.Task{tasks.CategoryMemoryImmediate: {task}})
}

func dispatchWorkflowHeartbeatNotification(
	ctx context.Context,
	shardCtx historyi.ShardContext,
	notification *WorkflowHeartbeatNotification,
) {
	matchingClient, err := shardCtx.GetMatchingClient()
	if err != nil {
		shardCtx.GetLogger().Error("WHN: Unable to get Matching client for workflow heartbeat notification", tag.Error(err))
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
		shardCtx.GetLogger().Error("WHN: Unable to encode workflow heartbeat notification", tag.Error(err))
		return
	}
	matchCtx, matchCancel := context.WithTimeout(
		ctx,
		shardCtx.GetConfig().WorkflowHeartbeatNotificationDispatchTimeout(notification.namespaceName),
	)
	defer matchCancel()
	dispatchStart := time.Now()
	resp, err := matchingClient.DispatchNexusTask(matchCtx, &matchingservice.DispatchNexusTaskRequest{
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
	recordWorkflowHeartbeatNotificationResult(shardCtx, notification, resp, err, time.Since(dispatchStart))
}

func recordWorkflowHeartbeatNotificationResult(
	shardCtx historyi.ShardContext,
	notification *WorkflowHeartbeatNotification,
	resp *matchingservice.DispatchNexusTaskResponse,
	err error,
	latency time.Duration,
) {
	handler := shardCtx.GetMetricsHandler().WithTags(
		metrics.NamespaceTag(notification.namespaceName),
		metrics.EventTypeTag(notification.inputEventTypeName),
	)
	metrics.WorkflowHeartbeatNotificationDeliveryLatency.With(handler).Record(latency)
	var result commonnexus.DispatchResult
	if err == nil {
		result = commonnexus.ClassifyStartOperationDispatch(resp)
	}
	resultTag := workflowHeartbeatNotificationResult(result, err)
	var workerResult *workerpb.WorkflowHeartbeatNotificationResult
	if result.Outcome == commonnexus.DispatchOutcomeSyncSuccess {
		workerResult = decodeWorkflowHeartbeatNotificationResult(resp)
		resultTag = "invalid_response"
		if workerResult != nil {
			resultTag = workerpb.WorkflowHeartbeatNotificationResult_Outcome_name[int32(workerResult.GetOutcome())]
		}
	}
	metrics.WorkflowHeartbeatNotificationResults.With(handler).Record(1, metrics.StringTag("result", resultTag))
	logger := log.With(shardCtx.GetLogger(),
		tag.WorkflowNamespace(notification.namespaceName),
		tag.WorkflowID(notification.workflowKey.WorkflowID),
		tag.WorkflowRunID(notification.workflowKey.RunID),
	)
	switch {
	case err != nil:
		logger.Error("WHN: Unable to dispatch workflow heartbeat notification", tag.Error(err))
	case result.Outcome == commonnexus.DispatchOutcomeRequestTimeout:
		// No worker took the notification in time, which is expected for best effort delivery.
	case workerResult.GetOutcome() == workerpb.WorkflowHeartbeatNotificationResult_OUTCOME_TASK_TOKEN_MISMATCH:
		logger.Info("WHN: Workflow heartbeat notification task token mismatch",
			tag.NewStringTag("event_type", notification.inputEventTypeName),
			tag.NewStringTag("notification_task_token", base64.StdEncoding.EncodeToString(notification.taskToken)),
			tag.NewStringTag("current_task_token", base64.StdEncoding.EncodeToString(workerResult.GetCurrentTaskToken())),
		)
	default:
		if workerErr := commonnexus.DispatchResultToError(result); workerErr != nil {
			logger.Error("WHN: Worker failed workflow heartbeat notification", tag.Error(workerErr))
		}
	}
}

// workflowHeartbeatNotificationResult reuses the Nexus dispatch outcome tag values for the worker's answer.
func workflowHeartbeatNotificationResult(result commonnexus.DispatchResult, err error) string {
	switch {
	case err == nil:
		return result.OutcomeTag().Value
	case common.IsContextDeadlineExceededErr(err):
		return "dispatch_timeout"
	default:
		return "dispatch_error"
	}
}

// decodeWorkflowHeartbeatNotificationResult returns nil if the worker's sync answer isn't a single WHN result.
func decodeWorkflowHeartbeatNotificationResult(resp *matchingservice.DispatchNexusTaskResponse) *workerpb.WorkflowHeartbeatNotificationResult {
	payload := resp.GetResponse().GetStartOperation().GetSyncSuccess().GetPayload()
	if payload == nil {
		return nil
	}
	var commandResponse workerservicepb.ExecuteCommandsResponse
	if err := proto.Unmarshal(payload.GetData(), &commandResponse); err != nil || len(commandResponse.GetResults()) != 1 {
		return nil
	}
	result := commandResponse.GetResults()[0].GetWorkflowHeartbeatNotification()
	if _, ok := workerpb.WorkflowHeartbeatNotificationResult_Outcome_name[int32(result.GetOutcome())]; result == nil || !ok {
		return nil
	}
	return result
}

// hasPendingUpdateForWorkflowHeartbeatNotification checks for unsent Updates in the existing, current-version Registry.
// It reads c.updateRegistry directly; calling c.UpdateRegistry could clear or create a Registry.
func (c *ContextImpl) hasPendingUpdateForWorkflowHeartbeatNotification() bool {
	return c.updateRegistry != nil &&
		c.updateRegistry.FailoverVersion() == c.MutableState.GetCurrentVersion() &&
		c.updateRegistry.HasOutgoingMessages(false)
}

// RebuildCurrentWorkflowTaskToken rebuilds the task token given to the worker for the started workflow task.
// It returns nil if no workflow task is started or its started clock is unknown.
func RebuildCurrentWorkflowTaskToken(key definition.WorkflowKey, info *persistencespb.WorkflowExecutionInfo) *tokenspb.Task {
	if info.GetWorkflowTaskStartedEventId() == common.EmptyEventID || info.GetWorkflowTaskStartedClock() == nil {
		return nil
	}
	return tasktoken.NewWorkflowTaskToken(
		key.NamespaceID,
		key.WorkflowID,
		key.RunID,
		info.GetWorkflowTaskScheduledEventId(),
		info.GetWorkflowTaskStartedEventId(),
		common.CloneProto(info.GetWorkflowTaskStartedTime()),
		info.GetWorkflowTaskAttempt(),
		common.CloneProto(info.GetWorkflowTaskStartedClock()),
		info.GetWorkflowTaskVersion(),
	)
}
