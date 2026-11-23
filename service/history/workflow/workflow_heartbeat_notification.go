package workflow

import (
	"context"
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
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/workercommands"
	historyi "go.temporal.io/server/service/history/interfaces"
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

	// TODO(whn): Distinguish prior WHN-eligible events from other buffered events. Counting all buffered
	// events avoids a scan but can miss the first-input metric and suppress WHN for a later eligible event.
	hadBufferedInput := c.MutableState.NumBufferedEvents() > len(newBufferedEvents)
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

// SendWorkflowHeartbeatNotification is called after the input transaction succeeds and returns without blocking.
// A background goroutine dispatches the notification to the worker's control queue through Matching and waits
// asynchronously for the worker's response, up to WorkflowHeartbeatNotificationDispatchTimeout. It is best
// effort: failures are only logged.
func SendWorkflowHeartbeatNotification(
	ctx context.Context,
	shardCtx historyi.ShardContext,
	notification *WorkflowHeartbeatNotification,
) {
	if notification == nil || !isWorkflowHeartbeatNotificationEnabled(shardCtx, notification.namespaceName, notification.workflowKey.WorkflowID) {
		return
	}
	// TODO(experimental@whn): consider a bounded queue instead of a goroutine per notification, to cap
	// concurrency and drop notifications when full. Options:
	//   - The history in-memory task queue. It currently only supports timed history tasks (CategoryMemoryTimer),
	//     so it would first need to support immediate tasks. Its timer fires at the task's exact visibility time,
	//     but when all scheduler workers are busy it pushes the task back by 1s (workerBusyRescheduleDelay), which
	//     is longer than WorkflowHeartbeatNotificationDispatchTimeout (500ms default). Check it is fast enough.
	//   - A dedicated ctasks.FIFOScheduler with its own worker pool, where TrySubmit drops when the queue is full.
	go func() {
		matchingClient, err := shardCtx.GetMatchingClient()
		if err != nil {
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
			shardCtx.GetLogger().Warn("Unable to encode workflow heartbeat notification", tag.Error(err))
			return
		}
		matchCtx, matchCancel := context.WithTimeout(
			context.WithoutCancel(ctx),
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
		handler := shardCtx.GetMetricsHandler().WithTags(
			metrics.NamespaceTag(notification.namespaceName),
			metrics.EventTypeTag(notification.inputEventTypeName),
		)
		metrics.WorkflowHeartbeatNotificationDeliveryLatency.With(handler).Record(time.Since(dispatchStart))
		metrics.WorkflowHeartbeatNotificationResults.With(handler).Record(1,
			metrics.StringTag("result", workflowHeartbeatNotificationResult(resp, err)))
		if err != nil {
			shardCtx.GetLogger().Warn("Unable to dispatch workflow heartbeat notification",
				tag.WorkflowNamespace(notification.namespaceName),
				tag.WorkflowID(notification.workflowKey.WorkflowID),
				tag.WorkflowRunID(notification.workflowKey.RunID),
				tag.Error(err))
		}
	}()
}

// workflowHeartbeatNotificationResult reuses the Nexus dispatch outcome tag values for the worker's answer.
func workflowHeartbeatNotificationResult(resp *matchingservice.DispatchNexusTaskResponse, err error) string {
	switch {
	case err == nil:
		return commonnexus.ClassifyStartOperationDispatch(resp).OutcomeTag().Value
	case common.IsContextDeadlineExceededErr(err):
		return "dispatch_timeout"
	default:
		return "dispatch_error"
	}
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
