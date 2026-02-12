// Handles dispatching activity cancellation requests to workers via Nexus control queue.
// When a workflow requests activity cancellation, CancelActivityNexusTask is created and
// processed here to send the cancellation to the worker running the activity.

package history

import (
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/nexus"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/proto"
)

func (t *transferQueueActiveTaskExecutor) processCancelActivityTask(
	ctx context.Context,
	task *tasks.CancelActivityNexusTask,
) (retError error) {
	if !t.config.EnableActivityCancellationNexusTask() {
		return nil
	}

	if len(task.ScheduledEventIDs) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(ctx, t.shardContext, weContext, task, t.metricHandler, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	// If control queue is not set, it means the worker that this activity belongs to does not support Nexus tasks.
	if task.WorkerControlTaskQueue == "" {
		return nil
	}

	taskTokens, err := buildActivityTaskTokens(mutableState, task)
	if err != nil {
		return err
	}
	if len(taskTokens) == 0 {
		return nil
	}

	return t.dispatchCancelTaskToWorker(ctx, task.NamespaceID, task.WorkerControlTaskQueue, taskTokens)
}

// buildActivityTaskTokens builds task tokens for activities that need cancellation.
// Returns the serialized task tokens and any error.
func buildActivityTaskTokens(
	mutableState historyi.MutableState,
	task *tasks.CancelActivityNexusTask,
) ([][]byte, error) {
	var taskTokens [][]byte
	for _, scheduledEventID := range task.ScheduledEventIDs {
		ai, ok := mutableState.GetActivityInfo(scheduledEventID)
		if !ok || !ai.CancelRequested || ai.StartedEventId == common.EmptyEventID {
			continue
		}

		taskToken := &tokenspb.Task{
			NamespaceId:      task.NamespaceID,
			WorkflowId:       task.WorkflowID,
			RunId:            task.RunID,
			ScheduledEventId: scheduledEventID,
			Attempt:          ai.Attempt,
			ActivityId:       ai.ActivityId,
			StartedEventId:   ai.StartedEventId,
			Version:          ai.Version,
		}
		taskTokenBytes, err := taskToken.Marshal()
		if err != nil {
			return nil, err
		}
		taskTokens = append(taskTokens, taskTokenBytes)
	}
	return taskTokens, nil
}

// dispatchCancelTaskToWorker sends activity cancellation request to the worker's Nexus control queue.
func (t *transferQueueActiveTaskExecutor) dispatchCancelTaskToWorker(
	ctx context.Context,
	namespaceID string,
	controlQueueName string,
	taskTokens [][]byte,
) error {
	cancelPayload := &workerpb.CancelActivitiesRequestPayload{
		TaskTokens: taskTokens,
	}
	payloadBytes, err := proto.Marshal(cancelPayload)
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("failed to marshal cancel activities payload: %v", err))
	}

	nexusRequest := &nexuspb.Request{
		Header: map[string]string{},
		Variant: &nexuspb.Request_StartOperation{
			StartOperation: &nexuspb.StartOperationRequest{
				Service:   nexus.WorkerControlService,
				Operation: nexus.CancelActivitiesOperation,
				Payload: &commonpb.Payload{
					Metadata: map[string][]byte{
						"encoding": []byte("proto"),
					},
					Data: payloadBytes,
				},
			},
		},
	}

	resp, err := t.matchingRawClient.DispatchNexusTask(ctx, &matchingservice.DispatchNexusTaskRequest{
		NamespaceId: namespaceID,
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: controlQueueName,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Request: nexusRequest,
	})
	if err != nil {
		t.logger.Warn("Failed to dispatch activity cancel to worker",
			tag.NewStringTag("control_queue", controlQueueName),
			tag.Error(err))
		return err
	}

	if resp.GetRequestTimeout() != nil {
		t.logger.Warn("No worker polling control queue for activity cancel",
			tag.NewStringTag("control_queue", controlQueueName))
		return serviceerror.NewUnavailable("no worker polling control queue")
	}

	return nil
}
