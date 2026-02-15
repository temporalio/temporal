package history

import (
	"context"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workerpb "go.temporal.io/api/worker/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
)

const (
	// workerControlService is the Nexus service name for server-to-worker control commands.
	workerControlService = "temporal.api.worker.v1.WorkerService"
	// notifyActivityOperation is the Nexus operation name for activity notification requests.
	notifyActivityOperation = "notify-activity"
)

// notifyActivityTaskDispatcher handles dispatching notify activity tasks to workers.
type notifyActivityTaskDispatcher struct {
	matchingRawClient resource.MatchingRawClient
	config            *configs.Config
	logger            log.Logger
}

func newNotifyActivityTaskDispatcher(
	matchingRawClient resource.MatchingRawClient,
	config *configs.Config,
	logger log.Logger,
) *notifyActivityTaskDispatcher {
	return &notifyActivityTaskDispatcher{
		matchingRawClient: matchingRawClient,
		config:            config,
		logger:            logger,
	}
}

func (d *notifyActivityTaskDispatcher) execute(
	ctx context.Context,
	mutableState historyi.MutableState,
	task *tasks.NotifyActivityTask,
) error {
	if !d.config.EnableActivityCancellationNexusTask() {
		return nil
	}

	if len(task.ScheduledEventIDs) == 0 {
		return nil
	}

	taskTokens, err := d.buildTaskTokens(mutableState, task)
	if err != nil {
		return err
	}
	if len(taskTokens) == 0 {
		return nil
	}

	return d.dispatchToWorker(ctx, task, taskTokens)
}

func (d *notifyActivityTaskDispatcher) buildTaskTokens(
	mutableState historyi.MutableState,
	task *tasks.NotifyActivityTask,
) ([][]byte, error) {
	var taskTokens [][]byte
	for _, scheduledEventID := range task.ScheduledEventIDs {
		ai, ok := mutableState.GetActivityInfo(scheduledEventID)
		if !ok || ai.StartedEventId == common.EmptyEventID {
			continue
		}
		if task.NotificationType == enumsspb.ACTIVITY_NOTIFICATION_TYPE_CANCEL && !ai.CancelRequested {
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

func (d *notifyActivityTaskDispatcher) dispatchToWorker(
	ctx context.Context,
	task *tasks.NotifyActivityTask,
	taskTokens [][]byte,
) error {
	notificationRequest := &workerpb.ActivityNotificationRequest{
		NotificationType: workerpb.ActivityNotificationType(task.NotificationType),
		TaskTokens:       taskTokens,
	}
	requestPayload, err := payload.Encode(notificationRequest)
	if err != nil {
		return fmt.Errorf("failed to encode activity notification request: %w", err)
	}

	nexusRequest := &nexuspb.Request{
		Header: map[string]string{},
		Variant: &nexuspb.Request_StartOperation{
			StartOperation: &nexuspb.StartOperationRequest{
				Service:   workerControlService,
				Operation: notifyActivityOperation,
				Payload:   requestPayload,
			},
		},
	}

	resp, err := d.matchingRawClient.DispatchNexusTask(ctx, &matchingservice.DispatchNexusTaskRequest{
		NamespaceId: task.NamespaceID,
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: task.Destination,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Request: nexusRequest,
	})
	if err != nil {
		d.logger.Warn("Failed to dispatch activity notification to worker",
			tag.NewStringTag("control_queue", task.Destination),
			tag.Error(err))
		return err
	}

	if resp.GetRequestTimeout() != nil {
		d.logger.Warn("No worker polling control queue for activity notification",
			tag.NewStringTag("control_queue", task.Destination))
		return fmt.Errorf("no worker polling control queue")
	}

	return nil
}
