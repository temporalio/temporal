package history

import (
	"context"
	"errors"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	workerservicepb "go.temporal.io/api/nexusservices/workerservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/tasks"
)

const (
	activityCommandTaskTimeout = time.Second * 10 * debug.TimeoutMultiplier
)

// activityCommandTaskDispatcher handles dispatching activity command tasks to workers.
type activityCommandTaskDispatcher struct {
	matchingRawClient resource.MatchingRawClient
	config            *configs.Config
	metricsHandler    metrics.Handler
	logger            log.Logger
}

func newActivityCommandTaskDispatcher(
	matchingRawClient resource.MatchingRawClient,
	config *configs.Config,
	metricsHandler metrics.Handler,
	logger log.Logger,
) *activityCommandTaskDispatcher {
	return &activityCommandTaskDispatcher{
		matchingRawClient: matchingRawClient,
		config:            config,
		metricsHandler:    metricsHandler,
		logger:            logger,
	}
}

func (d *activityCommandTaskDispatcher) execute(
	ctx context.Context,
	task *tasks.ActivityCommandTask,
) error {
	if !d.config.EnableActivityCancellationNexusTask() {
		return nil
	}

	if len(task.TaskTokens) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, activityCommandTaskTimeout)
	defer cancel()

	return d.dispatchToWorker(ctx, task)
}

const (
	workerServiceName        = "temporal.api.nexusservices.workerservice.v1.WorkerService"
	executeCommandsOperation = "ExecuteCommands"
)

func (d *activityCommandTaskDispatcher) dispatchToWorker(
	ctx context.Context,
	task *tasks.ActivityCommandTask,
) error {
	commands := make([]*workerpb.WorkerCommand, 0, len(task.TaskTokens))
	for _, token := range task.TaskTokens {
		commands = append(commands, &workerpb.WorkerCommand{
			Type: &workerpb.WorkerCommand_CancelActivity{
				CancelActivity: &workerpb.CancelActivityCommand{
					TaskToken: token,
				},
			},
		})
	}

	request := &workerservicepb.ExecuteCommandsRequest{
		Commands: commands,
	}
	requestPayload, err := payload.Encode(request)
	if err != nil {
		return fmt.Errorf("failed to encode worker commands request: %w", err)
	}

	nexusRequest := &nexuspb.Request{
		Header: map[string]string{},
		Variant: &nexuspb.Request_StartOperation{
			StartOperation: &nexuspb.StartOperationRequest{
				Service:   workerServiceName,
				Operation: executeCommandsOperation,
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
		d.logger.Warn("Failed to dispatch activity command to worker",
			tag.NewStringTag("control_queue", task.Destination),
			tag.Error(err))
		return err
	}

	return d.handleDispatchResponse(resp, task.Destination)
}

func (d *activityCommandTaskDispatcher) handleDispatchResponse(
	resp *matchingservice.DispatchNexusTaskResponse,
	controlQueue string,
) error {
	// Check for timeout (no worker polling)
	if resp.GetRequestTimeout() != nil {
		d.logger.Warn("No worker polling control queue for activity command",
			tag.NewStringTag("control_queue", controlQueue))
		return errors.New("no worker polling control queue")
	}

	// Check for worker handler failure
	if failure := resp.GetFailure(); failure != nil {
		d.logger.Warn("Worker handler failed for activity command",
			tag.NewStringTag("control_queue", controlQueue),
			tag.NewStringTag("failure_message", failure.GetMessage()))
		return fmt.Errorf("worker handler failed: %s", failure.GetMessage())
	}

	// Check operation-level response
	nexusResp := resp.GetResponse()
	if nexusResp == nil {
		return nil
	}

	startOpResp := nexusResp.GetStartOperation()
	if startOpResp == nil {
		return nil
	}

	// Check for operation failure (terminal - don't retry)
	if opFailure := startOpResp.GetFailure(); opFailure != nil {
		d.logger.Warn("Activity command operation failure",
			tag.NewStringTag("control_queue", controlQueue),
			tag.NewStringTag("failure_message", opFailure.GetMessage()))
		return nil
	}

	return nil
}

