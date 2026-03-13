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
	workerCommandsTaskTimeout = time.Second * 10 * debug.TimeoutMultiplier
)

// workerCommandsTaskDispatcher handles dispatching worker commands to workers via Nexus.
type workerCommandsTaskDispatcher struct {
	matchingRawClient resource.MatchingRawClient
	config            *configs.Config
	metricsHandler    metrics.Handler
	logger            log.Logger
}

func newWorkerCommandsTaskDispatcher(
	matchingRawClient resource.MatchingRawClient,
	config *configs.Config,
	metricsHandler metrics.Handler,
	logger log.Logger,
) *workerCommandsTaskDispatcher {
	return &workerCommandsTaskDispatcher{
		matchingRawClient: matchingRawClient,
		config:            config,
		metricsHandler:    metricsHandler,
		logger:            logger,
	}
}

func (d *workerCommandsTaskDispatcher) execute(
	ctx context.Context,
	task *tasks.WorkerCommandsTask,
) error {
	if !d.config.EnableCancelActivityWorkerCommand() {
		return nil
	}

	if len(task.Commands) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, workerCommandsTaskTimeout)
	defer cancel()

	return d.dispatchToWorker(ctx, task)
}

const (
	workerServiceName        = "temporal.api.nexusservices.workerservice.v1.WorkerService"
	executeCommandsOperation = "ExecuteCommands"
)

func (d *workerCommandsTaskDispatcher) dispatchToWorker(
	ctx context.Context,
	task *tasks.WorkerCommandsTask,
) error {
	request := &workerservicepb.ExecuteCommandsRequest{
		Commands: task.Commands,
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
		d.logger.Warn("Failed to dispatch worker commands",
			tag.NewStringTag("control_queue", task.Destination),
			tag.Error(err))
		return err
	}

	return d.handleDispatchResponse(resp, task.Destination)
}

func (d *workerCommandsTaskDispatcher) handleDispatchResponse(
	resp *matchingservice.DispatchNexusTaskResponse,
	controlQueue string,
) error {
	if resp.GetRequestTimeout() != nil {
		d.logger.Warn("No worker polling control queue",
			tag.NewStringTag("control_queue", controlQueue))
		return errors.New("no worker polling control queue")
	}

	if failure := resp.GetFailure(); failure != nil {
		d.logger.Warn("Worker handler failed",
			tag.NewStringTag("control_queue", controlQueue),
			tag.NewStringTag("failure_message", failure.GetMessage()))
		return fmt.Errorf("worker handler failed: %s", failure.GetMessage())
	}

	nexusResp := resp.GetResponse()
	if nexusResp == nil {
		return nil
	}

	startOpResp := nexusResp.GetStartOperation()
	if startOpResp == nil {
		return nil
	}

	if opFailure := startOpResp.GetFailure(); opFailure != nil {
		d.logger.Warn("Worker command operation failure",
			tag.NewStringTag("control_queue", controlQueue),
			tag.NewStringTag("failure_message", opFailure.GetMessage()))
		return nil
	}

	return nil
}
