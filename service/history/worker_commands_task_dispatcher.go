package history

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
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

// workerCommandsTaskDispatcher dispatches worker commands to workers via Nexus.
//
// Failure scenarios:
//   - No worker polling: matching returns RequestTimeout -> *nexus.HandlerError{Type: UpstreamTimeout}.
//     Retryable -- worker may come up later.
//   - Worker crashes after receiving the task: matching blocks waiting for a response until
//     context deadline, then returns RequestTimeout. Indistinguishable from "no worker polling".
//     Safe to retry because commands are idempotent (e.g., cancelling a missing activity is a
//     no-op success per the worker contract).
//   - Transport/RPC failure: *nexus.HandlerError. Retryable.
//   - Operation failure (worker explicitly returns error): *nexus.OperationError. Permanent --
//     the worker contract requires success for all defined commands, so this indicates a bug
//     or version incompatibility.
//
// TODO: Add a worker-commands-specific retry cap (e.g., 5 attempts) instead of relying on the
// shared outbound queue HistoryTaskDLQUnexpectedErrorAttempts (default 70). These commands are
// best-effort with heartbeat timeout as fallback, so excessive retries waste resources.
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
		d.logger.Info("Worker commands feature disabled, dropping task",
			tag.WorkflowID(task.WorkflowID),
			tag.WorkflowRunID(task.RunID),
			tag.NewStringTag("control_queue", task.Destination),
			tag.NewInt("command_count", len(task.Commands)),
		)
		return nil
	}

	if len(task.Commands) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, workerCommandsTaskTimeout)
	defer cancel()

	return d.dispatchToWorker(ctx, task)
}

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
				Service:   workerservicepb.WorkerService.ServiceName,
				Operation: workerservicepb.WorkerService.ExecuteCommands.Name(),
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
		metrics.WorkerCommandsSent.With(d.metricsHandler).Record(1, metrics.OutcomeTag("rpc_error"))
		return fmt.Errorf("failed to dispatch worker commands to control queue %s: %w", task.Destination, err)
	}

	nexusErr := dispatchResponseToError(resp)
	if nexusErr == nil {
		metrics.WorkerCommandsSent.With(d.metricsHandler).Record(1, metrics.OutcomeTag("success"))
		return nil
	}

	return d.handleError(nexusErr, task)
}

func (d *workerCommandsTaskDispatcher) handleError(nexusErr error, task *tasks.WorkerCommandsTask) error {
	var opErr *nexus.OperationError
	if errors.As(nexusErr, &opErr) {
		// Operation-level failure: the worker received and processed the request but returned
		// an error. Permanent -- the worker contract requires success for all defined commands,
		// so this indicates a bug or version incompatibility. Retrying won't help.
		d.logger.Error("Worker returned operation failure for worker commands",
			tag.WorkflowID(task.WorkflowID),
			tag.WorkflowRunID(task.RunID),
			tag.NewStringTag("control_queue", task.Destination),
			tag.NewInt("command_count", len(task.Commands)),
			tag.Error(nexusErr))
		metrics.WorkerCommandsSent.With(d.metricsHandler).Record(1, metrics.OutcomeTag("operation_error"))
		return nil
	}

	var handlerErr *nexus.HandlerError
	if errors.As(nexusErr, &handlerErr) {
		if handlerErr.Type == nexus.HandlerErrorTypeUpstreamTimeout {
			d.logger.Warn("No worker polling control queue",
				tag.NewStringTag("control_queue", task.Destination))
			metrics.WorkerCommandsSent.With(d.metricsHandler).Record(1, metrics.OutcomeTag("no_poller"))
			return nexusErr
		}

		d.logger.Warn("Worker commands transport failure",
			tag.NewStringTag("control_queue", task.Destination),
			tag.Error(nexusErr))
		metrics.WorkerCommandsSent.With(d.metricsHandler).Record(1, metrics.OutcomeTag("transport_error"))
		return nexusErr
	}

	d.logger.Warn("Worker commands unexpected error",
		tag.NewStringTag("control_queue", task.Destination),
		tag.Error(nexusErr))
	metrics.WorkerCommandsSent.With(d.metricsHandler).Record(1, metrics.OutcomeTag("unexpected_error"))
	return nexusErr
}
