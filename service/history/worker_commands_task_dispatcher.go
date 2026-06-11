package history

import (
	"context"
	"time"

	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/workercommands"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/tasks"
)

const (
	workerCommandsTaskTimeout    = time.Second * 10 * debug.TimeoutMultiplier
	workerCommandsMaxTaskAttempt = 3
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
//   - Worker failure (worker explicitly returns error): *temporal.ApplicationError or
//     *temporal.CanceledError. Permanent — the worker contract requires success for all
//     defined commands, so this indicates a bug or version incompatibility.
//
// Retryable errors are capped at workerCommandsMaxTaskAttempt attempts (in-memory). These
// commands are best-effort — the activity will eventually time out anyway — so excessive
// retries waste resources. The counter resets on shard movement, which is acceptable.
type workerCommandsTaskDispatcher struct {
	matchingClient resource.MatchingClient
	config         *configs.Config
	metricsHandler metrics.Handler
	logger         log.Logger
}

func newWorkerCommandsTaskDispatcher(
	matchingClient resource.MatchingClient,
	config *configs.Config,
	metricsHandler metrics.Handler,
	logger log.Logger,
) *workerCommandsTaskDispatcher {
	return &workerCommandsTaskDispatcher{
		matchingClient: matchingClient,
		config:         config,
		metricsHandler: metricsHandler,
		logger:         logger,
	}
}

func (d *workerCommandsTaskDispatcher) execute(
	ctx context.Context,
	task *tasks.WorkerCommandsTask,
	attempt int,
	namespaceName string,
) error {
	if attempt > workerCommandsMaxTaskAttempt {
		d.logger.Info("Worker commands task exceeded max attempts, dropping",
			tag.WorkflowID(task.WorkflowID),
			tag.WorkflowRunID(task.RunID),
			tag.NewStringTag("control_queue", task.Destination),
			tag.Attempt(int32(attempt)),
		)
		metrics.WorkerCommandsSent.With(d.metricsHandler).Record(1, metrics.OutcomeTag("max_attempts_exceeded"))
		return nil
	}

	if !d.config.EnableCancelActivityWorkerCommand(namespaceName) {
		d.logger.Info("Worker commands feature disabled, dropping task",
			tag.WorkflowNamespace(namespaceName),
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

	return workercommands.DispatchToWorker(
		ctx,
		d.matchingClient,
		d.metricsHandler,
		d.logger,
		task.NamespaceID,
		task.Destination,
		task.Commands,
	)
}
