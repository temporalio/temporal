package workercommands

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
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
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/proto"
)

const (
	DispatchTimeout = time.Second * 10 * debug.TimeoutMultiplier
	MaxTaskAttempts = 3

	// Nexus service and operation names for worker commands.
	// TODO: Replace with workerservicepb.WorkerService.ServiceName and
	// workerservicepb.WorkerService.ExecuteCommands.Name() once the Nexus service
	// descriptor is published in go.temporal.io/api.
	ServiceName   = "temporal.api.nexusservices.workerservice.v1.WorkerService"
	OperationName = "ExecuteCommands"
)

// Dispatcher dispatches worker commands to workers via Nexus.
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
// Retryable errors are capped at MaxTaskAttempts attempts (in-memory). These
// commands are best-effort — the activity will eventually time out anyway — so excessive
// retries waste resources. The counter resets on shard movement, which is acceptable.
type Dispatcher struct {
	matchingClient resource.MatchingClient
	config         *configs.Config
	metricsHandler metrics.Handler
	logger         log.Logger
}

func NewDispatcher(
	matchingClient resource.MatchingClient,
	config *configs.Config,
	metricsHandler metrics.Handler,
	logger log.Logger,
) *Dispatcher {
	return &Dispatcher{
		matchingClient: matchingClient,
		config:         config,
		metricsHandler: metricsHandler,
		logger:         logger,
	}
}

func (d *Dispatcher) Execute(
	ctx context.Context,
	task *tasks.WorkerCommandsTask,
	attempt int,
	namespaceName string,
) error {
	if attempt > MaxTaskAttempts {
		d.logger.Info("Worker commands task exceeded max attempts, dropping",
			tag.WorkflowID(task.WorkflowID),
			tag.WorkflowRunID(task.RunID),
			tag.NewStringTag("control_queue", task.Destination),
			tag.Attempt(int32(attempt)),
		)
		d.recordCommandMetrics(task.Commands, namespaceName, "max_attempts_exceeded")
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

	ctx, cancel := context.WithTimeout(ctx, DispatchTimeout)
	defer cancel()

	return d.dispatchToWorker(ctx, task, namespaceName)
}

func (d *Dispatcher) dispatchToWorker(
	ctx context.Context,
	task *tasks.WorkerCommandsTask,
	namespaceName string,
) error {
	request := &workerservicepb.ExecuteCommandsRequest{
		Commands: task.Commands,
	}
	// Encode as binary/protobuf using the standard Temporal payload format.
	// Worker commands are handled directly by SDK Core (not by lang-SDK Nexus handlers),
	// so we use binary/protobuf which Core can decode natively via prost. The standard
	// payload.Encode() uses json/protobuf encoding, which Core does not support because
	// it normally delegates Nexus payload deserialization to the lang SDK.
	requestData, err := proto.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to encode worker commands request: %w", err)
	}
	requestPayload := &commonpb.Payload{
		Metadata: map[string][]byte{
			"encoding": []byte("binary/protobuf"),
		},
		Data: requestData,
	}

	nexusRequest := &nexuspb.Request{
		Header: map[string]string{},
		Variant: &nexuspb.Request_StartOperation{
			StartOperation: &nexuspb.StartOperationRequest{
				Service:   ServiceName,
				Operation: OperationName,
				Payload:   requestPayload,
			},
		},
	}

	resp, err := d.matchingClient.DispatchNexusTask(ctx, &matchingservice.DispatchNexusTaskRequest{
		NamespaceId: task.NamespaceID,
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: task.Destination,
			Kind: enumspb.TASK_QUEUE_KIND_WORKER_COMMANDS,
		},
		Request: nexusRequest,
	})
	if err != nil {
		d.recordCommandMetrics(task.Commands, namespaceName, "rpc_error")
		return fmt.Errorf("failed to dispatch worker commands to control queue %s: %w", task.Destination, err)
	}

	nexusErr := commonnexus.MatchingDispatchResponseToError(resp)
	if nexusErr == nil {
		d.recordCommandMetrics(task.Commands, namespaceName, "success")
		return nil
	}

	return d.handleError(nexusErr, task, namespaceName)
}

func (d *Dispatcher) handleError(nexusErr error, task *tasks.WorkerCommandsTask, namespaceName string) error {
	var handlerErr *nexus.HandlerError
	if errors.As(nexusErr, &handlerErr) {
		// Handler-level error (transport, timeout, internal). These are constructed by
		// MatchingDispatchResponseToError for non-worker-returned failures.
		if handlerErr.Type == nexus.HandlerErrorTypeUpstreamTimeout {
			d.logger.Warn("No worker polling control queue",
				tag.NewStringTag("control_queue", task.Destination))
			d.recordCommandMetrics(task.Commands, namespaceName, "no_poller")
			return nexusErr
		}

		if !handlerErr.Retryable() {
			d.logger.Error("Worker commands non-retryable handler error",
				tag.NewStringTag("control_queue", task.Destination),
				tag.Error(nexusErr))
			d.recordCommandMetrics(task.Commands, namespaceName, "non_retryable_error")
			return nil
		}

		d.logger.Warn("Worker commands transport failure",
			tag.NewStringTag("control_queue", task.Destination),
			tag.Error(nexusErr))
		d.recordCommandMetrics(task.Commands, namespaceName, "transport_error")
		return nexusErr
	}

	// Worker-returned failure (ApplicationError, CanceledError, etc.). The worker received
	// and processed the request but returned an error. Permanent — the worker contract
	// requires success for all defined commands, so this indicates a bug or version
	// incompatibility. Retrying won't help.
	d.logger.Error("Worker returned failure for worker commands",
		tag.WorkflowID(task.WorkflowID),
		tag.WorkflowRunID(task.RunID),
		tag.NewStringTag("control_queue", task.Destination),
		tag.NewInt("command_count", len(task.Commands)),
		tag.Error(nexusErr))
	d.recordCommandMetrics(task.Commands, namespaceName, "worker_error")
	return nil
}

func (d *Dispatcher) recordCommandMetrics(commands []*workerpb.WorkerCommand, namespaceName string, outcome string) {
	for _, cmd := range commands {
		metrics.WorkerCommandsSent.With(d.metricsHandler).Record(
			1,
			metrics.NamespaceTag(namespaceName),
			metrics.OutcomeTag(outcome),
			metrics.StringTag("command_type", CommandTypeName(cmd)),
		)
	}
}

func CommandTypeName(cmd *workerpb.WorkerCommand) string {
	switch cmd.GetType().(type) {
	case *workerpb.WorkerCommand_CancelActivity:
		return "cancel_activity"
	default:
		return "unknown"
	}
}
