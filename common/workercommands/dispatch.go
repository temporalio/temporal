package workercommands

import (
	"context"
	"errors"
	"fmt"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	workerservicepb "go.temporal.io/api/nexusservices/workerservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/resource"
	"google.golang.org/protobuf/proto"
)

const (
	// Nexus service and operation names for worker commands.
	// TODO: Replace with workerservicepb.WorkerService.ServiceName and
	// workerservicepb.WorkerService.ExecuteCommands.Name() once the Nexus service
	// descriptor is published in go.temporal.io/api.
	ServiceName   = "temporal.api.nexusservices.workerservice.v1.WorkerService"
	OperationName = "ExecuteCommands"
)

// DispatchToWorker dispatches worker commands to a worker's control queue via Nexus.
// It encodes the commands as binary/protobuf (which SDK Core can decode natively via prost),
// sends them via DispatchNexusTask to matching, and handles the response.
// Returns nil on success or permanent (non-retryable) errors. Returns an error for
// retryable failures so the caller can retry.
func DispatchToWorker(
	ctx context.Context,
	matchingClient resource.MatchingClient,
	metricsHandler metrics.Handler,
	logger log.Logger,
	namespaceID string,
	controlQueue string,
	commands []*workerpb.WorkerCommand,
) error {
	request := &workerservicepb.ExecuteCommandsRequest{
		Commands: commands,
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

	resp, err := matchingClient.DispatchNexusTask(ctx, &matchingservice.DispatchNexusTaskRequest{
		NamespaceId: namespaceID,
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: controlQueue,
			Kind: enumspb.TASK_QUEUE_KIND_WORKER_COMMANDS,
		},
		Request: nexusRequest,
	})
	if err != nil {
		logger.Warn("Failed to dispatch worker commands",
			tag.NewStringTag("control_queue", controlQueue),
			tag.Error(err))
		metrics.WorkerCommandsSent.With(metricsHandler).Record(1, metrics.OutcomeTag("rpc_error"))
		return err
	}

	nexusErr := commonnexus.DispatchResponseToError(resp)
	if nexusErr == nil {
		metrics.WorkerCommandsSent.With(metricsHandler).Record(1, metrics.OutcomeTag("success"))
		return nil
	}

	return HandleDispatchError(nexusErr, controlQueue, metricsHandler, logger)
}

// HandleDispatchError classifies a Nexus dispatch error and records the appropriate metric.
// Returns nil for permanent errors (caller should not retry) and the original error for
// retryable failures.
func HandleDispatchError(nexusErr error, controlQueue string, metricsHandler metrics.Handler, logger log.Logger) error {
	var handlerErr *nexus.HandlerError
	if errors.As(nexusErr, &handlerErr) {
		if handlerErr.Type == nexus.HandlerErrorTypeUpstreamTimeout {
			logger.Warn("No worker polling control queue",
				tag.NewStringTag("control_queue", controlQueue))
			metrics.WorkerCommandsSent.With(metricsHandler).Record(1, metrics.OutcomeTag("no_poller"))
			return nexusErr
		}

		if !handlerErr.Retryable() {
			logger.Error("Worker commands non-retryable handler error",
				tag.NewStringTag("control_queue", controlQueue),
				tag.Error(nexusErr))
			metrics.WorkerCommandsSent.With(metricsHandler).Record(1, metrics.OutcomeTag("non_retryable_error"))
			return nil
		}

		logger.Warn("Worker commands transport failure",
			tag.NewStringTag("control_queue", controlQueue),
			tag.Error(nexusErr))
		metrics.WorkerCommandsSent.With(metricsHandler).Record(1, metrics.OutcomeTag("transport_error"))
		return nexusErr
	}

	// Worker-returned failure (ApplicationError, CanceledError, etc.). The worker received
	// and processed the request but returned an error. Permanent — the worker contract
	// requires success for all defined commands, so this indicates a bug or version
	// incompatibility. Retrying won't help.
	logger.Error("Worker returned failure for worker commands",
		tag.NewStringTag("control_queue", controlQueue),
		tag.Error(nexusErr))
	metrics.WorkerCommandsSent.With(metricsHandler).Record(1, metrics.OutcomeTag("worker_error"))
	return nil
}
