package history

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	workerservicepb "go.temporal.io/api/nexusservices/workerservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
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
//   - No worker polling: matching returns RequestTimeout → *nexus.HandlerError{Type: UpstreamTimeout}.
//     Retryable — worker may come up later.
//   - Worker crashes after receiving the task: matching blocks waiting for a response until
//     context deadline, then returns RequestTimeout. Indistinguishable from "no worker polling".
//     Safe to retry because commands are idempotent (e.g., cancelling a missing activity is a
//     no-op success per the worker contract).
//   - Transport/RPC failure: *nexus.HandlerError. Retryable.
//   - Operation failure (worker explicitly returns error): *nexus.OperationError. Permanent —
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
		d.logger.Warn("Failed to dispatch worker commands",
			tag.NewStringTag("control_queue", task.Destination),
			tag.Error(err))
		metrics.WorkerCommandsDispatchFailure.With(d.metricsHandler).Record(1)
		return err
	}

	nexusErr := dispatchResponseToError(resp)
	if nexusErr == nil {
		metrics.WorkerCommandsDispatchSuccess.With(d.metricsHandler).Record(1)
		return nil
	}

	return d.handleError(nexusErr, task.Destination)
}

func (d *workerCommandsTaskDispatcher) handleError(nexusErr error, controlQueue string) error {
	var opErr *nexus.OperationError
	if errors.As(nexusErr, &opErr) {
		// Operation-level failure: the worker received and processed the request but returned
		// an error. Permanent — the worker contract requires success for all defined commands,
		// so this indicates a bug or version incompatibility. Retrying won't help.
		d.logger.Error("Worker returned operation failure for worker commands",
			tag.NewStringTag("control_queue", controlQueue),
			tag.Error(nexusErr))
		metrics.WorkerCommandsOperationFailure.With(d.metricsHandler).Record(1)
		return nil
	}

	var handlerErr *nexus.HandlerError
	if errors.As(nexusErr, &handlerErr) {
		if handlerErr.Type == nexus.HandlerErrorTypeUpstreamTimeout {
			d.logger.Warn("No worker polling control queue",
				tag.NewStringTag("control_queue", controlQueue))
			metrics.WorkerCommandsDispatchNoPoller.With(d.metricsHandler).Record(1)
			return nexusErr
		}

		d.logger.Warn("Worker commands transport failure",
			tag.NewStringTag("control_queue", controlQueue),
			tag.Error(nexusErr))
		metrics.WorkerCommandsDispatchFailure.With(d.metricsHandler).Record(1)
		return nexusErr
	}

	d.logger.Warn("Worker commands unexpected error",
		tag.NewStringTag("control_queue", controlQueue),
		tag.Error(nexusErr))
	metrics.WorkerCommandsDispatchFailure.With(d.metricsHandler).Record(1)
	return nexusErr
}

// dispatchResponseToError converts a DispatchNexusTaskResponse proto into a Nexus SDK error.
// Returns nil if the response indicates success. Handles the outer dispatch envelope (timeout,
// handler error, failure) and delegates to startOperationResponseToError for the inner response.
func dispatchResponseToError(resp *matchingservice.DispatchNexusTaskResponse) error {
	switch t := resp.GetOutcome().(type) {
	case *matchingservice.DispatchNexusTaskResponse_Failure:
		return handlerErrorFromTemporalFailure(t.Failure)
	case *matchingservice.DispatchNexusTaskResponse_HandlerError: //nolint:staticcheck // Deprecated case kept for backward compatibility.
		return handlerErrorFromDeprecatedProto(t.HandlerError) //nolint:staticcheck // Deprecated field kept for backward compatibility.
	case *matchingservice.DispatchNexusTaskResponse_RequestTimeout:
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUpstreamTimeout, "upstream timeout")
	case *matchingservice.DispatchNexusTaskResponse_Response:
		return startOperationResponseToError(t.Response.GetStartOperation())
	default:
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "empty outcome")
	}
}

// startOperationResponseToError converts a StartOperationResponse proto into a Nexus SDK error.
// Returns nil if the response indicates success (SyncSuccess or AsyncSuccess).
func startOperationResponseToError(resp *nexuspb.StartOperationResponse) error {
	switch t := resp.GetVariant().(type) {
	case *nexuspb.StartOperationResponse_SyncSuccess:
		return nil
	case *nexuspb.StartOperationResponse_AsyncSuccess:
		return nil
	case *nexuspb.StartOperationResponse_OperationError:
		//nolint:staticcheck // Deprecated variant still in use for backward compatibility.
		opErr := &nexus.OperationError{
			Message: "operation error",
			//nolint:staticcheck // Deprecated function still in use for backward compatibility.
			State: nexus.OperationState(t.OperationError.GetOperationState()),
			Cause: &nexus.FailureError{
				//nolint:staticcheck // Deprecated function still in use for backward compatibility.
				Failure: commonnexus.ProtoFailureToNexusFailure(t.OperationError.GetFailure()),
			},
		}
		if err := nexusrpc.MarkAsWrapperError(nexusrpc.DefaultFailureConverter(), opErr); err != nil {
			return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
		}
		return opErr
	case *nexuspb.StartOperationResponse_Failure:
		return operationErrorFromTemporalFailure(t.Failure)
	default:
		return nil
	}
}

func handlerErrorFromTemporalFailure(failure *failurepb.Failure) error {
	nf, err := commonnexus.TemporalFailureToNexusFailure(failure)
	if err != nil {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
	}
	he, err := nexusrpc.DefaultFailureConverter().FailureToError(nf)
	if err != nil {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
	}
	return he
}

func handlerErrorFromDeprecatedProto(he *nexuspb.HandlerError) *nexus.HandlerError {
	var retryBehavior nexus.HandlerErrorRetryBehavior
	//nolint:exhaustive // unspecified is the default
	switch he.GetRetryBehavior() {
	case enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE:
		retryBehavior = nexus.HandlerErrorRetryBehaviorRetryable
	case enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE:
		retryBehavior = nexus.HandlerErrorRetryBehaviorNonRetryable
	default:
	}
	//nolint:staticcheck // Deprecated function still in use for backward compatibility.
	cause := commonnexus.ProtoFailureToNexusFailure(he.GetFailure())
	return &nexus.HandlerError{
		//nolint:staticcheck // Deprecated function still in use for backward compatibility.
		Type:          nexus.HandlerErrorType(he.GetErrorType()),
		RetryBehavior: retryBehavior,
		Cause:         &nexus.FailureError{Failure: cause},
	}
}

func operationErrorFromTemporalFailure(failure *failurepb.Failure) error {
	state := nexus.OperationStateFailed
	if failure.GetCanceledFailureInfo() != nil {
		state = nexus.OperationStateCanceled
	}
	nf, err := commonnexus.TemporalFailureToNexusFailure(failure)
	if err != nil {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
	}
	cause, err := nexusrpc.DefaultFailureConverter().FailureToError(nf)
	if err != nil {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
	}
	opErr := &nexus.OperationError{
		State:   state,
		Message: "operation error",
		Cause:   cause,
	}
	if err := nexusrpc.MarkAsWrapperError(nexusrpc.DefaultFailureConverter(), opErr); err != nil {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
	}
	return opErr
}
