package callback

import (
	"context"
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

// PROTOTYPE
//
// invokeWorkerCallback delivers a completed Nexus operation's result to a worker for processing. It
// builds a Nexus CompletionRequest and dispatches it to the target worker's task queue via the
// matching service. The matching dispatch is synchronous: it blocks until the worker handles the
// task and responds with a CompletionResponse (or the request times out). Once that response is
// received, the callback is done.
type invokeWorkerCallback struct {
	callback   *callbackspb.Callback_Nexus
	completion nexusrpc.CompleteOperationOptions
	requestID  string
}

func (c invokeWorkerCallback) WrapError(_ invocationResult, err error) error {
	return err
}

func (c invokeWorkerCallback) Invoke(
	ctx context.Context,
	// IMPORTANT: The namespace of the callback's execution. Which we expect to ALSO match the
	// namespace the worker callback dispatches into. Otherwise, worker callbacks could dispatch tasks
	// into _other_ namespaces, and that would be a big security problem.
	ns *namespace.Namespace,
	h *invocationTaskHandler,
	task *callbackspb.InvocationTask,
	taskAttr chasm.TaskAttributes,
) invocationResult {
	// Gather routing information from the callback's headers.
	header := c.callback.GetHeader()
	targetEndpointName := header[commonnexus.WorkerCallbackTargetEndpointHeader]
	targetService := header[commonnexus.WorkerCallbackTargetServiceHeader]
	targetOperation := header[commonnexus.WorkerCallbackTargetOperationHeader]
	if targetEndpointName == "" || targetService == "" || targetOperation == "" {
		// Misconfigured callback: nothing a retry can fix. Fail permanently.
		err := logInternalError(h.logger, "worker callback missing required header", nil)
		return invocationResultFail{err}
	}

	if h.endpointRegistry == nil {
		// Should not happen in the history service, where the registry is provided. Retry rather than
		// fail permanently — a misconfiguration is operator-fixable.
		return invocationResultRetry{err: logInternalError(h.logger,
			"worker callback dispatch requires an EndpointRegistry, but none is configured", nil)}
	}

	// Resolve the target Nexus endpoint to its worker target, which supplies the destination
	// namespace and task queue.
	entry, err := h.endpointRegistry.GetByName(ctx, ns.ID(), targetEndpointName)
	if err != nil {
		if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
			// Unknown endpoint: a retry won't fix it. Fail permanently.
			return invocationResultFail{logInternalError(h.logger,
				fmt.Sprintf("worker callback target endpoint %q not found", targetEndpointName), err)}
		}
		// Transient registry/persistence errors are retryable.
		return invocationResultRetry{err: fmt.Errorf("resolve target endpoint %q: %w", targetEndpointName, err)}
	}

	workerTarget, ok := entry.GetEndpoint().GetSpec().GetTarget().GetVariant().(*persistencespb.NexusEndpointTarget_Worker_)
	if !ok {
		// Worker callbacks can only be delivered to a worker target (not an external URL).
		return invocationResultFail{logInternalError(h.logger,
			fmt.Sprintf("worker callback target endpoint %q is not a worker target", targetEndpointName), nil)}
	}
	targetNamespaceID := workerTarget.Worker.GetNamespaceId()
	targetTaskQueue := workerTarget.Worker.GetTaskQueue()

	// SECURITY: confirm the endpoint dispatches into the same namespace as the callback component
	// being invoked. Otherwise a worker callback could dispatch completions into _other_ namespaces,
	// and that would be a big security problem.
	if ns.ID().String() != targetNamespaceID {
		err := fmt.Errorf("target namespace (%s) does not match invocation namespace (%s)", targetNamespaceID, ns.ID().String())
		return invocationResultFail{logInternalError(h.logger, "namespace mismatch", err)}
	}

	// Build the completion describing the operation that finished. Service/Operation/OperationId of
	// the originating operation are not yet populated by the CompletionSource (see
	// nexusoperation.Operation.GetNexusCompletion); they can be filled in once that data is wired up.
	// Links are likewise stubbed by the source for now, so they are left unset here.
	operationCompletion := &nexuspb.CompletionRequest_NexusOperationCompletion{
		OperationToken: c.completion.OperationToken,
	}
	if c.completion.Error != nil {
		failure, err := c.completionFailure()
		if err != nil {
			return invocationResultFail{logInternalError(h.logger, "failed to convert completion failure", err)}
		}
		operationCompletion.Failure = failure
	} else if c.completion.Result != nil {
		result, ok := c.completion.Result.(*commonpb.Payload)
		if !ok {
			err := logInternalError(h.logger, fmt.Sprintf("expected *commonpb.Payload result, got %T", c.completion.Result), nil)
			return invocationResultFail{err}
		}
		operationCompletion.Result = result
	}

	nexusRequest := &nexuspb.Request{
		Header: map[string]string{},
		Variant: &nexuspb.Request_Completion{
			Completion: &nexuspb.CompletionRequest{
				Service:   targetService,
				Operation: targetOperation,
				// Idempotency key derived from the callback's stable request ID, so a re-delivered
				// side-effect task dedupes onto the same completion instead of being processed twice.
				RequestId: fmt.Sprintf("worker-callback-%s", c.requestID),
				Variant: &nexuspb.CompletionRequest_NexusOperation{
					NexusOperation: operationCompletion,
				},
			},
		},
	}

	if h.matchingClient == nil {
		// Should not happen in the history service, where the client is provided. If it does, retry
		// rather than fail permanently — a misconfiguration is operator-fixable.
		return invocationResultRetry{err: logInternalError(h.logger,
			"worker callback dispatch requires a MatchingClient, but none is configured", nil)}
	}

	// Dispatch the completion to the worker's Nexus task queue. ctx is already deadline-bounded by
	// the task handler (config.RequestTimeout); matching blocks until the worker responds via
	// RespondNexusTaskCompleted or the deadline fires.
	resp, err := h.matchingClient.DispatchNexusTask(ctx, &matchingservice.DispatchNexusTaskRequest{
		NamespaceId: targetNamespaceID,
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: targetTaskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Request: nexusRequest,
	})
	if err != nil {
		if isRetryableRPCResponse(err) {
			return invocationResultRetry{err: err}
		}
		return invocationResultFail{err}
	}

	return dispatchResponseToInvocationResult(resp)
}

// completionFailure converts the callback's completion error into a Temporal API failure, mirroring
// invocableInternal.getHistoryRequest. The operation error is unwrapped so the handler receives the
// underlying cause.
func (c invokeWorkerCallback) completionFailure() (*failurepb.Failure, error) {
	failure, err := nexusrpc.DefaultFailureConverter().ErrorToFailure(c.completion.Error)
	if err != nil {
		return nil, fmt.Errorf("failed to convert error to failure: %w", err)
	}
	// Unwrap the operation error; the handler on the other side expects the underlying cause.
	if failure.Cause != nil {
		failure = *failure.Cause
	}
	apiFailure, err := commonnexus.NexusFailureToTemporalFailure(failure)
	if err != nil {
		return nil, fmt.Errorf("failed to convert failure type: %w", err)
	}
	return apiFailure, nil
}

// dispatchResponseToInvocationResult maps a synchronous matching DispatchNexusTask outcome onto a
// callback invocation result, following the same semantics as the worker-commands dispatcher:
//   - successful completion (no failure) -> OK
//   - worker-returned failure -> permanent fail (the worker received and processed the task)
//   - request timeout (no poller / worker not up yet) -> retry
//   - anything else -> permanent fail
func dispatchResponseToInvocationResult(resp *matchingservice.DispatchNexusTaskResponse) invocationResult {
	switch outcome := resp.GetOutcome().(type) {
	case *matchingservice.DispatchNexusTaskResponse_Response:
		completion := outcome.Response.GetCompletion()
		if completion == nil {
			return invocationResultFail{fmt.Errorf("unexpected non-completion response variant: %T", outcome.Response.GetVariant())}
		}
		if failure := completion.GetFailure(); failure != nil {
			return invocationResultFail{fmt.Errorf("worker callback handler failed: %s", failure.GetMessage())}
		}
		return invocationResultOK{}
	case *matchingservice.DispatchNexusTaskResponse_Failure:
		return invocationResultFail{fmt.Errorf("worker failed the callback task: %s", outcome.Failure.GetMessage())}
	case *matchingservice.DispatchNexusTaskResponse_RequestTimeout:
		return invocationResultRetry{err: fmt.Errorf("no worker polling task queue for worker callback")}
	default:
		return invocationResultFail{fmt.Errorf("empty or unknown dispatch outcome: %T", outcome)}
	}
}
