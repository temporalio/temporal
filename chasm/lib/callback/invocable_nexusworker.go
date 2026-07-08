package callback

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/payload"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
)

const (
	// CompletionServiceName is the well-known Nexus service name that worker completion callbacks are
	// dispatched to. The completion worker registers a Nexus service with this name to receive
	// operation-completion notifications.
	CompletionServiceName = "temporal.nexus.v1.CompletionService"
	// CompletionOperationName is the well-known Nexus operation invoked on CompletionServiceName to deliver
	// a source operation's completion to the worker.
	CompletionOperationName = "OnComplete"
)

// invocableNexusWorker is an invocable that delivers a completion callback to a Nexus service worker by
// dispatching a new Nexus StartOperation task directly to the worker's task queue via
// MatchingService.DispatchNexusTask. This is more efficient than the invocableOutbound variant (which POSTs
// the completion over HTTP and relies on the frontend to route it back to matching), because it hands the
// task to the waiting worker through matching without a round-trip through the HTTP Nexus machinery.
type invocableNexusWorker struct {
	callback   *callbackspb.Callback_NexusWorker
	completion nexusrpc.CompleteOperationOptions
	// requestID is used as the Nexus request ID so that retries of this callback are idempotent on the
	// handler side.
	requestID string
}

func (n invocableNexusWorker) WrapError(result invocationResult, err error) error {
	// A retry indicates the destination (worker task queue) is unresponsive; surface it as a
	// DestinationDownError so the outbound queue's circuit breaker can trip for this destination.
	if retry, ok := result.(invocationResultRetry); ok {
		return queueserrors.NewDestinationDownError(retry.err.Error(), err)
	}
	return err
}

// buildCompletionInput constructs the OnCompleteHandlerInput delivered to the completion handler from the
// source operation's outcome and the callback's source context.
//
// TODO(chrsmith): Populate OnCompleteHandlerInput.SourceOperation (endpoint/service/operation/token/links).
// That metadata is not carried on the NexusWorker callback proto today, so it is left unset for now.
func (n invocableNexusWorker) buildCompletionInput() (*nexuspb.OnCompleteHandlerInput, error) {
	outcome := &nexuspb.OnCompleteHandlerInput_Outcome{}
	if n.completion.Error != nil {
		// Unsuccessful completion: convert the Nexus operation error into a Temporal failure, mirroring
		// invocableInternal.getHistoryRequest.
		failure, err := nexusrpc.DefaultFailureConverter().ErrorToFailure(n.completion.Error)
		if err != nil {
			return nil, fmt.Errorf("failed to convert error to failure: %w", err)
		}
		// Unwrap the operation error; the handler expects the underlying cause.
		if failure.Cause != nil {
			failure = *failure.Cause
		}
		apiFailure, err := commonnexus.NexusFailureToTemporalFailure(failure)
		if err != nil {
			return nil, fmt.Errorf("failed to convert failure type: %w", err)
		}
		outcome.Result = &nexuspb.OnCompleteHandlerInput_Outcome_Failure{Failure: apiFailure}
	} else {
		// Successful completion. The result payload may legitimately be nil.
		var result *commonpb.Payload
		if n.completion.Result != nil {
			p, ok := n.completion.Result.(*commonpb.Payload)
			if !ok {
				return nil, fmt.Errorf("invalid result, expected a payload, got: %T", n.completion.Result)
			}
			result = p
		}
		outcome.Result = &nexuspb.OnCompleteHandlerInput_Outcome_Success{Success: result}
	}

	// SourceContext is carried as Payloads on the callback but the handler input holds a single Payload.
	var sourceContext *commonpb.Payload
	if payloads := n.callback.GetSourceContext().GetPayloads(); len(payloads) > 0 {
		sourceContext = payloads[0]
	}

	return &nexuspb.OnCompleteHandlerInput{
		Outcome:       outcome,
		SourceContext: sourceContext,
	}, nil
}

func (n invocableNexusWorker) Invoke(
	ctx context.Context,
	ns *namespace.Namespace,
	h *invocationTaskHandler,
	task *callbackspb.InvocationTask,
	taskAttr chasm.TaskAttributes,
) invocationResult {
	// The NexusWorker variant requires the matching client to dispatch the task. If it is missing the task is
	// unprocessable in this process and there is no point retrying it here.
	if h.matchingClient == nil {
		return invocationResultFail{queueserrors.NewUnprocessableTaskError(
			"nexus worker callbacks require a matching client to be configured",
		)}
	}

	taskQueueName := n.callback.GetTaskqueueName()
	if taskQueueName == "" {
		return invocationResultFail{queueserrors.NewUnprocessableTaskError(
			"nexus worker callback is missing a task queue name",
		)}
	}

	// Build the OnCompleteHandlerInput carrying the source operation's outcome and the user-supplied source
	// context, and encode it as the StartOperation input for the completion handler.
	completionInput, err := n.buildCompletionInput()
	if err != nil {
		return invocationResultFail{queueserrors.NewUnprocessableTaskError(
			"failed to build nexus worker callback input: " + err.Error(),
		)}
	}
	// Encode using the standard Temporal payload format (json/protobuf). The completion handler is a
	// lang-SDK Nexus handler, so the SDK decodes the payload into an OnCompleteHandlerInput for us.
	inputPayload, err := payload.Encode(completionInput)
	if err != nil {
		return invocationResultFail{queueserrors.NewUnprocessableTaskError(
			"failed to encode nexus worker callback input: " + err.Error(),
		)}
	}

	request := &matchingservice.DispatchNexusTaskRequest{
		NamespaceId: ns.ID().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueueName,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Request: &nexuspb.Request{
			Header: map[string]string{},
			Variant: &nexuspb.Request_StartOperation{
				StartOperation: &nexuspb.StartOperationRequest{
					Service:   CompletionServiceName,
					Operation: CompletionOperationName,
					RequestId: n.requestID,
					Payload:   inputPayload,
				},
			},
		},
	}

	startTime := time.Now()
	resp, callErr := h.matchingClient.DispatchNexusTask(ctx, request)

	// The RPC to matching may itself fail (transport error), otherwise the worker's outcome is encoded in the
	// response and surfaced as a Nexus error.
	invocationErr := callErr
	if invocationErr == nil {
		invocationErr = commonnexus.MatchingDispatchResponseToError(resp)
	}

	namespaceTag := metrics.NamespaceTag(ns.Name().String())
	destTag := metrics.DestinationTag(taskAttr.Destination)
	outcomeMetricTag := metrics.OutcomeTag(outcomeTag(ctx, invocationErr))
	h.metricsHandler.Counter(RequestCounter.Name()).Record(1, namespaceTag, destTag, outcomeMetricTag)
	h.metricsHandler.Timer(RequestLatencyHistogram.Name()).Record(time.Since(startTime), namespaceTag, destTag, outcomeMetricTag)

	if invocationErr == nil {
		return invocationResultOK{}
	}
	// A transport-level RPC failure talking to matching is classified by its gRPC status code.
	if callErr != nil {
		return n.resultForRPCError(h.logger, callErr)
	}
	return n.resultForError(h.logger, invocationErr)
}

// resultForRPCError maps a gRPC transport error from the DispatchNexusTask call to an invocationResult.
func (n invocableNexusWorker) resultForRPCError(logger log.Logger, callErr error) invocationResult {
	retryable := isRetryableRPCResponse(callErr)
	logger.Error("Nexus worker callback dispatch RPC failed", tag.Error(callErr), tag.Bool("retryable", retryable))
	if retryable {
		return invocationResultRetry{callErr}
	}
	return invocationResultFail{callErr}
}

// resultForError maps an error derived from the DispatchNexusTask response to an invocationResult.
func (n invocableNexusWorker) resultForError(logger log.Logger, callErr error) invocationResult {
	// An OperationError means the worker received and processed the callback but the operation returned a
	// non-successful outcome. Retrying won't change the deterministic result, so treat it as a terminal
	// failure of the callback.
	if _, ok := errors.AsType[*nexus.OperationError](callErr); ok {
		logger.Error("Nexus worker callback operation completed unsuccessfully", tag.Error(callErr))
		return invocationResultFail{callErr}
	}

	retryable := isRetryableCallError(callErr)
	logger.Error("Nexus worker callback request failed", tag.Error(callErr), tag.Bool("retryable", retryable))
	if retryable {
		return invocationResultRetry{callErr}
	}
	return invocationResultFail{callErr}
}
