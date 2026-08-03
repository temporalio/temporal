package callback

import (
	"context"
	"errors"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	notificationpb "go.temporal.io/api/notificationservice/v1"
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

// invocableWorker is an invocable that delivers a completion to a Temporal worker by dispatching a Nexus
// StartOperation task to the worker's task queue via MatchingService.DispatchNexusTask.
//
// Unlike invocableOutbound, which POSTs the completion to an arbitrary address, worker callbacks target a
// Nexus service registered on a worker polling within the source operation's own namespace. This is faster
// and more efficient than round tripping through the frontend's Nexus HTTP endpoint.
type invocableWorker struct {
	callback   *callbackspb.Callback_Worker
	completion nexusrpc.CompleteOperationOptions
	// requestID is sent as the Nexus request ID so that a redelivery of this callback is idempotent from
	// the handler's perspective.
	//
	// The source request ID is from when the worker callback was attached. If multiple worker callbacks
	// were added in a single request, the end handler would see multiple worker callback invocations
	// using the same request ID.
	requestID string
	attempt   int32
}

func (n invocableWorker) WrapError(result invocationResult, err error) error {
	// A retryable failure means the target task queue is unresponsive (typically no worker is polling
	// it). Surface it as a DestinationDownError so the outbound queue's circuit breaker trips for that
	// task queue alone.
	if retry, ok := result.(invocationResultRetry); ok {
		return queueserrors.NewDestinationDownError(retry.err.Error(), err)
	}
	return err
}

func (n invocableWorker) Invoke(
	ctx context.Context,
	ns *namespace.Namespace,
	h *invocationTaskHandler,
	task *callbackspb.InvocationTask,
	taskAttr chasm.TaskAttributes,
) invocationResult {
	if h.matchingClient == nil {
		return invocationResultFail{queueserrors.NewUnprocessableTaskError(
			"worker callbacks require a matching client to be configured",
		)}
	}

	request, err := n.buildDispatchRequest(ns)
	if err != nil {
		return invocationResultFail{queueserrors.NewUnprocessableTaskError(err.Error())}
	}

	logger := log.With(h.logger,
		tag.WorkflowNamespace(ns.Name().String()),
		tag.Operation("DispatchWorkerCallback"),
		tag.NewStringTag("task-queue", n.callback.GetTaskQueueName()),
		tag.Attempt(n.attempt),
	)

	// Attempt to dispatch the Nexus task synchronously.
	startTime := time.Now()
	resp, rpcErr := h.matchingClient.DispatchNexusTask(ctx, request)
	result, outcome := n.classifyDispatchResult(ctx, logger, resp, rpcErr)

	// Emit metrics.
	namespaceTag := metrics.NamespaceTag(ns.Name().String())
	destTag := metrics.DestinationTag(taskAttr.Destination)
	outcomeMetricTag := metrics.OutcomeTag(outcome)
	h.metricsHandler.Counter(RequestCounter.Name()).Record(1, namespaceTag, destTag, outcomeMetricTag)
	h.metricsHandler.Timer(RequestLatencyHistogram.Name()).Record(time.Since(startTime), namespaceTag, destTag, outcomeMetricTag)

	return result
}

// buildDispatchRequest builds the matching request that hands the completion to the worker as a Nexus
// StartOperation task.
func (n invocableWorker) buildDispatchRequest(ns *namespace.Namespace) (*matchingservice.DispatchNexusTaskRequest, error) {
	taskQueueName := n.callback.GetTaskQueueName()
	if taskQueueName == "" {
		return nil, errors.New("worker callback is missing a task queue name")
	}

	onComplete, err := n.buildOnCompleteRequest()
	if err != nil {
		return nil, err
	}
	// The handler is a lang-SDK Nexus operation, so encode the input with the standard Temporal payload
	// format (json/protobuf) that its data converter decodes back into an OnCompleteRequest.
	//
	// TODO(chrsmith): This needs to be tagged in such a way that any client-side encryption will NOT
	// attempt to decode the payload. (Because it was constructed by the Temporal server, and not the
	// client.) This will be addressed in a follow-up PR.
	input, err := payload.Encode(onComplete)
	if err != nil {
		return nil, fmt.Errorf("failed to encode worker callback input: %w", err)
	}

	req := &matchingservice.DispatchNexusTaskRequest{
		NamespaceId: ns.ID().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueueName,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Request: &nexuspb.Request{
			Header: map[string]string{},
			// The invoker here is the server itself, which always understands Temporal failures. Without
			// this, workers answer with the legacy wire format, whose operation errors and handler errors
			// classifyDispatchResult cannot tell apart from a delivery failure - so a terminally failed
			// completion would be retried forever.
			Capabilities: &nexuspb.Request_Capabilities{TemporalFailureResponses: true},
			Variant: &nexuspb.Request_StartOperation{
				StartOperation: &nexuspb.StartOperationRequest{
					Service:   n.callback.GetService(),
					Operation: n.callback.GetOperation(),
					RequestId: n.requestID,
					Payload:   input,
					Links:     commonnexus.ConvertLinksToProto(n.completion.Links),
				},
			},
		},
	}
	return req, nil
}

// buildOnCompleteRequest builds the input delivered to the worker's completion handler from the source
// operation's outcome and the context the callback was registered with.
func (n invocableWorker) buildOnCompleteRequest() (*notificationpb.OnCompleteRequest, error) {
	outcome := &notificationpb.OnCompleteRequest_Outcome{}
	if n.completion.Error != nil {
		failure, err := nexusToTemporalFailure(n.completion.Error)
		if err != nil {
			return nil, err
		}
		outcome.Result = &notificationpb.OnCompleteRequest_Outcome_Failure{Failure: failure}
	} else {
		// A successful operation may legitimately have no result, in which case the success variant is
		// still set, just without payloads.
		var payloads *commonpb.Payloads
		if n.completion.Result != nil {
			p, ok := n.completion.Result.(*commonpb.Payload)
			if !ok {
				return nil, fmt.Errorf("invalid result, expected a payload, got: %T", n.completion.Result)
			}
			payloads = &commonpb.Payloads{Payloads: []*commonpb.Payload{p}}
		}
		outcome.Result = &notificationpb.OnCompleteRequest_Outcome_Success{Success: payloads}
	}

	return &notificationpb.OnCompleteRequest{
		Outcome:       outcome,
		SourceContext: n.callback.GetSourceContext(),
	}, nil
}

// classifyDispatchResult maps the result of the dispatch RPC onto an invocation result and a metrics outcome tag.
func (n invocableWorker) classifyDispatchResult(
	callCtx context.Context,
	logger log.Logger,
	resp *matchingservice.DispatchNexusTaskResponse,
	rpcErr error,
) (invocationResult, string) {
	if rpcErr != nil {
		// The RPC to matching itself failed, e.g. matching is unavailable or rejected the request.
		retryable := isRetryableRPCResponse(rpcErr)
		logger.Error("Worker callback dispatch failed", tag.Error(rpcErr), tag.Bool("retryable", retryable))
		if retryable {
			return invocationResultRetry{rpcErr}, outcomeTag(callCtx, rpcErr)
		}
		return invocationResultFail{rpcErr}, outcomeTag(callCtx, rpcErr)
	}

	// Note that an async response counts as delivered: the handler accepted the completion and started
	// an operation to process it. The callback does not wait for that operation to finish.
	err := commonnexus.MatchingDispatchResponseToError(resp)
	if err == nil {
		return invocationResultOK{}, "success"
	}

	if startOperationFailed(resp) {
		// The worker received the completion but its operation failed. That outcome is the handler's
		// answer, not a delivery problem, so the callback fails permanently instead of retrying.
		logger.Error("Worker callback operation failed", tag.Error(err))
		return invocationResultFail{err}, "operation-failed"
	}

	// Everything else is a delivery-level error: no worker polling the task queue (an upstream timeout),
	// a handler error returned by the worker, or an unrecognized response.
	retryable := isRetryableCallError(err)
	logger.Error("Worker callback request failed", tag.Error(err), tag.Bool("retryable", retryable))
	if retryable {
		return invocationResultRetry{err}, outcomeTag(callCtx, err)
	}
	return invocationResultFail{err}, outcomeTag(callCtx, err)
}

// startOperationFailed reports whether the worker handled the task and failed the operation, as opposed to
// failing to handle the task at all.
func startOperationFailed(resp *matchingservice.DispatchNexusTaskResponse) bool {
	outcome, ok := resp.GetOutcome().(*matchingservice.DispatchNexusTaskResponse_Response)
	if !ok {
		return false
	}
	_, failed := outcome.Response.GetStartOperation().GetVariant().(*nexuspb.StartOperationResponse_Failure)
	return failed
}
