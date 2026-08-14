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
	notificationpb "go.temporal.io/api/notificationservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/payload"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	// It identifies the request that registered the callback, not the callback itself, so every
	// callback registered by a single API call carries the same value. A handler that dedupes on the
	// request ID sees those as one request, and must use SourceContext to tell them apart.
	requestID string
	attempt   int32
}

func (n invocableWorker) WrapError(result invocationResult, err error) error {
	// Surface a retryable failure as a DestinationDownError so that repeated failures trip the
	// outbound queue's circuit breaker for this task queue alone, whether the task queue is
	// unresponsive or its worker keeps rejecting the delivery.
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
	logger := log.With(h.logger,
		tag.WorkflowNamespace(ns.Name().String()),
		tag.Operation("DispatchWorkerCallback"),
		tag.NewStringTag("task-queue", n.callback.GetTaskQueueName()),
		tag.Attempt(n.attempt),
	)

	startTime := time.Now()
	result, outcome := n.dispatch(ctx, logger, h, ns, startTime)

	namespaceTag := metrics.NamespaceTag(ns.Name().String())
	destTag := metrics.DestinationTag(taskAttr.Destination)
	outcomeMetricTag := metrics.OutcomeTag(outcome)
	h.metricsHandler.Counter(RequestCounter.Name()).Record(1, namespaceTag, destTag, outcomeMetricTag)
	h.metricsHandler.Timer(RequestLatencyHistogram.Name()).Record(time.Since(startTime), namespaceTag, destTag, outcomeMetricTag)

	return result
}

// dispatch hands the completion to matching and returns the invocation result along with the metrics
// outcome tag to be recorded.
func (n invocableWorker) dispatch(
	ctx context.Context,
	logger log.Logger,
	h *invocationTaskHandler,
	ns *namespace.Namespace,
	scheduledTime time.Time,
) (invocationResult, string) {
	request, err := n.buildDispatchRequest(ns, scheduledTime)
	if err != nil {
		// No attempt can make this callback dispatchable, so fail it permanently.
		logger.Error("Worker callback cannot be dispatched", tag.Error(err))
		return invocationResultFail{err}, "invalid-request"
	}

	resp, rpcErr := h.matchingClient.DispatchNexusTask(ctx, request)
	return n.classifyDispatchResult(logger, resp, rpcErr)
}

func (n invocableWorker) buildDispatchRequest(
	ns *namespace.Namespace,
	scheduledTime time.Time,
) (*matchingservice.DispatchNexusTaskRequest, error) {
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
	// The size of the input is deliberately not checked here. The completion it carries already passed
	// BlobSizeLimitError where it entered the server, and matching's gRPC limit is orders of magnitude
	// above that, so a second check could only reject a completion that is legal everywhere else.

	req := &matchingservice.DispatchNexusTaskRequest{
		NamespaceId: ns.ID().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueueName,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Request: &nexuspb.Request{
			ScheduledTime: timestamppb.New(scheduledTime),
			Variant: &nexuspb.Request_StartOperation{
				StartOperation: &nexuspb.StartOperationRequest{
					Service:   n.callback.GetService(),
					Operation: n.callback.GetOperation(),
					RequestId: n.requestID,
					Payload:   input,
					// TODO(chrsmith): We need to map these links to a NexusOperationCallback, and not a NexusOperation.
					// Since the Nexus request being made wasn't started by the SANO, but a callback attached to the SANO.
					Links: commonnexus.ConvertLinksToProto(n.completion.Links),
				},
			},
			Capabilities: &nexuspb.Request_Capabilities{
				TemporalFailureResponses: true,
			},
		},
	}
	return req, nil
}

// buildOnCompleteRequest builds the input delivered to the worker's completion handler from the source
// operation's outcome and the context the callback was registered with.
func (n invocableWorker) buildOnCompleteRequest() (*notificationpb.OnCompleteRequest, error) {
	req := &notificationpb.OnCompleteRequest{
		SourceContext: common.CloneProto(n.callback.GetSourceContext()),
	}

	if n.completion.Error != nil {
		failure, err := nexusToTemporalFailure(n.completion.Error)
		if err != nil {
			return nil, err
		}
		req.Result = &notificationpb.OnCompleteRequest_Failure{Failure: failure}
		return req, nil
	}

	var result *commonpb.Payload
	switch typed := n.completion.Result.(type) {
	case nil:
		// No payload present.
	case *commonpb.Payload:
		result = typed
	default:
		return nil, fmt.Errorf("invalid result, expected a payload, got: %T", n.completion.Result)
	}

	// A successful operation may legitimately have no result. The success variant always carries a
	// payload on the wire, and a payload with no encoding fails the handler's data converter, so
	// send the same binary/null representation of "no value" that the Nexus HTTP path produces.
	if result == nil {
		var err error
		if result, err = payload.Encode(nil); err != nil {
			return nil, fmt.Errorf("failed to encode empty worker callback result: %w", err)
		}
	}

	req.Result = &notificationpb.OnCompleteRequest_Success{Success: result}
	return req, nil
}

// classifyDispatchResult maps the result of the dispatch RPC onto an invocation result and the "outcome" tag to emit in metrics.
func (n invocableWorker) classifyDispatchResult(
	logger log.Logger,
	resp *matchingservice.DispatchNexusTaskResponse,
	rpcErr error,
) (invocationResult, string) {
	if rpcErr != nil {
		// The RPC to matching itself failed, e.g. matching is unavailable or rejected the request.
		retryable := isRetryableRPCResponse(rpcErr)
		logger = log.With(logger, tag.Bool("retryable", retryable))

		// A rejection describes the request we sent, which is built entirely out of what the caller
		// registered, so it is theirs to fix and safe to surface. Everything else describes the state
		// of the server and is blinded behind a reference ID.
		var userFacingErr error
		if isRequestRejection(rpcErr) {
			logger.Error("Worker callback dispatch rejected", tag.Error(rpcErr))
			userFacingErr = rpcErr
		} else {
			userFacingErr = logInternalError(logger, "Worker callback dispatch failed", rpcErr)
		}

		if retryable {
			return invocationResultRetry{userFacingErr}, "rpc-error"
		}
		return invocationResultFail{userFacingErr}, "rpc-error"
	}

	// There wasn't an RPC error, but any application-level (e.g. the end Handler) errors would be
	// part of the response.
	outcome, recognized := dispatchOutcomeTag(resp)

	// Note that an async response counts as delivered: the handler accepted the completion and started
	// an operation to process it. The callback does not wait for that operation to finish.
	err := commonnexus.MatchingDispatchResponseToError(resp)
	if err == nil {
		return invocationResultOK{}, outcome
	}

	if !recognized {
		// A response this server cannot interpret, e.g. an empty outcome or a variant added by a newer
		// matching. There is nothing to act on and no attempt would produce an outcome we understand
		// any better, so fail permanently rather than retry forever and hold the destination's circuit
		// breaker open. Note that MatchingDispatchResponseToError reports this as a retryable internal
		// handler error, so the check has to come before the retryability check below.
		logger.Error("Worker callback received an unrecognized dispatch response", tag.Error(err))
		return invocationResultFail{err}, outcome
	}

	if startOperationFailed(resp) {
		// The worker received the completion but its operation failed. That outcome is the handler's
		// answer, not a delivery problem, so the callback fails permanently instead of retrying.
		logger.Error("Worker callback operation failed", tag.Error(err))
		return invocationResultFail{err}, outcome
	}

	// Everything else is a delivery-level error: no worker polling the task queue (an upstream timeout)
	// or a handler error returned by the worker.
	//
	// Only a handler error says whether another attempt is worthwhile. Anything else is a failure the
	// worker chose to report, e.g. an application error sent via RespondNexusTaskFailed, and repeating
	// the delivery would get the same answer, so the callback fails permanently.
	handlerErr, ok := errors.AsType[*nexus.HandlerError](err)
	retryable := ok && handlerErr.Retryable()
	logger.Error("Worker callback request failed", tag.Error(err), tag.Bool("retryable", retryable))
	if retryable {
		return invocationResultRetry{err}, outcome
	}
	return invocationResultFail{err}, outcome
}

// dispatchOutcomeTag names a dispatch outcome for metrics. Values are hyphenated to match the ones
// invocableOutbound records on the same metric.
//
// The second return value reports whether the outcome is one this server knows how to interpret; see
// classifyDispatchResult for why that matters.
func dispatchOutcomeTag(resp *matchingservice.DispatchNexusTaskResponse) (string, bool) {
	switch t := resp.GetOutcome().(type) {
	case *matchingservice.DispatchNexusTaskResponse_Failure:
		handlerFailure := t.Failure.GetNexusHandlerFailureInfo()
		if handlerFailure == nil {
			// The worker failed the task with something other than a handler error.
			return "worker-failure", true
		}
		return "handler-error:" + handlerErrorTypeTag(handlerFailure.GetType()), true
	case *matchingservice.DispatchNexusTaskResponse_HandlerError: //nolint:staticcheck // Deprecated, still sent by older workers.
		//nolint:staticcheck // Deprecated field on a deprecated variant.
		return "handler-error:" + handlerErrorTypeTag(t.HandlerError.GetErrorType()), true
	case *matchingservice.DispatchNexusTaskResponse_RequestTimeout:
		return "handler-timeout", true
	case *matchingservice.DispatchNexusTaskResponse_Response:
		switch t.Response.GetStartOperation().GetVariant().(type) {
		case *nexuspb.StartOperationResponse_SyncSuccess:
			return "sync-success", true
		case *nexuspb.StartOperationResponse_AsyncSuccess:
			return "async-success", true
		case *nexuspb.StartOperationResponse_OperationError: //nolint:staticcheck // Deprecated, still sent by older workers.
			return "operation-error", true
		case *nexuspb.StartOperationResponse_Failure:
			return "failure", true
		}
	}
	return "unrecognized-outcome", false
}

// handlerErrorTypes are the handler error types that may appear in a metric tag. The type a worker
// reports is an arbitrary string it chose when constructing the handler error, so anything outside
// the Nexus spec is collapsed rather than given its own time series.
var handlerErrorTypes = map[string]struct{}{
	string(nexus.HandlerErrorTypeBadRequest):        {},
	string(nexus.HandlerErrorTypeUnauthenticated):   {},
	string(nexus.HandlerErrorTypeUnauthorized):      {},
	string(nexus.HandlerErrorTypeNotFound):          {},
	string(nexus.HandlerErrorTypeRequestTimeout):    {},
	string(nexus.HandlerErrorTypeConflict):          {},
	string(nexus.HandlerErrorTypeResourceExhausted): {},
	string(nexus.HandlerErrorTypeInternal):          {},
	string(nexus.HandlerErrorTypeNotImplemented):    {},
	string(nexus.HandlerErrorTypeUnavailable):       {},
	string(nexus.HandlerErrorTypeUpstreamTimeout):   {},
}

// handlerErrorTypeTag bounds the cardinality a worker can introduce into the outcome tag.
func handlerErrorTypeTag(errType string) string {
	if _, ok := handlerErrorTypes[errType]; ok {
		return errType
	}
	return "UNKNOWN"
}

// startOperationFailed reports whether the worker handled the task and failed the operation, as opposed to
// failing to handle the task at all.
func startOperationFailed(resp *matchingservice.DispatchNexusTaskResponse) bool {
	outcome, ok := resp.GetOutcome().(*matchingservice.DispatchNexusTaskResponse_Response)
	if !ok {
		return false
	}
	switch outcome.Response.GetStartOperation().GetVariant().(type) {
	case *nexuspb.StartOperationResponse_Failure,
		*nexuspb.StartOperationResponse_OperationError: //nolint:staticcheck // Deprecated, still sent by older workers.
		return true
	default:
		return false
	}
}
