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
	"go.temporal.io/api/notificationservice/v1"
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
	"google.golang.org/protobuf/types/known/timestamppb"
)

// invocableNexusHandler is an invocable that delivers a completion to a Temporal worker by dispatching a Nexus
// StartOperation task to the worker's task queue via MatchingService.DispatchNexusTask.
//
// Unlike invocableOutbound, which POSTs the completion to an arbitrary address, NexusHandler callbacks target a
// Nexus service registered on a worker polling within the source operation's own namespace. This is faster
// and more efficient than round tripping through the frontend's Nexus HTTP endpoint.
type invocableNexusHandler struct {
	callback   *callbackspb.Callback_NexusHandler
	completion nexusrpc.CompleteOperationOptions

	// completionSourceTag is the fully qualified name of the CHASM component that produced this
	// completion, e.g. "workflow.workflow" or "activity.activity".
	completionSourceTag string
	businessID, runID   string

	// requestID is sent as the Nexus request ID so that a redelivery of this callback is idempotent from
	// the handler's perspective.
	requestID string
	attempt   int32
}

// buildOnCompleteRequest builds the input delivered to the worker's completion handler from the source
// operation's outcome and the context the callback was registered with.
func (n invocableNexusHandler) buildOnCompleteRequest() (*notificationservice.OnCompleteRequest, error) {
	onCompReq := &notificationservice.OnCompleteRequest{
		SourceContext: n.callback.GetSourceContext(),
	}

	if n.completion.Error != nil {
		failure, err := commonnexus.OperationErrorToTemporalFailure(n.completion.Error)
		if err != nil {
			return nil, err
		}
		onCompReq.Result = &notificationservice.OnCompleteRequest_Failure{Failure: failure}
		return onCompReq, nil
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
			return nil, fmt.Errorf("failed to encode empty NexusHandler callback result: %w", err)
		}
	}

	onCompReq.Result = &notificationservice.OnCompleteRequest_Success{Success: result}
	return onCompReq, nil
}

func (n invocableNexusHandler) buildDispatchRequest(
	ns *namespace.Namespace,
	scheduledTime time.Time,
) (*matchingservice.DispatchNexusTaskRequest, error) {
	taskQueueName := n.callback.GetTaskQueueName()
	if taskQueueName == "" {
		return nil, errors.New("NexusHandler callback is missing a task queue name")
	}

	onComplete, err := n.buildOnCompleteRequest()
	if err != nil {
		return nil, err
	}
	// The handler is a lang-SDK Nexus operation, so encode the input with the standard Temporal payload
	// format (json/protobuf) that its data converter decodes back into an OnCompleteRequest.
	//
	// TODO(temporalio/temporal/issues/11891): Mark the Payload as a "system payload" to avoid the payload
	// being decoded on the client-side by mistake.
	input, err := payload.Encode(onComplete)
	if err != nil {
		return nil, fmt.Errorf("failed to encode NexusHandler callback input: %w", err)
	}

	req := &matchingservice.DispatchNexusTaskRequest{
		NamespaceId: ns.ID().String(),
		// The delivery lands on whatever version the task queue currently routes to by default:
		// DispatchNexusTaskRequest carries no versioning directive, so matching decides, and it
		// applies the task queue's assignment rules to every Nexus task alike.
		//
		// There is no way to pin a callback to the version that registered it, which is what a
		// pinned workflow gets for its own activities. Wiring that through would mean carrying the
		// version on the callback and adding a directive to this request; until then, a handler
		// receiving completions has to stay compatible across the versions it is rolled through.
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
					// TODO(temporal/issues/11889): These links will be wrong. Backlinks to the source of the Nexus completion
					// should be to the *callback attached* to the completion's source. Not the completion directly.
					// e.g. a Link_Callback to "SANO xxx callback yyy", and not "SANO xxx".
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

func (n invocableNexusHandler) Invoke(
	ctx context.Context,
	ns *namespace.Namespace,
	h *invocationTaskHandler,
	task *callbackspb.InvocationTask,
	taskAttr chasm.TaskAttributes,
) invocationResult {
	logger := log.With(h.logger,
		tag.WorkflowNamespace(ns.Name().String()),
		tag.Operation("DispatchNexusHandlerCallback"),
		tag.WorkflowID(n.businessID),
		tag.WorkflowRunID(n.runID),
		tag.NexusCompletionSource(n.completionSourceTag),
		tag.NewStringTag("task-queue", n.callback.GetTaskQueueName()),
		tag.Attempt(n.attempt),
	)

	// nolint:forbidigo // Wall-clock RPC measurement, not component state; Invoke has no chasm.Context.
	startTime := time.Now()

	result, outcome := n.dispatch(ctx, startTime, logger, h, ns)

	// Record metrics.
	tags := []metrics.Tag{
		metrics.NamespaceTag(ns.Name().String()),
		metrics.DestinationTag(taskAttr.Destination),
		metrics.OutcomeTag(string(outcome)),
		metrics.NexusCompletionSourceTag(n.completionSourceTag),
	}
	NexusHandlerRequestCounter.With(h.metricsHandler).Record(1, tags...)
	NexusHandlerRequestLatencyHistogram.With(h.metricsHandler).Record(time.Since(startTime), tags...)

	return result
}

// dispatch hands the completion to matching and returns the invocationResult together with the value
// of the metrics outcome tag to record for this attempt.
func (n invocableNexusHandler) dispatch(
	ctx context.Context,
	startTime time.Time,
	logger log.Logger,
	h *invocationTaskHandler,
	ns *namespace.Namespace,
) (invocationResult, outcomeTag) {
	// Build the DispatchNexusTaskRequest for the Matching Service.
	dispatchReq, err := n.buildDispatchRequest(ns, startTime)
	if err != nil {
		logger.Error("Failed to build NexusHandler callback request", tag.Error(err))
		return invocationResultFail{err}, outcomeRequestBuildError
	}

	// Send it to the worker.
	resp, rpcErr := h.matchingClient.DispatchNexusTask(ctx, dispatchReq)
	if rpcErr != nil {
		// The task never reached a worker, so this is a problem between history and matching.
		// Every other dispatch error is internal to Temporal and not something the namespace's users can
		// fix, so only a reference ID to the logged error is surfaced to them.
		handlerErr := commonnexus.ConvertGRPCError(rpcErr, false)
		retryable := isRetryableCallError(handlerErr)
		logger = log.With(logger, tag.Bool("retryable", retryable))
		userFacingErr := logInternalError(logger, "NexusHandler callback dispatch failed", rpcErr)

		// Derive the outcome metric based on the gRPC error received.
		outcome := grpcErrorOutcome(rpcErr)
		if ctx.Err() != nil {
			outcome = outcomeRequestTimeout
		}

		if retryable {
			return invocationResultRetry{userFacingErr}, outcome
		}
		return invocationResultFail{userFacingErr}, outcome
	}

	// The RPC succeeded, so whatever the worker had to say is in the response.
	dispatchResult := commonnexus.ClassifyStartOperationDispatch(resp)
	outcome := dispatchResult.OutcomeTag().Value
	// Collapse "sync_success" and "async_success" into outcomeSuccess for clarity.
	// The Nexus handler code in the Frontend service is interested in that detail, but here
	// the sync/async aspect is not relevant.
	if dispatchResult.Outcome.Succeeded() {
		outcome = string(outcomeSuccess)
	}
	return n.classifyDispatchResult(logger, dispatchResult), outcomeTag(outcome)
}

// classifyDispatchResult inspects the response from the Nexus handler to determine how to handle
// the NexusHandler callback: delivered, worth another attempt, or permanently failed.
func (n invocableNexusHandler) classifyDispatchResult(
	logger log.Logger,
	result commonnexus.DispatchResult,
) invocationResult {
	if result.Outcome.Succeeded() {
		// Both flavors of success count as delivered. An async start means the worker accepted the
		// completion and started an operation to process it; either way the callback is done, it does
		// not wait for that operation to finish.
		return invocationResultOK{}
	}

	// Every remaining outcome carries an error: what the worker reported, or one that
	// DispatchResultToError synthesizes when nothing usable came back.
	err := commonnexus.DispatchResultToError(result)

	switch result.Outcome {
	case commonnexus.DispatchOutcomeOperationFailure:
		// The worker received the completion and answered with a failure of its own: an operation that
		// resolved as failed or canceled. That is the handler's verdict on this completion rather than
		// a delivery problem, and redelivering would collect the same verdict, so the callback fails
		// permanently.
		logger.Error("NexusHandler callback resulted in an operation error", tag.Error(err))
		return invocationResultFail{err}

	case commonnexus.DispatchOutcomeHandlerFailure,
		commonnexus.DispatchOutcomeRequestTimeout,
		commonnexus.DispatchOutcomeUnrecognized:
		// The completion was never handled: the worker refused the task with a Nexus handler error,
		// nobody answered it before matching gave up, or this build cannot read the response. Only a
		// handler error says whether another attempt is worthwhile, and the latter two reach us as
		// handler errors synthesized above, so all three ask the same question.
		handlerErr, ok := errors.AsType[*nexus.HandlerError](err)
		retryable := ok && handlerErr.Retryable()
		logger.Error("NexusHandler callback resulted in a handler error", tag.Error(err), tag.Bool("retryable", retryable))
		if retryable {
			return invocationResultRetry{err}
		}
		return invocationResultFail{err}

	default:
		// An outcome this build does not know about and that Succeeded() did not vouch for. Treat it
		// like an unreadable response and keep retrying, in the hope the mismatch is transient.
		logger.Error("NexusHandler callback got an unhandled dispatch outcome",
			tag.NewStringTag("dispatch-outcome", string(result.Outcome)), tag.Error(err))
		return invocationResultRetry{err}
	}
}

// isDestinationDown returns whether a retryable delivery failure indicates the destination is
// unavailable for subsequent retries.
func isDestinationDown(err error) bool {
	handlerErr, ok := errors.AsType[*nexus.HandlerError](err)
	if !ok {
		// Was not a worker-produced error, so the RPC to matching itself failed.
		return true
	}
	// Any other retryable handler error should be considered a DestinationDown error, and trip
	// the circuit breaker. (HandlerErrorTypeResourceExhausted, HandlerErrorTypeUnavailable, etc.)
	return handlerErr.Retryable()
}

func (n invocableNexusHandler) WrapError(result invocationResult, err error) error {
	// A DestinationDownError counts against the outbound queue's circuit breaker for this task
	// queue, which holds back every callback targeting it. Note that this means a single broken
	// Nexus handler (e.g. always timing out) would open the circuit breaker and block every
	// Nexus handler on the same task queue for delivering NexusHandler callbacks.
	if retry, ok := result.(invocationResultRetry); ok && isDestinationDown(retry.err) {
		return queueserrors.NewDestinationDownError(retry.err.Error(), err)
	}
	return err
}
