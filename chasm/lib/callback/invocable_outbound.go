package callback

import (
	"context"
	"errors"
	"net/http/httptrace"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	queuescommon "go.temporal.io/server/service/history/queues/common"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
)

// invocableOutbound is an invocable that delivers the Nexus operation completion data to an external destination for
// cross-namespace or cross-cell callbacks.
type invocableOutbound struct {
	callback   *callbackspb.Callback_Nexus
	completion nexusrpc.CompleteOperationOptions
	// completionSourceTag is the fully qualified name of the CHASM component that produced this
	// completion, e.g. "workflow.workflow" or "activity.activity".
	completionSourceTag string
	businessID, runID   string
	attempt             int32
}

func (n invocableOutbound) WrapError(result invocationResult, err error) error {
	if retry, ok := result.(invocationResultRetry); ok {
		return queueserrors.NewDestinationDownError(retry.err.Error(), err)
	}
	return err
}

func (n invocableOutbound) Invoke(
	ctx context.Context,
	ns *namespace.Namespace,
	h *invocationTaskHandler,
	task *callbackspb.InvocationTask,
	taskAttr chasm.TaskAttributes,
) invocationResult {
	if h.httpTraceProvider != nil {
		traceLogger := log.With(h.logger,
			tag.WorkflowNamespace(ns.Name().String()),
			tag.Operation("CompleteNexusOperation"),
			tag.Destination(taskAttr.Destination),
			tag.WorkflowID(n.businessID),
			tag.WorkflowRunID(n.runID),
			tag.NexusCompletionSource(n.completionSourceTag),
			tag.AttemptStart(time.Now().UTC()),
			tag.Attempt(n.attempt),
		)
		if trace := h.httpTraceProvider.NewTrace(n.attempt, traceLogger); trace != nil {
			ctx = httptrace.WithClientTrace(ctx, trace)
		}
	}

	client := nexusrpc.NewCompletionHTTPClient(nexusrpc.CompletionHTTPClientOptions{
		HTTPCaller: h.httpCallerProvider(queuescommon.NamespaceIDAndDestination{
			NamespaceID: ns.ID().String(),
			Destination: taskAttr.Destination,
		}),
		Serializer: commonnexus.PayloadSerializer,
	})

	// nolint:forbidigo // Wall-clock RPC measurement, not component state; Invoke has no chasm.Context.
	startTime := time.Now()

	// Make the call.
	n.completion.Header = n.callback.Header
	err := client.CompleteOperation(ctx, n.callback.Url, n.completion)

	// Record metrics.
	tags := []metrics.Tag{
		metrics.NamespaceTag(ns.Name().String()),
		metrics.DestinationTag(taskAttr.Destination),
		metrics.OutcomeTag(string(outboundOutcome(ctx, err))),
		metrics.NexusCompletionSourceTag(n.completionSourceTag),
	}
	h.metricsHandler.Counter(RequestCounter.Name()).Record(1, tags...)
	h.metricsHandler.Timer(RequestLatencyHistogram.Name()).Record(time.Since(startTime), tags...)

	if err != nil {
		retryable := isRetryableCallError(err)
		h.logger.Error(
			"Callback request failed",
			tag.Error(err),
			tag.WorkflowNamespace(ns.Name().String()),
			tag.Destination(taskAttr.Destination),
			tag.WorkflowID(n.businessID),
			tag.WorkflowRunID(n.runID),
			tag.NexusCompletionSource(n.completionSourceTag),
			tag.Attempt(n.attempt),
			tag.Bool("retryable", retryable),
		)
		if retryable {
			return invocationResultRetry{err}
		}
		return invocationResultFail{err}
	}
	return invocationResultOK{}
}

func isRetryableCallError(err error) bool {
	if handlerError, ok := errors.AsType[*nexus.HandlerError](err); ok {
		return handlerError.Retryable()
	}
	return true
}

func outboundOutcome(callCtx context.Context, callErr error) outcomeTag {
	if callErr != nil {
		if callCtx.Err() != nil {
			return outcomeRequestTimeout
		}
		if handlerErr, ok := errors.AsType[*nexus.HandlerError](callErr); ok {
			return handlerErrorOutcome(handlerErr)
		}
		return outcomeUnknownError
	}
	return outcomeSuccess
}
