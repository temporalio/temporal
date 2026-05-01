package callback

import (
	"context"
	"errors"
	"net/http/httptrace"
	"strings"
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
	"go.temporal.io/server/components/nexusoperations"
	queuescommon "go.temporal.io/server/service/history/queues/common"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
)

// invocableOutbound is an invocable that delivers the Nexus operation completion data to an external destination for
// cross-namespace or cross-cell callbacks.
type invocableOutbound struct {
	callback          *callbackspb.Callback_Nexus
	completion        nexusrpc.CompleteOperationOptions
	workflowID, runID string
	attempt           int32
}

func (n invocableOutbound) WrapError(result invocationResult, err error) error {
	if retry, ok := result.(invocationResultRetry); ok {
		return queueserrors.NewDestinationDownError(retry.err.Error(), err)
	}
	// invocationResultRetryNoCB is intentionally NOT wrapped as DestinationDownError
	// to avoid triggering the circuit breaker for transient server-side conditions.
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
			tag.String("destination", taskAttr.Destination),
			tag.WorkflowID(n.workflowID),
			tag.WorkflowRunID(n.runID),
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
	// Make the call and record metrics.
	startTime := time.Now()

	n.completion.Header = n.callback.Header
	if n.callback.GetToken() != "" {
		if n.completion.Header == nil {
			n.completion.Header = nexus.Header{}
		}
		n.completion.Header.Set(commonnexus.CallbackTokenHeader, n.callback.GetToken())
	}
	err := client.CompleteOperation(ctx, n.callback.Url, n.completion)

	namespaceTag := metrics.NamespaceTag(ns.Name().String())
	destTag := metrics.DestinationTag(taskAttr.Destination)
	outcomeTag := metrics.OutcomeTag(outcomeTag(ctx, err))
	h.metricsHandler.Counter(RequestCounter.Name()).Record(1, namespaceTag, destTag, outcomeTag)
	h.metricsHandler.Timer(RequestLatencyHistogram.Name()).Record(time.Since(startTime), namespaceTag, destTag, outcomeTag)

	if err != nil {
		retryable := isRetryableCallError(err)
		h.logger.Error("Callback request failed", tag.Error(err), tag.Bool("retryable", retryable))
		if retryable {
			// Check if this is a transient "operation not started yet" error.
			// This should not trigger the circuit breaker since the destination
			// is reachable — the operation just hasn't been started by its handler yet.
			//
			// TODO(chrsmith): Unresolved comment. "Instead of creating a new `NoCB` result, you can put this functionality in `WrapError` above."
			if isOperationNotStartedError(err) {
				return invocationResultRetryNoCB{err}
			}
			return invocationResultRetry{err}
		}
		return invocationResultFail{err}
	}
	return invocationResultOK{}
}

func isRetryableCallError(err error) bool {
	var handlerError *nexus.HandlerError
	if errors.As(err, &handlerError) {
		return handlerError.Retryable()
	}
	return true
}

// isOperationNotStartedError detects the specific "operation not started yet" error
// returned when a completion arrives before the Nexus start handler has returned.
func isOperationNotStartedError(err error) bool {
	var handlerError *nexus.HandlerError
	// TODO(chrsmith): https://github.com/temporalio/temporal/pull/9805/changes#r3106006542
	// > @bergundy: At minimum we should verify the handler error type here. Please also use errors.AsType.
	// > We should also put a TODO to only do this special case when calling into the "system",
	// > I believe retryable worker callbacks should always trip the circuit breaker.
	if errors.As(err, &handlerError) {
		return strings.Contains(handlerError.Message, nexusoperations.ErrMsgOperationNotStarted)
	}
	return false
}

func outcomeTag(callCtx context.Context, callErr error) string {
	if callErr != nil {
		if callCtx.Err() != nil {
			return "request-timeout"
		}
		var handlerErr *nexus.HandlerError
		if errors.As(callErr, &handlerErr) {
			return "handler-error:" + string(handlerErr.Type)
		}
		return "unknown-error"
	}
	return "success"
}
