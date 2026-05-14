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

func (n invocableOutbound) isSystemCallback() bool {
	c := n.callback
	if c == nil {
		return false
	}
	return c.Url == commonnexus.SystemCallbackURL
}

func (n invocableOutbound) WrapError(result invocationResult, err error) error {
	// If the error is due to a completion of a Nexus operation being delivered before the
	// operation has officially started, we want to avoid triggering the circuit breakers.
	// Since the actual destination is working fine, and the failure is due to a data race.
	if errors.Is(err, nexusoperations.ErrOperationNotStarted) {
		return err
	}

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

	// If the outbound call is to a standalone callback, then supply the Nexus
	// operation's token in the request.
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

		// If the error from trying to resolve a Nexus operation that hasn't yet been marked
		// as started, it is safe to retry. (n.WrapError will ensure repeated failures of this
		// kind won't cause the SystemCallback to trip the circuit breaker.)
		isErrNotStarted := errors.Is(err, nexusoperations.ErrOperationNotStarted)
		if n.isSystemCallback() && isErrNotStarted {
			retryable = true
		}

		if retryable {
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
