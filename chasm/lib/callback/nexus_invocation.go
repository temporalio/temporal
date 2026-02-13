package callback

import (
	"context"
	"errors"
	"net/http"
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

var retryable4xxErrorTypes = []int{
	http.StatusRequestTimeout,
	http.StatusTooManyRequests,
}

type nexusInvocation struct {
	nexus             *callbackspb.Callback_Nexus
	completion        nexusrpc.CompleteOperationOptions
	workflowID, runID string
	attempt           int32
}

func (n nexusInvocation) WrapError(result invocationResult, err error) error {
	if retry, ok := result.(invocationResultRetry); ok {
		return queueserrors.NewDestinationDownError(retry.err.Error(), err)
	}
	return err
}

func (n nexusInvocation) Invoke(
	ctx context.Context,
	ns *namespace.Namespace,
	e InvocationTaskExecutor,
	task *callbackspb.InvocationTask,
	taskAttr chasm.TaskAttributes,
) invocationResult {
	if e.httpTraceProvider != nil {
		traceLogger := log.With(e.logger,
			tag.WorkflowNamespace(ns.Name().String()),
			tag.Operation("CompleteNexusOperation"),
			tag.String("destination", taskAttr.Destination),
			tag.WorkflowID(n.workflowID),
			tag.WorkflowRunID(n.runID),
			tag.AttemptStart(time.Now().UTC()),
			tag.Attempt(n.attempt),
		)
		if trace := e.httpTraceProvider.NewTrace(n.attempt, traceLogger); trace != nil {
			ctx = httptrace.WithClientTrace(ctx, trace)
		}
	}

	client := nexusrpc.NewCompletionHTTPClient(nexusrpc.CompletionHTTPClientOptions{
		HTTPCaller: e.httpCallerProvider(queuescommon.NamespaceIDAndDestination{
			NamespaceID: ns.ID().String(),
			Destination: taskAttr.Destination,
		}),
		Serializer: commonnexus.PayloadSerializer,
	})
	// Make the call and record metrics.
	startTime := time.Now()

	n.completion.Header = n.nexus.Header
	err := client.CompleteOperation(ctx, n.nexus.Url, n.completion)

	namespaceTag := metrics.NamespaceTag(ns.Name().String())
	destTag := metrics.DestinationTag(taskAttr.Destination)
	outcomeTag := metrics.OutcomeTag(outcomeTag(ctx, err))
	e.metricsHandler.Counter(RequestCounter.Name()).Record(1, namespaceTag, destTag, outcomeTag)
	e.metricsHandler.Timer(RequestLatencyHistogram.Name()).Record(time.Since(startTime), namespaceTag, destTag, outcomeTag)

	if err != nil {
		retryable := isRetryableCallError(err)
		e.logger.Error("Callback request failed", tag.Error(err), tag.Bool("retryable", retryable))
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
