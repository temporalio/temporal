package callbacks

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptrace"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	persistencespb "go.temporal.io/server/api/persistence/v1"
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

type CanGetNexusCompletion interface {
	GetNexusCompletion(ctx context.Context, requestID string) (nexusrpc.OperationCompletion, error)
}

type nexusInvocation struct {
	nexus             *persistencespb.Callback_Nexus
	completion        nexusrpc.OperationCompletion
	workflowID, runID string
	attempt           int32
}

func (n nexusInvocation) WrapError(result invocationResult, err error) error {
	if failure, ok := result.(invocationResultRetry); ok {
		return queueserrors.NewDestinationDownError(failure.err.Error(), err)
	}
	return err
}

func (n nexusInvocation) Invoke(ctx context.Context, ns *namespace.Namespace, e taskExecutor, task InvocationTask) invocationResult {
	if e.HTTPTraceProvider != nil {
		traceLogger := log.With(e.Logger,
			tag.WorkflowNamespace(ns.Name().String()),
			tag.Operation("CompleteNexusOperation"),
			tag.String("destination", task.destination),
			tag.WorkflowID(n.workflowID),
			tag.WorkflowRunID(n.runID),
			tag.AttemptStart(time.Now().UTC()),
			tag.Attempt(n.attempt),
		)
		if trace := e.HTTPTraceProvider.NewTrace(n.attempt, traceLogger); trace != nil {
			ctx = httptrace.WithClientTrace(ctx, trace)
		}
	}

	client := nexusrpc.NewCompletionHTTPClient(nexusrpc.CompletionHTTPClientOptions{
		HTTPCaller: e.HTTPCallerProvider(queuescommon.NamespaceIDAndDestination{
			NamespaceID: ns.ID().String(),
			Destination: task.Destination(),
		}),
		Serializer: commonnexus.PayloadSerializer,
	})
	// Make the call and record metrics.
	startTime := time.Now()

	for k, v := range n.nexus.Header {
		n.completion.SetHeader(k, v)
	}
	err := client.CompleteOperation(ctx, n.nexus.Url, n.completion)

	namespaceTag := metrics.NamespaceTag(ns.Name().String())
	destTag := metrics.DestinationTag(task.Destination())
	statusCodeTag := metrics.OutcomeTag(outcomeTag(ctx, err))
	e.MetricsHandler.Counter(RequestCounter.Name()).Record(1, namespaceTag, destTag, statusCodeTag)
	e.MetricsHandler.Timer(RequestLatencyHistogram.Name()).Record(time.Since(startTime), namespaceTag, destTag, statusCodeTag)

	if err == nil {
		return invocationResultOK{}
	}
	retryable := isRetryableCallError(err)
	e.Logger.Error("Callback request failed", tag.Error(err), tag.Bool("retryable", retryable))
	if retryable {
		return invocationResultRetry{err}
	}
	return invocationResultFail{err}
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

func isRetryableCallError(err error) bool {
	var handlerError *nexus.HandlerError
	if errors.As(err, &handlerError) {
		return handlerError.Retryable()
	}
	return true
}
