package callbacks

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptrace"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	commonpb "go.temporal.io/api/common/v1"
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
	GetNexusCompletion(ctx context.Context, requestID string) (nexusrpc.CompleteOperationOptions, error)
}

type nexusInvocation struct {
	nexus             *persistencespb.Callback_Nexus
	completion        nexusrpc.CompleteOperationOptions
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
		Propagator: e.Propagator,
	})
	// Make the call and record metrics.
	var span trace.Span
	if e.TracerProvider != nil {
		tracer := e.TracerProvider.Tracer("go.temporal.io/server")
		attrs := []attribute.KeyValue{
			attribute.String("http.request.method", "POST"),
			attribute.String("temporalWorkflowID", n.workflowID),
			attribute.String("temporalNexusNamespace", ns.Name().String()),
		}
		if payload := nexusCompletionPayload(n.completion); payload != "" {
			attrs = append(attrs, attribute.String("http.request.payload", payload))
		}
		ctx, span = tracer.Start(ctx, "CompleteNexusOperation",
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(attrs...),
		)
		defer span.End()
	}
	startTime := time.Now()

	n.completion.Header = n.nexus.Header
	err := client.CompleteOperation(ctx, n.nexus.Url, n.completion)
	if span.IsRecording() {
		if err != nil {
			span.SetAttributes(attribute.String("http.response.status_code", "error"))
		} else {
			span.SetAttributes(attribute.String("http.response.status_code", "200"))
		}
	}

	namespaceTag := metrics.NamespaceTag(ns.Name().String())
	destTag := metrics.DestinationTag(task.Destination())
	statusCodeTag := metrics.OutcomeTag(outcomeTag(ctx, err))
	e.MetricsHandler.Counter(RequestCounter.Name()).Record(1, namespaceTag, destTag, statusCodeTag)
	e.MetricsHandler.Timer(RequestLatencyHistogram.Name()).Record(time.Since(startTime), namespaceTag, destTag, statusCodeTag)

	if err != nil {
		retryable := isRetryableCallError(err)
		e.Logger.Error("Callback request failed", tag.Error(err), tag.Bool("retryable", retryable))
		if retryable {
			return invocationResultRetry{err}
		}
		return invocationResultFail{err}
	}
	return invocationResultOK{}
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

// nexusCompletionPayload serializes the nexus completion result or error as a
// JSON string suitable for the http.request.payload span attribute.
func nexusCompletionPayload(c nexusrpc.CompleteOperationOptions) string {
	if c.Error != nil {
		m := map[string]string{
			"state":   string(c.Error.State),
			"message": c.Error.Message,
		}
		b, _ := json.Marshal(m)
		return string(b)
	}
	if p, ok := c.Result.(*commonpb.Payload); ok && p != nil {
		return string(p.GetData())
	}
	return ""
}
