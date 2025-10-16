package callback

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/http/httptrace"
	"slices"
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
	"go.temporal.io/server/service/history/queues"
)

var retryable4xxErrorTypes = []int{
	http.StatusRequestTimeout,
	http.StatusTooManyRequests,
}

type CanGetNexusCompletion interface {
	GetNexusCompletion(ctx context.Context, requestID string) (nexusrpc.OperationCompletion, error)
}

// The Invoker component is responsible for executing callbacks.
//
// This is the CHASM port that combines functionality from HSM's:
// - nexusInvocation struct (components/callbacks/nexus_invocation.go:35-40)
// - callbackInvokable interface implementation (nexus_invocation.go:63-131)
type Invoker struct {
	chasm.UnimplementedComponent

	*callbackspb.InvokerState

	// Reference to the Callback component being invoked
	Callback chasm.Field[*Callback]

	// Interface to retrieve Nexus operation completion data
	CanGetNexusCompletion chasm.Field[CanGetNexusCompletion]

	// Fields from HSM's nexusInvocation struct (nexus_invocation.go:35-40)
	// These hold the invocation context needed for the HTTP request
	completion        nexusrpc.OperationCompletion
	workflowID, runID string
	attempt           int32
	nexus             *callbackspb.Callback_Nexus
}

func NewInvoker(ctx chasm.MutableContext, callback *Callback) *Invoker {
	return &Invoker{
		InvokerState: &callbackspb.InvokerState{
			NamespaceId: callback.NamespaceID.String(),
		},
		Callback: chasm.ComponentPointerTo(ctx, callback),
	}
}

func (i *Invoker) LifecycleState(ctx chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

// Invoke executes the HTTP callback request for a Nexus operation completion.
//
// This is the CHASM port of nexusInvocation.Invoke() from nexus_invocation.go:63-131.
// It performs the same HTTP request logic but returns CHASM's CallbackStatus enum
// instead of HSM's invocationResult types (invocationResultOK, invocationResultRetry,
// invocationResultFail).
//
// The return value mapping:
// - HSM invocationResultOK -> CHASM CALLBACK_STATUS_SUCCEEDED
// - HSM invocationResultRetry -> CHASM CALLBACK_STATUS_BACKING_OFF
// - HSM invocationResultFail -> CHASM CALLBACK_STATUS_FAILED
func (i *Invoker) Invoke(
	ctx context.Context,
	ns *namespace.Namespace,
	e *InvocationTaskExecutor,
	taskAttributes chasm.TaskAttributes,
	task *callbackspb.InvocationTask,
) callbackspb.CallbackStatus {
	if e.HTTPTraceProvider != nil {
		traceLogger := log.With(e.Logger,
			tag.WorkflowNamespace(ns.Name().String()),
			tag.Operation("CompleteNexusOperation"),
			tag.NewStringTag("destination", taskAttributes.Destination),
			tag.WorkflowID(i.workflowID),
			tag.WorkflowRunID(i.runID),
		)
		if trace := e.HTTPTraceProvider.NewTrace(i.attempt, traceLogger); trace != nil {
			ctx = httptrace.WithClientTrace(ctx, trace)
		}
	}

	request, err := nexusrpc.NewCompletionHTTPRequest(ctx, task.Url, i.completion)
	if err != nil {
		return callbackspb.CALLBACK_STATUS_FAILED
	}
	if request.Header == nil {
		request.Header = make(http.Header)
	}
	for k, v := range i.nexus.Header {
		request.Header.Set(k, v)
	}

	caller := e.HTTPCallerProvider(queues.NamespaceIDAndDestination{
		NamespaceID: ns.ID().String(),
		Destination: taskAttributes.Destination,
	})
	startTime := time.Now()
	response, err := caller(request)

	namespaceTag := metrics.NamespaceTag(ns.Name().String())
	destTag := metrics.DestinationTag(taskAttributes.Destination)
	statusCodeTag := metrics.OutcomeTag(outcomeTag(ctx, response, err))
	e.MetricsHandler.Counter(RequestCounter.Name()).Record(1, namespaceTag, destTag, statusCodeTag)
	e.MetricsHandler.Timer(RequestLatencyHistogram.Name()).Record(time.Since(startTime), namespaceTag, destTag, statusCodeTag)

	if err != nil {
		e.Logger.Error("Callback request failed with error", tag.Error(err))
		return callbackspb.CALLBACK_STATUS_BACKING_OFF
	}

	if response.StatusCode >= 200 && response.StatusCode < 300 {
		// Body is not read but should be discarded to keep the underlying TCP connection alive.
		// Just in case something unexpected happens while discarding or closing the body,
		// propagate errors to the machine.
		if _, err = io.Copy(io.Discard, response.Body); err == nil {
			if err = response.Body.Close(); err != nil {
				e.Logger.Error("Callback request failed with error", tag.Error(err))
				return callbackspb.CALLBACK_STATUS_BACKING_OFF
			}
		}
		return callbackspb.CALLBACK_STATUS_SUCCEEDED
	}

	retryable := isRetryableHTTPResponse(response)
	err = readHandlerErrFromResponse(response, e.Logger)
	e.Logger.Error("Callback request failed", tag.Error(err), tag.NewStringTag("status", response.Status), tag.NewBoolTag("retryable", retryable))
	if retryable {
		return callbackspb.CALLBACK_STATUS_BACKING_OFF
	}
	return callbackspb.CALLBACK_STATUS_FAILED
}

// isRetryableHTTPResponse determines if an HTTP response should trigger a retry.
func isRetryableHTTPResponse(response *http.Response) bool {
	return response.StatusCode >= 500 || slices.Contains(retryable4xxErrorTypes, response.StatusCode)
}

// outcomeTag generates a metric tag for the HTTP call outcome.
func outcomeTag(callCtx context.Context, response *http.Response, callErr error) string {
	if callErr != nil {
		if callCtx.Err() != nil {
			return "request-timeout"
		}
		return "unknown-error"
	}
	return fmt.Sprintf("status:%d", response.StatusCode)
}

// readHandlerErrFromResponse reads and replaces the http response body and attempts to
// deserialize it into a Nexus failure. If successful, returns a nexus.HandlerError with
// the deserialized failure as the Cause. If there is an error reading the body or during
// deserialization, returns a nexus.HandlerError with a generic Cause based on response status.
//
// TODO: This logic is duplicated in the frontend handler for forwarded requests.
// Eventually it should live in the Nexus SDK.
func readHandlerErrFromResponse(response *http.Response, logger log.Logger) error {
	handlerErr := &nexus.HandlerError{
		Type:  commonnexus.HandlerErrorTypeFromHTTPStatus(response.StatusCode),
		Cause: fmt.Errorf("request failed with: %v", response.Status),
	}

	body, err := readAndReplaceBody(response)
	if err != nil {
		logger.Error("Error reading response body for non-ok callback request", tag.Error(err), tag.NewStringTag("status", response.Status))
		return err
	}

	if !isMediaTypeJSON(response.Header.Get("Content-Type")) {
		logger.Error("received invalid content-type header for non-OK HTTP response to CompleteOperation request", tag.Value(response.Header.Get("Content-Type")))
		return handlerErr
	}

	var failure nexus.Failure
	err = json.Unmarshal(body, &failure)
	if err != nil {
		logger.Error("failed to deserialize Nexus Failure from HTTP response to CompleteOperation request", tag.Error(err))
		return handlerErr
	}

	handlerErr.Cause = &nexus.FailureError{Failure: failure}
	return handlerErr
}

// readAndReplaceBody reads the response body in its entirety and closes it, and then
// replaces the original response body with an in-memory buffer.
// The body is replaced even when there was an error reading the entire body.
func readAndReplaceBody(response *http.Response) ([]byte, error) {
	responseBody := response.Body
	body, err := io.ReadAll(responseBody)
	_ = responseBody.Close()
	response.Body = io.NopCloser(bytes.NewReader(body))
	return body, err
}

// isMediaTypeJSON checks if the content type is application/json.
func isMediaTypeJSON(contentType string) bool {
	if contentType == "" {
		return false
	}
	mediaType, _, err := mime.ParseMediaType(contentType)
	return err == nil && mediaType == "application/json"
}
