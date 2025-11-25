package callbacks

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

func isRetryableHTTPResponse(response *http.Response) bool {
	return response.StatusCode >= 500 || slices.Contains(retryable4xxErrorTypes, response.StatusCode)
}

func outcomeTag(callCtx context.Context, response *http.Response, callErr error) string {
	if callErr != nil {
		if callCtx.Err() != nil {
			return "request-timeout"
		}
		return "unknown-error"
	}
	return fmt.Sprintf("status:%d", response.StatusCode)
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
			tag.NewStringTag("destination", task.destination),
			tag.WorkflowID(n.workflowID),
			tag.WorkflowRunID(n.runID),
			tag.AttemptStart(time.Now().UTC()),
			tag.Attempt(n.attempt),
		)
		if trace := e.HTTPTraceProvider.NewTrace(n.attempt, traceLogger); trace != nil {
			ctx = httptrace.WithClientTrace(ctx, trace)
		}
	}

	request, err := nexusrpc.NewCompletionHTTPRequest(ctx, n.nexus.Url, n.completion)
	if err != nil {
		return invocationResultFail{queueserrors.NewUnprocessableTaskError(
			fmt.Sprintf("failed to construct Nexus request: %v", err),
		)}
	}
	if request.Header == nil {
		request.Header = make(http.Header)
	}
	for k, v := range n.nexus.Header {
		request.Header.Set(k, v)
	}

	caller := e.HTTPCallerProvider(queuescommon.NamespaceIDAndDestination{
		NamespaceID: ns.ID().String(),
		Destination: task.Destination(),
	})
	// Make the call and record metrics.
	startTime := time.Now()
	response, err := caller(request)

	namespaceTag := metrics.NamespaceTag(ns.Name().String())
	destTag := metrics.DestinationTag(task.Destination())
	statusCodeTag := metrics.OutcomeTag(outcomeTag(ctx, response, err))
	e.MetricsHandler.Counter(RequestCounter.Name()).Record(1, namespaceTag, destTag, statusCodeTag)
	e.MetricsHandler.Timer(RequestLatencyHistogram.Name()).Record(time.Since(startTime), namespaceTag, destTag, statusCodeTag)

	if err != nil {
		e.Logger.Error("Callback request failed with error", tag.Error(err))
		return invocationResultRetry{err}
	}

	if response.StatusCode >= 200 && response.StatusCode < 300 {
		// Body is not read but should be discarded to keep the underlying TCP connection alive.
		// Just in case something unexpected happens while discarding or closing the body,
		// propagate errors to the machine.
		if _, err = io.Copy(io.Discard, response.Body); err == nil {
			if err = response.Body.Close(); err != nil {
				e.Logger.Error("Callback request failed with error", tag.Error(err))
				return invocationResultRetry{err}
			}
		}
		return invocationResultOK{}
	}

	retryable := isRetryableHTTPResponse(response)
	err = readHandlerErrFromResponse(response, e.Logger)
	e.Logger.Error("Callback request failed", tag.Error(err), tag.NewStringTag("status", response.Status), tag.NewBoolTag("retryable", retryable))
	if retryable {
		return invocationResultRetry{err}
	}
	return invocationResultFail{err}
}

// Reads and replaces the http response body and attempts to deserialize it into a Nexus failure. If successful,
// returns a nexus.HandlerError with the deserialized failure as the Cause. If there is an error reading the body or
// during deserialization, returns a nexus.HandlerError with a generic Cause based on response status.
// TODO: This logic is duplicated in the frontend handler for forwarded requests. Eventually it should live in the Nexus SDK.
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

// readAndReplaceBody reads the response body in its entirety and closes it, and then replaces the original response
// body with an in-memory buffer.
// The body is replaced even when there was an error reading the entire body.
func readAndReplaceBody(response *http.Response) ([]byte, error) {
	responseBody := response.Body
	body, err := io.ReadAll(responseBody)
	_ = responseBody.Close()
	response.Body = io.NopCloser(bytes.NewReader(body))
	return body, err
}

func isMediaTypeJSON(contentType string) bool {
	if contentType == "" {
		return false
	}
	mediaType, _, err := mime.ParseMediaType(contentType)
	return err == nil && mediaType == "application/json"
}
