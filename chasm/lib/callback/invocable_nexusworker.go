package callback

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http/httptrace"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/nexus/nexusrpc"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
)

// invocableNexusWorker is an invocable that delivers a completion callback to a Nexus service worker by
// initiating a new Nexus operation against the target endpoint. It is the counterpart to invocableOutbound
// (which POSTs the completion to a URL) but instead uses the StartOperation Nexus API, mirroring the behavior
// of the Nexus Operation component's operationInvocationTaskHandler
// (chasm/lib/nexusoperation/operation_tasks.go).
type invocableNexusWorker struct {
	callback   *callbackspb.Callback_NexusWorker
	completion nexusrpc.CompleteOperationOptions
	// requestID is used as the Nexus request ID so that retries of this callback are idempotent on the
	// handler side.
	requestID string
}

func (n invocableNexusWorker) WrapError(result invocationResult, err error) error {
	// TODO: Revisit this. How does sync matching result in a destination down error?
	if retry, ok := result.(invocationResultRetry); ok {
		return queueserrors.NewDestinationDownError(retry.err.Error(), err)
	}
	return err
}

func (n invocableNexusWorker) Invoke(
	ctx context.Context,
	ns *namespace.Namespace,
	h *invocationTaskHandler,
	task *callbackspb.InvocationTask,
	taskAttr chasm.TaskAttributes,
) invocationResult {
	// The NexusWorker variant requires the Nexus operation dependencies to be wired. If they are missing the
	// task is unprocessable in this process and there is no point retrying it here.
	if h.endpointRegistry == nil || h.clientProvider == nil {
		return invocationResultFail{queueserrors.NewUnprocessableTaskError(
			"nexus worker callbacks require the endpoint registry and client provider to be configured",
		)}
	}

	endpointName := n.callback.GetEndpoint()
	if endpointName == "" {
		return invocationResultFail{queueserrors.NewUnprocessableTaskError(
			"nexus worker callback is missing an endpoint name",
		)}
	}

	// Resolve the endpoint from the registry so we can build a client targeting the correct worker.
	endpoint, err := h.endpointRegistry.GetByName(ctx, ns.ID(), endpointName)
	if err != nil {
		if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
			h.logger.Error("endpoint not found while processing nexus worker callback",
				tag.NewStringTag("endpoint", endpointName), tag.Error(err))
			return invocationResultFail{fmt.Errorf("nexus endpoint %q not registered: %w", endpointName, err)}
		}
		// Transient failure looking up the endpoint; retry later.
		return invocationResultRetry{fmt.Errorf("failed to resolve nexus endpoint %q: %w", endpointName, err)}
	}

	client, err := h.clientProvider(ctx, ns.ID().String(), endpoint, n.callback.GetService())
	if err != nil {
		return invocationResultRetry{fmt.Errorf("failed to build nexus client: %w", err)}
	}

	// Optionally attach HTTP tracing, mirroring invocableOutbound.
	if h.httpTraceProvider != nil {
		traceLogger := log.With(h.logger,
			tag.WorkflowNamespace(ns.Name().String()),
			tag.Operation("StartOperation"),
			tag.String("destination", endpointName),
			tag.RequestID(n.requestID),
			tag.NexusOperation(n.callback.GetOperation()),
			tag.AttemptStart(time.Now().UTC()),
			tag.Attempt(task.GetAttempt()),
		)
		if trace := h.httpTraceProvider.NewTrace(task.GetAttempt(), traceLogger); trace != nil {
			ctx = httptrace.WithClientTrace(ctx, trace)
		}
	}

	options := nexus.StartOperationOptions{
		RequestID: n.requestID,
		Header:    nexus.Header(maps.Clone(n.callback.GetNexusHeader())),
		Links:     n.completion.Links,
	}

	startTime := time.Now()
	response, callErr := client.StartOperation(ctx, n.callback.GetOperation(), n.callback.GetInput(), options)
	// We don't track the started operation, but we must drain the sync result body to avoid leaking the
	// underlying HTTP connection.
	if callErr == nil && response.Successful != nil {
		var payload *commonpb.Payload
		if consumeErr := response.Successful.Consume(&payload); consumeErr != nil {
			h.logger.Warn("failed to consume nexus worker callback result", tag.Error(consumeErr))
		}
	}

	namespaceTag := metrics.NamespaceTag(ns.Name().String())
	destTag := metrics.DestinationTag(endpointName)
	outcomeMetricTag := metrics.OutcomeTag(outcomeTag(ctx, callErr))
	h.metricsHandler.Counter(RequestCounter.Name()).Record(1, namespaceTag, destTag, outcomeMetricTag)
	h.metricsHandler.Timer(RequestLatencyHistogram.Name()).Record(time.Since(startTime), namespaceTag, destTag, outcomeMetricTag)

	if callErr != nil {
		return n.resultForError(h.logger, callErr)
	}
	return invocationResultOK{}
}

// resultForError maps an error returned by StartOperation to an invocationResult.
func (n invocableNexusWorker) resultForError(logger log.Logger, callErr error) invocationResult {
	// An OperationError means the worker received and processed the callback but the operation returned a
	// non-successful outcome. Retrying won't change the deterministic result, so treat it as a terminal
	// failure of the callback.
	if _, ok := errors.AsType[*nexus.OperationError](callErr); ok {
		logger.Error("Nexus worker callback operation completed unsuccessfully", tag.Error(callErr))
		return invocationResultFail{callErr}
	}

	retryable := isRetryableCallError(callErr)
	logger.Error("Nexus worker callback request failed", tag.Error(callErr), tag.Bool("retryable", retryable))
	if retryable {
		return invocationResultRetry{callErr}
	}
	return invocationResultFail{callErr}
}
