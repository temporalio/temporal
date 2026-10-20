package nexusoperation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"text/template"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/resource"
	"go.uber.org/fx"
)

// ClientProvider provides a nexus client for a given endpoint.
type ClientProvider func(ctx context.Context, namespaceID string, entry *persistencespb.NexusEndpointEntry, service string) (*nexusrpc.HTTPClient, error)

// commonTaskHandlerOptions is the fx parameter object for common options supplied to common task handlers.
type commonTaskHandlerOptions struct {
	fx.In

	Config *Config

	MetricsHandler metrics.Handler
	Logger         log.Logger
}

// InvocationTaskHandlerOptions groups the common dependencies shared by the invocation and cancellation task
// handlers. It embeds fx.In so that dig can flatten its fields when embedded in other dig.In structs.
type InvocationTaskHandlerOptions struct {
	fx.In

	Config            *Config
	NamespaceRegistry namespace.Registry
	MetricsHandler    metrics.Handler
	Logger            log.Logger
	ClientProvider    ClientProvider
	EndpointRegistry  commonnexus.EndpointRegistry
	HTTPTraceProvider commonnexus.HTTPClientTraceProvider
	HistoryClient     resource.HistoryClient
	ChasmRegistry     *chasm.Registry
}

func (o InvocationTaskHandlerOptions) toBase() nexusTaskHandlerBase {
	return nexusTaskHandlerBase{
		config:            o.Config,
		namespaceRegistry: o.NamespaceRegistry,
		metricsHandler:    o.MetricsHandler,
		logger:            log.With(o.Logger, tag.NexusStageCallerOutbound),
		clientProvider:    o.ClientProvider,
		endpointRegistry:  o.EndpointRegistry,
		httpTraceProvider: o.HTTPTraceProvider,
		historyClient:     o.HistoryClient,
		chasmRegistry:     o.ChasmRegistry,
	}
}

// nexusTaskHandlerBase contains common dependencies shared by the invocation and cancellation task handlers.
type nexusTaskHandlerBase struct {
	config            *Config
	namespaceRegistry namespace.Registry
	metricsHandler    metrics.Handler
	logger            log.Logger
	clientProvider    ClientProvider
	endpointRegistry  commonnexus.EndpointRegistry
	httpTraceProvider commonnexus.HTTPClientTraceProvider
	historyClient     resource.HistoryClient
	chasmRegistry     *chasm.Registry
}

func (b *nexusTaskHandlerBase) buildCallbackURL(
	ns *namespace.Namespace,
	endpoint *persistencespb.NexusEndpointEntry,
) (string, error) {
	return buildCallbackURL(b.config.CallbackURLTemplate(), ns, endpoint)
}

func buildCallbackFromTemplate(callbackTemplate *template.Template, ns *namespace.Namespace) (string, error) {
	if callbackTemplate == nil {
		return "", serviceerror.NewInternalf("dynamic config %q is unset", CallbackURLTemplate.Key().String())
	}
	builder := &strings.Builder{}
	err := callbackTemplate.Execute(builder, struct{ NamespaceName, NamespaceID string }{
		NamespaceName: ns.Name().String(),
		NamespaceID:   ns.ID().String(),
	})
	if err != nil {
		return "", fmt.Errorf("failed to format callback URL: %w", err)
	}
	return builder.String(), nil
}

// newInvocation creates an invocation for the given endpoint, selecting the appropriate implementation
// based on the time available for the request and the endpoint type.
//
// ctx must be the call context returned by setupCallContext. remainingOpTimeout is the remaining operation timeout of
// type timeoutType, or maxDuration if the operation has no timeout.
func (b *nexusTaskHandlerBase) newInvocation(
	ctx context.Context,
	ns *namespace.Namespace,
	endpoint *persistencespb.NexusEndpointEntry,
	endpointName string,
	service string,
	remainingOpTimeout time.Duration,
	timeoutType enumspb.TimeoutType,
	traceCtx invocationTraceContext,
) (invocation, error) {
	minRequestTimeout := b.config.MinRequestTimeout(ns.Name().String())
	if remainingOpTimeout < minRequestTimeout {
		return &invocationTimeout{timeoutType}, nil
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < minRequestTimeout {
		// The operation has enough time left but the request doesn't, because of the task deadline or RequestTimeout.
		// Fail this attempt only and let it be retried.
		return invocationRequestDeadline{}, nil
	}
	if endpointName == commonnexus.SystemEndpoint {
		return b.newInvocationSystem(ns), nil
	}
	return b.newInvocationHTTP(ctx, ns, endpoint, service, traceCtx)
}

// lookupEndpoint gets an endpoint from the registry, preferring to look up by ID and falling back to name lookup.
// The fallback is needed because endpoints may be deleted and recreated with the same name but a different ID.
// In that case, the ID stored in the operation state becomes stale, but the name-based lookup still resolves correctly.
// Returns a nil entry if the endpoint name is the system nexus endpoint.
func (b *nexusTaskHandlerBase) lookupEndpoint(ctx context.Context, namespaceID namespace.ID, endpointID, endpointName string) (*persistencespb.NexusEndpointEntry, error) {
	// Skip endpoint lookup for system-internal operations.
	if endpointName == commonnexus.SystemEndpoint {
		return nil, nil
	}

	entry, err := b.endpointRegistry.GetByID(ctx, endpointID)
	if err != nil {
		if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
			// Endpoint was not found by ID, fall back to name lookup.
			return b.endpointRegistry.GetByName(ctx, namespaceID, endpointName)
		}
		return nil, err
	}
	return entry, nil
}

// setupCallContext creates the context for a single start or cancel request and attaches the failure source tracking
// value.
//
// The request is bound by requestTimeout and remainingOpTimeout, and by the deadline that the task executor sets on
// ctx. That deadline also covers loading the component and saving the result: 10s for tasks on the outbound queue and
// 3s for system endpoint tasks, which run on the transfer queue (see taskDestination). requestTimeout must be shorter
// than the task deadline to leave time to save the result.
func (b *nexusTaskHandlerBase) setupCallContext(
	ctx context.Context,
	requestTimeout time.Duration,
	remainingOpTimeout time.Duration,
) (context.Context, context.CancelFunc) {
	callCtx, cancel := context.WithTimeout(ctx, min(requestTimeout, remainingOpTimeout))
	callCtx = context.WithValue(callCtx, commonnexus.FailureSourceContextKey, &atomic.Value{})
	return callCtx, cancel
}

// recordCallOutcome records metrics and logs errors for the outcome of an outbound Nexus call.
func (b *nexusTaskHandlerBase) recordCallOutcome(
	endpoint *persistencespb.NexusEndpointEntry,
	outcomeTag string,
	callErr error,
	callDuration time.Duration,
	failureSource string,
	traceCtx invocationTraceContext,
) {
	methodTag := metrics.NexusMethodTag(traceCtx.operationTag)
	namespaceTag := metrics.NamespaceTag(traceCtx.namespaceName)
	var destTag metrics.Tag
	if endpoint != nil {
		destTag = metrics.DestinationTag(endpoint.Endpoint.Spec.GetName())
	} else {
		destTag = metrics.DestinationTag(traceCtx.endpointName)
	}
	outcomeMetricTag := metrics.OutcomeTag(outcomeTag)
	failureSourceTag := metrics.FailureSourceTag(failureSource)
	OutboundRequestCounter.With(b.metricsHandler).Record(1, namespaceTag, destTag, methodTag, outcomeMetricTag, failureSourceTag)
	OutboundRequestLatency.With(b.metricsHandler).Record(callDuration, namespaceTag, destTag, methodTag, outcomeMetricTag, failureSourceTag)

	b.logCallFailure(traceCtx, callErr, failureSource)
}

// logCallFailure logs a failed outbound Nexus call.
func (b *nexusTaskHandlerBase) logCallFailure(traceCtx invocationTraceContext, callErr error, failureSource string) {
	if callErr == nil {
		return
	}
	tags := append(traceCtx.tags(), tag.Error(callErr))
	_, isTimeoutBelowMin := errors.AsType[*operationTimeoutBelowMinError](callErr)
	if failureSource == commonnexus.FailureSourceWorker || isTimeoutBelowMin {
		b.logger.Debug("Nexus request failed", tags...)
	} else {
		b.logger.Error("Nexus request failed", tags...)
	}
}
