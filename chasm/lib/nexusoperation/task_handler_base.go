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

// invocationTaskHandlerOptions groups the common dependencies shared by the invocation and cancellation task
// handlers. It does not include fx.In — concrete fx option structs embed this and add fx.In themselves.
type invocationTaskHandlerOptions struct {
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

func (o invocationTaskHandlerOptions) toBase() nexusTaskHandlerBase {
	return nexusTaskHandlerBase{
		config:            o.Config,
		namespaceRegistry: o.NamespaceRegistry,
		metricsHandler:    o.MetricsHandler,
		logger:            o.Logger,
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
	// endpoint is nil for system-internal operations where endpoint lookup is skipped.
	// These always use the system callback URL since the callback is handled internally.
	if endpoint == nil {
		return commonnexus.SystemCallbackURL, nil
	}
	target := endpoint.GetEndpoint().GetSpec().GetTarget().GetVariant()
	if !b.config.UseSystemCallbackURL() {
		return buildCallbackFromTemplate(b.config.CallbackURLTemplate(), ns)
	}
	switch target.(type) {
	case *persistencespb.NexusEndpointTarget_Worker_:
		return commonnexus.SystemCallbackURL, nil
	case *persistencespb.NexusEndpointTarget_External_:
		return buildCallbackFromTemplate(b.config.CallbackURLTemplate(), ns)
	default:
		return "", fmt.Errorf("unknown endpoint target type: %T", target)
	}
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
// based on the call timeout and endpoint type.
func (b *nexusTaskHandlerBase) newInvocation(
	ctx context.Context,
	ns *namespace.Namespace,
	endpoint *persistencespb.NexusEndpointEntry,
	endpointName string,
	service string,
	callTimeout time.Duration,
	timeoutType enumspb.TimeoutType,
	traceCtx invocationTraceContext,
) (invocation, error) {
	if callTimeout < b.config.MinRequestTimeout(ns.Name().String()) {
		return &invocationTimeout{timeoutType}, nil
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

// setupCallContext creates a context with a timeout and attaches the failure source tracking value.
func (b *nexusTaskHandlerBase) setupCallContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	callCtx = context.WithValue(callCtx, commonnexus.FailureSourceContextKey, &atomic.Value{})
	return callCtx, cancel
}

// recordCallOutcome records metrics and logs errors for the outcome of an outbound Nexus call.
func (b *nexusTaskHandlerBase) recordCallOutcome(
	ns *namespace.Namespace,
	endpoint *persistencespb.NexusEndpointEntry,
	endpointName string,
	methodName string,
	outcomeTag string,
	callErr error,
	callDuration time.Duration,
	failureSource string,
) {
	methodTag := metrics.NexusMethodTag(methodName)
	namespaceTag := metrics.NamespaceTag(ns.Name().String())
	var destTag metrics.Tag
	if endpoint != nil {
		destTag = metrics.DestinationTag(endpoint.Endpoint.Spec.GetName())
	} else {
		destTag = metrics.DestinationTag(endpointName)
	}
	outcomeMetricTag := metrics.OutcomeTag(outcomeTag)
	failureSourceTag := metrics.FailureSourceTag(failureSource)
	OutboundRequestCounter.With(b.metricsHandler).Record(1, namespaceTag, destTag, methodTag, outcomeMetricTag, failureSourceTag)
	OutboundRequestLatency.With(b.metricsHandler).Record(callDuration, namespaceTag, destTag, methodTag, outcomeMetricTag, failureSourceTag)

	if callErr != nil {
		_, isTimeoutBelowMin := errors.AsType[*operationTimeoutBelowMinError](callErr)
		if failureSource == commonnexus.FailureSourceWorker || isTimeoutBelowMin {
			b.logger.Debug(fmt.Sprintf("Nexus %s request failed", methodName), tag.Error(callErr))
		} else {
			b.logger.Error(fmt.Sprintf("Nexus %s request failed", methodName), tag.Error(callErr))
		}
	}
}
