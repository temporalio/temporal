package frontend

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/rpc/interceptor"
	interceptornexus "go.temporal.io/server/common/rpc/interceptor/nexus"
)

type nexusForwardingInterceptor struct {
	logger                 log.Logger
	clusterMetadata        cluster.Metadata
	redirectionInterceptor *interceptor.Redirection
	forwardingClients      frontendHTTPClientCache
	serviceConfig          *Config
	httpTraceProvider      commonnexus.HTTPClientTraceProvider
}

type frontendHTTPClientCache interface {
	Get(targetClusterName string) (*common.FrontendHTTPClient, error)
}

func newNexusForwardingInterceptor(
	logger log.Logger,
	clusterMetadata cluster.Metadata,
	redirectionInterceptor *interceptor.Redirection,
	forwardingClients *cluster.FrontendHTTPClientCache,
	serviceConfig *Config,
	httpTraceProvider commonnexus.HTTPClientTraceProvider,
) *nexusForwardingInterceptor {
	return &nexusForwardingInterceptor{
		logger:                 logger,
		clusterMetadata:        clusterMetadata,
		redirectionInterceptor: redirectionInterceptor,
		forwardingClients:      forwardingClients,
		serviceConfig:          serviceConfig,
		httpTraceProvider:      httpTraceProvider,
	}
}

func (i *nexusForwardingInterceptor) InterceptNexus(
	ctx context.Context,
	in interceptornexus.InterceptorInput,
	next interceptornexus.HandlerFunc,
) (out any, retErr error) {
	info := in.ForwardingInfo()
	header := in.Header()
	namespaceEntry, err := in.NamespaceEntry()
	if err != nil {
		return nil, &interceptornexus.InterceptorError{
			Err:     err,
			Outcome: "interceptor_failed",
		}
	}
	currentCluster := i.clusterMetadata.GetCurrentClusterName()
	targetCluster := namespaceEntry.ActiveClusterName(namespace.RoutingKey{ID: info.BusinessID})
	if !namespaceEntry.IsGlobalNamespace() || targetCluster == currentCluster {
		return next(ctx, in)
	}
	if !i.shouldForwardRequest(ctx, header, namespaceEntry) {
		return nil, &interceptornexus.InterceptorError{
			Err:     nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnavailable, "cluster inactive"),
			Outcome: "namespace_inactive_forwarding_disabled",
		}
	}

	interceptornexus.SetOutcomeOverride(ctx, interceptornexus.OutcomeRequestForwarded)

	// this is the user-facing operation identity, and the DCRedirection prefix
	// matches the convention the gRPC redirection path uses for the same metrics.
	metricsHandler, forwardStartTime := i.redirectionInterceptor.BeforeCall(
		interceptor.DCRedirectionMetricsPrefix + in.MethodName(),
	)
	defer func() {
		redirectionErr := retErr
		if taggedErr, ok := errors.AsType[*interceptornexus.InterceptorError](retErr); ok {
			redirectionErr = taggedErr.Err
		}
		i.redirectionInterceptor.AfterCall(metricsHandler, forwardStartTime, targetCluster, namespaceEntry.Name().String(), redirectionErr)
	}()

	switch request := in.(type) {
	case interceptornexus.StartOpInput:
		out, retErr = i.forwardStartOperation(ctx, request, info, namespaceEntry, targetCluster)
	case interceptornexus.CancelOpInput:
		retErr = i.forwardCancelOperation(ctx, request, info, namespaceEntry, targetCluster)
	case interceptornexus.CompleteOpInput:
		retErr = i.forwardCompleteOperation(ctx, request, info, namespaceEntry, targetCluster)
	default:
		return nil, &interceptornexus.InterceptorError{
			Err: nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnavailable, "forwarding failed, unknown operation type"),
		}
	}
	return out, retErr
}

func (i *nexusForwardingInterceptor) shouldForwardRequest(
	ctx context.Context,
	header headers.HeaderGetter,
	namespaceEntry *namespace.Namespace,
) bool {
	redirectAllowed, err := strconv.ParseBool(header.Get(interceptor.DCRedirectionContextHeaderName))
	if err != nil {
		redirectAllowed = true
	}
	return redirectAllowed &&
		i.redirectionInterceptor.RedirectionAllowed(ctx) &&
		namespaceEntry.IsGlobalNamespace() &&
		i.serviceConfig.EnableNamespaceNotActiveAutoForwarding(namespaceEntry.Name().String())
}

func (i *nexusForwardingInterceptor) forwardStartOperation(
	ctx context.Context,
	request interceptornexus.StartOpInput,
	info interceptornexus.ForwardingInfo,
	namespaceEntry *namespace.Namespace,
	targetCluster string,
) (any, error) {
	logger := log.With(
		i.logger,
		tag.SourceCluster(i.clusterMetadata.GetCurrentClusterName()),
		tag.TargetCluster(targetCluster),
	)
	request.StartOperationOptions.Header[interceptor.DCRedirectionAPIHeaderName] = "true"
	request.StartOperationOptions.Header[interceptor.DCRedirectionSourceCellHeaderName] = i.clusterMetadata.GetCurrentClusterName()
	client, err := i.nexusClientForActiveCluster(ctx, request.ServiceName(), info, namespaceEntry, targetCluster)
	if err != nil {
		return nil, err
	}
	ctx = i.withForwardingTrace(ctx, "StartNexusOperation", request.OperationName(), request.StartOperationOptions.RequestID, info, namespaceEntry, targetCluster)
	response, err := client.StartOperation(ctx, request.OperationName(), request.StartOperationInput.Reader, request.StartOperationOptions)
	if err != nil {
		logger.Error("received error from remote cluster for forwarded Nexus start operation request", tag.Error(err))
		return nil, &interceptornexus.InterceptorError{Err: err, Outcome: "forwarded_request_error"}
	}
	if response.Successful != nil {
		return &nexus.HandlerStartOperationResultSync[any]{Value: response.Successful.Reader}, nil
	}
	return &nexus.HandlerStartOperationResultAsync{OperationToken: response.Pending.Token}, nil
}

func (i *nexusForwardingInterceptor) forwardCancelOperation(
	ctx context.Context,
	request interceptornexus.CancelOpInput,
	info interceptornexus.ForwardingInfo,
	namespaceEntry *namespace.Namespace,
	targetCluster string,
) error {
	logger := log.With(
		i.logger,
		tag.SourceCluster(i.clusterMetadata.GetCurrentClusterName()),
		tag.TargetCluster(targetCluster),
	)
	request.CancelOperationOptions.Header[interceptor.DCRedirectionAPIHeaderName] = "true"
	request.CancelOperationOptions.Header[interceptor.DCRedirectionSourceCellHeaderName] = i.clusterMetadata.GetCurrentClusterName()
	client, err := i.nexusClientForActiveCluster(ctx, request.ServiceName(), info, namespaceEntry, targetCluster)
	if err != nil {
		return err
	}
	handle, err := client.NewOperationHandle(request.OperationName(), request.CancellationToken)
	if err != nil {
		logger.Warn("invalid Nexus cancel operation", tag.Error(err))
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid operation")
	}
	ctx = i.withForwardingTrace(ctx, "CancelNexusOperation", request.OperationName(), "", info, namespaceEntry, targetCluster)
	if err := handle.Cancel(ctx, request.CancelOperationOptions); err != nil {
		logger.Error("received error from remote cluster for forwarded Nexus cancel operation request", tag.Error(err))
		return &interceptornexus.InterceptorError{Err: err, Outcome: "forwarded_request_error"}
	}
	return nil
}

func (i *nexusForwardingInterceptor) forwardCompleteOperation(
	ctx context.Context,
	request interceptornexus.CompleteOpInput,
	info interceptornexus.ForwardingInfo,
	namespaceEntry *namespace.Namespace,
	targetCluster string,
) error {
	logger := log.With(
		i.logger,
		tag.SourceCluster(i.clusterMetadata.GetCurrentClusterName()),
		tag.TargetCluster(targetCluster),
	)
	client, err := i.forwardingClients.Get(targetCluster)
	if err != nil {
		logger.Error("unable to get HTTP client for forward request", tag.Operation("CompleteNexusOperation"), tag.WorkflowNamespace(namespaceEntry.Name().String()), tag.Error(err), tag.SourceCluster(i.clusterMetadata.GetCurrentClusterName()), tag.TargetCluster(targetCluster))
		return &interceptornexus.InterceptorError{Err: nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error"), Outcome: "request_forwarding_failed"}
	}
	forwardURL, err := url.JoinPath(client.BaseURL(), commonnexus.RouteCompletionCallback.Path(namespaceEntry.Name().String()))
	if err != nil {
		logger.Error("failed to construct forwarding request URL", tag.Operation("CompleteNexusOperation"), tag.WorkflowNamespace(namespaceEntry.Name().String()), tag.Error(err), tag.TargetCluster(targetCluster))
		return &interceptornexus.InterceptorError{Err: nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error"), Outcome: "request_forwarding_failed"}
	}
	request.CompletionRequest.HTTPRequest.Header.Set(interceptor.DCRedirectionAPIHeaderName, "true")
	request.CompletionRequest.HTTPRequest.Header.Set(interceptor.DCRedirectionSourceCellHeaderName, i.clusterMetadata.GetCurrentClusterName())
	info.OriginalRequestHeaders.Set(interceptor.DCRedirectionAPIHeaderName, "true")
	info.OriginalRequestHeaders.Set(interceptor.DCRedirectionSourceCellHeaderName, i.clusterMetadata.GetCurrentClusterName())
	completion, err := completeOperationOptions(request.CompletionRequest)
	if err != nil {
		return err
	}
	ctx = i.withForwardingTrace(ctx, "CompleteNexusOperation", "", "", info, namespaceEntry, targetCluster)
	err = nexusrpc.NewCompletionHTTPClient(nexusrpc.CompletionHTTPClientOptions{
		// completions dont report a failure source back to the caller through headers
		HTTPCaller: (&nexusForwardingHTTPHeaderWrapper{client: client, originalRequestHeaders: info.OriginalRequestHeaders}).Do,
	}).CompleteOperation(ctx, forwardURL, completion)
	if err != nil {
		return &interceptornexus.InterceptorError{Err: err, Outcome: "forwarded_request_error"}
	}
	return nil
}

func completeOperationOptions(request *nexusrpc.CompletionRequest) (nexusrpc.CompleteOperationOptions, error) {
	switch request.State {
	case nexus.OperationStateSucceeded:
		return nexusrpc.CompleteOperationOptions{Result: request.Result.Reader, OperationToken: request.OperationToken, StartTime: request.StartTime, CloseTime: request.CloseTime, Links: request.Links}, nil
	case nexus.OperationStateFailed, nexus.OperationStateCanceled:
		return nexusrpc.CompleteOperationOptions{Error: request.Error, OperationToken: request.OperationToken, StartTime: request.StartTime, CloseTime: request.CloseTime, Links: request.Links}, nil
	default:
		return nexusrpc.CompleteOperationOptions{}, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid operation state: %q", request.State)
	}
}

func (i *nexusForwardingInterceptor) nexusClientForActiveCluster(
	ctx context.Context,
	service string,
	info interceptornexus.ForwardingInfo,
	namespaceEntry *namespace.Namespace,
	targetCluster string,
) (*nexusrpc.HTTPClient, error) {
	var setFailureSource func(string) // required for setting the source in case of a failure
	if oc, ok := operationContextFromContext(ctx); ok {
		setFailureSource = oc.setFailureSource
	}
	logger := log.With(
		i.logger,
		tag.SourceCluster(i.clusterMetadata.GetCurrentClusterName()),
		tag.TargetCluster(targetCluster),
	)
	httpClient, err := i.forwardingClients.Get(targetCluster)
	if err != nil {
		logger.Error("failed to forward Nexus request: error creating HTTP client", tag.Error(err), tag.SourceCluster(i.clusterMetadata.GetCurrentClusterName()), tag.TargetCluster(targetCluster))
		return nil, &interceptornexus.InterceptorError{Err: nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "request forwarding failed"), Outcome: "request_forwarding_failed"}
	}
	var baseURL string
	if i.serviceConfig.NexusForwardRequestUseEndpoint() && info.EndpointID != "" {
		baseURL, err = url.JoinPath(httpClient.BaseURL(), commonnexus.RouteDispatchNexusTaskByEndpoint.Path(info.EndpointID))
	} else {
		baseURL, err = url.JoinPath(httpClient.BaseURL(), commonnexus.RouteDispatchNexusTaskByNamespaceAndTaskQueue.Path(commonnexus.NamespaceAndTaskQueue{Namespace: namespaceEntry.Name().String(), TaskQueue: info.TaskQueue}))
	}
	if err != nil {
		logger.Error("failed to forward Nexus request: error constructing ServiceBaseURL", tag.URL(httpClient.BaseURL()), tag.WorkflowNamespace(namespaceEntry.Name().String()), tag.WorkflowTaskQueueName(info.TaskQueue), tag.Error(err))
		return nil, &interceptornexus.InterceptorError{Err: nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "request forwarding failed"), Outcome: "request_forwarding_failed"}
	}
	return nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{
		HTTPCaller: (&nexusForwardingHTTPHeaderWrapper{client: httpClient, originalRequestHeaders: info.OriginalRequestHeaders, setFailureSource: setFailureSource}).Do,
		BaseURL:    baseURL,
		Service:    service,
	})
}

func (i *nexusForwardingInterceptor) withForwardingTrace(
	ctx context.Context,
	method string,
	operation string,
	requestID string,
	info interceptornexus.ForwardingInfo,
	namespaceEntry *namespace.Namespace,
	targetCluster string,
) context.Context {
	if i.httpTraceProvider == nil {
		return ctx
	}
	traceLogger := log.With(i.logger,
		tag.Operation(method),
		tag.WorkflowNamespace(namespaceEntry.Name().String()),
		tag.RequestID(requestID),
		tag.NexusOperation(operation),
		tag.Endpoint(info.EndpointName),
		tag.AttemptStart(time.Now().UTC()),
		tag.SourceCluster(i.clusterMetadata.GetCurrentClusterName()),
		tag.TargetCluster(targetCluster),
	)
	if trace := i.httpTraceProvider.NewForwardingTrace(traceLogger); trace != nil {
		return httptrace.WithClientTrace(ctx, trace)
	}
	return ctx
}

type nexusForwardingHTTPHeaderWrapper struct {
	client                 *common.FrontendHTTPClient
	originalRequestHeaders http.Header
	setFailureSource       func(string)
}

func (f *nexusForwardingHTTPHeaderWrapper) Do(request *http.Request) (*http.Response, error) {
	// for forwarded requests, copy the original HTTP headers without sanitization.
	for name, values := range f.originalRequestHeaders {
		if request.Header.Get(name) == "" {
			request.Header.Set(name, values[0])
		}
	}
	response, err := f.client.Do(request)
	if err != nil {
		return nil, err
	}

	if source := response.Header.Get(commonnexus.FailureSourceHeaderName); source != "" && f.setFailureSource != nil {
		f.setFailureSource(source)
	}
	return response, nil
}
