package frontend

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/rpc/interceptor"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// user-agent header contains Nexus SDK client info in the form <sdk-name>/v<sdk-version>
	headerUserAgent        = "user-agent"
	clientNameVersionDelim = "/v"
)

// Generic Nexus context that is not bound to a specific operation.
// Includes fields extracted from an incoming Nexus request before being handled by the Nexus HTTP handler.
type nexusContext struct {
	// Whether to use the new Temporal failure responses path.
	// Set from the incoming nexus request's "temporal-nexus-failure-support" header.
	callerFailureSupport                 bool
	requestStartTime                     time.Time
	apiName                              string
	namespaceName                        string
	taskQueue                            string
	endpointName                         string
	endpointID                           string
	claims                               *authorization.Claims
	namespaceValidationInterceptor       *interceptor.NamespaceValidatorInterceptor
	namespaceRateLimitInterceptor        interceptor.NamespaceRateLimitInterceptor
	namespaceConcurrencyLimitInterceptor *interceptor.ConcurrentRequestLimitInterceptor
	rateLimitInterceptor                 *interceptor.RateLimitInterceptor
	responseHeaders                      map[string]string
	responseHeadersMutex                 sync.Mutex
	originalRequestHeaders               http.Header // Original HTTP request headers to be used for forwarded requests.
}

// Context for a specific Nexus operation, includes a resolved namespace, and a bound metrics handler and logger.
type operationContext struct {
	*nexusContext
	method          string
	clusterMetadata cluster.Metadata
	namespace       *namespace.Namespace
	// "Special" metrics handler that should only be passed to interceptors, which require a different set of
	// pre-baked tags than the "normal" metricsHandler.
	metricsHandlerForInterceptors metrics.Handler
	metricsHandler                metrics.Handler
	logger                        log.Logger
	clientVersionChecker          headers.VersionChecker
	auth                          *authorization.Interceptor
	telemetryInterceptor          *interceptor.TelemetryInterceptor
	requestErrorHandler           *interceptor.RequestErrorHandler
	redirectionInterceptor        *interceptor.Redirection
	forwardingEnabledForNamespace dynamicconfig.BoolPropertyFnWithNamespaceFilter
	headersBlacklist              dynamicconfig.TypedPropertyFn[*regexp.Regexp]
	metricTagConfig               dynamicconfig.TypedPropertyFn[chasmnexus.NexusMetricTagConfig]
}

func (c *operationContext) matchingRequest(req *nexuspb.Request) *matchingservice.DispatchNexusTaskRequest {
	req.Endpoint = c.endpointName
	return &matchingservice.DispatchNexusTaskRequest{
		NamespaceId: c.namespace.ID().String(),
		TaskQueue:   &taskqueuepb.TaskQueue{Name: c.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Request:     req,
	}
}

func (c *operationContext) augmentContext(ctx context.Context, header nexus.Header) context.Context {
	ctx = interceptor.WithTelemetryContext(ctx, c)
	ctx = interceptor.WithNexusAPIName(ctx, c.apiName)
	ctx = interceptor.WithNexusEndpointName(ctx, c.endpointName)
	ctx = interceptor.WithNexusNamespace(ctx, c.namespace)
	if userAgent, ok := header[headerUserAgent]; ok {
		// Use SplitN for efficiency but enforce exactly one delimiter to preserve the
		// original (pre-SplitN) strictness where additional delimiters cause us to ignore
		// the header instead of coalescing trailing data into the version string.
		if strings.Count(userAgent, clientNameVersionDelim) == 1 { // exact single occurrence
			parts := strings.SplitN(userAgent, clientNameVersionDelim, 2)
			if len(parts) == 2 { // always true given Count==1, kept for defensive clarity
				mdIncoming, ok := metadata.FromIncomingContext(ctx)
				if !ok {
					mdIncoming = metadata.MD{}
				}
				mdIncoming.Set(headers.ClientNameHeaderName, parts[0])
				mdIncoming.Set(headers.ClientVersionHeaderName, parts[1])
				ctx = metadata.NewIncomingContext(ctx, mdIncoming)
			}
		}
	}
	return ctx
}

func (c *operationContext) MetricsHandler(_ error) metrics.Handler {
	// start/cancel handlers already have the error, just return the handler
	return c.metricsHandler
}

func (c *operationContext) MetricsHandlerForInterceptors() metrics.Handler {
	return c.metricsHandlerForInterceptors
}

func (c *operationContext) MetricsLogger() log.Logger {
	return c.logger
}

func (c *operationContext) SetMetricsOutcome(outcome string) {
	c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag(outcome))
}

func (c *operationContext) SetFailureSource(source string) {
	c.setFailureSource(source)
}

// replaces registerRequestErrorHandler and the cleanupFunctions
// registry. Errors that a worker produced are already reported by the worker's own
// cluster, so only other sources are handled here.
func (c *operationContext) HandleRequestError(err error) {
	if err == nil {
		return
	}
	source, ok := c.responseHeaders[commonnexus.FailureSourceHeaderName]
	if !ok || source == commonnexus.FailureSourceWorker {
		return
	}
	c.requestErrorHandler.HandleError(
		// The request is only read to extract workflow log tags, which is keyed off the
		// gRPC full method. Nexus has none, so it is never used.
		nil,
		"",
		c.metricsHandlerForInterceptors,
		[]tag.Tag{tag.Operation(c.method), tag.WorkflowNamespace(c.namespaceName)},
		err,
		c.namespace.Name(),
	)
}

// required as operations might panic before the interceptor chain is
// invoked and panics are handled by the outermost telemetry interceptor
func captureOperationPanic(logger log.Logger, errPtr *error) {
	recovered := recover() //nolint:revive
	if recovered == nil {
		return
	}
	err, ok := recovered.(error)
	if !ok {
		err = fmt.Errorf("panic: %v", recovered)
	}
	logger.Error("Panic captured", tag.SysStackTrace(string(debug.Stack())), tag.Error(err))
	*errPtr = err
}

func (c *operationContext) sanitizeRequestHeaders(request *matchingservice.DispatchNexusTaskRequest) {
	if request.GetRequest().GetHeader() == nil {
		return
	}

	sanitizedHeaders := make(map[string]string, len(request.Request.Header))
	headersBlacklist := c.headersBlacklist()
	for name, value := range request.Request.Header {
		if !headersBlacklist.MatchString(name) {
			sanitizedHeaders[name] = value
		}
	}
	request.Request.Header = sanitizedHeaders
}

// enrichNexusOperationMetrics enhances metrics with additional Nexus operation context based on configuration.
func (c *operationContext) enrichNexusOperationMetrics(service, operation string, requestHeader nexus.Header) {
	conf := c.metricTagConfig()

	var tags []metrics.Tag

	if conf.IncludeServiceTag {
		tags = append(tags, metrics.NexusServiceTag(service))
	}

	if conf.IncludeOperationTag {
		tags = append(tags, metrics.NexusOperationTag(operation))
	}

	for _, mapping := range conf.HeaderTagMappings {
		tags = append(tags, metrics.StringTag(mapping.TargetTag, requestHeader.Get(mapping.SourceHeader)))
	}

	if len(tags) > 0 {
		c.metricsHandler = c.metricsHandler.WithTags(tags...)
	}
}

// enrichNexusOperationLogs adds Nexus operation context to the handler-side logger.
func (c *operationContext) enrichNexusOperationLogs(service, operation, requestID string) {
	tags := []tag.Tag{
		tag.NexusService(service),
		tag.NexusOperation(operation),
		tag.Endpoint(c.endpointName),
	}
	if requestID != "" {
		tags = append(tags, tag.RequestID(requestID))
	}
	c.logger = log.With(c.logger, tags...)
}

// Key to extract a nexusContext object from a context.Context.
type nexusContextKey struct{}

// A Nexus Handler implementation.
// Dispatches Nexus requests as Nexus tasks to workers via matching.
type nexusHandler struct {
	nexus.UnimplementedHandler
	logger            log.Logger
	metricsHandler    metrics.Handler
	clusterMetadata   cluster.Metadata
	namespaceRegistry namespace.Registry
	matchingClient    matchingservice.MatchingServiceClient
	auth              *authorization.Interceptor
	// telemetryInterceptor          *interceptor.TelemetryInterceptor
	// requestErrorHandler           *interceptor.RequestErrorHandler
	// redirectionInterceptor        *interceptor.Redirection
	forwardingEnabledForNamespace dynamicconfig.BoolPropertyFnWithNamespaceFilter
	forwardingClients             *cluster.FrontendHTTPClientCache
	payloadSizeLimit              dynamicconfig.IntPropertyFnWithNamespaceFilter
	headersBlacklist              dynamicconfig.TypedPropertyFn[*regexp.Regexp]
	useForwardByEndpoint          dynamicconfig.BoolPropertyFn
	metricTagConfig               dynamicconfig.TypedPropertyFn[chasmnexus.NexusMetricTagConfig]
	httpTraceProvider             commonnexus.HTTPClientTraceProvider
	nexusInterceptors             []interceptor.NexusInterceptor
}

// Extracts a nexusContext from the given ctx and returns an operationContext with tagged metrics and logging.
// Resolves the context's namespace name to a registered Namespace.
func (h *nexusHandler) getOperationContext(ctx context.Context, method string) (*operationContext, error) {
	nc, ok := ctx.Value(nexusContextKey{}).(*nexusContext)
	if !ok {
		return nil, errors.New("no nexus context set on context")
	}
	oc := operationContext{
		nexusContext:         nc,
		method:               method,
		clusterMetadata:      h.clusterMetadata,
		clientVersionChecker: headers.NewDefaultVersionChecker(),
		auth:                 h.auth,
		// telemetryInterceptor:          h.telemetryInterceptor,
		// requestErrorHandler:           h.requestErrorHandler,
		// redirectionInterceptor:        h.redirectionInterceptor,
		forwardingEnabledForNamespace: h.forwardingEnabledForNamespace,
		headersBlacklist:              h.headersBlacklist,
		metricTagConfig:               h.metricTagConfig,
	}
	oc.metricsHandlerForInterceptors = h.metricsHandler.WithTags(
		metrics.OperationTag(method),
		metrics.NamespaceTag(nc.namespaceName),
	)
	oc.metricsHandler = h.metricsHandler.WithTags(
		metrics.NamespaceTag(nc.namespaceName),
		metrics.NexusEndpointTag(nc.endpointName),
		metrics.NexusMethodTag(method),
		// default to internal error unless overridden by handler
		metrics.OutcomeTag("internal_error"),
	)

	var err error
	if oc.namespace, err = h.namespaceRegistry.GetNamespace(namespace.Name(nc.namespaceName)); err != nil {
		metrics.NexusRequests.With(oc.metricsHandler).Record(
			1,
			metrics.OutcomeTag("namespace_not_found"),
		)

		if _, ok := errors.AsType[*serviceerror.NamespaceNotFound](err); ok {
			return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "namespace not found: %q", nc.namespaceName)
		}
		return nil, commonnexus.ConvertGRPCError(err, false)
	}
	oc.forwardingEnabledForNamespace = h.forwardingEnabledForNamespace
	oc.logger = log.With(h.logger, tag.Operation(method), tag.WorkflowNamespace(nc.namespaceName))
	return &oc, nil
}

// StartOperation implements the nexus.Handler interface.
func (h *nexusHandler) StartOperation(
	ctx context.Context,
	service, operation string,
	input *nexus.LazyValue,
	options nexus.StartOperationOptions,
) (result nexus.HandlerStartOperationResult[any], retErr error) {
	oc, err := h.getOperationContext(ctx, "StartNexusOperation")
	if err != nil {
		return nil, err
	}
	ctx = oc.augmentContext(ctx, options.Header)
	oc.enrichNexusOperationMetrics(service, operation, options.Header)
	oc.enrichNexusOperationLogs(service, operation, options.RequestID)
	defer captureOperationPanic(oc.logger, &retErr)

	var links []*nexuspb.Link
	for _, nexusLink := range options.Links {
		links = append(links, &nexuspb.Link{
			Url:  nexusLink.URL.String(),
			Type: nexusLink.Type,
		})
	}

	startOperationRequest := nexuspb.StartOperationRequest{
		Service:        service,
		Operation:      operation,
		Callback:       options.CallbackURL,
		CallbackHeader: options.CallbackHeader,
		RequestId:      options.RequestID,
		Links:          links,
	}
	request := oc.matchingRequest(&nexuspb.Request{
		ScheduledTime: timestamppb.New(oc.requestStartTime),
		Header:        options.Header,
		Variant: &nexuspb.Request_StartOperation{
			StartOperation: &startOperationRequest,
		},
		Capabilities: &nexuspb.Request_Capabilities{
			TemporalFailureResponses: oc.callerFailureSupport,
		},
	})

	finalHandler := func(ctx context.Context, _ interceptor.NexusInterceptorInput) (any, error) {
		return h.finalStartHandler(ctx, oc, operation, input, &startOperationRequest, request)
	}

	nexusOpInput := interceptor.NewStartNexusOpInput(service, operation, oc.namespaceName, options, input)
	nexusOpInput.WithForwardingInfo(interceptor.NexusForwardingInfo{
		OriginalRequestHeaders: oc.originalRequestHeaders,
		TaskQueue:              oc.taskQueue,
		EndpointID:             oc.endpointID,
		EndpointName:           oc.endpointName,
	})
	chainedHandler := interceptor.ChainNexusInterceptors(finalHandler, h.nexusInterceptors)
	out, err := chainedHandler(ctx, nexusOpInput)
	if err != nil {
		if taggedErr, ok := errors.AsType[*interceptor.InterceptorError](err); ok {
			return nil, taggedErr.Err
		}
		return nil, err
	}
	res, ok := out.(nexus.HandlerStartOperationResult[any])
	if !ok {
		return nil, fmt.Errorf("unexpected Nexus start interceptor result type %T", out)
	}
	return res, nil
}

//nolint:revive,cognitive-complexity: this is just a shift of existing code
func (h *nexusHandler) finalStartHandler(ctx context.Context,
	oc *operationContext,
	operation string,
	input *nexus.LazyValue,
	startOperationRequest *nexuspb.StartOperationRequest,
	request *matchingservice.DispatchNexusTaskRequest,
) (any, error) {
	oc.sanitizeRequestHeaders(request)
	var err error
	// Transform nexus Content to temporal Payload with common/nexus PayloadSerializer.
	if err = input.Consume(&startOperationRequest.Payload); err != nil {
		oc.logger.Warn("invalid input", tag.Error(err))
		return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid input")
	}
	if startOperationRequest.Payload.Size() > h.payloadSizeLimit(oc.namespaceName) {
		oc.logger.Error("payload size exceeds error limit for Nexus StartOperation request", tag.Operation(operation), tag.WorkflowNamespace(oc.namespaceName))
		return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "input exceeds size limit")
	}

	// Dispatch the request to be sync matched with a worker polling on the nexusContext taskQueue.
	// matchingClient sets a context timeout of 60 seconds for this request, this should be enough for any Nexus
	// RPC.
	response, err := h.matchingClient.DispatchNexusTask(ctx, request)
	if err != nil {
		oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("matching_timeout"))
		oc.logger.Error("received error from matching service for Nexus StartOperation request", tag.Error(err))
		return nil, commonnexus.ConvertGRPCError(err, false)
	}
	// Convert to standard Nexus SDK response.
	switch t := response.GetOutcome().(type) {
	case *matchingservice.DispatchNexusTaskResponse_Failure:
		// Set the failure source to "worker" if we've reached this case.
		// Failure conversions errors below are the user's fault, as it implies that malformed completions were sent from
		// the worker.
		oc.setFailureSource(commonnexus.FailureSourceWorker)
		oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("handler_error:" + t.Failure.GetNexusHandlerFailureInfo().GetType()))
		nf, err := commonnexus.TemporalFailureToNexusFailureInPlace(t.Failure)
		if err != nil {
			oc.logger.Error("error converting Temporal failure to Nexus failure", tag.Error(err), tag.Operation(operation), tag.WorkflowNamespace(oc.namespaceName))
			return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
		}
		he, err := nexusrpc.DefaultFailureConverter().FailureToError(nf)
		if err != nil {
			oc.logger.Error("error converting Nexus failure to Nexus HandlerError", tag.Error(err), tag.Operation(operation), tag.WorkflowNamespace(oc.namespaceName))
			return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
		}
		return nil, he

	case *matchingservice.DispatchNexusTaskResponse_HandlerError:
		// Deprecated case. Replaced with DispatchNexusTaskResponse_Failure
		oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("handler_error:" + t.HandlerError.GetErrorType()))
		oc.setFailureSource(commonnexus.FailureSourceWorker)
		err := convertOutcomeToNexusHandlerError(t)
		return nil, err

	case *matchingservice.DispatchNexusTaskResponse_RequestTimeout:
		oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("handler_timeout"))
		oc.setFailureSource(commonnexus.FailureSourceWorker)
		return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUpstreamTimeout, "upstream timeout")

	case *matchingservice.DispatchNexusTaskResponse_Response:
		switch t := t.Response.GetStartOperation().GetVariant().(type) {
		case *nexuspb.StartOperationResponse_SyncSuccess:
			oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("sync_success"))
			links := parseLinks(t.SyncSuccess.GetLinks(), oc.logger)
			nexus.AddHandlerLinks(ctx, links...)
			return &nexus.HandlerStartOperationResultSync[any]{
				Value: t.SyncSuccess.GetPayload(),
			}, nil

		case *nexuspb.StartOperationResponse_AsyncSuccess:
			oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("async_success"))
			token := t.AsyncSuccess.GetOperationToken()
			if token == "" {
				token = t.AsyncSuccess.GetOperationId()
			}
			links := parseLinks(t.AsyncSuccess.GetLinks(), oc.logger)
			nexus.AddHandlerLinks(ctx, links...)
			return &nexus.HandlerStartOperationResultAsync{
				OperationToken: token,
			}, nil

		case *nexuspb.StartOperationResponse_OperationError:
			oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("operation_error"))
			oc.setFailureSource(commonnexus.FailureSourceWorker)
			opErr := &nexus.OperationError{
				Message: "operation error",
				// nolint:staticcheck // Deprecated function still in use for backward compatibility.
				State: nexus.OperationState(t.OperationError.GetOperationState()),
				Cause: &nexus.FailureError{
					// nolint:staticcheck // Deprecated function still in use for backward compatibility.
					Failure: commonnexus.ProtoFailureToNexusFailure(t.OperationError.GetFailure()),
				},
			}
			if err := nexusrpc.MarkAsWrapperError(nexusrpc.DefaultFailureConverter(), opErr); err != nil {
				oc.logger.Error("error converting OperationError to Nexus failure", tag.Error(err), tag.Operation(operation), tag.WorkflowNamespace(oc.namespaceName))
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
			}
			return nil, opErr

		case *nexuspb.StartOperationResponse_Failure:
			// Set the failure source to "worker" if we've reached this case.
			// Failure conversions errors below are the user's fault, as it implies that malformed completions were sent from
			// the worker.
			oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("failure"))
			oc.setFailureSource(commonnexus.FailureSourceWorker)
			nf, err := commonnexus.TemporalFailureToNexusFailureInPlace(t.Failure)
			if err != nil {
				oc.logger.Error("error converting Temporal failure to Nexus failure", tag.Error(err), tag.Operation(operation), tag.WorkflowNamespace(oc.namespaceName))
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
			}
			cause, err := nexusrpc.DefaultFailureConverter().FailureToError(nf)
			if err != nil {
				oc.logger.Error("error converting Nexus failure to Nexus OperationError", tag.Error(err), tag.Operation(operation), tag.WorkflowNamespace(oc.namespaceName))
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
			}
			state := nexus.OperationStateFailed
			if t.Failure.GetCanceledFailureInfo() != nil {
				state = nexus.OperationStateCanceled
			}
			opErr := &nexus.OperationError{
				State:   state,
				Message: "operation error",
				Cause:   cause,
			}
			if err := nexusrpc.MarkAsWrapperError(nexusrpc.DefaultFailureConverter(), opErr); err != nil {
				oc.logger.Error("error converting OperationError to Nexus failure", tag.Error(err), tag.Operation(operation), tag.WorkflowNamespace(oc.namespaceName))
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
			}
			return nil, opErr
		}
	}
	// This is the worker's fault.
	oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("handler_error:EMPTY_OUTCOME"))
	oc.setFailureSource(commonnexus.FailureSourceWorker)

	return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "empty outcome")
}

func parseLinks(links []*nexuspb.Link, logger log.Logger) []nexus.Link {
	var nexusLinks []nexus.Link
	for _, link := range links {
		linkURL, err := url.Parse(link.Url)
		if err != nil {
			// TODO(rodrigozhou): links are non-essential for the execution of the workflow,
			// so ignoring the error for now; we will revisit how to handle these errors later.
			logger.Error("failed to parse link url", tag.URL(link.Url), tag.Error(err))
			continue
		}
		nexusLinks = append(nexusLinks, nexus.Link{
			URL:  linkURL,
			Type: link.GetType(),
		})
	}
	return nexusLinks
}

func (h *nexusHandler) CancelOperation(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) (retErr error) {
	oc, err := h.getOperationContext(ctx, "CancelNexusOperation")
	if err != nil {
		return err
	}
	ctx = oc.augmentContext(ctx, options.Header)
	oc.enrichNexusOperationMetrics(service, operation, options.Header)
	oc.enrichNexusOperationLogs(service, operation, "")
	defer captureOperationPanic(oc.logger, &retErr)

	request := oc.matchingRequest(&nexuspb.Request{
		Header:        options.Header,
		ScheduledTime: timestamppb.New(oc.requestStartTime),
		Variant: &nexuspb.Request_CancelOperation{
			CancelOperation: &nexuspb.CancelOperationRequest{
				Service:        service,
				Operation:      operation,
				OperationToken: token,
				// TODO(bergundy): Remove this fallback after the 1.27 release.
				OperationId: token,
			},
		},
		Capabilities: &nexuspb.Request_Capabilities{
			TemporalFailureResponses: oc.callerFailureSupport,
		},
	})

	finalHandler := func(ctx context.Context, _ interceptor.NexusInterceptorInput) (any, error) {
		return nil, h.finalCancelHandler(ctx, oc, operation, request)
	}

	nexusInterceptorInput := interceptor.NewCancelNexusOpInput(service, operation, oc.namespaceName, options, token)
	nexusInterceptorInput.WithForwardingInfo(interceptor.NexusForwardingInfo{
		OriginalRequestHeaders: oc.originalRequestHeaders,
		TaskQueue:              oc.taskQueue,
		EndpointID:             oc.endpointID,
		EndpointName:           oc.endpointName,
	})
	chainedHandler := interceptor.ChainNexusInterceptors(finalHandler, h.nexusInterceptors)
	_, err = chainedHandler(ctx, nexusInterceptorInput)
	if err != nil {
		if taggedErr, ok := errors.AsType[*interceptor.InterceptorError](err); ok {
			return taggedErr.Err
		}
		return err
	}
	return nil
}

func (h *nexusHandler) finalCancelHandler(
	ctx context.Context,
	oc *operationContext,
	operation string,
	request *matchingservice.DispatchNexusTaskRequest,
) error {
	oc.sanitizeRequestHeaders(request)

	// Dispatch the request to be sync matched with a worker polling on the nexusContext taskQueue.
	// matchingClient sets a context timeout of 60 seconds for this request, this should be enough for any Nexus
	// RPC.
	response, err := h.matchingClient.DispatchNexusTask(ctx, request)
	if err != nil {
		oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("matching_timeout"))
		oc.logger.Error("received error from matching service for Nexus CancelOperation request", tag.Error(err))
		return commonnexus.ConvertGRPCError(err, false)
	}
	// Convert to standard Nexus SDK response.
	switch t := response.GetOutcome().(type) {
	case *matchingservice.DispatchNexusTaskResponse_Failure:
		oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("handler_error:" + t.Failure.GetNexusHandlerFailureInfo().GetType()))
		// Set the failure source to "worker" if we've reached this case.
		// Failure conversions errors below are the user's fault, as it implies that malformed completions were sent from
		// the worker.
		oc.setFailureSource(commonnexus.FailureSourceWorker)
		nf, err := commonnexus.TemporalFailureToNexusFailureInPlace(t.Failure)
		if err != nil {
			oc.logger.Error("error converting Temporal failure to Nexus failure", tag.Error(err), tag.Operation(operation), tag.WorkflowNamespace(oc.namespaceName))
			return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
		}
		he, err := nexusrpc.DefaultFailureConverter().FailureToError(nf)
		if err != nil {
			oc.logger.Error("error converting Nexus failure to Nexus HandlerError", tag.Error(err), tag.Operation(operation), tag.WorkflowNamespace(oc.namespaceName))
			return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
		}
		return he

	case *matchingservice.DispatchNexusTaskResponse_HandlerError:
		// Deprecated case. Replaced with DispatchNexusTaskResponse_Failure
		oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("handler_error:" + t.HandlerError.GetErrorType()))
		oc.setFailureSource(commonnexus.FailureSourceWorker)
		err := convertOutcomeToNexusHandlerError(t)
		return err

	case *matchingservice.DispatchNexusTaskResponse_RequestTimeout:
		oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("handler_timeout"))
		oc.setFailureSource(commonnexus.FailureSourceWorker)
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUpstreamTimeout, "upstream timeout")

	case *matchingservice.DispatchNexusTaskResponse_Response:
		oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("success"))
		return nil
	}
	// This is the worker's fault.
	oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("handler_error:EMPTY_OUTCOME"))
	oc.setFailureSource(commonnexus.FailureSourceWorker)

	return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "empty outcome")
}

func convertOutcomeToNexusHandlerError(resp *matchingservice.DispatchNexusTaskResponse_HandlerError) *nexus.HandlerError {
	var retryBehavior nexus.HandlerErrorRetryBehavior
	// nolint:exhaustive // unspecified is the default
	switch resp.HandlerError.RetryBehavior {
	case enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE:
		retryBehavior = nexus.HandlerErrorRetryBehaviorRetryable
	case enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE:
		retryBehavior = nexus.HandlerErrorRetryBehaviorNonRetryable
	}
	// nolint:staticcheck // Deprecated function still in use for backward compatibility.
	cause := commonnexus.ProtoFailureToNexusFailure(resp.HandlerError.GetFailure())
	return &nexus.HandlerError{
		// nolint:staticcheck // Deprecated function still in use for backward compatibility.
		Type:          nexus.HandlerErrorType(resp.HandlerError.GetErrorType()),
		RetryBehavior: retryBehavior,
		Cause:         &nexus.FailureError{Failure: cause},
	}
}

func (nc *nexusContext) setFailureSource(source string) {
	nc.responseHeadersMutex.Lock()
	defer nc.responseHeadersMutex.Unlock()
	nc.responseHeaders[commonnexus.FailureSourceHeaderName] = source
}
