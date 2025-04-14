// The MIT License
//
// Copyright (c) 2023 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package frontend

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/components/nexusoperations"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// user-agent header contains Nexus SDK client info in the form <sdk-name>/v<sdk-version>
const headerUserAgent = "user-agent"
const clientNameVersionDelim = "/v"

// Generic Nexus context that is not bound to a specific operation.
// Includes fields extracted from an incoming Nexus request before being handled by the Nexus HTTP handler.
type nexusContext struct {
	requestStartTime                     time.Time
	apiName                              string
	namespaceName                        string
	taskQueue                            string
	endpointName                         string
	claims                               *authorization.Claims
	namespaceValidationInterceptor       *interceptor.NamespaceValidatorInterceptor
	namespaceRateLimitInterceptor        interceptor.NamespaceRateLimitInterceptor
	namespaceConcurrencyLimitInterceptor *interceptor.ConcurrentRequestLimitInterceptor
	rateLimitInterceptor                 *interceptor.RateLimitInterceptor
	responseHeaders                      map[string]string
	responseHeadersMutex                 sync.Mutex
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
	redirectionInterceptor        *interceptor.Redirection
	forwardingEnabledForNamespace dynamicconfig.BoolPropertyFnWithNamespaceFilter
	headersBlacklist              *dynamicconfig.GlobalCachedTypedValue[*regexp.Regexp]
	metricTagConfig               *dynamicconfig.GlobalCachedTypedValue[*nexusoperations.NexusMetricTagConfig]
	cleanupFunctions              []func(map[string]string, error)
}

// Panic handler and metrics recording function.
// Used as a deferred statement in Nexus handler methods.
func (c *operationContext) capturePanicAndRecordMetrics(ctxPtr *context.Context, errPtr *error) {
	recovered := recover() //nolint:revive
	if recovered != nil {
		err, ok := recovered.(error)
		if !ok {
			err = fmt.Errorf("panic: %v", recovered)
		}

		st := string(debug.Stack())

		c.logger.Error("Panic captured", tag.SysStackTrace(st), tag.Error(err))
		*errPtr = err
	}

	// Record Nexus-specific metrics
	metrics.NexusRequests.With(c.metricsHandler).Record(1)
	metrics.NexusLatency.With(c.metricsHandler).Record(time.Since(c.requestStartTime))

	// Record general telemetry metrics
	metrics.ServiceRequests.With(c.metricsHandlerForInterceptors).Record(1)
	c.telemetryInterceptor.RecordLatencyMetrics(*ctxPtr, c.requestStartTime, c.metricsHandlerForInterceptors)

	for _, fn := range c.cleanupFunctions {
		fn(c.responseHeaders, *errPtr)
	}
}

func (c *operationContext) matchingRequest(req *nexuspb.Request) *matchingservice.DispatchNexusTaskRequest {
	return &matchingservice.DispatchNexusTaskRequest{
		NamespaceId: c.namespace.ID().String(),
		TaskQueue:   &taskqueuepb.TaskQueue{Name: c.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Request:     req,
	}
}

func (c *operationContext) augmentContext(ctx context.Context, header nexus.Header) context.Context {
	ctx = metrics.AddMetricsContext(ctx)
	ctx = interceptor.AddTelemetryContext(ctx, c.metricsHandlerForInterceptors)
	ctx = interceptor.PopulateCallerInfo(
		ctx,
		func() string { return c.namespaceName },
		func() string { return c.method },
	)
	if userAgent, ok := header[headerUserAgent]; ok {
		parts := strings.Split(userAgent, clientNameVersionDelim)
		if len(parts) == 2 {
			mdIncoming, ok := metadata.FromIncomingContext(ctx)
			if !ok {
				mdIncoming = metadata.MD{}
			}
			mdIncoming.Set(headers.ClientNameHeaderName, parts[0])
			mdIncoming.Set(headers.ClientVersionHeaderName, parts[1])
			ctx = metadata.NewIncomingContext(ctx, mdIncoming)
		}
	}
	return headers.Propagate(ctx)
}

func (c *operationContext) interceptRequest(
	ctx context.Context,
	request *matchingservice.DispatchNexusTaskRequest,
	header nexus.Header,
) error {
	err := c.auth.Authorize(ctx, c.claims, &authorization.CallTarget{
		APIName:           c.apiName,
		Namespace:         c.namespaceName,
		NexusEndpointName: c.endpointName,
		Request:           request,
	})
	if err != nil {
		c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("unauthorized"))
		return commonnexus.AdaptAuthorizeError(err)
	}

	if err := c.namespaceValidationInterceptor.ValidateState(c.namespace, c.apiName); err != nil {
		c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("invalid_namespace_state"))
		return commonnexus.ConvertGRPCError(err, false)
	}

	if !c.namespace.ActiveInCluster(c.clusterMetadata.GetCurrentClusterName()) {
		if c.shouldForwardRequest(ctx, header) {
			// Handler methods should have special logic to forward requests if this method returns
			// a serviceerror.NamespaceNotActive error.
			c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("request_forwarded"))
			handler, forwardStartTime := c.redirectionInterceptor.BeforeCall(c.apiName)
			c.cleanupFunctions = append(c.cleanupFunctions, func(_ map[string]string, retErr error) {
				c.redirectionInterceptor.AfterCall(handler, forwardStartTime, c.namespace.ActiveClusterName(), retErr)
			})
			return serviceerror.NewNamespaceNotActive(
				c.namespaceName,
				c.clusterMetadata.GetCurrentClusterName(),
				c.namespace.ActiveClusterName(),
			)
		}
		c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("namespace_inactive_forwarding_disabled"))
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeUnavailable, "cluster inactive")
	}

	c.cleanupFunctions = append(c.cleanupFunctions, func(respHeaders map[string]string, retErr error) {
		if retErr != nil {
			if source, ok := respHeaders[commonnexus.FailureSourceHeaderName]; ok && source != commonnexus.FailureSourceWorker {
				c.telemetryInterceptor.HandleError(
					request,
					"",
					c.metricsHandlerForInterceptors,
					[]tag.Tag{tag.Operation(c.method), tag.WorkflowNamespace(c.namespaceName)},
					retErr,
					c.namespace.Name(),
				)
			}
		}
	})

	cleanup, err := c.namespaceConcurrencyLimitInterceptor.Allow(
		c.namespace.Name(),
		c.apiName,
		c.metricsHandlerForInterceptors,
		request,
	)
	c.cleanupFunctions = append(c.cleanupFunctions, func(map[string]string, error) { cleanup() })
	if err != nil {
		c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("namespace_concurrency_limited"))
		return commonnexus.ConvertGRPCError(err, false)
	}

	if err := c.namespaceRateLimitInterceptor.Allow(c.namespace.Name(), c.apiName, header); err != nil {
		c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("namespace_rate_limited"))
		return commonnexus.ConvertGRPCError(err, true)
	}

	if err := c.rateLimitInterceptor.Allow(c.apiName, header); err != nil {
		c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("global_rate_limited"))
		return commonnexus.ConvertGRPCError(err, true)
	}

	if err := c.clientVersionChecker.ClientSupported(ctx); err != nil {
		c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("unsupported_client"))
		converted := commonnexus.ConvertGRPCError(err, true)
		return converted
	}

	// THIS MUST BE THE LAST STEP IN interceptRequest.
	// Sanitize headers.
	if request.GetRequest().GetHeader() != nil {
		// Making a copy to ensure the original map is not modified as it might be used somewhere else.
		sanitizedHeaders := make(map[string]string, len(request.Request.Header))
		headersBlacklist := c.headersBlacklist.Get()
		for name, value := range request.Request.Header {
			if !headersBlacklist.MatchString(name) {
				sanitizedHeaders[name] = value
			}
		}
		request.Request.Header = sanitizedHeaders
	}

	// DO NOT ADD ANY STEPS HERE. ALL STEPS MUST BE BEFORE HEADERS SANITIZATION.

	return nil
}

// Combines logic from RedirectionInterceptor.redirectionAllowed and some from
// SelectedAPIsForwardingRedirectionPolicy.getTargetClusterAndIsNamespaceNotActiveAutoForwarding so all
// redirection conditions can be checked at once. If either of those methods are updated, this should
// be kept in sync.
func (c *operationContext) shouldForwardRequest(ctx context.Context, header nexus.Header) bool {
	redirectHeader := header.Get(interceptor.DCRedirectionContextHeaderName)
	redirectAllowed, err := strconv.ParseBool(redirectHeader)
	if err != nil {
		redirectAllowed = true
	}
	return redirectAllowed &&
		c.redirectionInterceptor.RedirectionAllowed(ctx) &&
		c.namespace.IsGlobalNamespace() &&
		len(c.namespace.ClusterNames()) > 1 &&
		c.forwardingEnabledForNamespace(c.namespaceName)
}

// enrichNexusOperationMetrics enhances metrics with additional Nexus operation context based on configuration.
func (c *operationContext) enrichNexusOperationMetrics(service, operation string, requestHeader nexus.Header) {
	conf := c.metricTagConfig.Get()
	if conf == nil {
		return
	}

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

// Key to extract a nexusContext object from a context.Context.
type nexusContextKey struct{}

// A Nexus Handler implementation.
// Dispatches Nexus requests as Nexus tasks to workers via matching.
type nexusHandler struct {
	nexus.UnimplementedHandler
	logger                        log.Logger
	metricsHandler                metrics.Handler
	clusterMetadata               cluster.Metadata
	namespaceRegistry             namespace.Registry
	matchingClient                matchingservice.MatchingServiceClient
	auth                          *authorization.Interceptor
	telemetryInterceptor          *interceptor.TelemetryInterceptor
	redirectionInterceptor        *interceptor.Redirection
	forwardingEnabledForNamespace dynamicconfig.BoolPropertyFnWithNamespaceFilter
	forwardingClients             *cluster.FrontendHTTPClientCache
	payloadSizeLimit              dynamicconfig.IntPropertyFnWithNamespaceFilter
	headersBlacklist              *dynamicconfig.GlobalCachedTypedValue[*regexp.Regexp]
	metricTagConfig               *dynamicconfig.GlobalCachedTypedValue[*nexusoperations.NexusMetricTagConfig]
	httpTraceProvider             commonnexus.HTTPClientTraceProvider
}

// Extracts a nexusContext from the given ctx and returns an operationContext with tagged metrics and logging.
// Resolves the context's namespace name to a registered Namespace.
func (h *nexusHandler) getOperationContext(ctx context.Context, method string) (*operationContext, error) {
	nc, ok := ctx.Value(nexusContextKey{}).(*nexusContext)
	if !ok {
		return nil, errors.New("no nexus context set on context")
	}
	oc := operationContext{
		nexusContext:                  nc,
		method:                        method,
		clusterMetadata:               h.clusterMetadata,
		clientVersionChecker:          headers.NewDefaultVersionChecker(),
		auth:                          h.auth,
		telemetryInterceptor:          h.telemetryInterceptor,
		redirectionInterceptor:        h.redirectionInterceptor,
		forwardingEnabledForNamespace: h.forwardingEnabledForNamespace,
		headersBlacklist:              h.headersBlacklist,
		metricTagConfig:               h.metricTagConfig,
		cleanupFunctions:              make([]func(map[string]string, error), 0),
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

		var nfe *serviceerror.NamespaceNotFound
		if errors.As(err, &nfe) {
			return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeNotFound, "namespace not found: %q", nc.namespaceName)
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
	defer oc.capturePanicAndRecordMetrics(&ctx, &retErr)

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
	})

	if err := oc.interceptRequest(ctx, request, options.Header); err != nil {
		var notActiveErr *serviceerror.NamespaceNotActive
		if errors.As(err, &notActiveErr) {
			return h.forwardStartOperation(ctx, service, operation, input, options, oc)
		}
		return nil, err
	}

	// Transform nexus Content to temporal Payload with common/nexus PayloadSerializer.
	if err = input.Consume(&startOperationRequest.Payload); err != nil {
		oc.logger.Warn("invalid input", tag.Error(err))
		return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid input")
	}
	if startOperationRequest.Payload.Size() > h.payloadSizeLimit(oc.namespaceName) {
		oc.logger.Error("payload size exceeds error limit for Nexus StartOperation request", tag.Operation(operation), tag.WorkflowNamespace(oc.namespaceName))
		return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "input exceeds size limit")
	}

	// Dispatch the request to be sync matched with a worker polling on the nexusContext taskQueue.
	// matchingClient sets a context timeout of 60 seconds for this request, this should be enough for any Nexus
	// RPC.
	response, err := h.matchingClient.DispatchNexusTask(ctx, request)
	if err != nil {
		if common.IsContextDeadlineExceededErr(err) {
			oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("handler_timeout"))
			return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeUpstreamTimeout, "upstream timeout")
		}
		return nil, commonnexus.ConvertGRPCError(err, false)
	}
	// Convert to standard Nexus SDK response.
	switch t := response.GetOutcome().(type) {
	case *matchingservice.DispatchNexusTaskResponse_HandlerError:
		oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("handler_error"))

		oc.nexusContext.setFailureSource(commonnexus.FailureSourceWorker)

		err := h.convertOutcomeToNexusHandlerError(t)
		return nil, err

	case *matchingservice.DispatchNexusTaskResponse_Response:
		switch t := t.Response.GetStartOperation().GetVariant().(type) {
		case *nexuspb.StartOperationResponse_SyncSuccess:
			oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("sync_success"))
			return &nexus.HandlerStartOperationResultSync[any]{
				Value: t.SyncSuccess.GetPayload(),
				Links: parseLinks(t.SyncSuccess.GetLinks(), oc.logger),
			}, nil

		case *nexuspb.StartOperationResponse_AsyncSuccess:
			oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("async_success"))

			token := t.AsyncSuccess.GetOperationToken()
			if token == "" {
				token = t.AsyncSuccess.GetOperationId()
			}
			return &nexus.HandlerStartOperationResultAsync{
				OperationToken: token,
				Links:          parseLinks(t.AsyncSuccess.GetLinks(), oc.logger),
			}, nil

		case *nexuspb.StartOperationResponse_OperationError:
			oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("operation_error"))

			oc.nexusContext.setFailureSource(commonnexus.FailureSourceWorker)

			err := &nexus.OperationError{
				State: nexus.OperationState(t.OperationError.GetOperationState()),
				Cause: &nexus.FailureError{
					Failure: commonnexus.ProtoFailureToNexusFailure(t.OperationError.GetFailure()),
				},
			}
			return nil, err
		}
	}
	// This is the worker's fault.
	oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("handler_error"))

	oc.nexusContext.setFailureSource(commonnexus.FailureSourceWorker)

	return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "empty outcome")
}

func parseLinks(links []*nexuspb.Link, logger log.Logger) []nexus.Link {
	var nexusLinks []nexus.Link
	for _, link := range links {
		linkURL, err := url.Parse(link.Url)
		if err != nil {
			// TODO(rodrigozhou): links are non-essential for the execution of the workflow,
			// so ignoring the error for now; we will revisit how to handle these errors later.
			logger.Error(fmt.Sprintf("failed to parse link url: %s", link.Url), tag.Error(err))
			continue
		}
		nexusLinks = append(nexusLinks, nexus.Link{
			URL:  linkURL,
			Type: link.GetType(),
		})
	}
	return nexusLinks
}

// forwardStartOperation forwards the StartOperation request to the active cluster using an HTTP request.
// Inputs and response values are passed as Reader objects to avoid reading bodies and bypass serialization.
func (h *nexusHandler) forwardStartOperation(
	ctx context.Context,
	service string,
	operation string,
	input *nexus.LazyValue,
	options nexus.StartOperationOptions,
	oc *operationContext,
) (nexus.HandlerStartOperationResult[any], error) {
	options.Header[interceptor.DCRedirectionApiHeaderName] = "true"

	client, err := h.nexusClientForActiveCluster(oc, service)
	if err != nil {
		return nil, err
	}

	if h.httpTraceProvider != nil {
		traceLogger := log.With(h.logger,
			tag.Operation(oc.method),
			tag.WorkflowNamespace(oc.namespaceName),
			tag.RequestID(options.RequestID),
			tag.NexusOperation(operation),
			tag.Endpoint(oc.endpointName),
			tag.AttemptStart(time.Now().UTC()),
			tag.SourceCluster(h.clusterMetadata.GetCurrentClusterName()),
			tag.TargetCluster(oc.namespace.ActiveClusterName()),
		)
		if trace := h.httpTraceProvider.NewForwardingTrace(traceLogger); trace != nil {
			ctx = httptrace.WithClientTrace(ctx, trace)
		}
	}

	resp, err := client.StartOperation(ctx, operation, input.Reader, options)
	if err != nil {
		oc.logger.Error("received error from remote cluster for forwarded Nexus start operation request.", tag.Error(err))
		oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("forwarded_request_error"))
		return nil, err
	}

	if resp.Successful != nil {
		return &nexus.HandlerStartOperationResultSync[any]{Value: resp.Successful.Reader}, nil
	}
	// If Nexus client did not return an error, one of Successful or Pending will always be set.
	return &nexus.HandlerStartOperationResultAsync{OperationToken: resp.Pending.Token}, nil
}

func (h *nexusHandler) CancelOperation(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) (retErr error) {
	oc, err := h.getOperationContext(ctx, "CancelNexusOperation")
	if err != nil {
		return err
	}
	ctx = oc.augmentContext(ctx, options.Header)
	oc.enrichNexusOperationMetrics(service, operation, options.Header)
	defer oc.capturePanicAndRecordMetrics(&ctx, &retErr)

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
	})
	if err := oc.interceptRequest(ctx, request, options.Header); err != nil {
		var notActiveErr *serviceerror.NamespaceNotActive
		if errors.As(err, &notActiveErr) {
			return h.forwardCancelOperation(ctx, service, operation, token, options, oc)
		}
		return err
	}

	// Dispatch the request to be sync matched with a worker polling on the nexusContext taskQueue.
	// matchingClient sets a context timeout of 60 seconds for this request, this should be enough for any Nexus
	// RPC.
	response, err := h.matchingClient.DispatchNexusTask(ctx, request)
	if err != nil {
		if common.IsContextDeadlineExceededErr(err) {
			oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("handler_timeout"))
			return nexus.HandlerErrorf(nexus.HandlerErrorTypeUpstreamTimeout, "upstream timeout")
		}
		return commonnexus.ConvertGRPCError(err, false)
	}
	// Convert to standard Nexus SDK response.
	switch t := response.GetOutcome().(type) {
	case *matchingservice.DispatchNexusTaskResponse_HandlerError:
		oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("handler_error"))

		oc.nexusContext.setFailureSource(commonnexus.FailureSourceWorker)

		err := h.convertOutcomeToNexusHandlerError(t)
		return err
	case *matchingservice.DispatchNexusTaskResponse_Response:
		oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("success"))
		return nil
	}
	// This is the worker's fault.
	oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("handler_error"))

	oc.nexusContext.setFailureSource(commonnexus.FailureSourceWorker)

	return nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "empty outcome")
}

func (h *nexusHandler) forwardCancelOperation(
	ctx context.Context,
	service string,
	operation string,
	id string,
	options nexus.CancelOperationOptions,
	oc *operationContext,
) error {
	options.Header[interceptor.DCRedirectionApiHeaderName] = "true"

	client, err := h.nexusClientForActiveCluster(oc, service)
	if err != nil {
		return err
	}

	handle, err := client.NewHandle(operation, id)
	if err != nil {
		oc.logger.Warn("invalid Nexus cancel operation.", tag.Error(err))
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid operation")
	}

	if h.httpTraceProvider != nil {
		traceLogger := log.With(h.logger,
			tag.Operation(oc.method),
			tag.WorkflowNamespace(oc.namespaceName),
			tag.NexusOperation(operation),
			tag.Endpoint(oc.endpointName),
			tag.AttemptStart(time.Now().UTC()),
			tag.SourceCluster(h.clusterMetadata.GetCurrentClusterName()),
			tag.TargetCluster(oc.namespace.ActiveClusterName()),
		)
		if trace := h.httpTraceProvider.NewForwardingTrace(traceLogger); trace != nil {
			ctx = httptrace.WithClientTrace(ctx, trace)
		}
	}

	err = handle.Cancel(ctx, options)
	if err != nil {
		oc.logger.Error("received error from remote cluster for forwarded Nexus cancel operation request.", tag.Error(err))
		oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("forwarded_request_error"))
		return err
	}

	return nil
}

func (h *nexusHandler) nexusClientForActiveCluster(oc *operationContext, service string) (*nexus.HTTPClient, error) {
	httpClient, err := h.forwardingClients.Get(oc.namespace.ActiveClusterName())
	if err != nil {
		oc.logger.Error("failed to forward Nexus request. error creating HTTP client", tag.Error(err), tag.SourceCluster(oc.namespace.ActiveClusterName()), tag.TargetCluster(oc.namespace.ActiveClusterName()))
		oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("request_forwarding_failed"))
		return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "request forwarding failed")
	}

	wrappedHttpDo := func(req *http.Request) (*http.Response, error) {
		response, err := httpClient.Do(req)
		if err != nil {
			return nil, err
		}

		if failureSource := response.Header.Get(commonnexus.FailureSourceHeaderName); failureSource != "" {
			oc.nexusContext.setFailureSource(failureSource)
		}

		return response, nil
	}

	baseURL, err := url.JoinPath(
		httpClient.BaseURL(),
		commonnexus.RouteDispatchNexusTaskByNamespaceAndTaskQueue.Path(commonnexus.NamespaceAndTaskQueue{
			Namespace: oc.namespaceName,
			TaskQueue: oc.taskQueue,
		}))
	if err != nil {
		oc.logger.Error(fmt.Sprintf("failed to forward Nexus request. error constructing ServiceBaseURL. baseURL=%s namespace=%s task_queue=%s", httpClient.BaseURL(), oc.namespaceName, oc.taskQueue), tag.Error(err))
		oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("request_forwarding_failed"))
		return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "request forwarding failed")
	}

	return nexus.NewHTTPClient(nexus.HTTPClientOptions{
		HTTPCaller: wrappedHttpDo,
		BaseURL:    baseURL,
		Service:    service,
	})
}

func (h *nexusHandler) convertOutcomeToNexusHandlerError(resp *matchingservice.DispatchNexusTaskResponse_HandlerError) *nexus.HandlerError {
	var retryBehavior nexus.HandlerErrorRetryBehavior
	// nolint:exhaustive // unspecified is the default
	switch resp.HandlerError.RetryBehavior {
	case enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE:
		retryBehavior = nexus.HandlerErrorRetryBehaviorRetryable
	case enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE:
		retryBehavior = nexus.HandlerErrorRetryBehaviorNonRetryable
	}
	handlerError := &nexus.HandlerError{
		Type: nexus.HandlerErrorType(resp.HandlerError.GetErrorType()),
		Cause: &nexus.FailureError{
			Failure: commonnexus.ProtoFailureToNexusFailure(resp.HandlerError.GetFailure()),
		},
		RetryBehavior: retryBehavior,
	}

	switch handlerError.Type {
	case nexus.HandlerErrorTypeUpstreamTimeout,
		nexus.HandlerErrorTypeUnauthenticated,
		nexus.HandlerErrorTypeUnauthorized,
		nexus.HandlerErrorTypeBadRequest,
		nexus.HandlerErrorTypeResourceExhausted,
		nexus.HandlerErrorTypeNotFound,
		nexus.HandlerErrorTypeNotImplemented,
		nexus.HandlerErrorTypeUnavailable,
		nexus.HandlerErrorTypeInternal:
		return handlerError
	default:
		h.logger.Warn("received unknown or unset Nexus handler error type", tag.Value(handlerError.Type))
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
	}
}

func (nc *nexusContext) setFailureSource(source string) {
	nc.responseHeadersMutex.Lock()
	defer nc.responseHeadersMutex.Unlock()
	nc.responseHeaders[commonnexus.FailureSourceHeaderName] = source
}
