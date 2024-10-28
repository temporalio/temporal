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
	"net/url"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/taskqueue/v1"
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
	namespaceRateLimitInterceptor        *interceptor.NamespaceRateLimitInterceptor
	namespaceConcurrencyLimitInterceptor *interceptor.ConcurrentRequestLimitInterceptor
	rateLimitInterceptor                 *interceptor.RateLimitInterceptor
	responseHeaders                      map[string]string
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
	cleanupFunctions              []func(map[string]string, error)
}

// Panic handler and metrics recording function.
// Used as a deferred statement in Nexus handler methods.
func (c *operationContext) capturePanicAndRecordMetrics(ctxPtr *context.Context, errPtr *error) {
	recovered := recover() //nolint:revive
	if recovered != nil {
		err, ok := recovered.(error)
		if !ok {
			err = fmt.Errorf("panic: %v", recovered) //nolint:goerr113
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
		TaskQueue:   &taskqueue.TaskQueue{Name: c.taskQueue, Kind: enums.TASK_QUEUE_KIND_NORMAL},
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
			return metadata.NewIncomingContext(ctx, metadata.New(map[string]string{
				headers.ClientNameHeaderName:    parts[0],
				headers.ClientVersionHeaderName: parts[1],
			}))
		}
	}
	return ctx
}

func (c *operationContext) interceptRequest(ctx context.Context, request *matchingservice.DispatchNexusTaskRequest, header nexus.Header) error {
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
			// Handler methods should have special logic to forward requests if this method returns a serviceerror.NamespaceNotActive error.
			c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("request_forwarded"))
			handler, forwardStartTime := c.redirectionInterceptor.BeforeCall(c.apiName)
			c.cleanupFunctions = append(c.cleanupFunctions, func(_ map[string]string, retErr error) {
				c.redirectionInterceptor.AfterCall(handler, forwardStartTime, c.namespace.ActiveClusterName(), retErr)
			})
			return serviceerror.NewNamespaceNotActive(c.namespaceName, c.clusterMetadata.GetCurrentClusterName(), c.namespace.ActiveClusterName())
		}
		c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("namespace_inactive_forwarding_disabled"))
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeUnavailable, "cluster inactive")
	}

	c.cleanupFunctions = append(c.cleanupFunctions, func(respHeaders map[string]string, retErr error) {
		if retErr != nil {
			if source, ok := respHeaders[nexusFailureSourceHeaderName]; ok && source != failureSourceWorker {
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

	cleanup, err := c.namespaceConcurrencyLimitInterceptor.Allow(c.namespace.Name(), c.apiName, c.metricsHandlerForInterceptors, request)
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
}

// Extracts a nexusContext from the given ctx and returns an operationContext with tagged metrics and logging.
// Resolves the context's namespace name to a registered Namespace.
func (h *nexusHandler) getOperationContext(ctx context.Context, method string) (*operationContext, error) {
	nc, ok := ctx.Value(nexusContextKey{}).(*nexusContext)
	if !ok {
		return nil, errors.New("no nexus context set on context") //nolint:goerr113
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
			return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeDownstreamTimeout, "downstream timeout")
		}
		return nil, commonnexus.ConvertGRPCError(err, false)
	}
	// Convert to standard Nexus SDK response.
	switch t := response.GetOutcome().(type) {
	case *matchingservice.DispatchNexusTaskResponse_HandlerError:
		oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("handler_error"))
		oc.responseHeaders[nexusFailureSourceHeaderName] = failureSourceWorker
		return nil, h.convertOutcomeToNexusHandlerError(t)

	case *matchingservice.DispatchNexusTaskResponse_Response:
		switch t := t.Response.GetStartOperation().GetVariant().(type) {
		case *nexuspb.StartOperationResponse_SyncSuccess:
			oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("sync_success"))
			return &nexus.HandlerStartOperationResultSync[any]{
				Value: t.SyncSuccess.GetPayload(),
			}, nil

		case *nexuspb.StartOperationResponse_AsyncSuccess:
			oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("async_success"))
			var nexusLinks []nexus.Link
			for _, link := range t.AsyncSuccess.GetLinks() {
				linkURL, err := url.Parse(link.Url)
				if err != nil {
					// TODO(rodrigozhou): links are non-essential for the execution of the workflow,
					// so ignoring the error for now; we will revisit how to handle these errors later.
					oc.logger.Error(fmt.Sprintf("failed to parse link url: %s", link.Url), tag.Error(err))
					continue
				}
				nexusLinks = append(nexusLinks, nexus.Link{
					URL:  linkURL,
					Type: link.GetType(),
				})
			}
			return &nexus.HandlerStartOperationResultAsync{
				OperationID: t.AsyncSuccess.GetOperationId(),
				Links:       nexusLinks,
			}, nil

		case *nexuspb.StartOperationResponse_OperationError:
			oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("operation_error"))
			oc.responseHeaders[nexusFailureSourceHeaderName] = failureSourceWorker
			return nil, &nexus.UnsuccessfulOperationError{
				State:   nexus.OperationState(t.OperationError.GetOperationState()),
				Failure: *commonnexus.ProtoFailureToNexusFailure(t.OperationError.GetFailure()),
			}
		}
	}
	// This is the worker's fault.
	oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("handler_error"))
	oc.responseHeaders[nexusFailureSourceHeaderName] = failureSourceWorker
	return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "empty outcome")
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

	resp, err := client.StartOperation(ctx, operation, input.Reader, options)
	if err != nil {
		oc.logger.Error("received error from remote cluster for forwarded Nexus start operation request.", tag.Error(err))
		oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("forwarded_request_error"))
		failureSource, handlerErr := handlerErrorFromClientError(err, h.logger)
		oc.responseHeaders[nexusFailureSourceHeaderName] = failureSource
		return nil, handlerErr
	}

	if resp.Successful != nil {
		return &nexus.HandlerStartOperationResultSync[any]{Value: resp.Successful.Reader}, nil
	}
	// If Nexus client did not return an error, one of Successful or Pending will always be set.
	return &nexus.HandlerStartOperationResultAsync{OperationID: resp.Pending.ID}, nil
}

func (h *nexusHandler) CancelOperation(ctx context.Context, service, operation, id string, options nexus.CancelOperationOptions) (retErr error) {
	oc, err := h.getOperationContext(ctx, "CancelNexusOperation")
	if err != nil {
		return err
	}
	defer oc.capturePanicAndRecordMetrics(&ctx, &retErr)

	request := oc.matchingRequest(&nexuspb.Request{
		Header:        options.Header,
		ScheduledTime: timestamppb.New(oc.requestStartTime),
		Variant: &nexuspb.Request_CancelOperation{
			CancelOperation: &nexuspb.CancelOperationRequest{
				Service:     service,
				Operation:   operation,
				OperationId: id,
			},
		},
	})
	if err := oc.interceptRequest(ctx, request, options.Header); err != nil {
		var notActiveErr *serviceerror.NamespaceNotActive
		if errors.As(err, &notActiveErr) {
			return h.forwardCancelOperation(ctx, service, operation, id, options, oc)
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
			return nexus.HandlerErrorf(nexus.HandlerErrorTypeDownstreamTimeout, "downstream timeout")
		}
		return commonnexus.ConvertGRPCError(err, false)
	}
	// Convert to standard Nexus SDK response.
	switch t := response.GetOutcome().(type) {
	case *matchingservice.DispatchNexusTaskResponse_HandlerError:
		oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("handler_error"))
		oc.responseHeaders[nexusFailureSourceHeaderName] = failureSourceWorker
		return h.convertOutcomeToNexusHandlerError(t)
	case *matchingservice.DispatchNexusTaskResponse_Response:
		oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("success"))
		return nil
	}
	// This is the worker's fault.
	oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("handler_error"))
	oc.responseHeaders[nexusFailureSourceHeaderName] = failureSourceWorker
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

	err = handle.Cancel(ctx, options)
	if err != nil {
		oc.logger.Error("received error from remote cluster for forwarded Nexus cancel operation request.", tag.Error(err))
		oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("forwarded_request_error"))
		failureSource, handlerErr := handlerErrorFromClientError(err, h.logger)
		oc.responseHeaders[nexusFailureSourceHeaderName] = failureSource
		return handlerErr
	}

	return nil
}

func (h *nexusHandler) nexusClientForActiveCluster(oc *operationContext, service string) (*nexus.Client, error) {
	httpClient, err := h.forwardingClients.Get(oc.namespace.ActiveClusterName())
	if err != nil {
		oc.logger.Error("failed to forward Nexus request. error creating HTTP client", tag.Error(err), tag.SourceCluster(oc.namespace.ActiveClusterName()), tag.TargetCluster(oc.namespace.ActiveClusterName()))
		oc.metricsHandler = oc.metricsHandler.WithTags(metrics.OutcomeTag("request_forwarding_failed"))
		return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "request forwarding failed")
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

	return nexus.NewClient(nexus.ClientOptions{
		HTTPCaller: httpClient.Do,
		BaseURL:    baseURL,
		Service:    service,
	})
}

func (h *nexusHandler) convertOutcomeToNexusHandlerError(resp *matchingservice.DispatchNexusTaskResponse_HandlerError) *nexus.HandlerError {
	handlerError := &nexus.HandlerError{
		Type:    nexus.HandlerErrorType(resp.HandlerError.GetErrorType()),
		Failure: commonnexus.ProtoFailureToNexusFailure(resp.HandlerError.GetFailure()),
	}

	switch handlerError.Type {
	case nexus.HandlerErrorTypeDownstreamTimeout,
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

func handlerErrorFromClientError(err error, logger log.Logger) (string, error) {
	var unexpectedRespErr *nexus.UnexpectedResponseError
	if errors.As(err, &unexpectedRespErr) {
		failure := unexpectedRespErr.Failure
		if unexpectedRespErr.Failure == nil {
			failure = &nexus.Failure{
				Message: unexpectedRespErr.Error(),
			}
		}

		handlerErr := &nexus.HandlerError{
			Type:    commonnexus.HandlerErrorTypeFromHTTPStatus(unexpectedRespErr.Response.StatusCode),
			Failure: failure,
		}
		if handlerErr.Type == nexus.HandlerErrorTypeInternal && unexpectedRespErr.Response.StatusCode != http.StatusInternalServerError {
			logger.Warn("received unknown status code on Nexus client unexpected response error", tag.Value(unexpectedRespErr.Response.StatusCode))
			handlerErr.Failure.Message = "internal error"
		}

		return unexpectedRespErr.Response.Header.Get(nexusFailureSourceHeaderName), handlerErr
	}

	// Let the nexus SDK handle this for us (log and convert to an internal error).
	return "", err
}
