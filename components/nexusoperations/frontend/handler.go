// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/service/frontend/configs"
	"go.uber.org/fx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var apiName = configs.CompleteNexusOperation

const (
	methodNameForMetrics = "CompleteNexusOperation"
	// user-agent header contains Nexus SDK client info in the form <sdk-name>/v<sdk-version>
	headerUserAgent        = "user-agent"
	clientNameVersionDelim = "/v"
)

type Config struct {
	Enabled                       dynamicconfig.BoolPropertyFn
	PayloadSizeLimit              dynamicconfig.IntPropertyFnWithNamespaceFilter
	ForwardingEnabledForNamespace dynamicconfig.BoolPropertyFnWithNamespaceFilter
}

type HandlerOptions struct {
	fx.In

	ClusterMetadata                      cluster.Metadata
	NamespaceRegistry                    namespace.Registry
	Logger                               log.Logger
	MetricsHandler                       metrics.Handler
	Config                               *Config
	CallbackTokenGenerator               *commonnexus.CallbackTokenGenerator
	HistoryClient                        resource.HistoryClient
	TelemetryInterceptor                 *interceptor.TelemetryInterceptor
	NamespaceValidationInterceptor       *interceptor.NamespaceValidatorInterceptor
	NamespaceRateLimitInterceptor        *interceptor.NamespaceRateLimitInterceptor
	NamespaceConcurrencyLimitInterceptor *interceptor.ConcurrentRequestLimitInterceptor
	RateLimitInterceptor                 *interceptor.RateLimitInterceptor
	AuthInterceptor                      *authorization.Interceptor
	RedirectionInterceptor               *interceptor.Redirection
	ForwardingClients                    *cluster.FrontendHTTPClientCache
}

type completionHandler struct {
	HandlerOptions
	clientVersionChecker    headers.VersionChecker
	preProcessErrorsCounter metrics.CounterIface
}

// CompleteOperation implements nexus.CompletionHandler.
func (h *completionHandler) CompleteOperation(ctx context.Context, r *nexus.CompletionRequest) (retErr error) {
	startTime := time.Now()
	if !h.Config.Enabled() {
		h.preProcessErrorsCounter.Record(1)
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeNotFound, "Nexus APIs are disabled")
	}
	nsNameEscaped := commonnexus.RouteCompletionCallback.Deserialize(mux.Vars(r.HTTPRequest))
	nsName, err := url.PathUnescape(nsNameEscaped)
	if err != nil {
		h.Logger.Error("failed to extract namespace from request", tag.Error(err))
		h.preProcessErrorsCounter.Record(1)
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid URL")
	}
	ns, err := h.NamespaceRegistry.GetNamespace(namespace.Name(nsName))
	if err != nil {
		h.Logger.Error("failed to get namespace for nexus completion request", tag.WorkflowNamespace(nsName), tag.Error(err))
		h.preProcessErrorsCounter.Record(1)
		var nfe *serviceerror.NamespaceNotFound
		if errors.As(err, &nfe) {
			return nexus.HandlerErrorf(nexus.HandlerErrorTypeNotFound, "namespace %q not found", nsName)
		}
		return commonnexus.ConvertGRPCError(err, false)
	}

	rCtx := &requestContext{
		completionHandler: h,
		namespace:         ns,
		logger:            log.With(h.Logger, tag.WorkflowNamespace(ns.Name().String())),
		metricsHandler:    h.MetricsHandler.WithTags(metrics.NamespaceTag(nsName)),
		metricsHandlerForInterceptors: h.MetricsHandler.WithTags(
			metrics.OperationTag(methodNameForMetrics),
			metrics.NamespaceTag(nsName),
		),
		requestStartTime: startTime,
	}
	ctx = rCtx.augmentContext(ctx, r.HTTPRequest.Header)
	defer rCtx.capturePanicAndRecordMetrics(&ctx, &retErr)

	if err := rCtx.interceptRequest(ctx, r); err != nil {
		var notActiveErr *serviceerror.NamespaceNotActive
		if errors.As(err, &notActiveErr) {
			return h.forwardCompleteOperation(ctx, r, rCtx)
		}
		return err
	}

	token, err := commonnexus.DecodeCallbackToken(r.HTTPRequest.Header.Get(commonnexus.CallbackTokenHeader))
	if err != nil {
		h.Logger.Error("failed to decode callback token", tag.WorkflowNamespace(ns.Name().String()), tag.Error(err))
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid callback token")
	}

	completion, err := h.CallbackTokenGenerator.DecodeCompletion(token)
	if err != nil {
		h.Logger.Error("failed to decode completion from token", tag.WorkflowNamespace(ns.Name().String()), tag.Error(err))
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid callback token")
	}

	logger := log.With(
		h.Logger,
		tag.WorkflowNamespace(ns.Name().String()),
		tag.WorkflowID(completion.GetWorkflowId()),
		tag.WorkflowRunID(completion.GetRunId()),
	)
	if completion.GetNamespaceId() != ns.ID().String() {
		logger.Error(
			"namespace ID in token doesn't match the token",
			tag.WorkflowNamespaceID(ns.ID().String()),
			tag.Error(err),
			tag.NewStringTag("completion-namespace-id", completion.GetNamespaceId()),
		)
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid callback token")
	}
	hr := &historyservice.CompleteNexusOperationRequest{
		Completion: completion,
		State:      string(r.State),
	}
	switch r.State { // nolint:exhaustive
	case nexus.OperationStateFailed, nexus.OperationStateCanceled:
		hr.Outcome = &historyservice.CompleteNexusOperationRequest_Failure{
			Failure: commonnexus.NexusFailureToProtoFailure(r.Failure),
		}
	case nexus.OperationStateSucceeded:
		var result *commonpb.Payload
		if err := r.Result.Consume(&result); err != nil {
			logger.Error("cannot deserialize payload from completion result", tag.Error(err))
			return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid result content")
		}
		if result.Size() > h.Config.PayloadSizeLimit(ns.Name().String()) {
			logger.Error("payload size exceeds error limit for Nexus CompleteOperation request", tag.WorkflowNamespace(ns.Name().String()))
			return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "result exceeds size limit")
		}
		hr.Outcome = &historyservice.CompleteNexusOperationRequest_Success{
			Success: result,
		}
	default:
		// The Nexus SDK ensures this never happens but just in case...
		logger.Error("invalid operation state in completion request", tag.NewStringTag("state", string(r.State)), tag.Error(err))
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid completion state")
	}
	_, err = h.HistoryClient.CompleteNexusOperation(ctx, hr)
	if err != nil {
		logger.Error("failed to process nexus completion request", tag.Error(err))
		var namespaceInactiveErr *serviceerror.NamespaceNotActive
		if errors.As(err, &namespaceInactiveErr) {
			return nexus.HandlerErrorf(nexus.HandlerErrorTypeUnavailable, "cluster inactive")
		}
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			return nexus.HandlerErrorf(nexus.HandlerErrorTypeNotFound, "operation not found")
		}
		return commonnexus.ConvertGRPCError(err, false)
	}
	return nil
}

func (h *completionHandler) forwardCompleteOperation(ctx context.Context, r *nexus.CompletionRequest, rCtx *requestContext) error {
	client, err := h.ForwardingClients.Get(rCtx.namespace.ActiveClusterName())
	if err != nil {
		h.Logger.Error("unable to get HTTP client for forward request", tag.Operation(apiName), tag.WorkflowNamespace(rCtx.namespace.Name().String()), tag.Error(err), tag.SourceCluster(h.ClusterMetadata.GetCurrentClusterName()), tag.TargetCluster(rCtx.namespace.ActiveClusterName()))
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
	}

	forwardURL, err := url.JoinPath(client.BaseURL(), commonnexus.RouteCompletionCallback.Path(rCtx.namespace.Name().String()))
	if err != nil {
		h.Logger.Error("failed to construct forwarding request URL", tag.Operation(apiName), tag.WorkflowNamespace(rCtx.namespace.Name().String()), tag.Error(err), tag.TargetCluster(rCtx.namespace.ActiveClusterName()))
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
	}

	forwardReq, err := http.NewRequestWithContext(ctx, r.HTTPRequest.Method, forwardURL, r.HTTPRequest.Body)
	if err != nil {
		h.Logger.Error("failed to construct forwarding HTTP request", tag.Operation(apiName), tag.WorkflowNamespace(rCtx.namespace.Name().String()), tag.Error(err))
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
	}

	if r.HTTPRequest.Header != nil {
		forwardReq.Header = r.HTTPRequest.Header.Clone()
	}
	forwardReq.Header.Set(interceptor.DCRedirectionApiHeaderName, "true")

	resp, err := client.Do(forwardReq)
	if err != nil {
		h.Logger.Error("received error from HTTP client when forwarding request", tag.Operation(apiName), tag.WorkflowNamespace(rCtx.namespace.Name().String()), tag.Error(err))
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
	}

	body, err := readAndReplaceBody(resp)
	if err != nil {
		h.Logger.Error("unable to read HTTP response for forwarded request", tag.Operation(apiName), tag.WorkflowNamespace(rCtx.namespace.Name().String()), tag.Error(err))
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
	}

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	if !isMediaTypeJSON(resp.Header.Get("Content-Type")) {
		h.Logger.Error("received invalid content-type header for non-OK HTTP response to forwarded request", tag.Operation(apiName), tag.WorkflowNamespace(rCtx.namespace.Name().String()), tag.Value(resp.Header.Get("Content-Type")))
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
	}

	var failure nexus.Failure
	err = json.Unmarshal(body, &failure)
	if err != nil {
		h.Logger.Error("failed to deserialize Nexus Failure from HTTP response to forwarded request", tag.Operation(apiName), tag.WorkflowNamespace(rCtx.namespace.Name().String()), tag.Error(err))
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
	}

	handlerErr := &nexus.HandlerError{
		Type:    commonnexus.HandlerErrorTypeFromHTTPStatus(resp.StatusCode),
		Failure: &failure,
	}

	if handlerErr.Type == nexus.HandlerErrorTypeInternal && resp.StatusCode != http.StatusInternalServerError {
		h.Logger.Warn("received unknown status code on Nexus client unexpected response error", tag.Value(resp.StatusCode))
		handlerErr.Failure.Message = "internal error"
	}

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

type requestContext struct {
	*completionHandler
	logger                        log.Logger
	metricsHandler                metrics.Handler
	metricsHandlerForInterceptors metrics.Handler
	namespace                     *namespace.Namespace
	cleanupFunctions              []func(error)
	requestStartTime              time.Time
	outcomeTag                    metrics.Tag
	forwarded                     bool
}

func (c *requestContext) augmentContext(ctx context.Context, header http.Header) context.Context {
	ctx = metrics.AddMetricsContext(ctx)
	ctx = interceptor.AddTelemetryContext(ctx, c.metricsHandlerForInterceptors)
	ctx = interceptor.PopulateCallerInfo(
		ctx,
		func() string { return c.namespace.Name().String() },
		func() string { return methodNameForMetrics },
	)
	if userAgent := header.Get(http.CanonicalHeaderKey(headerUserAgent)); userAgent != "" {
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

func (c *requestContext) capturePanicAndRecordMetrics(ctxPtr *context.Context, errPtr *error) {
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
	if *errPtr == nil {
		if c.forwarded {
			c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("request_forwarded"))
		} else {
			c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("success"))
		}
	} else if c.outcomeTag != nil {
		c.metricsHandler = c.metricsHandler.WithTags(c.outcomeTag)
	} else {
		var he *nexus.HandlerError
		if errors.As(*errPtr, &he) {
			c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("error_" + strings.ToLower(string(he.Type))))
		} else {
			c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("error_internal"))
		}
	}

	// Record Nexus-specific metrics
	c.metricsHandler.Counter(metrics.NexusCompletionRequests.Name()).Record(1)
	c.metricsHandler.Histogram(metrics.NexusCompletionLatencyHistogram.Name(), metrics.Milliseconds).Record(time.Since(c.requestStartTime).Milliseconds())

	// Record general telemetry metrics
	metrics.ServiceRequests.With(c.metricsHandlerForInterceptors).Record(1)
	c.TelemetryInterceptor.RecordLatencyMetrics(*ctxPtr, c.requestStartTime, c.metricsHandlerForInterceptors)

	for _, fn := range c.cleanupFunctions {
		fn(*errPtr)
	}
}

// TODO(bergundy): Merge this with the interceptRequest method in nexus_handler.go.
func (c *requestContext) interceptRequest(ctx context.Context, request *nexus.CompletionRequest) error {
	var tlsInfo *credentials.TLSInfo
	if request.HTTPRequest.TLS != nil {
		tlsInfo = &credentials.TLSInfo{
			State:          *request.HTTPRequest.TLS,
			CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.PrivacyAndIntegrity},
		}
	}

	authInfo := c.AuthInterceptor.GetAuthInfo(tlsInfo, request.HTTPRequest.Header, func() string {
		return "" // TODO: support audience getter
	})

	var claims *authorization.Claims
	var err error
	if authInfo != nil {
		claims, err = c.AuthInterceptor.GetClaims(authInfo)
		if err != nil {
			return err
		}
		// Make the auth info and claims available on the context.
		ctx = c.AuthInterceptor.EnhanceContext(ctx, authInfo, claims)
	}

	err = c.AuthInterceptor.Authorize(ctx, claims, &authorization.CallTarget{
		APIName:   apiName,
		Namespace: c.namespace.Name().String(),
		Request:   request,
	})
	if err != nil {
		return commonnexus.AdaptAuthorizeError(err)
	}

	if err := c.NamespaceValidationInterceptor.ValidateState(c.namespace, apiName); err != nil {
		c.outcomeTag = metrics.OutcomeTag("invalid_namespace_state")
		return commonnexus.ConvertGRPCError(err, false)
	}

	// Redirect if current cluster is passive for this namespace.
	if !c.namespace.ActiveInCluster(c.ClusterMetadata.GetCurrentClusterName()) {
		if c.shouldForwardRequest(ctx, request.HTTPRequest.Header) {
			c.forwarded = true
			handler, forwardStartTime := c.RedirectionInterceptor.BeforeCall(methodNameForMetrics)
			c.cleanupFunctions = append(c.cleanupFunctions, func(retErr error) {
				c.RedirectionInterceptor.AfterCall(handler, forwardStartTime, c.namespace.ActiveClusterName(), retErr)
			})
			// Handler methods should have special logic to forward requests if this method returns a serviceerror.NamespaceNotActive error.
			return serviceerror.NewNamespaceNotActive(c.namespace.Name().String(), c.ClusterMetadata.GetCurrentClusterName(), c.namespace.ActiveClusterName())
		}
		c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("namespace_inactive_forwarding_disabled"))
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeUnavailable, "cluster inactive")
	}

	c.cleanupFunctions = append(c.cleanupFunctions, func(retErr error) {
		if retErr != nil {
			c.TelemetryInterceptor.HandleError(
				request,
				"",
				c.metricsHandlerForInterceptors,
				[]tag.Tag{tag.Operation(methodNameForMetrics), tag.WorkflowNamespace(c.namespace.Name().String())},
				retErr,
				c.namespace.Name(),
			)
		}
	})

	cleanup, err := c.NamespaceConcurrencyLimitInterceptor.Allow(c.namespace.Name(), apiName, c.metricsHandlerForInterceptors, request)
	c.cleanupFunctions = append(c.cleanupFunctions, func(error) { cleanup() })
	if err != nil {
		c.outcomeTag = metrics.OutcomeTag("namespace_concurrency_limited")
		return commonnexus.ConvertGRPCError(err, false)
	}

	if err := c.NamespaceRateLimitInterceptor.Allow(c.namespace.Name(), apiName, request.HTTPRequest.Header); err != nil {
		c.outcomeTag = metrics.OutcomeTag("namespace_rate_limited")
		return commonnexus.ConvertGRPCError(err, true)
	}

	if err := c.RateLimitInterceptor.Allow(apiName, request.HTTPRequest.Header); err != nil {
		c.outcomeTag = metrics.OutcomeTag("global_rate_limited")
		return commonnexus.ConvertGRPCError(err, true)
	}

	if err := c.clientVersionChecker.ClientSupported(ctx); err != nil {
		c.outcomeTag = metrics.OutcomeTag("unsupported_client")
		return commonnexus.ConvertGRPCError(err, true)
	}

	return nil
}

// TODO: copied from nexus_handler.go; should be combined with other intercept logic.
// Combines logic from RedirectionInterceptor.redirectionAllowed and some from
// SelectedAPIsForwardingRedirectionPolicy.getTargetClusterAndIsNamespaceNotActiveAutoForwarding so all
// redirection conditions can be checked at once. If either of those methods are updated, this should
// be kept in sync.
func (c *requestContext) shouldForwardRequest(ctx context.Context, header http.Header) bool {
	redirectHeader := header.Get(interceptor.DCRedirectionContextHeaderName)
	redirectAllowed, err := strconv.ParseBool(redirectHeader)
	if err != nil {
		redirectAllowed = true
	}
	return redirectAllowed &&
		c.RedirectionInterceptor.RedirectionAllowed(ctx) &&
		c.namespace.IsGlobalNamespace() &&
		len(c.namespace.ClusterNames()) > 1 &&
		c.Config.ForwardingEnabledForNamespace(c.namespace.Name().String())
}
