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
	"context"
	"errors"
	"fmt"
	"net/url"
	"runtime/debug"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/dynamicconfig"
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
	"google.golang.org/grpc/status"
)

var apiName = configs.CompleteNexusOperation

type Config struct {
	Enabled dynamicconfig.BoolPropertyFn
}

type HandlerOptions struct {
	fx.In

	NamespaceRegistry                    namespace.Registry
	Logger                               log.Logger
	MetricsHandler                       metrics.Handler
	Config                               *Config
	CallbackTokenGenerator               *commonnexus.CallbackTokenGenerator
	HistoryClient                        resource.HistoryClient
	NamespaceValidationInterceptor       *interceptor.NamespaceValidatorInterceptor
	NamespaceRateLimitInterceptor        *interceptor.NamespaceRateLimitInterceptor
	NamespaceConcurrencyLimitInterceptor *interceptor.ConcurrentRequestLimitInterceptor
	RateLimitInterceptor                 *interceptor.RateLimitInterceptor
	AuthInterceptor                      *authorization.Interceptor
}

type completionHandler struct {
	HandlerOptions
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
			metrics.OperationTag(apiName),
			metrics.NamespaceTag(nsName),
		),
		requestStartTime: startTime,
	}
	defer rCtx.capturePanicAndRecordMetrics(&retErr)

	if err := rCtx.interceptRequest(ctx, r); err != nil {
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
		hr.Outcome = &historyservice.CompleteNexusOperationRequest_Success{
			Success: result,
		}
		// TODO(bergundy): Limit payload size.
	default:
		// The Nexus SDK ensures this never happens but just in case...
		logger.Error("invalid operation state in completion request", tag.NewStringTag("state", string(r.State)), tag.Error(err))
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid completion state")
	}
	_, err = h.HistoryClient.CompleteNexusOperation(ctx, hr)
	if err != nil {
		logger.Error("failed to process nexus completion request", tag.Error(err))
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			return nexus.HandlerErrorf(nexus.HandlerErrorTypeNotFound, "operation not found")
		}
		return commonnexus.ConvertGRPCError(err, false)
	}
	return nil
}

type requestContext struct {
	*completionHandler
	logger                        log.Logger
	metricsHandler                metrics.Handler
	metricsHandlerForInterceptors metrics.Handler
	namespace                     *namespace.Namespace
	cleanupFunctions              []func()
	requestStartTime              time.Time
	outcomeTag                    metrics.Tag
}

func (c *requestContext) capturePanicAndRecordMetrics(errPtr *error) {
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
		c.metricsHandler = c.metricsHandler.WithTags(metrics.NexusOutcomeTag("success"))
	} else if c.outcomeTag != nil {
		c.metricsHandler = c.metricsHandler.WithTags(c.outcomeTag)
	} else {
		var he *nexus.HandlerError
		if errors.As(*errPtr, &he) {
			c.metricsHandler = c.metricsHandler.WithTags(metrics.NexusOutcomeTag("error_" + strings.ToLower(string(he.Type))))
		} else {
			c.metricsHandler = c.metricsHandler.WithTags(metrics.NexusOutcomeTag("error_internal"))
		}
	}

	c.metricsHandler.Counter(metrics.NexusCompletionRequests.Name()).Record(1)
	c.metricsHandler.Histogram(metrics.NexusCompletionLatencyHistogram.Name(), metrics.Milliseconds).Record(time.Since(c.requestStartTime).Milliseconds())

	for _, fn := range c.cleanupFunctions {
		fn()
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
		c.outcomeTag = metrics.NexusOutcomeTag("invalid_namespace_state")
		return commonnexus.ConvertGRPCError(err, false)
	}
	// TODO: Redirect if current cluster is passive for this namespace.

	cleanup, err := c.NamespaceConcurrencyLimitInterceptor.Allow(c.namespace.Name(), apiName, c.metricsHandlerForInterceptors, request)
	_ = cleanup
	c.cleanupFunctions = append(c.cleanupFunctions, cleanup)
	if err != nil {
		c.outcomeTag = metrics.NexusOutcomeTag("namespace_concurrency_limited")
		return commonnexus.ConvertGRPCError(err, false)
	}

	if err := c.NamespaceRateLimitInterceptor.Allow(c.namespace.Name(), apiName, request.HTTPRequest.Header); err != nil {
		c.outcomeTag = metrics.NexusOutcomeTag("namespace_rate_limited")
		return commonnexus.ConvertGRPCError(err, true)
	}

	if err := c.RateLimitInterceptor.Allow(apiName, request.HTTPRequest.Header); err != nil {
		c.outcomeTag = metrics.NexusOutcomeTag("global_rate_limited")
		return commonnexus.ConvertGRPCError(err, true)
	}

	// TODO: Apply other relevant interceptors.
	return nil
}
