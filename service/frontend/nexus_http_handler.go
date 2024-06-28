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
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/gorilla/mux"
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/routing"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/service/frontend/configs"
)

// Small wrapper that does some pre-processing before handing requests over to the Nexus SDK's HTTP handler.
type NexusHTTPHandler struct {
	logger                               log.Logger
	nexusHandler                         http.Handler
	enpointRegistry                      commonnexus.EndpointRegistry
	namespaceRegistry                    namespace.Registry
	preprocessErrorCounter               metrics.CounterFunc
	auth                                 *authorization.Interceptor
	namespaceValidationInterceptor       *interceptor.NamespaceValidatorInterceptor
	namespaceRateLimitInterceptor        *interceptor.NamespaceRateLimitInterceptor
	namespaceConcurrencyLimitInterceptor *interceptor.ConcurrentRequestLimitInterceptor
	rateLimitInterceptor                 *interceptor.RateLimitInterceptor
	enabled                              dynamicconfig.BoolPropertyFn
}

func NewNexusHTTPHandler(
	serviceConfig *Config,
	matchingClient matchingservice.MatchingServiceClient,
	metricsHandler metrics.Handler,
	clusterMetadata cluster.Metadata,
	clientCache *cluster.FrontendHTTPClientCache,
	namespaceRegistry namespace.Registry,
	endpointRegistry commonnexus.EndpointRegistry,
	authInterceptor *authorization.Interceptor,
	telemetryInterceptor *interceptor.TelemetryInterceptor,
	redirectionInterceptor *interceptor.Redirection,
	namespaceValidationInterceptor *interceptor.NamespaceValidatorInterceptor,
	namespaceRateLimitInterceptor *interceptor.NamespaceRateLimitInterceptor,
	namespaceConcurrencyLimitIntercptor *interceptor.ConcurrentRequestLimitInterceptor,
	rateLimitInterceptor *interceptor.RateLimitInterceptor,
	logger log.Logger,
) *NexusHTTPHandler {
	return &NexusHTTPHandler{
		logger:                               logger,
		enpointRegistry:                      endpointRegistry,
		namespaceRegistry:                    namespaceRegistry,
		auth:                                 authInterceptor,
		namespaceValidationInterceptor:       namespaceValidationInterceptor,
		namespaceRateLimitInterceptor:        namespaceRateLimitInterceptor,
		namespaceConcurrencyLimitInterceptor: namespaceConcurrencyLimitIntercptor,
		rateLimitInterceptor:                 rateLimitInterceptor,
		enabled:                              serviceConfig.EnableNexusAPIs,
		preprocessErrorCounter:               metricsHandler.Counter(metrics.NexusRequestPreProcessErrors.Name()).Record,
		nexusHandler: nexus.NewHTTPHandler(nexus.HandlerOptions{
			Handler: &nexusHandler{
				logger:                        logger,
				metricsHandler:                metricsHandler,
				clusterMetadata:               clusterMetadata,
				namespaceRegistry:             namespaceRegistry,
				matchingClient:                matchingClient,
				auth:                          authInterceptor,
				telemetryInterceptor:          telemetryInterceptor,
				redirectionInterceptor:        redirectionInterceptor,
				forwardingEnabledForNamespace: serviceConfig.EnableNamespaceNotActiveAutoForwarding,
				forwardingClients:             clientCache,
				payloadSizeLimit:              serviceConfig.BlobSizeLimitError,
			},
			GetResultTimeout: serviceConfig.KeepAliveMaxConnectionIdle(),
			Logger:           log.NewSlogLogger(logger),
			Serializer:       commonnexus.PayloadSerializer,
		}),
	}
}

func (h *NexusHTTPHandler) RegisterRoutes(r *mux.Router) {
	r.PathPrefix("/" + commonnexus.RouteDispatchNexusTaskByNamespaceAndTaskQueue.Representation() + "/").
		HandlerFunc(h.dispatchNexusTaskByNamespaceAndTaskQueue)
	r.PathPrefix("/" + commonnexus.RouteDispatchNexusTaskByEndpoint.Representation() + "/").
		HandlerFunc(h.dispatchNexusTaskByEndpoint)
}

func (h *NexusHTTPHandler) writeNexusFailure(writer http.ResponseWriter, statusCode int, failure *nexus.Failure) {
	h.preprocessErrorCounter.Record(1)

	if failure == nil {
		writer.WriteHeader(statusCode)
		return
	}

	bytes, err := json.Marshal(failure)
	if err != nil {
		h.logger.Error("failed to marshal failure", tag.Error(err))
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(statusCode)

	if _, err := writer.Write(bytes); err != nil {
		h.logger.Error("failed to write response body", tag.Error(err))
	}
}

// Handler for [nexushttp.RouteSet.DispatchNexusTaskByNamespaceAndTaskQueue].
func (h *NexusHTTPHandler) dispatchNexusTaskByNamespaceAndTaskQueue(w http.ResponseWriter, r *http.Request) {
	if !h.enabled() {
		h.writeNexusFailure(w, http.StatusNotFound, &nexus.Failure{Message: "nexus endpoints disabled"})
		return
	}

	var err error
	nc := h.baseNexusContext(configs.DispatchNexusTaskByNamespaceAndTaskQueueAPIName)
	params := prepareRequest(commonnexus.RouteDispatchNexusTaskByNamespaceAndTaskQueue, w, r)

	if nc.taskQueue, err = url.PathUnescape(params.TaskQueue); err != nil {
		h.logger.Error("invalid URL", tag.Error(err))
		h.writeNexusFailure(w, http.StatusBadRequest, &nexus.Failure{Message: "invalid URL"})
		return
	}
	if nc.namespaceName, err = url.PathUnescape(params.Namespace); err != nil {
		h.logger.Error("invalid URL", tag.Error(err))
		h.writeNexusFailure(w, http.StatusBadRequest, &nexus.Failure{Message: "invalid URL"})
		return
	}
	if err = h.namespaceValidationInterceptor.ValidateName(nc.namespaceName); err != nil {
		h.logger.Error("invalid namespace name", tag.Error(err))
		h.writeNexusFailure(w, http.StatusBadRequest, &nexus.Failure{Message: err.Error()})
		return
	}

	r, err = h.parseTlsAndAuthInfo(r, nc)
	if err != nil {
		h.logger.Error("failed to get claims", tag.Error(err))
		h.writeNexusFailure(w, http.StatusUnauthorized, &nexus.Failure{Message: "unauthorized"})
		return
	}

	u, err := mux.CurrentRoute(r).URL("namespace", params.Namespace, "task_queue", params.TaskQueue)
	if err != nil {
		h.logger.Error("invalid URL", tag.Error(err))
		h.writeNexusFailure(w, http.StatusInternalServerError, &nexus.Failure{Message: "internal error"})
		return
	}

	h.serveResolvedURL(w, r, u, nc)
}

// Handler for [nexushttp.RouteSet.DispatchNexusTaskByEndpoint].
func (h *NexusHTTPHandler) dispatchNexusTaskByEndpoint(w http.ResponseWriter, r *http.Request) {
	if !h.enabled() {
		h.writeNexusFailure(w, http.StatusNotFound, &nexus.Failure{Message: "nexus endpoints disabled"})
		return
	}

	endpointIDEscaped := prepareRequest(commonnexus.RouteDispatchNexusTaskByEndpoint, w, r)

	endpointID, err := url.PathUnescape(endpointIDEscaped)
	if err != nil {
		h.logger.Error("invalid URL", tag.Error(err))
		h.writeNexusFailure(w, http.StatusBadRequest, &nexus.Failure{Message: "invalid URL"})
		return
	}
	endpointEntry, err := h.enpointRegistry.GetByID(r.Context(), endpointID)
	if err != nil {
		h.logger.Error("invalid Nexus endpoint ID", tag.Error(err))
		s, ok := status.FromError(err)
		if !ok {
			s = serviceerror.ToStatus(err)
		}
		switch s.Code() {
		case codes.NotFound:
			h.writeNexusFailure(w, http.StatusNotFound, &nexus.Failure{Message: "nexus endpoint not found"})
		case codes.DeadlineExceeded:
			h.writeNexusFailure(w, http.StatusRequestTimeout, &nexus.Failure{Message: "request timed out"})
		default:
			h.writeNexusFailure(w, http.StatusInternalServerError, &nexus.Failure{Message: "internal error"})
		}
		return
	}

	nc, ok := h.nexusContextFromEndpoint(endpointEntry, w)
	if !ok {
		// nexusContextFromEndpoint already writes the failure response.
		return
	}

	r, err = h.parseTlsAndAuthInfo(r, nc)
	if err != nil {
		h.logger.Error("failed to get claims", tag.Error(err))
		h.writeNexusFailure(w, http.StatusUnauthorized, &nexus.Failure{Message: "unauthorized"})
		return
	}

	u, err := mux.CurrentRoute(r).URL("endpoint", endpointIDEscaped)
	if err != nil {
		h.logger.Error("invalid URL", tag.Error(err))
		h.writeNexusFailure(w, http.StatusInternalServerError, &nexus.Failure{Message: "internal error"})
		return
	}

	h.serveResolvedURL(w, r, u, nc)
}

func (h *NexusHTTPHandler) baseNexusContext(apiName string) *nexusContext {
	return &nexusContext{
		namespaceValidationInterceptor:       h.namespaceValidationInterceptor,
		namespaceRateLimitInterceptor:        h.namespaceRateLimitInterceptor,
		namespaceConcurrencyLimitInterceptor: h.namespaceConcurrencyLimitInterceptor,
		rateLimitInterceptor:                 h.rateLimitInterceptor,
		apiName:                              apiName,
		requestStartTime:                     time.Now(),
		responseHeaders:                      make(map[string]string),
	}
}

// nexusContextFromEndpoint returns the nexus context for the given request and a boolean indicating whether the
// endpoint is valid for dispatching.
// For security reasons, at the moment only worker target endpoints are considered valid, in the future external
// endpoints may also be supported.
func (h *NexusHTTPHandler) nexusContextFromEndpoint(entry *persistencespb.NexusEndpointEntry, w http.ResponseWriter) (*nexusContext, bool) {
	switch v := entry.Endpoint.Spec.GetTarget().GetVariant().(type) {
	case *persistencespb.NexusEndpointTarget_Worker_:
		nsName, err := h.namespaceRegistry.GetNamespaceName(namespace.ID(v.Worker.GetNamespaceId()))
		if err != nil {
			h.logger.Error("failed to get namespace name by ID", tag.Error(err))
			var notFoundErr *serviceerror.NotFound
			if errors.As(err, &notFoundErr) {
				h.writeNexusFailure(w, http.StatusBadRequest, &nexus.Failure{Message: "invalid endpoint target"})
			} else {
				h.writeNexusFailure(w, http.StatusInternalServerError, &nexus.Failure{Message: "internal error"})
			}
			return nil, false
		}
		nc := h.baseNexusContext(configs.DispatchNexusTaskByEndpointAPIName)
		nc.namespaceName = nsName.String()
		nc.taskQueue = v.Worker.GetTaskQueue()
		nc.endpointName = entry.Endpoint.Spec.Name
		return nc, true
	default:
		h.writeNexusFailure(w, http.StatusBadRequest, &nexus.Failure{Message: "invalid endpoint target"})
		return nil, false
	}
}

func prepareRequest[T any](route routing.Route[T], w http.ResponseWriter, r *http.Request) T {
	// Limit the request body to max allowed Payload size.
	// Content headers are transformed to Payload metadata and contribute to the Payload size as well. A separate
	// limit is enforced on top of this in the nexusHandler.StartOperation method.
	r.Body = http.MaxBytesReader(w, r.Body, rpc.MaxNexusAPIRequestBodyBytes)

	vars := mux.Vars(r)
	return route.Deserialize(vars)
}

func (h *NexusHTTPHandler) parseTlsAndAuthInfo(r *http.Request, nc *nexusContext) (*http.Request, error) {
	var tlsInfo *credentials.TLSInfo
	if r.TLS != nil {
		tlsInfo = &credentials.TLSInfo{
			State:          *r.TLS,
			CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.PrivacyAndIntegrity},
		}
	}

	authInfo := h.auth.GetAuthInfo(tlsInfo, r.Header, func() string {
		return "" // TODO: support audience getter
	})

	var err error
	if authInfo != nil {
		nc.claims, err = h.auth.GetClaims(authInfo)
		if err != nil {
			return nil, err
		}
		// Make the auth info and claims available on the context.
		r = r.WithContext(h.auth.EnhanceContext(r.Context(), authInfo, nc.claims))
	}

	return r, nil
}

func (h *NexusHTTPHandler) serveResolvedURL(w http.ResponseWriter, r *http.Request, u *url.URL, nc *nexusContext) {
	// Attach Nexus context to response writer and request context.
	w = newNexusHTTPResponseWriter(w, nc)
	r = r.WithContext(context.WithValue(r.Context(), nexusContextKey{}, nc))

	// This whole mess is required to support escaped path vars.
	prefix, err := url.PathUnescape(u.Path)
	if err != nil {
		h.logger.Error("invalid URL", tag.Error(err))
		h.writeNexusFailure(w, http.StatusInternalServerError, &nexus.Failure{Message: "internal error"})
		return
	}
	prefix = path.Dir(prefix)
	r.URL.RawPath = ""
	http.StripPrefix(prefix, h.nexusHandler).ServeHTTP(w, r)
}
