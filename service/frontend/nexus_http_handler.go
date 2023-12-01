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
	"net/http"
	"net/url"
	"path"

	"github.com/gorilla/mux"
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/rpc"
	"google.golang.org/grpc/credentials"
)

// Small wrapper that does some pre-processing before handing requests over to the Nexus SDK's HTTP handler.
type NexusHTTPHandler struct {
	logger                 log.Logger
	nexusHandler           http.Handler
	preprocessErrorCounter metrics.CounterFunc
	auth                   *authorization.Interceptor
	enabled                func() bool
}

func NewNexusHTTPHandler(
	serviceConfig *Config,
	matchingClient matchingservice.MatchingServiceClient,
	metricsHandler metrics.Handler,
	namespaceRegistry namespace.Registry,
	authInterceptor *authorization.Interceptor,
	logger log.Logger,
) *NexusHTTPHandler {
	return &NexusHTTPHandler{
		logger:                 logger,
		auth:                   authInterceptor,
		enabled:                serviceConfig.EnableNexusHTTPHandler,
		preprocessErrorCounter: metricsHandler.Counter(metrics.NexusRequestPreProcessErrors.GetMetricName()).Record,
		nexusHandler: nexus.NewHTTPHandler(nexus.HandlerOptions{
			Handler: &nexusHandler{
				logger:            logger,
				metricsHandler:    metricsHandler,
				namespaceRegistry: namespaceRegistry,
				matchingClient:    matchingClient,
				auth:              authInterceptor,
			},
			GetResultTimeout: serviceConfig.KeepAliveMaxConnectionIdle(),
			Logger:           log.NewSlogLogger(logger),
			Serializer:       commonnexus.PayloadSerializer{},
		}),
	}
}

func (h *NexusHTTPHandler) RegisterRoutes(r *mux.Router) {
	r.PathPrefix("/api/v1/namespaces/{namespace}/task-queues/{task_queue}/dispatch-nexus-task/").HandlerFunc(h.dispatchNexusTaskByNamespaceAndTaskQueue)
	r.PathPrefix("/api/v1/services/{service}/").HandlerFunc(h.dispatchNexusTaskByService)
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

// Handler for /api/v1/namespaces/{namespace}/task-queues/{task_queue}/dispatch-nexus-task
func (h *NexusHTTPHandler) dispatchNexusTaskByNamespaceAndTaskQueue(w http.ResponseWriter, r *http.Request) {
	if !h.enabled() {
		h.writeNexusFailure(w, http.StatusNotFound, &nexus.Failure{Message: "nexus endpoints disabled"})
		return
	}
	// Limit the request body to max allowed Payload size.
	// Content headers are transformed to Payload metadata and contribute to the Payload size as well. A separate
	// limit is enforced on top of this in the nexusHandler.StartOperation method.
	r.Body = http.MaxBytesReader(w, r.Body, rpc.MaxNexusAPIRequestBodyBytes)

	var err error
	vars := mux.Vars(r)
	nc := nexusContext{
		// This name does not map to an underlying gRPC service. This format is used for consistency with the
		// gRPC API names on which the authorizer - the consumer of this string - may depend.
		apiName: "/temporal.api.nexusservice.v1.NexusService/DispatchNexusTask",
	}

	if nc.namespaceName, err = url.PathUnescape(vars["namespace"]); err != nil {
		h.logger.Error("invalid URL", tag.Error(err))
		h.writeNexusFailure(w, http.StatusBadRequest, &nexus.Failure{Message: "invalid URL"})
		return
	}
	if nc.taskQueue, err = url.PathUnescape(vars["task_queue"]); err != nil {
		h.logger.Error("invalid URL", tag.Error(err))
		h.writeNexusFailure(w, http.StatusBadRequest, &nexus.Failure{Message: "invalid URL"})
		return
	}
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
	if authInfo != nil {
		nc.claims, err = h.auth.GetClaims(authInfo)
		if err != nil {
			h.logger.Error("failed to get claims", tag.Error(err))
			h.writeNexusFailure(w, http.StatusUnauthorized, &nexus.Failure{Message: "unauthorized"})
			return
		}
		// Make the auth info and claims available on the context.
		r = r.WithContext(h.auth.EnhanceContext(r.Context(), authInfo, nc.claims))
	}

	r = r.WithContext(context.WithValue(r.Context(), nexusContextKey{}, nc))

	// This whole mess is required to support escaped path vars for namespace and task queue.
	u, err := mux.CurrentRoute(r).URL("namespace", vars["namespace"], "task_queue", vars["task_queue"])
	if err != nil {
		h.logger.Error("invalid URL", tag.Error(err))
		h.writeNexusFailure(w, http.StatusInternalServerError, &nexus.Failure{Message: "internal error"})
		return
	}
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

// Handler for /api/v1/services/{service}
func (h *NexusHTTPHandler) dispatchNexusTaskByService(w http.ResponseWriter, r *http.Request) {
	if !h.enabled() {
		h.writeNexusFailure(w, http.StatusNotFound, &nexus.Failure{Message: "nexus endpoints disabled"})
		return
	}
	// To be implemented once the service registry is implemented.
	w.WriteHeader(http.StatusNotImplemented)
}
