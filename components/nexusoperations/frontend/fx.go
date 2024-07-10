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
	"net/http"

	"github.com/gorilla/mux"
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.uber.org/fx"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/rpc"
)

var Module = fx.Module(
	"component.nexusoperations.frontend",
	fx.Provide(ConfigProvider),
	fx.Provide(commonnexus.NewCallbackTokenGenerator),
	fx.Invoke(RegisterHTTPHandler),
)

func ConfigProvider(coll *dynamicconfig.Collection) *Config {
	return &Config{
		Enabled:                       dynamicconfig.EnableNexus.Get(coll),
		PayloadSizeLimit:              dynamicconfig.BlobSizeLimitError.Get(coll),
		ForwardingEnabledForNamespace: dynamicconfig.EnableNamespaceNotActiveAutoForwarding.Get(coll),
	}
}

func RegisterHTTPHandler(options HandlerOptions, logger log.Logger, router *mux.Router) {
	h := nexus.NewCompletionHTTPHandler(nexus.CompletionHandlerOptions{
		Handler: &completionHandler{
			options,
			headers.NewDefaultVersionChecker(),
			options.MetricsHandler.Counter(metrics.NexusCompletionRequestPreProcessErrors.Name()),
		},
		Logger:     log.NewSlogLogger(logger),
		Serializer: commonnexus.PayloadSerializer,
	})
	router.Path("/" + commonnexus.RouteCompletionCallback.Representation()).HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Limit the request body to max allowed Payload size.
		// Content headers are transformed to Payload metadata and contribute to the Payload size as well. A separate
		// limit is enforced on top of this in the CompleteOperation method.
		r.Body = http.MaxBytesReader(w, r.Body, rpc.MaxNexusAPIRequestBodyBytes)
		h.ServeHTTP(w, r)
	})
}
