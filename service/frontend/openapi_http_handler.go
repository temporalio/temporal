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
	"bytes"
	"compress/gzip"
	"io"
	"net/http"

	"github.com/gorilla/mux"
	"go.temporal.io/api/temporalproto/openapi"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/service/frontend/configs"
)

// Small wrapper that does some pre-processing before handing requests over to the OpenAPI SDK's HTTP handler.
type OpenAPIHTTPHandler struct {
	logger               log.Logger
	rateLimitInterceptor *interceptor.RateLimitInterceptor
}

func NewOpenAPIHTTPHandler(
	rateLimitInterceptor *interceptor.RateLimitInterceptor,
	logger log.Logger,
) *OpenAPIHTTPHandler {
	return &OpenAPIHTTPHandler{
		logger:               logger,
		rateLimitInterceptor: rateLimitInterceptor,
	}
}

func (h *OpenAPIHTTPHandler) RegisterRoutes(r *mux.Router) {
	serve := func(version int, apiName string, contentType string, spec []byte) func(http.ResponseWriter, *http.Request) {
		return func(w http.ResponseWriter, r *http.Request) {
			if err := h.rateLimitInterceptor.Allow(apiName, r.Header); err != nil {
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}

			rdr, err := gzip.NewReader(bytes.NewReader(spec))
			if err != nil {
				h.logger.Error("failed to initialize openapi spec reader", tag.NewInt("version", version), tag.Error(err))
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			if _, err := io.Copy(w, rdr); err != nil {
				h.logger.Error("failed to send openapi spec", tag.NewInt("version", version), tag.Error(err))
			}
			if err := rdr.Close(); err != nil {
				h.logger.Error("failed to verify openapi spec checksum", tag.NewInt("version", version), tag.Error(err))
			}
		}
	}

	r.PathPrefix("/api/v1/swagger.json").Methods("GET").HandlerFunc(serve(
		2,
		configs.OpenAPIV2APIName,
		"application/vnd.oai.openapi+json;version=2.0",
		openapi.OpenAPIV2JSONSpec,
	))
	r.PathPrefix("/api/v1/openapi.yaml").Methods("GET").HandlerFunc(serve(
		3,
		configs.OpenAPIV3APIName,
		"application/vnd.oai.openapi;version=3.0",
		openapi.OpenAPIV3YAMLSpec,
	))
}
