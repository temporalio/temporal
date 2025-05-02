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

	r.PathPrefix("/swagger.json").Methods("GET").HandlerFunc(serve(
		2,
		configs.OpenAPIV2APIName,
		"application/vnd.oai.openapi+json;version=2.0",
		openapi.OpenAPIV2JSONSpec,
	))
	r.PathPrefix("/openapi.yaml").Methods("GET").HandlerFunc(serve(
		3,
		configs.OpenAPIV3APIName,
		"application/vnd.oai.openapi;version=3.0",
		openapi.OpenAPIV3YAMLSpec,
	))
}
