package frontend

import (
	"net/http"

	"github.com/gorilla/mux"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/components/nexusoperations"
	"go.uber.org/fx"
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
		MaxOperationTokenLength:       nexusoperations.MaxOperationTokenLength.Get(coll),
	}
}

func RegisterHTTPHandler(options HandlerOptions, logger log.Logger, router *mux.Router) {
	h := nexusrpc.NewCompletionHTTPHandler(nexusrpc.CompletionHandlerOptions{
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
	router.Path(commonnexus.PathCompletionCallbackNoIdentifier).HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Limit the request body to max allowed Payload size.
		// Content headers are transformed to Payload metadata and contribute to the Payload size as well. A separate
		// limit is enforced on top of this in the CompleteOperation method.
		r.Body = http.MaxBytesReader(w, r.Body, rpc.MaxNexusAPIRequestBodyBytes)
		h.ServeHTTP(w, r)
	})
}
