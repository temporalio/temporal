package frontend

import (
	"context"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/nexus-rpc/sdk-go/nexus"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/log"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/rpc"
	hsmnexus "go.temporal.io/server/components/nexusoperations/frontend"
)

type completionRouter struct {
	hsm   hsmnexus.CompletionHandler
	chasm chasmnexus.CompletionHandler
}

func (h *completionRouter) CompleteOperation(ctx context.Context, r *nexusrpc.CompletionRequest) error {
	completion := r.CompletionToken
	if completion == nil {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid callback token")
	}

	// TODO: need to handle migration between HSM and CHASM

	if len(completion.GetComponentRef()) > 0 {
		ref := &persistencespb.ChasmComponentRef{}
		if err := ref.Unmarshal(completion.GetComponentRef()); err != nil {
			return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid callback token")
		}
		return h.chasm.CompleteOperation(ctx, r)
	}
	return h.hsm.CompleteOperation(ctx, r)
}

func RegisterNexusCompletionHandler(
	hsmHandler hsmnexus.CompletionHandler,
	chasmHandler chasmnexus.CompletionHandler,
	logger log.Logger,
	router *mux.Router,
) {
	httpHandler := nexusrpc.NewCompletionHTTPHandler(nexusrpc.CompletionHandlerOptions{
		Handler: &completionRouter{
			hsm:   hsmHandler,
			chasm: chasmHandler,
		},
		Logger:     log.NewSlogLogger(logger),
		Serializer: commonnexus.PayloadSerializer,
	})

	router.Path("/" + commonnexus.RouteCompletionCallback.Representation()).HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Limit the request body to max allowed Payload size.
		// Content headers are transformed to Payload metadata and contribute to the Payload size as well. A separate
		// limit is enforced on top of this in the CompleteOperation method.
		r.Body = http.MaxBytesReader(w, r.Body, rpc.MaxNexusAPIRequestBodyBytes)
		httpHandler.ServeHTTP(w, r)
	})
	router.Path(commonnexus.PathCompletionCallbackNoIdentifier).HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Limit the request body to max allowed Payload size.
		// Content headers are transformed to Payload metadata and contribute to the Payload size as well. A separate
		// limit is enforced on top of this in the CompleteOperation method.
		r.Body = http.MaxBytesReader(w, r.Body, rpc.MaxNexusAPIRequestBodyBytes)
		httpHandler.ServeHTTP(w, r)
	})
}
