package nexusoperation

import (
	"context"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

type CompletionHandler struct{}

func newCompletionHandler() *CompletionHandler {
	return &CompletionHandler{}
}

func (h *CompletionHandler) CompleteOperation(
	context.Context,
	*nexusrpc.CompletionRequest,
) error {
	return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "CHASM nexus completion is not implemented")
}
