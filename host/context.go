package host

import (
	"context"
	"time"

	"github.com/temporalio/temporal/common/rpc"
)

// NewContext create new context with default timeout 90 seconds.
func NewContext() context.Context {
	ctx, _ := rpc.NewContextWithTimeoutAndHeaders(90 * time.Second)
	return ctx
}
