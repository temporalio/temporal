package testcore

import (
	"context"
	"time"

	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/rpc"
)

// NewContext create new context with default timeout 90 seconds.
func NewContext() context.Context {
	ctx, _ := rpc.NewContextWithTimeoutAndVersionHeaders(90 * time.Second * debug.TimeoutMultiplier)
	return ctx
}
