package testcore

import (
	"context"
	"time"

	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/testing/debugtimeout"
)

// NewContext create new context with default timeout 90 seconds.
func NewContext() context.Context {
	ctx, _ := rpc.NewContextWithTimeoutAndVersionHeaders(90 * time.Second * debugtimeout.Multiplier)
	return ctx
}
