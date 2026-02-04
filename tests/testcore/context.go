package testcore

import (
	"context"
	"time"

	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/rpc"
)

// NewContext creates a context with default 90-second timeout and RPC headers.
// If a parent context is provided, the returned context will be canceled when
// either the timeout expires OR the parent is canceled (whichever comes first).
//
// Usage:
//
//	ctx := testcore.NewContext()                    // Standalone RPC context
//	ctx := testcore.NewContext(env.Context())       // Derived from test context
func NewContext(parent ...context.Context) context.Context {
	if len(parent) > 0 && parent[0] != nil && parent[0] != context.Background() {
		// Create RPC context derived from parent
		ctx, _ := rpc.NewContextFromParentWithTimeoutAndVersionHeaders(
			parent[0],
			90*time.Second*debug.TimeoutMultiplier,
		)
		return ctx
	}

	// Create standalone RPC context
	ctx, _ := rpc.NewContextWithTimeoutAndVersionHeaders(defaultTestTimeout)
	return ctx
}
