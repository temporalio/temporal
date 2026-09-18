package testcore

import (
	"context"

	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/testing/testcontext"
)

// NewContext creates a context with default timeout and RPC headers.
//
// NOTE: If you're using testcore.NewEnv, you can use env.Context() directly - it already
// includes RPC headers. This function is primarily for legacy tests or creating standalone
// contexts outside of the TestEnv framework.
//
// If a parent context is provided, the returned context will be canceled when
// either the timeout expires OR the parent is canceled.
func NewContext(parent ...context.Context) context.Context {
	if len(parent) > 0 && parent[0] != nil {
		// Create RPC context derived from parent
		ctx, _ := rpc.NewContextFromParentWithTimeoutAndVersionHeaders(
			parent[0],
			testcontext.DefaultTimeout(),
		)
		return ctx
	}

	// Create standalone RPC context
	ctx, _ := rpc.NewContextWithTimeoutAndVersionHeaders(testcontext.DefaultTimeout())
	return ctx
}
