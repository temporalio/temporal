package testcore

import (
	"context"
	"testing"

	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/testing/testcontext"
)

type versionHeadersContextKey struct{}

// NewContext creates a context with default 90-second timeout and RPC headers.
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
			defaultTestTimeout,
		)
		return ctx
	}

	// Create standalone RPC context
	ctx, _ := rpc.NewContextWithTimeoutAndVersionHeaders(defaultTestTimeout)
	return ctx
}

// setupTestTimeoutWithContext creates a context that will be canceled on timeout,
// and reports the timeout error during cleanup. Returns a context that tests can
// use to be interrupted when timeout occurs. The context includes RPC version headers.
func setupTestTimeoutWithContext(t *testing.T) context.Context {
	t.Helper()
	return testcontext.New(
		t,
		testcontext.WithContextDecorator(versionHeadersContextKey{}, headers.SetVersions),
	)
}
