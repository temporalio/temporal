package testcore

import (
	"context"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/rpc"
)

// NewContext creates a context with default 90-second timeout and RPC headers.
//
// NOTE: If you're using testcore.NewEnv, you can use env.Context() directly - it already
// includes RPC headers. This function is primarily for legacy tests or creating standalone
// contexts outside of the testEnv framework.
//
// If a parent context is provided, the returned context will be canceled when
// either the timeout expires OR the parent is canceled.
func NewContext(parent ...context.Context) context.Context {
	if len(parent) > 0 && parent[0] != nil && parent[0] != context.Background() {
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

// calculateTimeout determines the appropriate timeout duration based on custom timeout,
// environment variable, test deadline, and default values. Returns 0 if timeout should be skipped.
//
// Priority order:
//  1. Custom timeout (via WithTimeout option)
//  2. TEMPORAL_TEST_TIMEOUT environment variable (in seconds)
//  3. Default 90 seconds
func calculateTimeout(customTimeout time.Duration) time.Duration {
	if customTimeout > 0 {
		return customTimeout * debug.TimeoutMultiplier
	}

	if envTimeout := os.Getenv("TEMPORAL_TEST_TIMEOUT"); envTimeout != "" {
		if seconds, err := strconv.Atoi(envTimeout); err == nil && seconds > 0 {
			return time.Duration(seconds) * time.Second * debug.TimeoutMultiplier
		}
	}

	return defaultTestTimeout
}

// setupTestTimeoutWithContext creates a context that will be canceled on timeout,
// and reports the timeout error during cleanup. Returns a context that tests can
// use to be interrupted when timeout occurs. The context includes RPC version headers.
func setupTestTimeoutWithContext(t *testing.T, customTimeout time.Duration) context.Context {
	t.Helper()

	timeout := calculateTimeout(customTimeout)
	if timeout <= 0 {
		return headers.SetVersions(t.Context())
	}

	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	ctx = headers.SetVersions(ctx)

	// Capture the error that caused context cancellation to avoid race conditions
	// between timeout expiration and explicit cancel() in cleanup
	var contextErr atomic.Value
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		// Store the error immediately to capture whether it was DeadlineExceeded or Canceled
		contextErr.Store(ctx.Err())
	}()

	// Register cleanup to cancel context and check timeout
	// t.Cleanup() functions run in LIFO order, so this runs after test code
	t.Cleanup(func() {
		cancel()
		<-done

		// Check the captured error, not the current ctx.Err()
		if err, ok := contextErr.Load().(error); ok && err == context.DeadlineExceeded {
			t.Errorf("Test exceeded timeout of %v", timeout)
		}
	})

	return ctx
}
