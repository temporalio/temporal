package testcore

import (
	"sync"
	"sync/atomic"
	"testing"

	"go.temporal.io/server/common/rpc/grpcfaults"
)

// RequestFault determines whether a fault should be injected before a gRPC handler runs.
// Return the error to inject, or nil to not inject a fault.
type RequestFault func(req any) error

// ResponseFault determines whether a fault should be injected after a gRPC handler runs.
// Return the error to inject, or nil to preserve the handler response and error.
type ResponseFault func(req, resp any, handlerErr error) error

func injectFault(t testing.TB, register func(func(any, error) *grpcfaults.Outcome) func()) func() {
	t.Helper()

	var fired atomic.Bool
	var logMu sync.Mutex
	loggingEnabled := true

	unregister := register(func(req any, injectedErr error) *grpcfaults.Outcome {
		if injectedErr != nil {
			fired.Store(true)
			logMu.Lock()
			if loggingEnabled {
				t.Logf("Fault injection fired: %T", req)
			}
			logMu.Unlock()
			return &grpcfaults.Outcome{Error: injectedErr}
		}
		return nil
	})

	t.Cleanup(func() {
		logMu.Lock()
		loggingEnabled = false
		logMu.Unlock()
		unregister()
		if !fired.Load() {
			t.Error("fault injection was registered but never fired - the fault was never injected")
		}
	})

	return unregister
}
