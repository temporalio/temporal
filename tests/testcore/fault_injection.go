package testcore

import (
	"sync"
	"sync/atomic"
	"testing"
)

// RPCRequestFault determines whether a fault should be injected before an RPC handler runs.
// Return the error to inject, or nil to not inject a fault.
type RPCRequestFault func(req any) error

// RPCResponseFault determines whether a fault should be injected after an RPC handler runs.
// Return the error to inject, or nil to preserve the handler response and error.
type RPCResponseFault func(req, resp any, handlerErr error) error

func injectRPCFault(t testing.TB, register func(func(any, error) (bool, any, error)) func()) func() {
	t.Helper()

	var fired atomic.Bool
	var logMu sync.Mutex
	loggingEnabled := true

	unregister := register(func(req any, injectedErr error) (bool, any, error) {
		if injectedErr != nil {
			fired.Store(true)
			logMu.Lock()
			if loggingEnabled {
				t.Logf("Fault injection fired: %T", req)
			}
			logMu.Unlock()
			return true, nil, injectedErr
		}
		return false, nil, nil
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
