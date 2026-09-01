package testcore

import (
	"sync"
	"sync/atomic"
	"testing"
)

// RequestFault determines whether a fault should be injected before a gRPC handler runs.
// Return the error to inject, or nil to not inject a fault.
type RequestFault func(req any) error

// ResponseFault determines whether a fault should be injected after a gRPC handler runs.
// Return the error to inject, or nil to preserve the handler response and error.
type ResponseFault func(req, resp any, handlerErr error) error

type faultTracker struct {
	t     testing.TB
	fired atomic.Bool

	mu             sync.Mutex
	loggingEnabled bool
}

func newFaultTracker(t testing.TB) *faultTracker {
	t.Helper()
	return &faultTracker{t: t, loggingEnabled: true}
}

func (f *faultTracker) markFired(req any) {
	f.fired.Store(true)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.loggingEnabled {
		f.t.Logf("Fault injection fired: %T", req)
	}
}

func (f *faultTracker) attach(unregister func()) func() {
	f.t.Cleanup(func() {
		f.mu.Lock()
		f.loggingEnabled = false
		f.mu.Unlock()
		unregister()
		if !f.fired.Load() {
			f.t.Error("fault injection was registered but never fired - the fault was never injected")
		}
	})

	return unregister
}
