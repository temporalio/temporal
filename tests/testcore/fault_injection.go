package testcore

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"go.temporal.io/server/common/rpc/grpcfaults"
	"go.temporal.io/server/common/rpc/httpfaults"
	"go.temporal.io/server/common/testing/faultinjectiontest"
	"go.temporal.io/testx/failpoint"
)

// RequestFault determines whether a fault should be injected before a gRPC handler runs.
// Return the error to inject, or nil to not inject a fault.
type RequestFault func(req any) error

// ResponseFault determines whether a fault should be injected after a gRPC handler runs.
// Return the error to inject, or nil to preserve the handler response and error.
type ResponseFault func(req, resp any, handlerErr error) error

// HTTPRequestFault determines whether a fault should be injected before an HTTP call.
// Return the error to fail the call with, an httpfaults.ResponseOverride to return a response
// without sending the request, or nil to not inject a fault.
type HTTPRequestFault func(context.Context, *http.Request) error

// HTTPResponseFault determines whether a fault should be injected after an HTTP call.
// Return the error that replaces the result of the call, an httpfaults.ResponseOverride to return
// a response without an error, or nil to preserve the result.
type HTTPResponseFault func(context.Context, *http.Request, *http.Response, error) error

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

// injectFault adds fault to the callbacks of the failpoint for key in the test cluster, scoped to
// the test's namespace. fault receives the argument of each execution and returns the request it
// checked and the error to inject, or nil. Several faults on the same key all run, in the order
// they were injected, until one returns an error. The test fails if the fault never fires.
func (e *TestEnv) injectFault(key failpoint.Key, fault func(arg any) (req any, err error)) func() {
	registry := e.cluster.host.testHooks.Failpoints()
	if registry == nil {
		e.t.Fatal("fault injection requires test hooks; use -tags test_dep")
	}
	tracker := newFaultTracker(e.t)
	remove, err := faultinjectiontest.AddCallback(registry, key.ForScope(e.nsID.String()), func(arg any) error {
		req, injectedErr := fault(arg)
		if injectedErr != nil {
			tracker.markFired(req)
		}
		return injectedErr
	})
	if err != nil {
		e.t.Fatalf("fault injection: %v", err)
	}
	return tracker.attach(remove)
}

// requestFault adapts fault to the argument of a gRPC request failpoint.
func requestFault(fault RequestFault) func(arg any) (any, error) {
	return func(arg any) (any, error) {
		return arg, fault(arg)
	}
}

// responseFault adapts fault to the argument of a gRPC response failpoint.
func responseFault(fault ResponseFault) func(arg any) (any, error) {
	return func(arg any) (any, error) {
		result, ok := arg.(grpcfaults.Result)
		if !ok {
			return nil, nil
		}
		return result.Request, fault(result.Request, result.Response, result.Error)
	}
}

// httpRequestFault adapts fault to the argument of an HTTP request failpoint.
func httpRequestFault(fault HTTPRequestFault) func(arg any) (any, error) {
	return func(arg any) (any, error) {
		req, ok := arg.(*httpfaults.Request)
		if !ok {
			return nil, nil
		}
		return req.Raw, fault(req.Raw.Context(), req.Raw)
	}
}

// httpResponseFault adapts fault to the argument of an HTTP response failpoint.
func httpResponseFault(fault HTTPResponseFault) func(arg any) (any, error) {
	return func(arg any) (any, error) {
		result, ok := arg.(httpfaults.Result)
		if !ok {
			return nil, nil
		}
		return result.Request.Raw, fault(result.Request.Raw.Context(), result.Request.Raw, result.Response, result.Error)
	}
}
