package testcore

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"go.temporal.io/server/common/rpc/grpcfaults"
	"go.temporal.io/server/common/rpc/httpfaults"
	"go.temporal.io/server/common/testing/failpointadapter"
	"go.temporal.io/testx/failpoint"
)

// RequestFault determines whether a fault should be injected before a gRPC handler runs.
// Return the error to inject, or nil to not inject a fault.
type RequestFault func(req any) error

// ResponseFault determines whether a fault should be injected after a gRPC handler runs.
// Return the error to inject, or nil to preserve the handler response and error.
type ResponseFault func(req, resp any, handlerErr error) error

// HTTPRequestFault determines whether a fault should be injected before an HTTP call.
// Return the error to fail the call with, or a response to return without sending the request, or
// nil and nil to not inject a fault. An error wins over a response.
type HTTPRequestFault func(context.Context, *http.Request) (*http.Response, error)

// HTTPResponseFault determines whether a fault should be injected after an HTTP call.
// Return the error that replaces the result of the call, or a response that replaces it without an
// error, or nil and nil to preserve the result. An error wins over a response. Returning the
// response of the call keeps it as it is; any other response replaces it, and the call's response
// is closed, so the replacement must not share its body.
type HTTPResponseFault func(context.Context, *http.Request, *http.Response, error) (*http.Response, error)

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

// addFaultCallback adds fault to the callbacks of the failpoint for key in the test cluster, scoped to
// the test's namespace, by ID and by name. fault receives the argument of each execution and
// returns the request it checked, whether it injected a fault (an error, or a replaced response),
// and the error to inject, or nil. Several faults on the same key all run, in the order
// they were injected, until one returns an error. The test fails if the fault never fires.
func (e *TestEnv) addFaultCallback(key failpoint.Key[error], fault func(arg any) (req any, injected bool, err error)) func() {
	registry := FailpointRegistry(e.cluster)
	if registry == nil {
		e.t.Fatal("fault injection requires a failpoint registry, but the test cluster has none")
	}
	// A request that only carries the namespace name, which the server can't resolve to the ID from
	// memory, executes with the name scope.
	scopes := []string{e.nsID.String(), failpointadapter.NameScope(e.nsName)}
	return addFaultCallbackToScopes(e.t, e.faultCallbacks, key, scopes, fault)
}

// addFaultCallbackToScopes adds fault to the callbacks of the failpoint for key in each of the scopes.
// The cleanup of t, which removes the callbacks, is registered before the first one is added, so
// it also removes the callbacks that were added if a later one collides with another failpoint,
// and no execution calls into the test after it ended.
func addFaultCallbackToScopes(
	t testing.TB,
	callbacks *faultCallbacks,
	key failpoint.Key[error],
	scopes []string,
	fault func(arg any) (req any, injected bool, err error),
) func() {
	tracker := newFaultTracker(t)
	callback := func(arg any) error {
		req, injected, injectedErr := fault(arg)
		if injected {
			tracker.markFired(req)
		}
		return injectedErr
	}
	var removes []func()
	unregister := tracker.attach(func() {
		for _, remove := range removes {
			remove()
		}
	})
	for _, scope := range scopes {
		remove, err := callbacks.add(key.ForScope(scope), callback)
		if err != nil {
			unregister()
			// The test fails with this error, not with the one for a fault that never fired.
			tracker.fired.Store(true)
			t.Fatalf("fault injection: %v", err)
		}
		removes = append(removes, remove)
	}
	return unregister
}

// requestFault adapts fault to the argument of a gRPC request failpoint.
func requestFault(fault RequestFault) func(arg any) (any, bool, error) {
	return func(arg any) (any, bool, error) {
		err := fault(arg)
		return arg, err != nil, err
	}
}

// responseFault adapts fault to the argument of a gRPC response failpoint.
func responseFault(fault ResponseFault) func(arg any) (any, bool, error) {
	return func(arg any) (any, bool, error) {
		result, ok := arg.(*grpcfaults.Result)
		if !ok {
			return nil, false, nil
		}
		err := fault(result.Request, result.Response, result.Error)
		return result.Request, err != nil, err
	}
}

// httpRequestFault adapts fault to the argument of an HTTP request failpoint. A response of the
// fault replaces the response of the call, unless the fault also returns an error.
func httpRequestFault(fault HTTPRequestFault) func(arg any) (any, bool, error) {
	return func(arg any) (any, bool, error) {
		req, ok := arg.(*httpfaults.Request)
		if !ok {
			return nil, false, nil
		}
		resp, err := fault(req.Raw.Context(), req.Raw)
		if err == nil && resp != nil {
			req.Replace(resp)
		}
		return req.Raw, err != nil || resp != nil, err
	}
}

// httpResponseFault adapts fault to the argument of an HTTP response failpoint. A response of the
// fault replaces the response of the call, unless the fault also returns an error.
func httpResponseFault(fault HTTPResponseFault) func(arg any) (any, bool, error) {
	return func(arg any) (any, bool, error) {
		result, ok := arg.(*httpfaults.Result)
		if !ok {
			return nil, false, nil
		}
		resp, err := fault(result.Request.Raw.Context(), result.Request.Raw, result.Response, result.Error)
		if err == nil && resp != nil {
			result.Replace(resp)
		}
		return result.Request.Raw, err != nil || resp != nil, err
	}
}
