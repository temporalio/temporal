//go:build test_dep

package faultinjectiontest

import (
	"context"

	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/rpc/faultinjection"
	"go.temporal.io/server/common/testing/failpoint"
	"go.temporal.io/server/common/testing/testhooks"
)

// Keys identifies namespace-scoped hooks.
type Keys[Callback any] struct {
	ByNamespaceID   testhooks.Key[Callback, namespace.ID]
	ByNamespaceName testhooks.Key[Callback, namespace.Name]
}

// HookKeys identifies request and response hooks.
type HookKeys[Req, Resp any] struct {
	Request  Keys[faultinjection.RequestCallback[Req, Resp]]
	Response Keys[faultinjection.ResponseCallback[Req, Resp]]
	// Failpoint returns the key of the failpoint that runs before request callbacks.
	Failpoint func(operation string) failpoint.Key
}

// Adapter connects a request and response generator to test hooks.
type Adapter[Req, Resp any] struct {
	testHooks    testhooks.TestHooks
	requestKeys  Keys[faultinjection.RequestCallback[Req, Resp]]
	responseKeys Keys[faultinjection.ResponseCallback[Req, Resp]]
	failpointKey func(operation string) failpoint.Key
}

// NewAdapter returns a request and response adapter.
func NewAdapter[Req, Resp any](testHooks testhooks.TestHooks, keys HookKeys[Req, Resp]) Adapter[Req, Resp] {
	return Adapter[Req, Resp]{
		testHooks:    testHooks,
		requestKeys:  keys.Request,
		responseKeys: keys.Response,
		failpointKey: keys.Failpoint,
	}
}

func (a Adapter[Req, Resp]) InstallRequestCallback(
	scope faultinjection.Scope,
	callback faultinjection.RequestCallback[Req, Resp],
) func() {
	return install(a.testHooks, a.requestKeys, scope, callback)
}

// GenerateRequest first executes the failpoint for the operation, scoped to the request's
// namespace ID if it has one. An error from the failpoint becomes the outcome. Otherwise, it runs
// the request callback for the request's namespace.
func (a Adapter[Req, Resp]) GenerateRequest(
	ctx context.Context,
	operation string,
	req Req,
) *faultinjection.Outcome[Resp] {
	scope := faultinjection.ScopeFromRequest(req)
	if a.failpointKey != nil {
		key := a.failpointKey(operation)
		if scope.NamespaceID != "" {
			key = key.ForNamespace(scope.NamespaceID)
		}
		if err := failpoint.Checkpoint(a.testHooks, key); err != nil {
			return &faultinjection.Outcome[Resp]{Error: err}
		}
	}
	if generate, ok := get(a.testHooks, a.requestKeys, scope); ok {
		return generate(ctx, operation, req)
	}
	return nil
}

func (a Adapter[Req, Resp]) InstallResponseCallback(
	scope faultinjection.Scope,
	callback faultinjection.ResponseCallback[Req, Resp],
) func() {
	return install(a.testHooks, a.responseKeys, scope, callback)
}

func (a Adapter[Req, Resp]) GenerateResponse(
	ctx context.Context,
	operation string,
	req Req,
	resp Resp,
	err error,
) *faultinjection.Outcome[Resp] {
	if generate, ok := get(a.testHooks, a.responseKeys, faultinjection.ScopeFromRequest(req)); ok {
		return generate(ctx, operation, req, resp, err)
	}
	return nil
}

func install[Callback any](
	testHooks testhooks.TestHooks,
	keys Keys[Callback],
	scope faultinjection.Scope,
	callback Callback,
) func() {
	switch {
	case scope.NamespaceID != "":
		return testhooks.Set(testHooks, keys.ByNamespaceID, callback, scope.NamespaceID)
	case scope.NamespaceName != "":
		return testhooks.Set(testHooks, keys.ByNamespaceName, callback, scope.NamespaceName)
	default:
		panic("fault injection test hooks require a namespace scope")
	}
}

func get[Callback any](
	testHooks testhooks.TestHooks,
	keys Keys[Callback],
	scope faultinjection.Scope,
) (Callback, bool) {
	if scope.NamespaceID != "" {
		return testhooks.Get(testHooks, keys.ByNamespaceID, scope.NamespaceID)
	}
	if scope.NamespaceName != "" {
		return testhooks.Get(testHooks, keys.ByNamespaceName, scope.NamespaceName)
	}
	var zero Callback
	return zero, false
}
