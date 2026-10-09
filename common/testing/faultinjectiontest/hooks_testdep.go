//go:build test_dep

package faultinjectiontest

import (
	"context"

	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/rpc/faults"
	"go.temporal.io/server/common/testing/testhooks"
)

// Keys identifies namespace-scoped hooks.
type Keys[Callback any] struct {
	ByNamespaceID   testhooks.Key[Callback, namespace.ID]
	ByNamespaceName testhooks.Key[Callback, namespace.Name]
}

// HookKeys identifies request and response hooks.
type HookKeys[Req, Resp any] struct {
	Request  Keys[faults.RequestCallback[Req, Resp]]
	Response Keys[faults.ResponseCallback[Req, Resp]]
}

// Adapter connects a request and response generator to test hooks.
type Adapter[Req, Resp any] struct {
	testHooks    testhooks.TestHooks
	requestKeys  Keys[faults.RequestCallback[Req, Resp]]
	responseKeys Keys[faults.ResponseCallback[Req, Resp]]
}

// NewAdapter returns a request and response adapter.
func NewAdapter[Req, Resp any](testHooks testhooks.TestHooks, keys HookKeys[Req, Resp]) Adapter[Req, Resp] {
	return Adapter[Req, Resp]{
		testHooks:    testHooks,
		requestKeys:  keys.Request,
		responseKeys: keys.Response,
	}
}

func (a Adapter[Req, Resp]) InstallRequestCallback(
	scope faults.Scope,
	callback faults.RequestCallback[Req, Resp],
) func() {
	return install(a.testHooks, a.requestKeys, scope, callback)
}

func (a Adapter[Req, Resp]) GenerateRequest(
	ctx context.Context,
	operation string,
	req Req,
) *faults.Outcome[Resp] {
	if generate, ok := get(a.testHooks, a.requestKeys, faults.ScopeFromRequest(req)); ok {
		return generate(ctx, operation, req)
	}
	return nil
}

func (a Adapter[Req, Resp]) InstallResponseCallback(
	scope faults.Scope,
	callback faults.ResponseCallback[Req, Resp],
) func() {
	return install(a.testHooks, a.responseKeys, scope, callback)
}

func (a Adapter[Req, Resp]) GenerateResponse(
	ctx context.Context,
	operation string,
	req Req,
	resp Resp,
	err error,
) *faults.Outcome[Resp] {
	if generate, ok := get(a.testHooks, a.responseKeys, faults.ScopeFromRequest(req)); ok {
		return generate(ctx, operation, req, resp, err)
	}
	return nil
}

func install[Callback any](
	testHooks testhooks.TestHooks,
	keys Keys[Callback],
	scope faults.Scope,
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
	scope faults.Scope,
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
