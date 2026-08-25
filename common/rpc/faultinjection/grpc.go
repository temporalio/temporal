package faultinjection

import (
	"context"
	"sync"

	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testhooks"
)

type rpcFaultStage int

const (
	rpcFaultStageRequest rpcFaultStage = iota
	rpcFaultStageResponse
)

// RPCRequestCallback is a callback function for pre-handler RPC fault injection.
type RPCRequestCallback func(ctx context.Context, fullMethod string, req any) (matched bool, newResp any, newErr error)

// RPCResponseCallback is a callback function for post-handler RPC fault injection.
type RPCResponseCallback func(ctx context.Context, fullMethod string, req, resp any, err error) (matched bool, newResp any, newErr error)

type rpcCallback func(ctx context.Context, fullMethod string, stage rpcFaultStage, req, resp any, err error) (matched bool, newResp any, newErr error)

// rpcCallbackEntry represents a registered RPC callback with its ID.
type rpcCallbackEntry struct {
	id       uint64
	callback rpcCallback
}

type rpcCallbackScope struct {
	stage         rpcFaultStage
	namespaceID   namespace.ID
	namespaceName namespace.Name
}

type rpcCallbackBucket struct {
	callbacks  []rpcCallbackEntry
	unregister func()
}

// RPCFaultGenerator handles fault injection for RPC requests and responses.
type RPCFaultGenerator struct {
	mu        sync.RWMutex
	testHooks testhooks.TestHooks
	callbacks map[rpcCallbackScope]*rpcCallbackBucket
	nextID    uint64
}

// NewRPCFaultGenerator creates a new RPCFaultGenerator instance.
func NewRPCFaultGenerator(testHooks testhooks.TestHooks) *RPCFaultGenerator {
	return &RPCFaultGenerator{
		testHooks: testHooks,
		callbacks: make(map[rpcCallbackScope]*rpcCallbackBucket),
	}
}

// RegisterRequestCallback registers a pre-handler RPC fault injection callback and returns a
// cleanup function that removes the callback when called.
func (r *RPCFaultGenerator) RegisterRequestCallback(namespaceID namespace.ID, namespaceName namespace.Name, cb RPCRequestCallback) func() {
	return r.registerCallback(namespaceID, namespaceName, rpcFaultStageRequest, func(ctx context.Context, fullMethod string, stage rpcFaultStage, req, _ any, _ error) (bool, any, error) {
		if stage != rpcFaultStageRequest {
			return false, nil, nil
		}
		return cb(ctx, fullMethod, req)
	})
}

// RegisterResponseCallback registers a post-handler RPC fault injection callback and returns a
// cleanup function that removes the callback when called.
func (r *RPCFaultGenerator) RegisterResponseCallback(namespaceID namespace.ID, namespaceName namespace.Name, cb RPCResponseCallback) func() {
	return r.registerCallback(namespaceID, namespaceName, rpcFaultStageResponse, func(ctx context.Context, fullMethod string, stage rpcFaultStage, req, resp any, err error) (bool, any, error) {
		if stage != rpcFaultStageResponse {
			return false, nil, nil
		}
		return cb(ctx, fullMethod, req, resp, err)
	})
}

func (r *RPCFaultGenerator) registerCallback(namespaceID namespace.ID, namespaceName namespace.Name, stage rpcFaultStage, cb rpcCallback) func() {
	if r == nil {
		return func() {}
	}

	var scopes []rpcCallbackScope
	if namespaceID != "" {
		scopes = append(scopes, rpcCallbackScope{stage: stage, namespaceID: namespaceID})
	}
	if namespaceName != "" {
		scopes = append(scopes, rpcCallbackScope{stage: stage, namespaceName: namespaceName})
	}
	if len(scopes) == 0 {
		return func() {}
	}

	r.mu.Lock()
	r.nextID++
	entry := rpcCallbackEntry{id: r.nextID, callback: cb}
	for _, scope := range scopes {
		bucket := r.callbacks[scope]
		if bucket == nil {
			bucket = &rpcCallbackBucket{unregister: r.installHook(scope)}
			r.callbacks[scope] = bucket
		}
		bucket.callbacks = append(bucket.callbacks, entry)
	}
	r.mu.Unlock()

	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		for _, scope := range scopes {
			bucket := r.callbacks[scope]
			if bucket == nil {
				continue
			}
			for i, existing := range bucket.callbacks {
				if existing.id == entry.id {
					bucket.callbacks = append(bucket.callbacks[:i], bucket.callbacks[i+1:]...)
					break
				}
			}
			if len(bucket.callbacks) == 0 {
				bucket.unregister()
				delete(r.callbacks, scope)
			}
		}
	}
}

func (r *RPCFaultGenerator) installHook(scope rpcCallbackScope) func() {
	switch {
	case scope.namespaceID != "" && scope.stage == rpcFaultStageRequest:
		return testhooks.Set(r.testHooks, testhooks.RPCRequestFaultGeneratorByNamespaceID, func(ctx context.Context, fullMethod string, req any) (bool, any, error) {
			return r.generate(ctx, fullMethod, scope, req, nil, nil)
		}, scope.namespaceID)
	case scope.namespaceID != "":
		return testhooks.Set(r.testHooks, testhooks.RPCResponseFaultGeneratorByNamespaceID, func(ctx context.Context, fullMethod string, req, resp any, err error) (bool, any, error) {
			return r.generate(ctx, fullMethod, scope, req, resp, err)
		}, scope.namespaceID)
	case scope.stage == rpcFaultStageRequest:
		return testhooks.Set(r.testHooks, testhooks.RPCRequestFaultGeneratorByNamespaceName, func(ctx context.Context, fullMethod string, req any) (bool, any, error) {
			return r.generate(ctx, fullMethod, scope, req, nil, nil)
		}, scope.namespaceName)
	default:
		return testhooks.Set(r.testHooks, testhooks.RPCResponseFaultGeneratorByNamespaceName, func(ctx context.Context, fullMethod string, req, resp any, err error) (bool, any, error) {
			return r.generate(ctx, fullMethod, scope, req, resp, err)
		}, scope.namespaceName)
	}
}

func (r *RPCFaultGenerator) generate(ctx context.Context, fullMethod string, scope rpcCallbackScope, req, resp any, err error) (bool, any, error) {
	if r == nil {
		return false, nil, nil
	}
	r.mu.RLock()
	bucket := r.callbacks[scope]
	if bucket == nil || len(bucket.callbacks) == 0 {
		r.mu.RUnlock()
		return false, nil, nil
	}
	callbacks := make([]rpcCallbackEntry, len(bucket.callbacks))
	copy(callbacks, bucket.callbacks)
	r.mu.RUnlock()

	for _, entry := range callbacks {
		if matched, newResp, newErr := entry.callback(ctx, fullMethod, scope.stage, req, resp, err); matched {
			return true, newResp, newErr
		}
	}
	return false, nil, nil
}
