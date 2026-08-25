package faultinjection

import (
	"context"
	"sync"

	"go.temporal.io/server/common/namespace"
	rpcinterceptor "go.temporal.io/server/common/rpc/interceptor"
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

type rpcCallback func(ctx context.Context, fullMethod string, req, resp any, err error) (matched bool, newResp any, newErr error)

// rpcCallbackEntry represents a registered RPC callback with its ID.
type rpcCallbackEntry struct {
	id       uint64
	callback rpcCallback
}

// RPCFaultScope identifies a namespace by ID, name, or both. An empty scope applies globally.
type RPCFaultScope struct {
	NamespaceID   namespace.ID
	NamespaceName namespace.Name
}

type rpcCallbackScope struct {
	RPCFaultScope
	stage rpcFaultStage
}

type rpcCallbackBucket struct {
	callbacks  []rpcCallbackEntry
	unregister func()
}

// RPCFaultGenerator handles fault injection for RPC requests and responses.
type RPCFaultGenerator struct {
	mu          sync.RWMutex
	callbacks   map[rpcCallbackScope]*rpcCallbackBucket
	nextID      uint64
	installHook func(rpcCallbackScope) func()
}

// NewRPCFaultGenerator creates a new RPCFaultGenerator instance.
func NewRPCFaultGenerator() *RPCFaultGenerator {
	return &RPCFaultGenerator{
		callbacks: make(map[rpcCallbackScope]*rpcCallbackBucket),
	}
}

// RegisterRequestCallback registers a pre-handler RPC fault injection callback and returns a
// cleanup function that removes the callback when called.
func (r *RPCFaultGenerator) RegisterRequestCallback(scope RPCFaultScope, cb RPCRequestCallback) func() {
	return r.registerCallback(scope, rpcFaultStageRequest, func(ctx context.Context, fullMethod string, req, _ any, _ error) (bool, any, error) {
		return cb(ctx, fullMethod, req)
	})
}

// RegisterResponseCallback registers a post-handler RPC fault injection callback and returns a
// cleanup function that removes the callback when called.
func (r *RPCFaultGenerator) RegisterResponseCallback(scope RPCFaultScope, cb RPCResponseCallback) func() {
	return r.registerCallback(scope, rpcFaultStageResponse, func(ctx context.Context, fullMethod string, req, resp any, err error) (bool, any, error) {
		return cb(ctx, fullMethod, req, resp, err)
	})
}

func (r *RPCFaultGenerator) registerCallback(scope RPCFaultScope, stage rpcFaultStage, cb rpcCallback) func() {
	if r == nil {
		return func() {}
	}

	var scopes []rpcCallbackScope
	if scope.NamespaceID != "" {
		scopes = append(scopes, rpcCallbackScope{RPCFaultScope: RPCFaultScope{NamespaceID: scope.NamespaceID}, stage: stage})
	}
	if scope.NamespaceName != "" {
		scopes = append(scopes, rpcCallbackScope{RPCFaultScope: RPCFaultScope{NamespaceName: scope.NamespaceName}, stage: stage})
	}
	if len(scopes) == 0 {
		scopes = append(scopes, rpcCallbackScope{stage: stage})
	}

	r.mu.Lock()
	r.nextID++
	entry := rpcCallbackEntry{id: r.nextID, callback: cb}
	for _, scope := range scopes {
		bucket := r.callbacks[scope]
		if bucket == nil {
			bucket = &rpcCallbackBucket{}
			if r.installHook != nil {
				bucket.unregister = r.installHook(scope)
			}
			r.callbacks[scope] = bucket
		}
		bucket.callbacks = append(bucket.callbacks, entry)
	}
	r.mu.Unlock()

	return func() {
		r.unregisterCallback(scopes, entry.id)
	}
}

func (r *RPCFaultGenerator) unregisterCallback(scopes []rpcCallbackScope, id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, scope := range scopes {
		bucket := r.callbacks[scope]
		if bucket == nil {
			continue
		}
		for i, existing := range bucket.callbacks {
			if existing.id == id {
				bucket.callbacks = append(bucket.callbacks[:i], bucket.callbacks[i+1:]...)
				break
			}
		}
		if len(bucket.callbacks) == 0 {
			if bucket.unregister != nil {
				bucket.unregister()
			}
			delete(r.callbacks, scope)
		}
	}
}

// GenerateRequest checks registered RPC callbacks before the handler runs.
func (r *RPCFaultGenerator) GenerateRequest(ctx context.Context, fullMethod string, req any) (bool, any, error) {
	return r.generateForRequest(ctx, fullMethod, rpcFaultStageRequest, req, nil, nil)
}

// GenerateResponse checks registered RPC callbacks after the handler runs.
func (r *RPCFaultGenerator) GenerateResponse(ctx context.Context, fullMethod string, req, resp any, err error) (bool, any, error) {
	return r.generateForRequest(ctx, fullMethod, rpcFaultStageResponse, req, resp, err)
}

func (r *RPCFaultGenerator) generateForRequest(ctx context.Context, fullMethod string, stage rpcFaultStage, req, resp any, err error) (bool, any, error) {
	if namespaceID, ok := namespaceIDFromRequest(req); ok {
		if matched, newResp, newErr := r.generate(ctx, fullMethod, rpcCallbackScope{
			RPCFaultScope: RPCFaultScope{NamespaceID: namespaceID},
			stage:         stage,
		}, req, resp, err); matched {
			return true, newResp, newErr
		}
	} else if namespaceName, ok := namespaceNameFromRequest(req); ok {
		if matched, newResp, newErr := r.generate(ctx, fullMethod, rpcCallbackScope{
			RPCFaultScope: RPCFaultScope{NamespaceName: namespaceName},
			stage:         stage,
		}, req, resp, err); matched {
			return true, newResp, newErr
		}
	}
	return r.generate(ctx, fullMethod, rpcCallbackScope{stage: stage}, req, resp, err)
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
		if matched, newResp, newErr := entry.callback(ctx, fullMethod, req, resp, err); matched {
			return true, newResp, newErr
		}
	}
	return false, nil, nil
}

func namespaceIDFromRequest(req any) (namespace.ID, bool) {
	request, ok := req.(rpcinterceptor.NamespaceIDGetter)
	if !ok || request.GetNamespaceId() == "" {
		return "", false
	}
	return namespace.ID(request.GetNamespaceId()), true
}

func namespaceNameFromRequest(req any) (namespace.Name, bool) {
	request, ok := req.(rpcinterceptor.NamespaceNameGetter)
	if !ok || request.GetNamespace() == "" {
		return "", false
	}
	return namespace.Name(request.GetNamespace()), true
}
