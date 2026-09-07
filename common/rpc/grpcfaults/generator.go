package grpcfaults

import (
	"context"
	"sync"

	"go.temporal.io/server/common/namespace"
)

type (
	namespaceNameGetter interface {
		GetNamespace() string
	}

	namespaceIDGetter interface {
		GetNamespaceId() string
	}
)

type faultStage int

const (
	faultStageRequest faultStage = iota
	faultStageResponse
)

// Outcome contains the response and error returned when a gRPC fault matches.
type Outcome struct {
	Response any
	Error    error
}

// RequestCallback is a callback function for pre-handler gRPC fault injection.
type RequestCallback func(ctx context.Context, fullMethod string, req any) *Outcome

// ResponseCallback is a callback function for post-handler gRPC fault injection.
type ResponseCallback func(ctx context.Context, fullMethod string, req, resp any, err error) *Outcome

// Hooks connects a CallbackGenerator to externally managed fault callbacks.
type Hooks interface {
	InstallRequestCallback(Scope, RequestCallback) func()
	InstallResponseCallback(Scope, ResponseCallback) func()
}

type callback func(ctx context.Context, fullMethod string, req, resp any, err error) *Outcome

// callbackEntry represents a registered gRPC callback with its ID.
type callbackEntry struct {
	id       uint64
	callback callback
}

// Scope identifies a namespace by ID, name, or both. An empty scope applies globally.
type Scope struct {
	NamespaceID   namespace.ID
	NamespaceName namespace.Name
}

type callbackScope struct {
	Scope
	stage faultStage
}

type callbackBucket struct {
	callbacks  []callbackEntry
	unregister func()
}

// CallbackGenerator handles fault injection for gRPC requests and responses.
type CallbackGenerator struct {
	mu        sync.RWMutex
	callbacks map[callbackScope]*callbackBucket
	nextID    uint64
	hooks     Hooks
}

// NewCallbackGenerator creates a new CallbackGenerator instance.
func NewCallbackGenerator() *CallbackGenerator {
	return NewCallbackGeneratorWithHooks(nil)
}

// NewCallbackGeneratorWithHooks creates a CallbackGenerator connected to external fault hooks.
func NewCallbackGeneratorWithHooks(hooks Hooks) *CallbackGenerator {
	return &CallbackGenerator{
		callbacks: make(map[callbackScope]*callbackBucket),
		hooks:     hooks,
	}
}

// RegisterRequestCallback registers a pre-handler gRPC fault injection callback and returns a
// cleanup function that removes the callback when called.
func (r *CallbackGenerator) RegisterRequestCallback(scope Scope, cb RequestCallback) func() {
	return r.registerCallback(scope, faultStageRequest, func(ctx context.Context, fullMethod string, req, _ any, _ error) *Outcome {
		return cb(ctx, fullMethod, req)
	})
}

// RegisterResponseCallback registers a post-handler gRPC fault injection callback and returns a
// cleanup function that removes the callback when called.
func (r *CallbackGenerator) RegisterResponseCallback(scope Scope, cb ResponseCallback) func() {
	return r.registerCallback(scope, faultStageResponse, func(ctx context.Context, fullMethod string, req, resp any, err error) *Outcome {
		return cb(ctx, fullMethod, req, resp, err)
	})
}

func (r *CallbackGenerator) registerCallback(faultScope Scope, stage faultStage, cb callback) func() {
	if r == nil {
		return func() {}
	}

	var callbackScopes []callbackScope
	if faultScope.NamespaceID != "" {
		callbackScopes = append(callbackScopes, callbackScope{Scope: Scope{NamespaceID: faultScope.NamespaceID}, stage: stage})
	}
	if faultScope.NamespaceName != "" {
		callbackScopes = append(callbackScopes, callbackScope{Scope: Scope{NamespaceName: faultScope.NamespaceName}, stage: stage})
	}
	if len(callbackScopes) == 0 {
		callbackScopes = append(callbackScopes, callbackScope{stage: stage})
	}

	r.mu.Lock()
	r.nextID++
	entry := callbackEntry{id: r.nextID, callback: cb}
	for _, callbackScope := range callbackScopes {
		bucket := r.callbacks[callbackScope]
		if bucket == nil {
			bucket = &callbackBucket{}
			if r.hooks != nil {
				bucket.unregister = r.installHooks(callbackScope)
			}
			r.callbacks[callbackScope] = bucket
		}
		bucket.callbacks = append(bucket.callbacks, entry)
	}
	r.mu.Unlock()

	return func() {
		r.unregisterCallback(callbackScopes, entry.id)
	}
}

func (r *CallbackGenerator) unregisterCallback(callbackScopes []callbackScope, id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, callbackScope := range callbackScopes {
		bucket := r.callbacks[callbackScope]
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
			delete(r.callbacks, callbackScope)
		}
	}
}

func (r *CallbackGenerator) installHooks(scope callbackScope) func() {
	if scope.stage == faultStageRequest {
		return r.hooks.InstallRequestCallback(scope.Scope, func(ctx context.Context, fullMethod string, req any) *Outcome {
			return r.generate(ctx, fullMethod, scope, req, nil, nil)
		})
	}
	return r.hooks.InstallResponseCallback(scope.Scope, func(ctx context.Context, fullMethod string, req, resp any, err error) *Outcome {
		return r.generate(ctx, fullMethod, scope, req, resp, err)
	})
}

// GenerateRequest checks registered gRPC callbacks before the handler runs.
func (r *CallbackGenerator) GenerateRequest(ctx context.Context, fullMethod string, req any) *Outcome {
	return r.generateFaultForStage(ctx, fullMethod, faultStageRequest, req, nil, nil)
}

// GenerateResponse checks registered gRPC callbacks after the handler runs.
func (r *CallbackGenerator) GenerateResponse(ctx context.Context, fullMethod string, req, resp any, err error) *Outcome {
	return r.generateFaultForStage(ctx, fullMethod, faultStageResponse, req, resp, err)
}

func (r *CallbackGenerator) generateFaultForStage(ctx context.Context, fullMethod string, stage faultStage, req, resp any, err error) *Outcome {
	if namespaceID, ok := namespaceIDFromRequest(req); ok {
		if outcome := r.generate(ctx, fullMethod, callbackScope{
			Scope: Scope{NamespaceID: namespaceID},
			stage: stage,
		}, req, resp, err); outcome != nil {
			return outcome
		}
	} else if namespaceName, ok := namespaceNameFromRequest(req); ok {
		if outcome := r.generate(ctx, fullMethod, callbackScope{
			Scope: Scope{NamespaceName: namespaceName},
			stage: stage,
		}, req, resp, err); outcome != nil {
			return outcome
		}
	}
	return r.generate(ctx, fullMethod, callbackScope{stage: stage}, req, resp, err)
}

func (r *CallbackGenerator) generate(ctx context.Context, fullMethod string, scope callbackScope, req, resp any, err error) *Outcome {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	bucket := r.callbacks[scope]
	if bucket == nil || len(bucket.callbacks) == 0 {
		r.mu.RUnlock()
		return nil
	}
	callbacks := make([]callbackEntry, len(bucket.callbacks))
	copy(callbacks, bucket.callbacks)
	r.mu.RUnlock()

	for _, entry := range callbacks {
		if outcome := entry.callback(ctx, fullMethod, req, resp, err); outcome != nil {
			return outcome
		}
	}
	return nil
}

func namespaceIDFromRequest(req any) (namespace.ID, bool) {
	request, ok := req.(namespaceIDGetter)
	if !ok || request.GetNamespaceId() == "" {
		return "", false
	}
	return namespace.ID(request.GetNamespaceId()), true
}

func namespaceNameFromRequest(req any) (namespace.Name, bool) {
	request, ok := req.(namespaceNameGetter)
	if !ok || request.GetNamespace() == "" {
		return "", false
	}
	return namespace.Name(request.GetNamespace()), true
}
