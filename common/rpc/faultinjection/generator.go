// Package faultinjection registers request and response faults for multiple transports.
package faultinjection

import (
	"context"
	"sync"

	"go.temporal.io/server/common/namespace"
)

type (
	faultScopeGetter interface {
		FaultScope() Scope
	}

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

// Outcome defines the result of a matched fault.
type Outcome[Resp any] struct {
	Response Resp
	Error    error
}

// RequestCallback checks a request before the handler runs.
type RequestCallback[Req, Resp any] func(ctx context.Context, operation string, req Req) *Outcome[Resp]

// ResponseCallback checks a result after the handler runs.
type ResponseCallback[Req, Resp any] func(ctx context.Context, operation string, req Req, resp Resp, err error) *Outcome[Resp]

// Generator checks for faults before and after a handler runs.
type Generator[Req, Resp any] interface {
	GenerateRequest(ctx context.Context, operation string, req Req) *Outcome[Resp]
	GenerateResponse(ctx context.Context, operation string, req Req, resp Resp, err error) *Outcome[Resp]
}

// Hooks installs callbacks outside the generator.
type Hooks[Req, Resp any] interface {
	InstallRequestCallback(Scope, RequestCallback[Req, Resp]) func()
	InstallResponseCallback(Scope, ResponseCallback[Req, Resp]) func()
}

type callback[Req, Resp any] func(ctx context.Context, operation string, req Req, resp Resp, err error) *Outcome[Resp]

// callbackEntry represents a registered callback with its ID.
type callbackEntry[Req, Resp any] struct {
	id       uint64
	callback callback[Req, Resp]
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

type callbackBucket[Req, Resp any] struct {
	callbacks  []callbackEntry[Req, Resp]
	unregister func()
}

// CallbackGenerator stores request and response fault callbacks.
type CallbackGenerator[Req, Resp any] struct {
	mu        sync.RWMutex
	callbacks map[callbackScope]*callbackBucket[Req, Resp]
	nextID    uint64
	hooks     Hooks[Req, Resp]
}

// NewCallbackGenerator returns a callback generator.
func NewCallbackGenerator[Req, Resp any]() *CallbackGenerator[Req, Resp] {
	return NewCallbackGeneratorWithHooks[Req, Resp](nil)
}

// NewCallbackGeneratorWithHooks returns a callback generator that uses hooks.
func NewCallbackGeneratorWithHooks[Req, Resp any](hooks Hooks[Req, Resp]) *CallbackGenerator[Req, Resp] {
	return &CallbackGenerator[Req, Resp]{
		callbacks: make(map[callbackScope]*callbackBucket[Req, Resp]),
		hooks:     hooks,
	}
}

// RegisterRequestCallback registers a request callback and returns its cleanup function.
func (r *CallbackGenerator[Req, Resp]) RegisterRequestCallback(scope Scope, cb RequestCallback[Req, Resp]) func() {
	return r.registerCallback(scope, faultStageRequest, func(ctx context.Context, operation string, req Req, _ Resp, _ error) *Outcome[Resp] {
		return cb(ctx, operation, req)
	})
}

// RegisterResponseCallback registers a response callback and returns its cleanup function.
func (r *CallbackGenerator[Req, Resp]) RegisterResponseCallback(scope Scope, cb ResponseCallback[Req, Resp]) func() {
	return r.registerCallback(scope, faultStageResponse, func(ctx context.Context, operation string, req Req, resp Resp, err error) *Outcome[Resp] {
		return cb(ctx, operation, req, resp, err)
	})
}

func (r *CallbackGenerator[Req, Resp]) registerCallback(faultScope Scope, stage faultStage, cb callback[Req, Resp]) func() {
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
	defer r.mu.Unlock()
	r.nextID++
	entry := callbackEntry[Req, Resp]{id: r.nextID, callback: cb}
	for _, callbackScope := range callbackScopes {
		bucket := r.callbacks[callbackScope]
		if bucket == nil {
			bucket = &callbackBucket[Req, Resp]{}
			if r.hooks != nil {
				bucket.unregister = r.installHooks(callbackScope)
			}
			r.callbacks[callbackScope] = bucket
		}
		bucket.callbacks = append(bucket.callbacks, entry)
	}
	return func() {
		r.unregisterCallback(callbackScopes, entry.id)
	}
}

func (r *CallbackGenerator[Req, Resp]) unregisterCallback(callbackScopes []callbackScope, id uint64) {
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

func (r *CallbackGenerator[Req, Resp]) installHooks(scope callbackScope) func() {
	if scope.stage == faultStageRequest {
		return r.hooks.InstallRequestCallback(scope.Scope, func(ctx context.Context, operation string, req Req) *Outcome[Resp] {
			var zero Resp
			return r.generate(ctx, operation, scope, req, zero, nil)
		})
	}
	return r.hooks.InstallResponseCallback(scope.Scope, func(ctx context.Context, operation string, req Req, resp Resp, err error) *Outcome[Resp] {
		return r.generate(ctx, operation, scope, req, resp, err)
	})
}

// GenerateRequest checks registered callbacks before the handler runs.
func (r *CallbackGenerator[Req, Resp]) GenerateRequest(ctx context.Context, operation string, req Req) *Outcome[Resp] {
	var zero Resp
	return r.generateFaultForStage(ctx, operation, faultStageRequest, req, zero, nil)
}

// GenerateResponse checks registered callbacks after the handler runs.
func (r *CallbackGenerator[Req, Resp]) GenerateResponse(ctx context.Context, operation string, req Req, resp Resp, err error) *Outcome[Resp] {
	return r.generateFaultForStage(ctx, operation, faultStageResponse, req, resp, err)
}

func (r *CallbackGenerator[Req, Resp]) generateFaultForStage(ctx context.Context, operation string, stage faultStage, req Req, resp Resp, err error) *Outcome[Resp] {
	scope := ScopeFromRequest(req)
	if scope.NamespaceID != "" {
		if outcome := r.generate(ctx, operation, callbackScope{
			Scope: Scope{NamespaceID: scope.NamespaceID},
			stage: stage,
		}, req, resp, err); outcome != nil {
			return outcome
		}
	}
	if scope.NamespaceName != "" {
		if outcome := r.generate(ctx, operation, callbackScope{
			Scope: Scope{NamespaceName: scope.NamespaceName},
			stage: stage,
		}, req, resp, err); outcome != nil {
			return outcome
		}
	}
	return r.generate(ctx, operation, callbackScope{stage: stage}, req, resp, err)
}

func (r *CallbackGenerator[Req, Resp]) generate(ctx context.Context, operation string, scope callbackScope, req Req, resp Resp, err error) *Outcome[Resp] {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	bucket := r.callbacks[scope]
	if bucket == nil || len(bucket.callbacks) == 0 {
		r.mu.RUnlock()
		return nil
	}
	callbacks := make([]callbackEntry[Req, Resp], len(bucket.callbacks))
	copy(callbacks, bucket.callbacks)
	r.mu.RUnlock()

	for _, entry := range callbacks {
		if outcome := entry.callback(ctx, operation, req, resp, err); outcome != nil {
			return outcome
		}
	}
	return nil
}

// ScopeFromRequest returns the namespace scope from req.
func ScopeFromRequest(req any) Scope {
	if request, ok := req.(faultScopeGetter); ok {
		return request.FaultScope()
	}
	if namespaceID := namespaceIDFromRequest(req); namespaceID != "" {
		return Scope{NamespaceID: namespace.ID(namespaceID)}
	}
	if namespaceName := namespaceNameFromRequest(req); namespaceName != "" {
		return Scope{NamespaceName: namespace.Name(namespaceName)}
	}
	return Scope{}
}

func namespaceIDFromRequest(req any) string {
	if request, ok := req.(namespaceIDGetter); ok {
		return request.GetNamespaceId()
	}
	return ""
}

func namespaceNameFromRequest(req any) string {
	if request, ok := req.(namespaceNameGetter); ok {
		return request.GetNamespace()
	}
	return ""
}
