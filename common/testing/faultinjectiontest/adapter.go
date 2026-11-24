// Package faultinjectiontest runs the failpoints of the gRPC and HTTP fault injection points in
// tests.
package faultinjectiontest

import (
	"context"
	"maps"
	"sync"
	"sync/atomic"

	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/rpc/faultinjection"
	"go.temporal.io/testx/failpoint"
)

// maxCachedOperations bounds the operation keys that an Adapter caches, since HTTP operations
// contain a path chosen by the caller. Keys of further operations are built on each call.
const maxCachedOperations = 1024

// NamespaceIDs resolves the ID of a namespace by its name. It returns "" if it cannot.
type NamespaceIDs func(name namespace.Name) namespace.ID

// RegistryNamespaceIDs resolves namespace IDs with the in-memory state of registry. It doesn't read
// through to persistence, so a request for a namespace that doesn't exist yet, such as
// RegisterNamespace, doesn't add the name to the negative lookup cache of the registry.
func RegistryNamespaceIDs(registry namespace.Registry) NamespaceIDs {
	return func(name namespace.Name) namespace.ID {
		ns, err := registry.GetNamespaceWithOptions(name, namespace.GetNamespaceOptions{DisableReadthrough: true})
		if err != nil {
			return ""
		}
		return ns.ID()
	}
}

// Keys are the failpoint keys of a transport boundary.
type Keys struct {
	// Request and Response return the keys of the failpoints that fire before and after the
	// handler of an operation.
	Request  func(operation string) failpoint.Key
	Response func(operation string) failpoint.Key
	// AnyRequest and AnyResponse are the keys of the failpoints that fire before and after the
	// handler of every operation, if the failpoint of the operation did not fire.
	AnyRequest  failpoint.Key
	AnyResponse failpoint.Key
	// Exempt reports whether no failpoint fires for an operation, such as one that configures
	// failpoints. It can be nil.
	Exempt func(operation string) bool
	// NoBuggify keeps BUGGIFY from injecting errors at this boundary, for example into the
	// requests that a server receives from clients. Registered failpoints still fire.
	NoBuggify bool
}

type operationKeys struct {
	request  failpoint.Key
	response failpoint.Key
	exempt   bool
	// fallbacks keeps the state of the registry's fallback, such as BUGGIFY, for the request and
	// response keys (see failpoint.FallbackErrorCached). It is nil until a caching fallback is
	// active for the operation, and for operations whose keys are not cached.
	fallbacks *fallbackCaches
}

// fallbackCaches are the failpoint.FallbackCaches of the stages of an operation.
type fallbackCaches struct {
	request  failpoint.FallbackCache
	response failpoint.FallbackCache
}

// Adapter is a faultinjection.Generator that executes error failpoints, scoped to the namespace ID
// of the request if it has one.
//
// Before the handler, it executes the failpoint of the operation with the request as the argument
// of the execution, then the failpoint of any operation, if the first did not fire. The first
// fired if it executed with any action, also if it paused, blocked and was released, or returned
// nil (see failpoint.ExecuteWithFired). An error fails the call. After the handler, it does the
// same with a faultinjection.Result as the argument. An error replaces the result of the handler;
// nil keeps it. Exempt operations run no failpoints.
//
// The registry's fallback, such as BUGGIFY (see go.temporal.io/testx/failpoint/buggify), can
// inject an error for the key of the operation at either stage, but only if no failpoint is
// registered for the operation or for any operation, enabled or not, so registered failpoints
// always take precedence.
type Adapter[Req, Resp any] struct {
	source     failpoint.Source
	keys       Keys
	namespaces NamespaceIDs

	// cache holds the keys of the operations seen so far. It is replaced on each change, so
	// lookups don't take a lock.
	cache   atomic.Pointer[map[string]operationKeys]
	cacheMu sync.Mutex
}

// NewAdapter returns an adapter that executes the failpoints for keys in the registry of source.
// namespaces resolves the ID of a request that only carries a namespace name. If it is nil, or
// the name cannot be resolved, only failpoints without a scope fire for the request.
func NewAdapter[Req, Resp any](source failpoint.Source, keys Keys, namespaces NamespaceIDs) *Adapter[Req, Resp] {
	a := &Adapter[Req, Resp]{source: source, keys: keys, namespaces: namespaces}
	a.cache.Store(&map[string]operationKeys{})
	return a
}

// GenerateRequest executes the request failpoints for the operation with req as the argument.
func (a *Adapter[Req, Resp]) GenerateRequest(_ context.Context, operation string, req Req) error {
	if !a.armed() {
		return nil
	}
	keys := a.operationKeys(operation)
	if keys.exempt {
		return nil
	}
	return execute(a, operation, keys, false, req, req)
}

// GenerateResponse executes the response failpoints for the operation with a
// faultinjection.Result of the handler as the argument.
func (a *Adapter[Req, Resp]) GenerateResponse(_ context.Context, operation string, req Req, resp Resp, err error) error {
	if !a.armed() {
		return nil
	}
	keys := a.operationKeys(operation)
	if keys.exempt {
		return nil
	}
	result := faultinjection.Result[Req, Resp]{Operation: operation, Request: req, Response: resp, Error: err}
	return execute(a, operation, keys, true, req, result)
}

// armed reports whether any failpoint is registered or BUGGIFY is enabled, so calls skip the
// lookups while neither is.
func (a *Adapter[Req, Resp]) armed() bool {
	registry := a.source.Failpoints()
	return registry != nil && registry.Armed()
}

// execute executes the failpoints of the request or response stage of an operation with arg, then
// the fallback. It is generic in the type of arg, so arg is only boxed for a registered failpoint.
func execute[Req, Resp, Arg any](
	a *Adapter[Req, Resp],
	operation string,
	keys operationKeys,
	response bool,
	req Req,
	arg Arg,
) error {
	operationKey, anyKey := keys.request, a.keys.AnyRequest
	if response {
		operationKey, anyKey = keys.response, a.keys.AnyResponse
	}
	if scope := a.namespaceID(req); scope != "" {
		operationKey = operationKey.ForScope(scope)
		anyKey = anyKey.ForScope(scope)
	}
	registry := a.source.Failpoints()
	if registry == nil {
		return nil
	}
	// Without a registered failpoint, such as while only BUGGIFY is enabled, there is nothing to
	// execute, so the stage skips boxing arg and goes to the fallback. FallbackErrorIfEmpty reads
	// the registry once for both, so a failpoint registered in between can't veto the fallback
	// after the stage already skipped the failpoints.
	if registry.Len() == 0 {
		if a.keys.NoBuggify {
			return nil
		}
		if empty, err := failpoint.FallbackErrorIfEmpty(a.source, a.fallbackCache(operation, keys, response), operationKey); empty {
			return err
		}
	}
	boxed := any(arg)
	if err, _, fired := failpoint.ExecuteRegistered[error](a.source, operationKey, boxed); fired {
		return err
	}
	if err, _, fired := failpoint.ExecuteRegistered[error](a.source, anyKey, boxed); fired {
		return err
	}
	if a.keys.NoBuggify {
		return nil
	}
	return failpoint.FallbackErrorCached(a.source, a.fallbackCache(operation, keys, response), operationKey, anyKey)
}

// fallbackCache returns the cache of a stage of an operation for the active fallback, or nil if
// the fallback doesn't cache or the operation isn't cached. It allocates the caches of an
// operation when a caching fallback is first active for it.
func (a *Adapter[Req, Resp]) fallbackCache(operation string, keys operationKeys, response bool) *failpoint.FallbackCache {
	caches := keys.fallbacks
	if caches == nil {
		caches = a.activateFallbackCaches(operation)
		if caches == nil {
			return nil
		}
	}
	if response {
		return &caches.response
	}
	return &caches.request
}

func (a *Adapter[Req, Resp]) activateFallbackCaches(operation string) *fallbackCaches {
	registry := a.source.Failpoints()
	if registry == nil {
		return nil
	}
	if _, ok := registry.ActiveFallback().(failpoint.CachingFallback); !ok {
		return nil
	}
	if _, ok := (*a.cache.Load())[operation]; !ok {
		return nil
	}
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()
	cache := *a.cache.Load()
	keys, ok := cache[operation]
	if !ok {
		return nil
	}
	if keys.fallbacks == nil {
		keys.fallbacks = new(fallbackCaches)
		next := maps.Clone(cache)
		next[operation] = keys
		a.cache.Store(&next)
	}
	return keys.fallbacks
}

// namespaceID returns the namespace ID of req, or "" if it has none.
func (a *Adapter[Req, Resp]) namespaceID(req Req) string {
	scope := faultinjection.ScopeFromRequest(req)
	if scope.NamespaceID != "" {
		return scope.NamespaceID.String()
	}
	if scope.NamespaceName == "" || a.namespaces == nil {
		return ""
	}
	return a.namespaces(scope.NamespaceName).String()
}

func (a *Adapter[Req, Resp]) operationKeys(operation string) operationKeys {
	if keys, ok := (*a.cache.Load())[operation]; ok {
		return keys
	}
	keys := operationKeys{
		request:  a.keys.Request(operation),
		response: a.keys.Response(operation),
		exempt:   a.keys.Exempt != nil && a.keys.Exempt(operation),
	}
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()
	cache := *a.cache.Load()
	if cached, ok := cache[operation]; ok {
		return cached
	}
	if len(cache) < maxCachedOperations {
		next := maps.Clone(cache)
		next[operation] = keys
		a.cache.Store(&next)
	}
	return keys
}
