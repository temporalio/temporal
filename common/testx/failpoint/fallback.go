package failpoint

import "sync/atomic"

// Fallback decides for the executions of keys that have no failpoint registered, for the key or
// its unscoped key, such as seeded random fault activation that applies to every key. A registry
// consults its fallback only while it is active (see Registry.SetFallbackActive), so a registered
// failpoint, enabled or not, always takes precedence.
type Fallback interface {
	// Error returns the error to inject into this execution of key, at a call site that expects
	// an error, such as Checkpoint, or nil. It reports its decisions with Registry.Emit.
	Error(key Key) error
}

// CachingFallback is implemented by fallbacks that keep state per key name, such as BUGGIFY, so
// that a call site that executes the same name repeatedly can keep the state of the name in a
// FallbackCache and skip the fallback's lookup of the name (see FallbackErrorCached).
type CachingFallback interface {
	Fallback
	// ErrorCached is Error for a call site that executes only the name of key with cache. It uses
	// the state that an earlier call stored in cache if it is still current, and else stores the
	// state that it looks up.
	ErrorCached(key Key, cache FallbackCacheSlot) error
}

// FallbackCache holds the state that a CachingFallback keeps for one key name, for a call site
// that executes the name repeatedly. The zero value is empty. A fallback reaches it through a
// FallbackCacheSlot, which only exposes what the same installation of the fallback stored, so a
// fallback that replaces another one never sees the other's state.
type FallbackCache struct {
	entry atomic.Pointer[fallbackCacheEntry]
}

// fallbackCacheEntry boxes the contents of a FallbackCache for an atomic pointer. owner is the
// installation of the fallback that stored value.
type fallbackCacheEntry struct {
	owner *installedFallback
	value any
}

// FallbackCacheSlot is the view of a FallbackCache that one installation of a fallback has. It is
// passed by value.
type FallbackCacheSlot struct {
	cache *FallbackCache
	owner *installedFallback
}

// Load returns the value that Store stored last in this installation of the fallback, or nil.
func (s FallbackCacheSlot) Load() any {
	if entry := s.cache.entry.Load(); entry != nil && entry.owner == s.owner {
		return entry.value
	}
	return nil
}

// Store replaces the value of the cache.
func (s FallbackCacheSlot) Store(value any) {
	s.cache.entry.Store(&fallbackCacheEntry{owner: s.owner, value: value})
}

// FallbackStatsReporter is implemented by fallbacks that count their decisions per key name. The
// FailpointService reports the counters with the registry's failpoints.
type FallbackStatsReporter interface {
	// FallbackStats returns the counters of the key names that the fallback saw, sorted by name.
	FallbackStats() []FallbackKeyStats
}

// FallbackKeyStats counts the executions of a key name that a fallback saw, in all scopes.
type FallbackKeyStats struct {
	Name string
	// Activated reports whether the fallback acts on the name at all.
	Activated bool
	// Hits counts the executions of the name, activated or not, with nothing registered for it,
	// and Fires those that the fallback fired.
	Hits  int64
	Fires int64
}

// lookupRegistered returns the failpoint registered for key or, for a scoped key, for the
// unscoped key.
func lookupRegistered(registry Registry, key Key) (any, bool) {
	return registry.lookup(key)
}

// Armed reports whether src has a registry in which a failpoint is registered or an active
// fallback is installed. Call sites that do work before they execute a failpoint, such as building
// its key, check it first, so they cost a nil check without a registry.
func Armed(src Source) bool {
	registry := src.Failpoints()
	return registry != nil && registry.Armed()
}

// Registered reports whether the registry of src has a failpoint registered for key or any of
// others, or for their unscoped keys, enabled or not, from one snapshot of the registry. Code that
// decides like a fallback, such as a branch at a call site, checks it first.
func Registered(src Source, key Key, others ...Key) bool {
	registry := src.Failpoints()
	return registry != nil && registry.Armed() && registry.registeredAny(newLookupKeys(key, others))
}

// ActiveRegistered executes the failpoint registered in registry for key or the unscoped key, if
// any, as Active does, and reports whether it fired with a value and whether one was registered,
// enabled or not, from one lookup. Code that decides like a fallback, such as a branch at a call
// site, uses it with the registry whose fallback it consults, so a failpoint removed concurrently
// can't make it miss both the failpoint and the fallback.
func ActiveRegistered(registry Registry, key Key) (active, registered bool) {
	if registry == nil || !registry.Armed() {
		return false, false
	}
	fpAny, ok := lookupRegistered(registry, key)
	if !ok {
		return false, false
	}
	_, set, _ := execute[any](registry, key, fpAny, nil)
	return set, true
}

// FallbackError returns the error that the active fallback of the registry of src injects into
// this execution of key, or nil. It is nil if a failpoint is registered for key or any of shadows,
// or for their unscoped keys, whether or not it is enabled. Execute and Checkpoint already include
// it; call sites that execute several keys for one operation, with ExecuteRegistered, call it
// last with the other keys as shadows, so a failpoint registered for any of the keys takes
// precedence over the fallback.
func FallbackError(src Source, key Key, shadows ...Key) error {
	return FallbackErrorCached(src, nil, key, shadows...)
}

// FallbackErrorCached is FallbackError for a call site that executes the name of key repeatedly:
// if the active fallback is a CachingFallback, it keeps the fallback's state for the name in
// cache between the calls. Each cache must only be used with one key name, in any scope. A nil
// cache caches nothing.
func FallbackErrorCached(src Source, cache *FallbackCache, key Key, shadows ...Key) error {
	registry := src.Failpoints()
	if registry == nil || !registry.Armed() {
		return nil
	}
	// One snapshot, so a change that moves a registration from one key to another can't make
	// both lookups miss. Len skips building the keys of the lookup while nothing is registered,
	// such as while only the fallback arms the registry.
	if registry.Len() > 0 && registry.registeredAny(newLookupKeys(key, shadows)) {
		return nil
	}
	return activeFallbackError(registry, cache, key)
}

// FallbackErrorIfEmpty is FallbackErrorCached for a call site that executes the failpoints of key
// itself while any failpoint is registered. It reads the number of registered failpoints once: if
// it is not zero, it reports empty false and does nothing, so the call site executes its failpoints
// and then calls FallbackErrorCached. Else it returns the error of the active fallback, without
// looking for a registration again, which a concurrent change could otherwise make the call site
// miss between the two reads.
func FallbackErrorIfEmpty(src Source, cache *FallbackCache, key Key) (empty bool, err error) {
	registry := src.Failpoints()
	if registry == nil {
		return true, nil
	}
	if registry.Len() > 0 {
		return false, nil
	}
	return true, activeFallbackError(registry, cache, key)
}

// activeFallbackError asks the active fallback of registry, if any, about key, with cache if the
// fallback caches.
func activeFallbackError(registry Registry, cache *FallbackCache, key Key) error {
	installed := registry.activeInstallation()
	if installed == nil {
		return nil
	}
	if cache != nil {
		if caching, ok := installed.f.(CachingFallback); ok {
			return caching.ErrorCached(key, FallbackCacheSlot{cache: cache, owner: installed})
		}
	}
	return installed.f.Error(key)
}
