package failpoint

// Fallback decides for the executions of keys that have no failpoint registered, for the key or
// its unscoped key, such as seeded random fault activation that applies to every key. A registry
// consults its fallback only while it is active (see Registry.SetFallbackActive), so a registered
// failpoint, enabled or not, always takes precedence.
type Fallback interface {
	// Error returns the error to inject into this execution of key, at a call site that expects
	// an error, such as Checkpoint, or nil. It reports its decisions with Registry.Emit.
	Error(key Key) error
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
	return registry != nil && registry.Armed() && registry.registeredAny(key, others)
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
	registry := src.Failpoints()
	if registry == nil || !registry.Armed() {
		return nil
	}
	// One snapshot, so a change that moves a registration from one key to another can't make
	// both lookups miss.
	if registry.registeredAny(key, shadows) {
		return nil
	}
	fallback := registry.ActiveFallback()
	if fallback == nil {
		return nil
	}
	return fallback.Error(key)
}
