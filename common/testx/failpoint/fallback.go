package failpoint

// Fallback decides for the executions of keys that have no failpoint registered, for the key or
// its unscoped key, such as seeded random fault activation that applies to every key. A registry
// consults its fallback only while it is active (see Registry.SetFallbackActive), so a registered
// failpoint, with an action or not, always takes precedence.
type Fallback interface {
	// Error returns the error to inject into this execution of key, at a call site that expects
	// an error, such as CheckError, or nil.
	Error(key Key[error]) error
}

// MayFire reports whether src has a registry in which a failpoint is registered or an active
// fallback is installed. Call sites that do work before they execute a failpoint, such as building
// its key, check it first, so they cost a nil check without a registry.
func MayFire(src Source) bool {
	if src == nil {
		return false
	}
	registry := src.Failpoints()
	return registry != nil && registry.MayFire()
}

// ExecuteSwitchRegistered executes the failpoint registered in registry for key or the unscoped
// key, if any, as ExecuteSwitch does, and reports whether it fired with a value and whether one was
// registered, with an action or not, from one lookup. Code that decides like a fallback, such as a
// branch at a call site, uses it with the registry whose fallback it consults, so a failpoint
// removed concurrently can't make it miss both the failpoint and the fallback.
func ExecuteSwitchRegistered[T any](registry *Registry, key Key[T]) (fired, registered bool) {
	if registry == nil || !registry.MayFire() {
		return false, false
	}
	fp, ok := registry.lookup(key.id())
	if !ok {
		return false, false
	}
	typed, ok := fp.(*Failpoint[T])
	if !ok {
		return false, true
	}
	_, set := typed.Execute()
	return set, true
}
