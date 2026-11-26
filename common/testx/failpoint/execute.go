package failpoint

// Trip executes the failpoint for key in registry, if registered, and reports whether it
// is on. For a key with a scope, a failpoint registered for the unscoped key applies when none is
// registered for the scope. Trip-style call sites use it with failpoints configured with On.
//
// A nil Registry has no failpoints.
func (k Key[T]) Trip(registry *Registry) bool {
	if registry == nil {
		return false
	}
	fp, ok := registry.lookup(k.id())
	if !ok {
		return false
	}
	typed, ok := fp.(*Failpoint[T])
	if !ok {
		// A failpoint of another type, registered under the name of the key, can't have been
		// configured for this call site.
		return false
	}
	return typed.isOn()
}
