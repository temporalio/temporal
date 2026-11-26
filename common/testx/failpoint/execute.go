package failpoint

// Execute executes the failpoint for key in registry, if registered and configured. For a
// key with a scope, a failpoint registered for the unscoped key applies when none is registered
// for the scope.
// It returns the configured value and whether the failpoint fired. A failpoint configured with On
// returns the zero value of T and true.
//
// A nil Registry has no failpoints.
func Execute[T any](registry *Registry, key Key[T]) (T, bool) {
	var zero T
	if registry == nil {
		return zero, false
	}
	fp, ok := registry.lookup(key.id())
	if !ok {
		return zero, false
	}
	typed, ok := fp.(*Failpoint[T])
	if !ok {
		// A failpoint of another type, registered under the name of the key, can't have been
		// configured for this call site.
		return zero, false
	}
	return typed.Execute()
}

// CheckError executes an error failpoint for key in registry and returns its error.
// It returns nil if registry is nil, the failpoint is not registered, off, or configured to return
// a nil error, so the caller continues as normal.
func CheckError(registry *Registry, key Key[error]) error {
	err, _ := Execute[error](registry, key)
	return err
}

// ExecuteSwitch executes the failpoint for key in registry, as Execute does, and
// reports whether it fired. Switch-style call sites use it with failpoints configured with On.
func ExecuteSwitch[T any](registry *Registry, key Key[T]) bool {
	_, ok := Execute(registry, key)
	return ok
}
