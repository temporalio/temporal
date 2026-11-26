package failpoint

// Execute executes the failpoint for key in the registry of src, if registered and configured. For a
// key with a scope, a failpoint registered for the unscoped key applies when none is registered
// for the scope.
// It returns the configured value and whether the failpoint fired. A failpoint configured with On
// returns the zero value of T and true.
//
// A nil Source or a Source without a registry, such as one in a build that disables failpoints, has
// no failpoints.
func Execute[T any](src Source, key Key[T]) (T, bool) {
	var zero T
	if src == nil {
		return zero, false
	}
	registry := src.Failpoints()
	if registry == nil || !registry.MayFire() {
		return zero, false
	}
	if fp, ok := registry.lookup(key.id()); ok {
		typed, ok := fp.(*Failpoint[T])
		if !ok {
			// A failpoint of another type, registered under the name of the key, can't have been
			// configured for this call site.
			return zero, false
		}
		return typed.Execute()
	}
	return executeFallback(registry, key)
}

// executeFallback returns the error that the active fallback of registry injects for key, which
// has no failpoint registered, if T is error.
func executeFallback[T any](registry *Registry, key Key[T]) (value T, set bool) {
	errorKey, isError := any(key).(Key[error])
	if !isError {
		return value, false
	}
	fallback := registry.ActiveFallback()
	if fallback == nil {
		return value, false
	}
	err := fallback.Error(errorKey)
	if err == nil {
		return value, false
	}
	// T is error, as checked above.
	value, set = any(err).(T)
	return value, set
}

// CheckError executes an error failpoint for key in the registry of src and returns its error.
// It returns nil if the failpoint is not registered, off, or configured to return a nil error, so
// the caller continues as normal.
func CheckError(src Source, key Key[error]) error {
	err, _ := Execute[error](src, key)
	return err
}

// ExecuteSwitch executes the failpoint for key in the registry of src, as Execute does, and
// reports whether it fired. Switch-style call sites use it with failpoints configured with On.
func ExecuteSwitch[T any](src Source, key Key[T]) bool {
	_, ok := Execute(src, key)
	return ok
}
