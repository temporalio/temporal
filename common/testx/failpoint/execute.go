package failpoint

// Execute executes the failpoint for key in registry, if registered and configured. For a
// key with a scope, a failpoint registered for the unscoped key applies when none is registered
// for the scope. When no failpoint is registered and T is error, Execute consults the registry's
// Policy, if set.
// It returns the configured value and whether the failpoint fired. A failpoint configured with On
// returns the zero value of T and true.
//
// A nil Registry has no failpoints.
func Execute[T any](registry *Registry, key Key[T]) (T, bool) {
	var zero T
	if registry == nil || !registry.MayFire() {
		return zero, false
	}
	fp, policy, ok := registry.lookupOrPolicy(key.id())
	if ok {
		typed, ok := fp.(*Failpoint[T])
		if !ok {
			// A failpoint of another type, registered under the name of the key, can't have been
			// configured for this call site.
			return zero, false
		}
		return typed.Execute()
	}
	return executePolicyError(policy, key)
}

// executePolicyError returns the error that policy injects for key, which has no failpoint
// registered, if T is error.
func executePolicyError[T any](policy Policy, key Key[T]) (value T, fired bool) {
	if policy == nil {
		return value, false
	}
	errorKey, isError := any(key).(Key[error])
	if !isError {
		return value, false
	}
	err := policy.Error(errorKey.Name(), errorKey.Scope())
	if err == nil {
		return value, false
	}
	// T is error, as checked above.
	value, fired = any(err).(T)
	return value, fired
}

// CheckError executes an error failpoint for key in registry and returns its error.
// It returns nil if registry is nil, the failpoint is not registered (and the registry's Policy
// returns nil), off, or configured to return a nil error, so the caller continues as normal.
func CheckError(registry *Registry, key Key[error]) error {
	err, _ := Execute[error](registry, key)
	return err
}

// ExecuteSwitch executes the failpoint for key in registry, as Execute does, and
// reports whether it fired. Switch-style call sites use it with failpoints configured with On.
// When no failpoint is registered, ExecuteSwitch consults the registry's Policy with key.Scope().
func ExecuteSwitch[T any](registry *Registry, key Key[T]) bool {
	return ExecuteSwitchWithPolicyScope(registry, key, key.Scope())
}

// ExecuteSwitchWithPolicyScope is ExecuteSwitch with a scope for the registry's Policy that differs
// from key.Scope(). Registered failpoints still match by key.Scope() (and the unscoped key), so a
// test can target an ephemeral ID (such as a namespace UUID) while the policy decides in a
// restart-stable scope (such as a namespace name).
func ExecuteSwitchWithPolicyScope[T any](registry *Registry, key Key[T], policyScope string) bool {
	if registry == nil || !registry.MayFire() {
		return false
	}
	fp, policy, ok := registry.lookupOrPolicy(key.id())
	if ok {
		typed, ok := fp.(*Failpoint[T])
		if !ok {
			return false
		}
		_, fired := typed.Execute()
		return fired
	}
	if policy == nil {
		return false
	}
	return policy.Switch(key.Name(), policyScope)
}
