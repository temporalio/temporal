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
		value, set, _ := typed.execute(nil)
		return value, set
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

// Result is the outcome of ExecuteInOrder.
type Result[T any] struct {
	// Value is the value that the failpoint set. It is the zero value unless Set.
	Value T
	// Set reports whether the failpoint set Value.
	Set bool
	// Fired reports whether a failpoint fired with any action, including one that paused, blocked
	// and was released, or called back. A failpoint without an action or skipped by its Trigger
	// did not fire.
	Fired bool
}

// ExecuteOptions configures ExecuteInOrder.
type ExecuteOptions struct {
	// Arg is passed to the callback of a failpoint configured with CallWithArg, for example the
	// request that the call site is about to handle.
	Arg any
	// PolicyKey overrides the key name passed to the Registry's Policy when no failpoint is
	// registered for any of keys. When empty (and ExplicitOnly is false), Policy decides for
	// keys[0].Name().
	PolicyKey string
	// PolicyScope overrides the scope passed to the Registry's Policy. When empty and PolicyKey is
	// empty, Policy decides for keys[0].Scope().
	PolicyScope string
	// ExplicitOnly skips the Registry's Policy even when no failpoint is registered.
	ExplicitOnly bool
}

// ExecuteInOrder executes the error failpoints registered for keys in registry, in order, passing
// opts.Arg to CallWithArg callbacks, and returns the result of the first failpoint that fired with
// any action. Consecutive keys with the same name form a scope-precedence group: the first
// registered scope in the group wins (even if disabled), falling back to the unscoped key. If no
// failpoint is registered for any of keys and !opts.ExplicitOnly, the registry's Policy decides for
// (opts.PolicyKey, opts.PolicyScope) (defaulting to keys[0]). The failpoints and policy are chosen
// from one snapshot of the registry and run outside the registry lock.
func ExecuteInOrder(registry *Registry, opts ExecuteOptions, keys ...Key[error]) Result[error] {
	if registry == nil || !registry.MayFire() {
		return Result[error]{}
	}
	registered, policy := registry.snapshot(keys)
	for _, e := range registered {
		fp, ok := e.(*Failpoint[error])
		if !ok {
			// A failpoint of another type, registered under the name of the key, can't have been
			// configured for this call site.
			continue
		}
		if value, set, fired := fp.execute(opts.Arg); fired {
			return Result[error]{Value: value, Set: set, Fired: true}
		}
	}
	if len(registered) > 0 || policy == nil || opts.ExplicitOnly {
		return Result[error]{}
	}
	policyName, policyScope := opts.PolicyKey, opts.PolicyScope
	if policyName == "" {
		if len(keys) == 0 {
			return Result[error]{}
		}
		policyName = keys[0].Name()
		if policyScope == "" {
			policyScope = keys[0].Scope()
		}
	}
	if err := policy.Error(policyName, policyScope); err != nil {
		return Result[error]{Value: err, Set: true, Fired: true}
	}
	return Result[error]{}
}

// CheckError executes an error failpoint for key in registry and returns its error.
// It returns nil if registry is nil, the failpoint is not registered (and the registry's Policy
// returns nil), paused or blocked and then released, or returned or called back with a nil error,
// so the caller continues as normal.
func CheckError(registry *Registry, key Key[error]) error {
	err, _ := Execute[error](registry, key)
	return err
}

// ExecuteSwitch executes the failpoint for key in registry, as Execute does, and
// reports whether it fired with a value. Switch-style call sites use it with failpoints configured
// with On; a failpoint that pauses, or blocks and is released, returns false. When no failpoint is
// registered, ExecuteSwitch consults the registry's Policy with key.Scope().
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
