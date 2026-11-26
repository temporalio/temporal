package failpoint

// Policy decides failpoints when no explicit failpoint is registered for a key.
//
// Set a Policy on a Registry with Registry.SetPolicy; pass nil to clear it. An explicitly
// registered failpoint always takes precedence over the policy—even one that is disabled or
// skipped by its trigger—so tests can force or suppress individual keys.
type Policy interface {
	// Error returns the error to inject for name in scope, or nil to let the call proceed.
	Error(name, scope string) error
	// Switch reports whether a switch-style call site for name in scope should take its alternate
	// path.
	Switch(name, scope string) bool
}
