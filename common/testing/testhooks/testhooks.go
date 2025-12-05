package testhooks

// TestHooks holds a registry of active test hooks. It should be obtained through fx and
// used with Get and Call.
//
// TestHooks are an inherently unclean way of writing tests. They require mixing test-only
// concerns into production code. In general you should prefer other ways of writing tests
// wherever possible, and only use TestHooks sparingly, as a last resort.
type TestHooks interface {
	testHooks()
}
