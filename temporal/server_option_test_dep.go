//go:build test_dep

package temporal

import "go.temporal.io/testx/failpoint"

// WithFailpointSource sets the failpoint registry of the server, which then enables failpoints
// without the startup switch (the failpoints.enabled config setting). Tests that run servers in one
// process use it to keep the registry of each server.
//
// It exists only in builds with the test_dep tag: a server without it can only enable failpoints
// with the startup switch.
func WithFailpointSource(registry *failpoint.Registry) ServerOption {
	return applyFunc(func(s *serverOptions) {
		s.failpointSource = registry
	})
}
