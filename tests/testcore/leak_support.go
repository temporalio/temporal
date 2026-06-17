package testcore

import "testing"

// NewUnpooledCluster builds a standalone functional test cluster (default
// configuration, no worker service) that is NOT served from the shared cluster
// pool, and returns a function that tears it down.
//
// It is intended for leak and benchmark harnesses that need an explicit
// create/teardown cycle per iteration. Regular tests should use [NewEnv].
func NewUnpooledCluster(t *testing.T) (tearDown func()) {
	tb := &FunctionalTestBase{}
	tb.SetT(t)
	tb.initAssertions()
	tb.setupCluster(withWorkerService(false))
	return func() {
		if err := tb.tearDownTestCluster(); err != nil {
			t.Logf("failed to tear down unpooled cluster: %v", err)
		}
	}
}
