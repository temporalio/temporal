package testcore

import "testing"

// NewUnpooledCluster builds a standalone functional test cluster that is NOT
// served from the shared cluster pool, and returns a function that tears it
// down. If enableWorkerService is true the per-namespace worker service is
// started, which exercises the worker-service shutdown path.
//
// It is intended for leak and benchmark harnesses that need an explicit
// create/teardown cycle per iteration. Regular tests should use [NewEnv].
func NewUnpooledCluster(t *testing.T, enableWorkerService bool) (tearDown func()) {
	tb := &FunctionalTestBase{}
	tb.SetT(t)
	tb.initAssertions()
	tb.setupCluster(withWorkerService(enableWorkerService))
	return func() {
		if err := tb.tearDownTestCluster(); err != nil {
			t.Logf("failed to tear down unpooled cluster: %v", err)
		}
	}
}
