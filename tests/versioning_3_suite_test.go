package tests

import (
	"testing"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

type versioning3TestSuite[T any] interface {
	T() *testing.T
	Run(string, func(T)) bool
}

func newVersioning3TestEnv(t *testing.T, opts ...testcore.TestOption) *VersioningTestEnv {
	opts = append([]testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.MatchingDeploymentWorkflowVersion, int(versioning3DeploymentWorkflowVersion)),

		// Make sure we don't hit the rate limiter in tests
		testcore.WithDynamicConfig(dynamicconfig.FrontendGlobalNamespaceNamespaceReplicationInducingAPIsRPS, 1000),
		testcore.WithDynamicConfig(dynamicconfig.FrontendMaxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance, 1),

		// This is overridden for tests using runVersioning3TestWithMatchingBehavior.
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 4),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 4),

		// Overriding the number of deployments that can be registered in a single namespace. Done only for this env
		// since it creates a large number of unique deployments in the test suite's namespace.
		testcore.WithDynamicConfig(dynamicconfig.MatchingMaxDeployments, 1000),

		// Keep deployment versions short because worker-deployment system workflow IDs must fit into 255 characters (database constraint).
		testcore.WithTestVars(func(tv *testvars.TestVars) *testvars.TestVars {
			return tv.WithDeploymentSeries("v3").WithBuildID("b")
		}),
	}, opts...)

	return newVersioningTestEnv(t, opts...)
}

func runVersioning3TestWithMatchingBehavior[T versioning3TestSuite[T]](
	s T,
	testFn func(*VersioningTestEnv, T),
	opts ...testcore.TestOption,
) {
	for _, behavior := range testcore.AllMatchingBehaviors() {
		s.Run(behavior.Name(), func(s T) {
			envOpts := append([]testcore.TestOption{}, opts...)
			envOpts = append(envOpts, behavior.Options()...)
			env := newVersioning3TestEnv(s.T(), envOpts...)
			behavior.InjectHooks(env)
			testFn(env, s)
		})
	}
}
