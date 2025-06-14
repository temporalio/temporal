package testenv

import (
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/testenv/action"
	"go.temporal.io/server/tests/testcore"
)

type physicalCluster struct {
	*testcore.FunctionalTestBase
}

func newPhysicalCluster(
	s *stamp.Scenario,
	interceptor *grpcInterceptor,
	configs []action.ClusterConfig,
) *physicalCluster {
	tbase := &testcore.FunctionalTestBase{}
	tbase.Logger = s.Logger()
	tbase.SetT(s.T()) // TODO: drop this; requires cluster tbase to be decoupled from testify

	// TODO: need to make this the 1st interceptor so it can observe every request
	// TODO: or use proxy instead? can be re-used for distributed test and doesn't require interceptor reshuffeling
	opts := []testcore.TestClusterOption{
		testcore.WithAdditionalGrpcInterceptors(interceptor.Interceptor()),
	}
	if len(configs) > 0 {
		overrides := make(map[dynamicconfig.Key]any, len(configs))
		for _, cfg := range configs {
			overrides[cfg.Key.Key()] = cfg.Val
		}
		opts = append(opts, testcore.WithDynamicConfigOverrides(overrides))
	}

	tbase.SetupSuiteWithCluster(opts...)

	return &physicalCluster{FunctionalTestBase: tbase}
}
