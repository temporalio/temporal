package testcore

import (
	"sync"
	"testing"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/parallelsuite"
)

type TestEnvSuite struct {
	parallelsuite.Suite[*TestEnvSuite]
}

func TestTestEnvSuite(t *testing.T) {
	parallelsuite.Run(t, &TestEnvSuite{})
}

func (s *TestEnvSuite) TestDedicatedClusterGuard_NoErrorWithoutExplicitRequest() {
	guard := newDedicatedClusterGuard(false)

	s.NoError(guard.validate())
}

func (s *TestEnvSuite) TestDedicatedClusterGuard_FailsWhenUnused() {
	guard := newDedicatedClusterGuard(true)

	s.EqualError(guard.validate(),
		`testcore.WithDedicatedCluster() was requested but no dedicated-cluster-only feature was used`)
}

func (s *TestEnvSuite) TestDedicatedClusterGuard_NoErrorAfterUse() {
	guard := newDedicatedClusterGuard(true)
	guard.record("global hook")

	s.NoError(guard.validate())
}

func (s *TestEnvSuite) TestDedicatedClusterGuard_ConcurrentRecord() {
	guard := newDedicatedClusterGuard(true)
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			guard.record("reason")
		})
	}
	wg.Wait()
	s.NoError(guard.validate())
}

func (s *TestEnvSuite) TestRequiresClusterStartup() {
	tests := []struct {
		name    string
		key     dynamicconfig.Key
		startup bool
	}{
		{name: "frontend trailer metadata", key: dynamicconfig.FrontendContextMetadataSetTrailer.Key(), startup: true},
		{name: "worker sticky cache", key: dynamicconfig.WorkerStickyCacheSize.Key(), startup: true},
		{name: "runtime global config", key: dynamicconfig.EnableChasm.Key(), startup: false},
	}
	for _, test := range tests {
		s.Run(test.name, func(s *TestEnvSuite) {
			s.Equal(test.startup, requiresClusterStartup(test.key))
		})
	}
}
