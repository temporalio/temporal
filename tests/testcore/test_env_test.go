package testcore

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/parallelsuite"
	testmonitor "go.temporal.io/server/tests/testcore/monitor"
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

func TestUmpireMonitorFactoryRequiresDedicatedCluster(t *testing.T) {
	wantErr := errors.New("factory failed")
	var calls int
	factory := func(log.Logger) (testmonitor.Monitor, error) {
		calls++
		return nil, wantErr
	}
	var options testOptions

	WithUmpireMonitorFactory(factory)(&options)

	require.True(t, options.dedicatedCluster)
	require.Equal(t, "custom Umpire monitor used", options.dedicatedReason)
	require.Len(t, options.clusterOptions, 1)
	params := ApplyTestClusterOptions(options.clusterOptions)
	require.NotNil(t, params.UmpireMonitorFactory)
	monitor, err := params.UmpireMonitorFactory(nil)
	require.Nil(t, monitor)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, 1, calls)
}
