package testcore

import (
	"os"
	"os/exec"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/testing/parallelsuite"
)

type historyTaskRecorderCluster struct {
	Cluster
	recorder *HistoryTaskRecorder
}

func (c historyTaskRecorderCluster) GetHistoryTaskRecorder() *HistoryTaskRecorder {
	return c.recorder
}

func TestHistoryTaskRecorderFor(t *testing.T) {
	recorder := &HistoryTaskRecorder{}

	t.Run("present", func(t *testing.T) {
		got, err := historyTaskRecorderFor(historyTaskRecorderCluster{recorder: recorder})
		require.NoError(t, err)
		require.Same(t, recorder, got)
	})

	t.Run("missing capability", func(t *testing.T) {
		got, err := historyTaskRecorderFor(struct{ Cluster }{})
		require.Nil(t, got)
		require.EqualError(t, err,
			"WithHistoryTaskRecorder requires a cluster that implements HistoryTaskRecorderProvider, got struct { testcore.Cluster }")
	})

	t.Run("disabled", func(t *testing.T) {
		got, err := historyTaskRecorderFor(historyTaskRecorderCluster{})
		require.Nil(t, got)
		require.EqualError(t, err,
			"WithHistoryTaskRecorder requires an enabled history task recorder on testcore.historyTaskRecorderCluster")
	})
}

const newEnvMissingHistoryTaskRecorderCapabilityEnv = "TESTCORE_NEW_ENV_MISSING_HISTORY_TASK_RECORDER_CAPABILITY"

type unusedClusterFactory struct{}

func (unusedClusterFactory) NewCluster(*testing.T, *ClusterConfig, log.Logger) (Cluster, error) {
	panic("unexpected cluster creation")
}

type missingHistoryTaskRecorderCluster struct {
	Cluster
	testBaseCalls *int
}

func (c missingHistoryTaskRecorderCluster) TearDownCluster() error {
	return nil
}

func (c missingHistoryTaskRecorderCluster) TestBase() *persistencetests.TestBase {
	(*c.testBaseCalls)++
	return nil
}

func TestNewEnv_MissingHistoryTaskRecorderCapabilitySkipsNamespaceRegistration(t *testing.T) {
	if os.Getenv(newEnvMissingHistoryTaskRecorderCapabilityEnv) != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestNewEnv_MissingHistoryTaskRecorderCapabilitySkipsNamespaceRegistration$")
		cmd.Env = append(os.Environ(), newEnvMissingHistoryTaskRecorderCapabilityEnv+"=1")
		output, err := cmd.CombinedOutput()
		require.Error(t, err)
		require.Contains(t, string(output), "WithHistoryTaskRecorder requires a cluster that implements HistoryTaskRecorderProvider")
		require.Contains(t, string(output), "new env namespace registration skipped")
		return
	}

	var testBaseCalls int
	t.Cleanup(func() {
		if testBaseCalls != 0 {
			t.Errorf("namespace registration reached TestBase %d times", testBaseCalls)
			return
		}
		t.Log("new env namespace registration skipped")
	})
	Run(t, unusedClusterFactory{}, func() {
		router := routerFor(t)
		router.dedicated.allSlots[0].cluster = &FunctionalTestBase{
			testCluster: missingHistoryTaskRecorderCluster{testBaseCalls: &testBaseCalls},
		}
		NewEnv(t, func(options *testOptions) {
			WithHistoryTaskRecorder()(options)
			// Reuse the preseeded cluster to isolate NewEnv's capability validation.
			options.clusterOptions = nil
		})
	})
}

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
