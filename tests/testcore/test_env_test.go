package testcore

import (
	"sync"
	"testing"

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testlogger"
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

func (s *TestEnvSuite) TestStartNamespaceLogCapture() {
	testLogger := testlogger.NewTestLogger(s.T(), testlogger.FailOnExpectedErrorOnly)
	env := &TestEnv{
		FunctionalTestBase: &FunctionalTestBase{externalNamespace: namespace.Name("external")},
		Logger:             testLogger,
		nsName:             namespace.Name("primary"),
		nsID:               namespace.ID("primary-id"),
		t:                  s.T(),
	}

	capture := env.StartNamespaceLogCapture()

	testLogger.Info("primary name", tag.WorkflowNamespace("primary"))
	testLogger.Info("primary ID", tag.WorkflowNamespaceID("primary-id"))
	testLogger.Info("external name", tag.WorkflowNamespace("external"))
	testLogger.Info("unrelated name", tag.WorkflowNamespace("unrelated"))
	testLogger.Info("unrelated ID", tag.WorkflowNamespaceID("unrelated-id"))

	testLogger.Info("target only", tag.NexusEndpointTargetNamespaceID("primary-id"))
	testLogger.Info("unscoped")

	s.ElementsMatch([]testlogger.CapturedLog{
		{
			Level:   testlogger.Info,
			Message: "primary name",
			Tags:    []tag.Tag{tag.WorkflowNamespace("primary")},
		},
		{
			Level:   testlogger.Info,
			Message: "primary ID",
			Tags:    []tag.Tag{tag.WorkflowNamespaceID("primary-id")},
		},
	}, capture.Snapshot())
}
