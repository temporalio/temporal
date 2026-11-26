package testcore

import (
	"fmt"
	"sync"
	"testing"

	"go.temporal.io/server/common/dynamicconfig"
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
		functionalTestBase: &functionalTestBase{externalNamespace: namespace.Name("external")},
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

type recordingT struct {
	fatals []string
}

func (*recordingT) Helper() {}

func (r *recordingT) Fatalf(format string, args ...any) {
	r.fatals = append(r.fatals, fmt.Sprintf(format, args...))
}

func (s *TestEnvSuite) TestRequireValidDynamicConfig() {
	for name, tc := range map[string]struct {
		setting dynamicconfig.GenericSetting
		value   any
		valid   bool
	}{
		"valid value":                   {setting: dynamicconfig.EnableChasm, value: true, valid: true},
		"invalid value":                 {setting: dynamicconfig.EnableChasm, value: "not a bool"},
		"valid constrained value":       {setting: dynamicconfig.EnableChasm, value: []dynamicconfig.ConstrainedValue{{Value: true}}, valid: true},
		"invalid constrained value":     {setting: dynamicconfig.EnableChasm, value: []dynamicconfig.ConstrainedValue{{Value: true}, {Value: "not a bool"}}},
		"valid string duration":         {setting: dynamicconfig.FrontendResourceExhaustedRetryDelayFloor, value: "1s", valid: true},
		"invalid string for an integer": {setting: dynamicconfig.MatchingNumTaskqueueReadPartitions, value: "x"},
	} {
		s.Run(name, func(s *TestEnvSuite) {
			t := &recordingT{}
			requireValidDynamicConfig(t, tc.setting, tc.value)
			if tc.valid {
				s.Empty(t.fatals)
				return
			}
			s.Len(t.fatals, 1)
			s.Contains(t.fatals[0], "invalid value for setting "+tc.setting.Key().String())
		})
	}
}
