package testcore

import (
	"testing"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/taskpoller"
)

type TestEnv struct {
	*FunctionalTestBase
	Logger        log.Logger
	cluster       *TestCluster
	dynamicConfig map[dynamicconfig.Key]any
	nsName        namespace.Name
	taskPoller    *taskpoller.TaskPoller
}

func NewTestEnv(t *testing.T, dynamicConfigOverrides ...map[dynamicconfig.Key]any) *TestEnv {
	// Enforce parallel test execution.
	t.Parallel()

	// Merge all provided dynamic config maps.
	var dcOverrides map[dynamicconfig.Key]any
	for _, m := range dynamicConfigOverrides {
		if dcOverrides == nil {
			dcOverrides = make(map[dynamicconfig.Key]any)
		}
		for k, v := range m {
			dcOverrides[k] = v
		}
	}

	tbase := basePool.acquire(t, dcOverrides)
	cluster := tbase.GetTestCluster()

	// Release back to pool when test completes.
	t.Cleanup(func() {
		basePool.release(tbase)
	})

	// Create a dedicated namespace for the test.
	ns := namespace.Name(RandomizeStr(t.Name()))
	if _, err := tbase.RegisterNamespace(
		ns,
		1, // 1 day retention
		enumspb.ARCHIVAL_STATE_DISABLED,
		"",
		"",
	); err != nil {
		t.Fatalf("Failed to register namespace: %v", err)
	}

	return &TestEnv{
		FunctionalTestBase: tbase,
		cluster:            cluster,
		dynamicConfig:      dcOverrides,
		nsName:             ns,
		Logger:             tbase.Logger,
		taskPoller:         taskpoller.New(t, cluster.FrontendClient(), ns.String()),
	}
}

func (s *TestEnv) GetTestCluster() *TestCluster {
	return s.cluster
}

func (s *TestEnv) FrontendClient() workflowservice.WorkflowServiceClient {
	return s.cluster.FrontendClient()
}

func (s *TestEnv) Namespace() namespace.Name {
	return s.nsName
}

func (s *TestEnv) TaskPoller() *taskpoller.TaskPoller {
	return s.taskPoller
}
