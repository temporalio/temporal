package testcore

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/historyrequire"
	"go.temporal.io/server/common/testing/taskpoller"
)

type Env interface {
	T() *testing.T
	Namespace() namespace.Name
	FrontendClient() workflowservice.WorkflowServiceClient
	GetTestCluster() *TestCluster
	CloseShard(namespaceID string, workflowID string)
	OverrideDynamicConfig(setting dynamicconfig.GenericSetting, value any) (cleanup func())
}

type testEnv struct {
	*FunctionalTestBase
	*require.Assertions
	historyrequire.HistoryRequire

	Logger log.Logger

	cluster       *TestCluster
	dynamicConfig map[dynamicconfig.Key]any
	nsName        namespace.Name
	taskPoller    *taskpoller.TaskPoller
	t             *testing.T
}

type TestOption func(*testOptions)

type testOptions struct {
	dedicatedCluster bool
	dynamicConfig    map[dynamicconfig.Key]any
}

// WithDedicatedCluster requests a dedicated (non-shared) cluster for the test.
// Use this for tests that have cluster-global side effects.
func WithDedicatedCluster() TestOption {
	return func(o *testOptions) {
		o.dedicatedCluster = true
	}
}

// WithDynamicConfig adds dynamic config overrides for the test.
// This implies WithDedicatedCluster.
func WithDynamicConfig(dc map[dynamicconfig.Key]any) TestOption {
	return func(o *testOptions) {
		o.dedicatedCluster = true
		if o.dynamicConfig == nil {
			o.dynamicConfig = make(map[dynamicconfig.Key]any)
		}
		maps.Copy(o.dynamicConfig, dc)
	}
}

// NewEnv creates a new test environment with access to a Temporal cluster.
// The test is automatically marked as parallel.
func NewEnv(t *testing.T, opts ...TestOption) *testEnv {
	t.Parallel()

	var options testOptions
	for _, opt := range opts {
		opt(&options)
	}

	base := testClusterPool.get(t, options.dedicatedCluster, options.dynamicConfig)
	cluster := base.GetTestCluster()

	// Create a dedicated namespace for the test to help with test isolation.
	ns := namespace.Name(RandomizeStr(t.Name()))
	if _, err := base.RegisterNamespace(
		ns,
		1, // 1 day retention
		enumspb.ARCHIVAL_STATE_DISABLED,
		"",
		"",
	); err != nil {
		t.Fatalf("Failed to register namespace: %v", err)
	}

	return &testEnv{
		FunctionalTestBase: base,
		Assertions:         require.New(t),
		HistoryRequire:     historyrequire.New(t),
		cluster:            cluster,
		dynamicConfig:      options.dynamicConfig,
		nsName:             ns,
		Logger:             base.Logger,
		taskPoller:         taskpoller.New(t, cluster.FrontendClient(), ns.String()),
		t:                  t,
	}
}

// Use test env-specific namespace here for test isolation.
func (s *testEnv) Namespace() namespace.Name {
	return s.nsName
}

func (s *testEnv) TaskPoller() *taskpoller.TaskPoller {
	return s.taskPoller
}

func (s *testEnv) T() *testing.T {
	return s.t
}
