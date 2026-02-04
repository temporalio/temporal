package testcore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/historyrequire"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testtelemetry"
	"go.temporal.io/server/common/testing/testvars"
)

var _ Env = (*testEnv)(nil)

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
	nsName        namespace.Name
	taskPoller    *taskpoller.TaskPoller
	t             *testing.T
	tv            *testvars.TestVars
	eventTracker  *testtelemetry.EventTracker
	waitForCalled bool
}

type TestOption func(*envOptions)

type envOptions struct {
	dedicatedCluster      bool
	dynamicConfigSettings []dynamicConfigOverride
	eventTracker          bool
}

type dynamicConfigOverride struct {
	setting dynamicconfig.GenericSetting
	value   any
}

// WithDedicatedCluster requests a dedicated (non-shared) cluster for the test.
// Use this for tests that have cluster-global side effects.
func WithDedicatedCluster() TestOption {
	return func(o *envOptions) {
		o.dedicatedCluster = true
	}
}

// WithDynamicConfig overrides a dynamic config setting for the test.
// For settings that can be namespace-scoped, a namespace constraint is applied.
// For all others that require a dedicated cluster, this implies `WithDedicatedCluster`.
func WithDynamicConfig(setting dynamicconfig.GenericSetting, value any) TestOption {
	return func(o *envOptions) {
		if err := setting.Validate(value); err != nil {
			panic(fmt.Sprintf("invalid value for setting %s: %v", setting.Key(), err))
		}
		if !canBeNamespaceScoped(setting.Precedence()) {
			o.dedicatedCluster = true
		}
		o.dynamicConfigSettings = append(o.dynamicConfigSettings, dynamicConfigOverride{setting: setting, value: value})
	}
}

// WithEventTracker enables OTEL event tracking for the test environment.
// This allows using WaitFor() to wait for specific OTEL events.
// If this option is used but WaitFor() is never called, the test will fail.
func WithEventTracker() TestOption {
	return func(o *envOptions) {
		o.eventTracker = true
	}
}

// NewEnv creates a new test environment with access to a Temporal cluster.
// The test is automatically marked as parallel.
func NewEnv(t *testing.T, opts ...TestOption) *testEnv {
	t.Parallel()

	var options envOptions
	for _, opt := range opts {
		opt(&options)
	}

	base := testClusterPool.get(t, options)
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

	env := &testEnv{
		FunctionalTestBase: base,
		Assertions:         require.New(t),
		HistoryRequire:     historyrequire.New(t),
		cluster:            cluster,
		nsName:             ns,
		Logger:             base.Logger,
		taskPoller:         taskpoller.New(t, cluster.FrontendClient(), ns.String()),
		t:                  t,
		tv:                 testvars.New(t),
	}

	// For shared clusters, apply all dynamic config settings as overrides.
	if !options.dedicatedCluster && len(options.dynamicConfigSettings) > 0 {
		for _, override := range options.dynamicConfigSettings {
			env.OverrideDynamicConfig(override.setting, override.value)
		}
	}

	// Create EventTracker if requested.
	if options.eventTracker {
		env.eventTracker = testtelemetry.NewEventTracker(t, base.memoryExporter)
		t.Cleanup(func() {
			if !env.waitForCalled {
				t.Fatalf("WithEventTracker() was used but WaitFor() was never called; remove unused option")
			}
		})
	}

	return env
}

// Use test env-specific namespace here for test isolation.
func (e *testEnv) Namespace() namespace.Name {
	return e.nsName
}

func (e *testEnv) TaskPoller() *taskpoller.TaskPoller {
	return e.taskPoller
}

func (e *testEnv) T() *testing.T {
	return e.t
}

func (e *testEnv) Tv() *testvars.TestVars {
	return e.tv
}

// OverrideDynamicConfig overrides a dynamic config setting for the duration of this test.
// For settings that can be namespace-scoped, a namespace constraint is applied.
// All others cannot be applied to a shared cluster and require `WithDedicatedCluster`.
func (e *testEnv) OverrideDynamicConfig(setting dynamicconfig.GenericSetting, value any) (cleanup func()) {
	if e.isShared {
		if !canBeNamespaceScoped(setting.Precedence()) {
			e.t.Fatalf("OverrideDynamicConfig for setting %s (precedence %v) cannot be called on a shared cluster; use testcore.WithDedicatedCluster()", setting.Key(), setting.Precedence())
		}

		// Wrap value with namespace constraint for test isolation on shared clusters.
		ns := e.nsName.String()
		if cvs, ok := value.([]dynamicconfig.ConstrainedValue); ok {
			result := make([]dynamicconfig.ConstrainedValue, len(cvs))
			for i, cv := range cvs {
				cv.Constraints.Namespace = ns
				result[i] = cv
			}
			value = result
		} else {
			value = []dynamicconfig.ConstrainedValue{{
				Constraints: dynamicconfig.Constraints{Namespace: ns},
				Value:       value,
			}}
		}
	}
	return e.cluster.host.overrideDynamicConfig(e.t, setting.Key(), value)
}

func canBeNamespaceScoped(p dynamicconfig.Precedence) bool {
	switch p {
	case dynamicconfig.PrecedenceNamespace,
		dynamicconfig.PrecedenceTaskQueue,
		dynamicconfig.PrecedenceDestination:
		return true
	default:
		return false
	}
}

// WaitFor waits until a specific telemetry event is observed or the timeout expires.
// Requires WithEventTracker() option to be set when creating the test environment.
//
// NOTE: Subsequent WaitFor invocations will only see events that occurred after the
// last matched event to avoid re-matching. Not concurrency safe.
func (e *testEnv) WaitFor(
	matcher testtelemetry.EventMatcher,
	timeout time.Duration,
) {
	e.t.Helper()

	if e.eventTracker == nil {
		e.t.Fatalf("WaitFor requires WithEventTracker() option")
	}
	e.waitForCalled = true

	ctx, cancel := context.WithTimeout(e.t.Context(), timeout)
	defer cancel()

	if err := e.eventTracker.WaitFor(ctx, matcher); err != nil {
		e.t.Fatalf("WaitFor failed: %v", err)
	}
}
