package testcore

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/historyrequire"
	"go.temporal.io/server/common/testing/taskpoller"
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

	cluster    *TestCluster
	nsName     namespace.Name
	taskPoller *taskpoller.TaskPoller
	t          *testing.T
	tv         *testvars.TestVars
	timeSource *clock.EventTimeSource
}

type EnvOption func(*envOptions)

type envOptions struct {
	dedicatedCluster      bool
	dynamicConfigSettings []dynamicConfigOverride
	fakeTimeSource        *clock.EventTimeSource // non-nil when using fake time
}

type dynamicConfigOverride struct {
	setting dynamicconfig.GenericSetting
	value   any
}

// WithDedicatedCluster requests a dedicated (non-shared) cluster for the test.
// Use this for tests that have cluster-global side effects.
func WithDedicatedCluster() EnvOption {
	return func(o *envOptions) {
		o.dedicatedCluster = true
	}
}

// WithDynamicConfig overrides a dynamic config setting for the test.
// For settings that can be namespace-scoped, a namespace constraint is applied.
// For all others that require a dedicated cluster, this implies `WithDedicatedCluster`.
func WithDynamicConfig(setting dynamicconfig.GenericSetting, value any) EnvOption {
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

// WithFakeTime configures the test to use an EventTimeSource for controlling time.
// This enables tests to advance time without waiting for real time to pass.
// Implies `WithDedicatedCluster` since time source is global to the cluster.
//
// Usage:
//
//	s := NewEnv(t, WithFakeTime())
//	// ... start workflow ...
//	s.AdvanceTime(5 * time.Second) // Advance fake time
func WithFakeTime() EnvOption {
	return func(o *envOptions) {
		o.dedicatedCluster = true
		o.fakeTimeSource = clock.NewEventTimeSource()
		o.fakeTimeSource.UseAsyncTimers(true)
	}
}

// NewEnv creates a new test environment with access to a Temporal cluster.
// The test is automatically marked as parallel.
func NewEnv(t *testing.T, opts ...EnvOption) *testEnv {
	t.Parallel()

	var options envOptions
	for _, opt := range opts {
		opt(&options)
	}

	// Initialize fake time source with current wall clock time if using fake time
	if options.fakeTimeSource != nil {
		options.fakeTimeSource.Update(time.Now().UTC())
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
		timeSource:         options.fakeTimeSource, // Only set if using fake time
	}

	// For shared clusters, apply all dynamic config settings as overrides.
	if !options.dedicatedCluster && len(options.dynamicConfigSettings) > 0 {
		for _, override := range options.dynamicConfigSettings {
			env.OverrideDynamicConfig(override.setting, override.value)
		}
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

// AdvanceTime advances the fake time source by the given duration.
// Only works when the test was created with WithFakeTime().
// After advancing time, goroutines need a small amount of real time to process.
func (e *testEnv) AdvanceTime(d time.Duration) {
	if e.timeSource == nil {
		e.t.Fatal("AdvanceTime called but test was not created with WithFakeTime()")
	}
	e.t.Logf("AdvanceTime: advancing time by %v", d)
	e.timeSource.Advance(d)
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
