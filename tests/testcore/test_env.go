package testcore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/historyrequire"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/testing/testvars"
)

var _ Env = (*testEnv)(nil)

type Env interface {
	T() *testing.T
	Namespace() namespace.Name
	NamespaceID() namespace.ID
	FrontendClient() workflowservice.WorkflowServiceClient
	GetTestCluster() *TestCluster
	CloseShard(namespaceID string, workflowID string)
	OverrideDynamicConfig(setting dynamicconfig.GenericSetting, value any) (cleanup func())
	InjectHook(hook testhooks.Hook) (cleanup func())
}

type testEnv struct {
	*FunctionalTestBase
	*require.Assertions
	historyrequire.HistoryRequire

	Logger log.Logger

	cluster    *TestCluster
	nsName     namespace.Name
	nsID       namespace.ID
	taskPoller *taskpoller.TaskPoller
	t          *testing.T
	tv         *testvars.TestVars
}

type TestOption func(*testOptions)

type testOptions struct {
	dedicatedCluster      bool
	dynamicConfigSettings []dynamicConfigOverride
}

type dynamicConfigOverride struct {
	setting dynamicconfig.GenericSetting
	value   any
}

// WithDedicatedCluster requests a dedicated (non-shared) cluster for the test.
// Use this for tests that have cluster-global side effects.
func WithDedicatedCluster() TestOption {
	return func(o *testOptions) {
		o.dedicatedCluster = true
	}
}

// WithDynamicConfig overrides a dynamic config setting for the test.
// For settings that can be namespace-scoped, a namespace constraint is applied.
// For all others that require a dedicated cluster, this implies `WithDedicatedCluster`.
func WithDynamicConfig(setting dynamicconfig.GenericSetting, value any) TestOption {
	return func(o *testOptions) {
		if err := setting.Validate(value); err != nil {
			panic(fmt.Sprintf("invalid value for setting %s: %v", setting.Key(), err))
		}
		if !canBeNamespaceScoped(setting.Precedence()) {
			o.dedicatedCluster = true
		}
		o.dynamicConfigSettings = append(o.dynamicConfigSettings, dynamicConfigOverride{setting: setting, value: value})
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

	// For dedicated clusters, pass all dynamic config settings at cluster creation.
	var startupConfig map[dynamicconfig.Key]any
	if options.dedicatedCluster && len(options.dynamicConfigSettings) > 0 {
		startupConfig = make(map[dynamicconfig.Key]any, len(options.dynamicConfigSettings))
		for _, override := range options.dynamicConfigSettings {
			startupConfig[override.setting.Key()] = override.value
		}
	}

	base := testClusterPool.get(t, options.dedicatedCluster, startupConfig)
	cluster := base.GetTestCluster()

	// Create a dedicated namespace for the test to help with test isolation.
	ns := namespace.Name(RandomizeStr(t.Name()))
	nsID, err := base.RegisterNamespace(
		ns,
		1, // 1 day retention
		enumspb.ARCHIVAL_STATE_DISABLED,
		"",
		"",
	)
	if err != nil {
		t.Fatalf("Failed to register namespace: %v", err)
	}

	env := &testEnv{
		FunctionalTestBase: base,
		Assertions:         require.New(t),
		HistoryRequire:     historyrequire.New(t),
		cluster:            cluster,
		nsName:             ns,
		nsID:               nsID,
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

	return env
}

// Use test env-specific namespace here for test isolation.
func (e *testEnv) Namespace() namespace.Name {
	return e.nsName
}

func (e *testEnv) NamespaceID() namespace.ID {
	return e.nsID
}

// InjectHook sets a test hook inside the cluster.
//
// It auto-detects the scope from the hook:
// - For namespace-scoped hooks: scopes it to the test's namespace
// - For global hooks: requires a dedicated cluster (fails early if used on shared cluster)
func (e *testEnv) InjectHook(hook testhooks.Hook) (cleanup func()) {
	var scope any
	switch hook.Scope() {
	case testhooks.ScopeNamespace:
		scope = e.nsID
	case testhooks.ScopeGlobal:
		if e.isShared {
			e.t.Fatal("InjectHook: global hooks require a dedicated cluster; use testcore.WithDedicatedCluster()")
		}
		scope = testhooks.GlobalScope
	default:
		e.t.Fatalf("InjectHook: unknown scope %v", hook.Scope())
	}
	return e.cluster.host.injectHook(e.t, hook, scope)
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
