package testcore

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/historyrequire"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
)

var (
	_                  Env = (*testEnv)(nil)
	sequentialSuites   sync.Map
	defaultTestTimeout = 90 * time.Second * debug.TimeoutMultiplier
)

type Env interface {
	T() *testing.T
	Namespace() namespace.Name
	FrontendClient() workflowservice.WorkflowServiceClient
	GetTestCluster() *TestCluster
	CloseShard(namespaceID string, workflowID string)
	OverrideDynamicConfig(setting dynamicconfig.GenericSetting, value any) (cleanup func())
	Context() context.Context
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
	ctx        context.Context
}

type TestOption func(*testOptions)

type testOptions struct {
	dedicatedCluster      bool
	dynamicConfigSettings []dynamicConfigOverride
	timeout               time.Duration
	disableTimeout        bool
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

// WithTimeout sets a custom timeout for the test. The test will fail if it runs longer
// than this duration. The timeout is multiplied by debug.TimeoutMultiplier when debugging.
func WithTimeout(duration time.Duration) TestOption {
	return func(o *testOptions) {
		o.timeout = duration
	}
}

// WithoutTimeout disables the default test timeout.
func WithoutTimeout() TestOption {
	return func(o *testOptions) {
		o.disableTimeout = true
	}
}

// MustRunSequential marks a test suite to run its tests sequentially instead
// of in parallel. Call this at the start of your test suite before any
// subtests are created.
func MustRunSequential(t *testing.T, reason string) {
	if strings.Contains(t.Name(), "/") {
		panic("MustRunSequential must be called from a top-level test, not a subtest")
	}
	if reason == "" {
		panic("MustRunSequential requires a reason")
	}
	sequentialSuites.Store(t.Name(), true)
}

// NewEnv creates a new test environment with access to a Temporal cluster.
// Tests are run in parallel - use MustRunSequential to run suite sequentially.
func NewEnv(t *testing.T, opts ...TestOption) *testEnv {
	// Check if this is a sequential suite by looking up the parent test name.
	suiteName := t.Name()
	if idx := strings.Index(suiteName, "/"); idx != -1 {
		suiteName = suiteName[:idx]
	}
	if _, sequential := sequentialSuites.Load(suiteName); !sequential {
		t.Parallel()
	}

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

	// Obtain the test cluster from the pool.
	base := testClusterPool.get(t, options.dedicatedCluster, startupConfig)
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

	// Setup test timeout monitoring with context
	ctx := context.Background()
	if !options.disableTimeout {
		ctx = setupTestTimeoutWithContext(t, options.timeout)
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
		ctx:                ctx,
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

// Context returns the test-level timeout context. This context will be canceled
// when the test timeout occurs. Use this as the parent context for all operations.
//
// For RPC operations that need headers, use:
//	ctx, _ := rpc.NewContextFromParentWithTimeoutAndVersionHeaders(env.Context(), 90*time.Second)
//
// For custom timeouts, use:
//	ctx, cancel := context.WithTimeout(env.Context(), 10*time.Second)
//	defer cancel()
func (e *testEnv) Context() context.Context {
	return e.ctx
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

// calculateTimeout determines the appropriate timeout duration based on custom timeout,
// environment variable, test deadline, and default values. Returns 0 if timeout should be skipped.
//
// Priority order:
//  1. Custom timeout (via WithTimeout option)
//  2. TEMPORAL_TEST_TIMEOUT environment variable (in seconds)
//  3. Test deadline (via -timeout flag)
//  4. Default 90 seconds
func calculateTimeout(t *testing.T, customTimeout time.Duration) time.Duration {
	if customTimeout > 0 {
		// Use custom timeout if provided
		return customTimeout * debug.TimeoutMultiplier
	}

	// Check for environment variable
	if envTimeout := os.Getenv("TEMPORAL_TEST_TIMEOUT"); envTimeout != "" {
		if seconds, err := strconv.Atoi(envTimeout); err == nil && seconds > 0 {
			return time.Duration(seconds) * time.Second * debug.TimeoutMultiplier
		}
	}

	deadline, hasDeadline := t.Deadline()
	if hasDeadline {
		// Leave 30 second buffer before hard deadline so cleanup can run
		timeout := time.Until(deadline) - (30 * time.Second)
		if timeout <= 0 {
			// Already close to deadline, skip timeout monitoring
			return 0
		}
		return timeout
	}

	return defaultTestTimeout
}

// setupTestTimeoutWithContext creates a context that will be canceled on timeout,
// and reports the timeout error during cleanup. Returns a context that tests can
// use to be interrupted when timeout occurs.
func setupTestTimeoutWithContext(t *testing.T, customTimeout time.Duration) context.Context {
	t.Helper()

	timeout := calculateTimeout(t, customTimeout)
	if timeout <= 0 {
		return context.Background()
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	var timedOut atomic.Bool
	go func() {
		<-ctx.Done()
		if ctx.Err() == context.DeadlineExceeded {
			timedOut.Store(true)
		}
	}()

	// Register cleanup to cancel context and check timeout
	// t.Cleanup() functions run in LIFO order, so this runs after test code
	t.Cleanup(func() {
		cancel()

		if timedOut.Load() {
			t.Errorf("Test exceeded timeout of %v", timeout)
		}
	})

	return ctx
}
