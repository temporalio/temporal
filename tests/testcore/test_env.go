package testcore

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/historyrequire"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.uber.org/goleak"
)

// goleakViolationCounter is used to generate unique filenames for goleak violations.
var goleakViolationCounter atomic.Int64

// GoleakDir is the directory where goleak violation files are written in CI.
const GoleakDir = "/tmp/goleak_violations"

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
}

type TestOption func(*testOptions)

type testOptions struct {
	dedicatedCluster      bool
	dynamicConfigSettings []dynamicConfigOverride
	clusterOptions        []TestClusterOption
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

// WithClusterOptions passes TestClusterOption parameters to the cluster creation.
// This implies `WithDedicatedCluster` as cluster options require a fresh cluster.
func WithClusterOptions(clusterOpts ...TestClusterOption) TestOption {
	return func(o *testOptions) {
		o.dedicatedCluster = true
		o.clusterOptions = append(o.clusterOptions, clusterOpts...)
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

	base := testClusterPool.get(t, options.dedicatedCluster, startupConfig, options.clusterOptions)
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

	// Check for goroutine leaks at the end of the test.
	t.Cleanup(func() {
		// Ignore known background goroutines from the test infrastructure.
		opts := []goleak.Option{
			goleak.IgnoreTopFunction("go.temporal.io/server/common/clock.(*EventTimeSource).runUntilWallClock"),
			goleak.IgnoreTopFunction("go.temporal.io/server/common/membership.(*ringpop).Start.func1"),
			goleak.IgnoreTopFunction("go.temporal.io/server/temporal.(*ServerImpl).Start.func1"),
			goleak.IgnoreTopFunction("google.golang.org/grpc.(*addrConn).resetTransport"),
			goleak.IgnoreTopFunction("google.golang.org/grpc.(*ccBalancerWrapper).watcher"),
			goleak.IgnoreTopFunction("google.golang.org/grpc/internal/grpcsync.(*CallbackSerializer).run"),
			goleak.IgnoreTopFunction("google.golang.org/grpc/internal/transport.(*controlBuffer).get"),
			goleak.IgnoreTopFunction("google.golang.org/grpc/internal/transport.(*http2Client).keepalive"),
			goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
		}
		if err := goleak.Find(opts...); err != nil {
			// In CI, write violation to a file for later collection.
			if os.Getenv("CI") != "" {
				_ = os.MkdirAll(GoleakDir, 0755)
				// Sanitize test name for use as filename.
				safeName := strings.ReplaceAll(t.Name(), "/", "_")
				safeName = strings.ReplaceAll(safeName, " ", "_")
				filename := filepath.Join(GoleakDir, fmt.Sprintf("%03d_%s.txt",
					goleakViolationCounter.Add(1), safeName))
				content := fmt.Sprintf("Test: %s\n\n%v", t.Name(), err)
				_ = os.WriteFile(filename, []byte(content), 0644)
			}
			t.Errorf("goleak: goroutine leak detected:\n%v", err)
		}
	})

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
