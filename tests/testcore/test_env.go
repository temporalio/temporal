package testcore

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/testing/testvars"
	"google.golang.org/grpc"
)

// shardSalt is used to distribute functional tests across shards.
// This value is automatically updated by the optimize-test-sharding workflow.
//
//go:embed shard_salt.txt
var shardSalt string

var (
	_                  Env = (*TestEnv)(nil)
	defaultTestTimeout     = 90 * time.Second * debug.TimeoutMultiplier
)

type Env interface {
	// T returns the *testing.T. Deprecated: use the suite's T() method instead.
	T() *testing.T
	Namespace() namespace.Name
	NamespaceID() namespace.ID
	FrontendClient() workflowservice.WorkflowServiceClient
	AdminClient() adminservice.AdminServiceClient
	GetTestCluster() *TestCluster
	CloseShard(namespaceID string, workflowID string)
	OverrideDynamicConfig(setting dynamicconfig.GenericSetting, value any) (cleanup func())
	Context() context.Context
	InjectHook(hook testhooks.Hook) (cleanup func())
}

type TestEnv struct {
	*FunctionalTestBase

	// Shadows FunctionalTestBase.Assertions with a per-test instance bound to
	// this TestEnv's own *testing.T, avoiding data races when parallel tests
	// share the same *FunctionalTestBase cluster.
	// TODO: remove once all tests are migrated to TestEnv (and no longer use FunctionalTestBase directly).
	*require.Assertions

	Logger log.Logger

	cluster    *TestCluster
	nsName     namespace.Name
	nsID       namespace.ID
	taskPoller *taskpoller.TaskPoller
	t          *testing.T
	tv         *testvars.TestVars
	ctx        context.Context

	sdkClientOnce sync.Once
	sdkClient     sdkclient.Client
	sdkWorkerOnce sync.Once
	sdkWorker     sdkworker.Worker
	sdkWorkerTQ   string
}

type TestOption func(*testOptions)

type testOptions struct {
	dedicatedCluster      bool
	dynamicConfigSettings []dynamicConfigOverride
	timeout               time.Duration
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

// Deprecated: this option is no longer required and will be removed once all callers have been updated.
func WithSdkWorker() TestOption {
	return func(o *testOptions) {
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
// The TEMPORAL_TEST_TIMEOUT environment variable can also set the default timeout in seconds.
func WithTimeout(duration time.Duration) TestOption {
	return func(o *testOptions) {
		o.timeout = duration
	}
}

// NewEnv creates a new test environment with access to a Temporal cluster.
func NewEnv(t *testing.T, opts ...TestOption) *TestEnv {
	t.Helper()

	// Check test sharding early, before any expensive operations.
	checkTestShard(t)

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
	baseName := strings.ReplaceAll(t.Name(), "/", "-")
	ns := namespace.Name(RandomizeStr(baseName))
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

	env := &TestEnv{
		FunctionalTestBase: base,
		Assertions:         require.New(t),
		cluster:            cluster,
		nsName:             ns,
		nsID:               nsID,
		Logger:             base.Logger,
		taskPoller:         taskpoller.New(t, cluster.FrontendClient(), ns.String()),
		t:                  t,
		tv:                 testvars.New(t),
		ctx:                setupTestTimeoutWithContext(t, options.timeout),
		sdkWorkerTQ:        RandomizeStr("tq-" + t.Name()),
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
func (e *TestEnv) Namespace() namespace.Name {
	return e.nsName
}

func (e *TestEnv) NamespaceID() namespace.ID {
	return e.nsID
}

// InjectHook sets a test hook inside the cluster.
//
// It auto-detects the scope from the hook:
// - For namespace-scoped hooks: scopes it to the test's namespace
// - For global hooks: requires a dedicated cluster (fails early if used on shared cluster)
func (e *TestEnv) InjectHook(hook testhooks.Hook) (cleanup func()) {
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

func (e *TestEnv) TaskPoller() *taskpoller.TaskPoller {
	return e.taskPoller
}

// NoError asserts that err is nil.
// Deprecated: use require.NoError with the parent test or suite instead.
// TODO: remove once all tests are migrated to TestEnv (and no longer use FunctionalTestBase directly).
func (e *TestEnv) NoError(err error, msgAndArgs ...any) {
	e.Assertions.NoError(err, msgAndArgs...)
}

// Error asserts that err is not nil.
// Deprecated: use require.Error with the parent test or suite instead.
// TODO: remove once all tests are migrated to TestEnv (and no longer use FunctionalTestBase directly).
func (e *TestEnv) Error(err error, msgAndArgs ...any) {
	e.Assertions.Error(err, msgAndArgs...)
}

// Run executes a subtest.
// Deprecated: use the suite's Run method instead.
// TODO: remove once all tests are migrated to TestEnv (and no longer use FunctionalTestBase directly).
func (e *TestEnv) Run(name string, subtest func()) bool {
	return e.FunctionalTestBase.Run(name, subtest)
}

// T returns the *testing.T. Deprecated: use the suite's T() method instead.
func (e *TestEnv) T() *testing.T {
	return e.t
}

func (e *TestEnv) Tv() *testvars.TestVars {
	return e.tv
}

// Context returns the test-level timeout context with RPC version headers already included.
// This context will be canceled when the test timeout occurs. Use this directly for all RPC
// operations - no need to wrap with NewContext or add headers manually.
//
// For custom timeouts, use:
//
//	ctx, cancel := context.WithTimeout(env.Context(), 10*time.Second)
//	defer cancel()
func (e *TestEnv) Context() context.Context {
	return e.ctx
}

// SdkClient returns the SDK client. It is lazily initialized on the first call.
func (e *TestEnv) SdkClient() sdkclient.Client {
	e.sdkClientOnce.Do(func() {
		clientOptions := sdkclient.Options{
			HostPort:  e.FrontendGRPCAddress(),
			Namespace: e.nsName.String(),
			Logger:    log.NewSdkLogger(e.Logger),
		}

		if provider := e.cluster.host.tlsConfigProvider; provider != nil {
			clientOptions.ConnectionOptions.TLS = provider.FrontendClientConfig
		}

		if interceptor := e.cluster.host.grpcClientInterceptor; interceptor != nil {
			clientOptions.ConnectionOptions.DialOptions = []grpc.DialOption{
				grpc.WithUnaryInterceptor(interceptor.Unary()),
				grpc.WithStreamInterceptor(interceptor.Stream()),
			}
		}

		var err error
		e.sdkClient, err = sdkclient.Dial(clientOptions)
		if err != nil {
			e.t.Fatalf("Failed to create SDK client: %v", err)
		}
		e.t.Cleanup(func() { e.sdkClient.Close() })
	})
	return e.sdkClient
}

// SdkWorker returns the SDK worker. It is lazily initialized on the first call.
func (e *TestEnv) SdkWorker() sdkworker.Worker {
	e.sdkWorkerOnce.Do(func() {
		client := e.SdkClient() // Ensure client is initialized
		e.sdkWorker = sdkworker.New(client, e.sdkWorkerTQ, sdkworker.Options{})
		if err := e.sdkWorker.Start(); err != nil {
			e.t.Fatalf("Failed to start SDK worker: %v", err)
		}
		e.t.Cleanup(func() { e.sdkWorker.Stop() })
	})
	return e.sdkWorker
}

// WorkerTaskQueue returns the task queue name used by the SDK Worker.
func (e *TestEnv) WorkerTaskQueue() string {
	return e.sdkWorkerTQ
}

// OverrideDynamicConfig overrides a dynamic config setting for the duration of this test.
// For settings that can be namespace-scoped, a namespace constraint is applied.
// All others cannot be applied to a shared cluster and require `WithDedicatedCluster`.
func (e *TestEnv) OverrideDynamicConfig(setting dynamicconfig.GenericSetting, value any) (cleanup func()) {
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
	return e.cluster.host.overrideDynamicConfigForTest(e.t, setting.Key(), value)
}

// StartGlobalMetricCapture starts a cluster-global metrics capture for this test and automatically stops it during cleanup.
// Metric capture is cluster-global, so it is only safe on dedicated clusters.
// Misuse detection is best-effort and only applies to queried metrics that produced recordings.
func (e *TestEnv) StartGlobalMetricCapture() *GlobalMetricCapture {
	if e.isShared {
		e.t.Fatal("StartGlobalMetricCapture cannot be called on a shared cluster; use testcore.WithDedicatedCluster()")
	}

	handler := e.cluster.host.CaptureMetricsHandler()
	if handler == nil {
		e.t.Fatal("StartGlobalMetricCapture is unavailable because metrics capture is not enabled on this cluster")
	}

	capture := handler.StartCapture()
	globalCapture := newGlobalMetricCapture(capture)
	e.t.Cleanup(func() {
		defer handler.StopCapture(capture)
		globalCapture.checkForNamespaceCaptureMisuse()
	})
	return globalCapture
}

// StartNamespaceMetricCapture starts a metrics capture scoped to this test's namespace.
// Namespace captures are safe on shared clusters because reads are restricted to
// per-metric namespace-filtered iteration and reject non-namespaced metrics.
func (e *TestEnv) StartNamespaceMetricCapture() *NamespaceMetricCapture {
	return e.StartNamespaceMetricCaptureFor(e.Namespace().String())
}

// StartNamespaceMetricCaptureFor starts a metrics capture scoped to the provided namespace.
func (e *TestEnv) StartNamespaceMetricCaptureFor(namespaceName string) *NamespaceMetricCapture {
	handler := e.cluster.host.CaptureMetricsHandler()
	if handler == nil {
		e.t.Fatal("StartNamespaceMetricCapture is unavailable because metrics capture is not enabled on this cluster")
	}

	capture := handler.StartCapture()
	e.t.Cleanup(func() {
		handler.StopCapture(capture)
	})
	return newNamespaceMetricCapture(capture, namespaceName)
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

// checkTestShard supports test sharding based on environment variables.
// This distributes tests across multiple CI shards for parallel execution.
func checkTestShard(t *testing.T) {
	totalStr := os.Getenv("TEST_TOTAL_SHARDS")
	indexStr := os.Getenv("TEST_SHARD_INDEX")
	if totalStr == "" || indexStr == "" {
		return
	}
	total, err := strconv.Atoi(totalStr)
	if err != nil || total < 1 {
		t.Fatal("Couldn't convert TEST_TOTAL_SHARDS")
	}
	index, err := strconv.Atoi(indexStr)
	if err != nil || index < 0 || index >= total {
		t.Fatal("Couldn't convert TEST_SHARD_INDEX")
	}

	nameToHash := t.Name() + strings.TrimSpace(shardSalt)
	testIndex := int(farm.Fingerprint32([]byte(nameToHash))) % total
	if testIndex != index {
		t.Skipf("Skipping %s in test shard %d/%d (it runs in %d)", t.Name(), index+1, total, testIndex+1)
	}
	t.Logf("Running %s in test shard %d/%d", t.Name(), index+1, total)
}
