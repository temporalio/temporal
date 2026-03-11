package testcore

import (
	"cmp"
	"context"
	_ "embed"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
	"go.opentelemetry.io/otel/trace"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/historyrequire"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/testing/testtelemetry"
	"go.temporal.io/server/common/testing/testvars"
	"google.golang.org/grpc"
)

// shardSalt is used to distribute functional tests across shards.
// This value is automatically updated by the optimize-test-sharding workflow.
//
//go:embed shard_salt.txt
var shardSalt string

var (
	_                  Env = (*testEnv)(nil)
	sequentialSuites   sync.Map
	defaultTestTimeout = 90 * time.Second * debug.TimeoutMultiplier
)

type Env interface {
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
	ctx        context.Context

	sdkClient       sdkclient.Client
	worker          sdkworker.Worker
	workerTaskQueue string
}

type TestOption func(*testOptions)

type testOptions struct {
	dedicatedCluster      bool
	dynamicConfigSettings []dynamicConfigOverride
	timeout               time.Duration
	sdkWorker             bool
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

// WithSdkWorker sets up an SDK client and worker for the test.
// Cleanup is handled automatically via t.Cleanup().
func WithSdkWorker() TestOption {
	return func(o *testOptions) {
		o.sdkWorker = true
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

// sequentialSuite holds state for a suite marked with MustRunSequential.
// It manages a single dedicated cluster shared by all tests in the suite.
type sequentialSuite struct {
	cluster *FunctionalTestBase
}

// MustRunSequential marks a test suite to run its tests sequentially instead
// of in parallel. Call this at the start of your test suite before any
// subtests are created. A single dedicated cluster will be created for this
// suite and torn down when the suite completes.
func MustRunSequential(t *testing.T, reason string) {
	if strings.Contains(t.Name(), "/") {
		panic("MustRunSequential must be called from a top-level test, not a subtest")
	}
	if reason == "" {
		panic("MustRunSequential requires a reason")
	}

	// Create a dedicated cluster for this suite.
	suite := &sequentialSuite{
		cluster: testClusterPool.createCluster(t, nil, false),
	}
	sequentialSuites.Store(t.Name(), suite)

	// Register cleanup to tear down the suite's cluster when the parent test completes.
	t.Cleanup(func() {
		sequentialSuites.Delete(t.Name())
		if err := suite.cluster.testCluster.TearDownCluster(); err != nil {
			t.Logf("Failed to tear down sequential suite cluster: %v", err)
		}
	})
}

// NewEnv creates a new test environment with access to a Temporal cluster.
//
// By default, tests are marked as parallel. Use MustRunSequential on the
// test's parent `testing.T` to run them sequentially instead.
func NewEnv(t *testing.T, opts ...TestOption) *testEnv {
	// Check test sharding early, before any expensive operations.
	checkTestShard(t)

	// Check if this is a sequential suite by looking up the parent test name.
	suiteName := t.Name()
	if idx := strings.Index(suiteName, "/"); idx != -1 {
		suiteName = suiteName[:idx]
	}
	suiteVal, sequential := sequentialSuites.Load(suiteName)
	if !sequential {
		t.Parallel()
	}

	var options testOptions
	for _, opt := range opts {
		opt(&options)
	}

	var base *FunctionalTestBase
	if sequential {
		// Sequential suites use a single dedicated cluster for all tests.
		suite := suiteVal.(*sequentialSuite)
		base = suite.cluster
		base.SetT(t)
	} else {
		// For dedicated clusters, pass all dynamic config settings at cluster creation.
		var startupConfig map[dynamicconfig.Key]any
		if options.dedicatedCluster && len(options.dynamicConfigSettings) > 0 {
			startupConfig = make(map[dynamicconfig.Key]any, len(options.dynamicConfigSettings))
			for _, override := range options.dynamicConfigSettings {
				startupConfig[override.setting.Key()] = override.value
			}
		}

		// Obtain the test cluster from the pool.
		base = testClusterPool.get(t, options.dedicatedCluster, startupConfig)
	}
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

	// Set up per-test OTEL tracing scoped to this namespace.
	var assertT require.TestingT = t
	if base.spanRouter != nil {
		assertT = setupTestOTEL(t, base.spanRouter, ns.String())
	}

	env := &testEnv{
		FunctionalTestBase: base,
		Assertions:         require.New(assertT),
		HistoryRequire:     historyrequire.New(assertT),
		cluster:            cluster,
		nsName:             ns,
		nsID:               nsID,
		Logger:             base.Logger,
		taskPoller:         taskpoller.New(t, cluster.FrontendClient(), ns.String()),
		t:                  t,
		tv:                 testvars.New(t),
		ctx:                setupTestTimeoutWithContext(t, options.timeout),
	}

	// For shared clusters, apply all dynamic config settings as overrides.
	if !options.dedicatedCluster && len(options.dynamicConfigSettings) > 0 {
		for _, override := range options.dynamicConfigSettings {
			env.OverrideDynamicConfig(override.setting, override.value)
		}
	}

	if options.sdkWorker {
		env.setupSdk()
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

// Context returns the test-level timeout context with RPC version headers already included.
// This context will be canceled when the test timeout occurs. Use this directly for all RPC
// operations - no need to wrap with NewContext or add headers manually.
//
// For custom timeouts, use:
//
//	ctx, cancel := context.WithTimeout(env.Context(), 10*time.Second)
//	defer cancel()
func (e *testEnv) Context() context.Context {
	return e.ctx
}

// SdkClient returns the SDK client created by WithSdkWorker.
// Panics if WithSdkWorker was not passed to NewEnv.
func (e *testEnv) SdkClient() sdkclient.Client {
	if e.sdkClient == nil {
		panic("SdkClient() requires WithSdkWorker option to be passed to NewEnv")
	}
	return e.sdkClient
}

// SdkWorker returns the SDK worker created by WithSdkWorker.
// Panics if WithSdkWorker was not passed to NewEnv.
func (e *testEnv) SdkWorker() sdkworker.Worker {
	if e.worker == nil {
		panic("SdkWorker() requires WithSdkWorker option to be passed to NewEnv")
	}
	return e.worker
}

// WorkerTaskQueue returns the task queue name used by the SDK Worker.
// Panics if WithSdkWorker was not passed to NewEnv.
func (e *testEnv) WorkerTaskQueue() string {
	if e.workerTaskQueue == "" {
		panic("WorkerTaskQueue() requires WithSdkWorker option to be passed to NewEnv")
	}
	return e.workerTaskQueue
}

func (e *testEnv) setupSdk() {
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

	e.workerTaskQueue = RandomizeStr(e.t.Name())
	e.worker = sdkworker.New(e.sdkClient, e.workerTaskQueue, sdkworker.Options{})
	if err = e.worker.Start(); err != nil {
		e.t.Fatalf("Failed to start SDK worker: %v", err)
	}
	e.t.Cleanup(func() { e.worker.Stop() })
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

// setupTestOTEL creates per-test OTEL tracing scoped to the given namespace.
// It creates a SpanRecorder that receives only spans for this namespace,
// a "Client" span for recording assertion failure events, and returns a
// TestingT wrapper that intercepts assertion failures.
// On test failure, collected traces are written to a file.
func setupTestOTEL(t *testing.T, router *testtelemetry.SpanRouter, ns string) *testtelemetry.AssertT {
	// Create a per-test recorder and register it with the router for this namespace.
	recorder := testtelemetry.NewSpanRecorder()
	unregister := router.Register(ns, recorder)

	// Use a SimpleSpanProcessor so the span is exported synchronously on End().
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(recorder)),
		sdktrace.WithResource(resource.NewSchemaless(
			semconv.ServiceNameKey.String("io.temporal.test"),
		)),
	)
	tracer := tp.Tracer("io.temporal.test")
	_, clientSpan := tracer.Start(
		context.Background(),
		t.Name(),
		trace.WithAttributes(attribute.String(telemetry.NamespaceKey, ns)),
	)

	// Wrap t to intercept assertion failures and record them on the client span.
	assertT := testtelemetry.NewAssertT(t, clientSpan)

	t.Cleanup(func() {
		// End the client span, marking it as error if the test failed.
		if t.Failed() {
			clientSpan.SetAttributes(attribute.Bool("test.failed", true))
		}
		clientSpan.End()

		// Stop receiving new spans from the router.
		unregister()

		// Write traces to file on test failure.
		if t.Failed() {
			outDir := cmp.Or(os.Getenv("TEMPORAL_TEST_OTEL_OUTPUT"), "testdata/otel")
			var validFilenameChars = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)
			fileName := validFilenameChars.ReplaceAllString(t.Name(), "-")
			fileName = fmt.Sprintf("traces.%s_%d.json", fileName, time.Now().Unix())
			if filePath, err := recorder.WriteToDir(outDir, fileName); err != nil {
				t.Logf("unable to write OTEL traces: %v", err)
			} else {
				t.Logf("wrote OTEL traces to %s", filePath)
			}
		}
		_ = recorder.Shutdown(context.Background())
	})

	return assertT
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
