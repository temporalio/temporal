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

	"github.com/dgryski/go-farm"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/temporal"
)

// shardSalt is used to distribute functional tests across shards.
// This value is automatically updated by the optimize-test-sharding workflow.
//
//go:embed shard_salt.txt
var shardSalt string

var _ Env = (*TestEnv)(nil)

type Env interface {
	// T returns the *testing.T.
	//
	// Deprecated: use the suite's T() method instead.
	T() *testing.T
	Namespace() namespace.Name
	NamespaceID() namespace.ID
	FrontendClient() workflowservice.WorkflowServiceClient
	AdminClient() adminservice.AdminServiceClient
	GetTestCluster() *TestCluster
	CloseShard(namespaceID string, workflowID string)
	OverrideDynamicConfig(setting dynamicconfig.GenericSetting, value any) (cleanup func())
	// Deprecated: use the suite's Context() method instead.
	Context() context.Context
	InjectHook(hook testhooks.Hook) (cleanup func())
}

type TestEnv struct {
	*FunctionalTestBase

	// Shadows FunctionalTestBase.Assertions with a per-test instance bound to
	// this TestEnv's own *testing.T.
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
	needWorkerService        bool
	reason                   string
	disableTestloggerFailure bool
	dynamicConfigSettings    []dynamicConfigOverride
	clusterOptions           []TestClusterOption
	testVars                 func(*testvars.TestVars) *testvars.TestVars
	historyTaskRecorder      bool
}

type dynamicConfigOverride struct {
	setting dynamicconfig.GenericSetting
	value   any
}

type versionHeadersContextKey struct{}

// WithDisableTestloggerFailure disables the test logger's behavior of failing
// the test when an error log matches a registered expectation (e.g. soft-assert
// errors tagged with tag.FailedAssertion). Use for tests that intentionally
// trigger and then verify soft-assert errors.
func WithDisableTestloggerFailure() TestOption {
	return func(o *testOptions) {
		o.disableTestloggerFailure = true
		o.reason = "testlogger failures disabled"
	}
}

// Deprecated: this option is no longer required and will be removed once all callers have been updated.
func WithSdkWorker() TestOption {
	return func(o *testOptions) {
	}
}

// WithTestVars customizes the default test variables for the environment.
func WithTestVars(fn func(*testvars.TestVars) *testvars.TestVars) TestOption {
	return func(o *testOptions) {
		o.testVars = fn
	}
}

// WithWorkerService enables the system worker service. The service is off by
// default to avoid the worker overhead.
func WithWorkerService(reason string) TestOption {
	return func(o *testOptions) {
		o.needWorkerService = true
		o.reason = "worker service required: " + reason
	}
}

// WithMTLS enables mutual TLS on the test's cluster.
func WithMTLS() TestOption {
	return func(o *testOptions) {
		o.clusterOptions = append(o.clusterOptions, withMTLS())
		o.reason = "mTLS enabled"
	}
}

// WithPersistenceFaultInjection configures persistence fault injection for the test cluster.
func WithPersistenceFaultInjection(cfg *config.FaultInjection) TestOption {
	return func(o *testOptions) {
		o.clusterOptions = append(o.clusterOptions, WithFaultInjectionConfig(cfg))
		o.reason = "fault injection config used"
	}
}

// WithArchival enables archival on the test's cluster.
func WithArchival() TestOption {
	return func(o *testOptions) {
		o.clusterOptions = append(o.clusterOptions, withArchivalConfig())
		o.reason = "archival enabled"
	}
}

// WithCustomArchivers configures custom history and visibility archiver factories
// on the test's cluster.
func WithCustomArchivers(historyFactory provider.CustomHistoryArchiverFactory, visibilityFactory provider.CustomVisibilityArchiverFactory) TestOption {
	return func(o *testOptions) {
		o.clusterOptions = append(o.clusterOptions, func(params *testClusterParams) {
			params.AdditionalServerOptions = append(params.AdditionalServerOptions,
				temporal.WithCustomHistoryArchiverFactory(historyFactory),
				temporal.WithCustomVisibilityArchiverFactory(visibilityFactory),
			)
		})
		o.reason = "custom archivers used"
	}
}

// WithLogger sets a custom logger for the test's cluster, letting a test intercept
// server log output.
func WithLogger(logger log.Logger) TestOption {
	return func(o *testOptions) {
		o.clusterOptions = append(o.clusterOptions, WithClusterLogger(logger))
		o.reason = "custom logger used"
	}
}

// WithHistoryShardCount sets the number of history shards for the test's cluster.
func WithHistoryShardCount(n int32) TestOption {
	return func(o *testOptions) {
		o.clusterOptions = append(o.clusterOptions, WithNumHistoryShards(n))
		o.reason = "custom history shard count used"
	}
}

func WithHistoryTaskRecorder() TestOption {
	return func(o *testOptions) {
		o.clusterOptions = append(o.clusterOptions, WithClusterHistoryTaskRecorder())
		o.reason = "task queue recorder used"
		o.historyTaskRecorder = true
	}
}

// WithDynamicConfig overrides a dynamic config setting for the test cluster.
func WithDynamicConfig(setting dynamicconfig.GenericSetting, value any) TestOption {
	return func(o *testOptions) {
		if err := setting.Validate(value); err != nil {
			panic(fmt.Sprintf("invalid value for setting %s: %v", setting.Key(), err))
		}
		o.dynamicConfigSettings = append(o.dynamicConfigSettings, dynamicConfigOverride{setting: setting, value: value})
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
	startupConfig := make(map[dynamicconfig.Key]any, len(options.dynamicConfigSettings))
	for _, override := range options.dynamicConfigSettings {
		startupConfig[override.setting.Key()] = override.value
	}

	base := testClusterRouter.get(t, clusterRequest{
		reason:            options.reason,
		needWorkerService: options.needWorkerService,
		dynamicConfig:     startupConfig,
		clusterOpts:       options.clusterOptions,
	})
	cluster := base.GetTestCluster()

	ns := base.Namespace()
	nsID := base.NamespaceID()

	tv := testvars.New(t)
	if options.testVars != nil {
		tv = options.testVars(tv)
	}

	// Attach version headers decorator to the test context.
	testcontext.AttachDecorator(t, versionHeadersContextKey{}, headers.SetVersions)

	env := &TestEnv{
		FunctionalTestBase: base,
		Assertions:         require.New(t),
		cluster:            cluster,
		nsName:             ns,
		nsID:               nsID,
		Logger:             base.Logger,
		taskPoller:         taskpoller.New(t, cluster.FrontendClient(), ns.String()),
		t:                  t,
		tv:                 tv,
		ctx:                testcontext.For(t),
		sdkWorkerTQ:        RandomizeStr("tq-" + t.Name()),
	}

	if options.disableTestloggerFailure {
		tl, ok := base.Logger.(*testlogger.TestLogger)
		if !ok {
			t.Fatalf("WithDisableTestloggerFailure requires a *testlogger.TestLogger logger, got %T", base.Logger)
		}
		prev := tl.FailOnError(false)
		t.Cleanup(func() { tl.FailOnError(prev) })
	}

	if options.historyTaskRecorder {
		recorder := cluster.GetHistoryTaskRecorder()
		require.NotNil(t, recorder)
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
// - For global hooks: scopes it to the test-owned cluster.
func (e *TestEnv) InjectHook(hook testhooks.Hook) (cleanup func()) {
	var scope any
	switch hook.Scope() {
	case testhooks.ScopeNamespace:
		scope = e.nsID
	case testhooks.ScopeGlobal:
		scope = testhooks.GlobalScope
	default:
		e.t.Fatalf("InjectHook: unknown scope %v", hook.Scope())
	}
	return e.cluster.host.injectHook(e.t, hook, scope)
}

func (e *TestEnv) SetOnAuthorize(
	fn func(context.Context, *authorization.Claims, *authorization.CallTarget) (authorization.Result, error),
) {
	e.t.Helper()
	e.cluster.host.SetOnAuthorize(fn)
	e.t.Cleanup(func() {
		e.cluster.host.SetOnAuthorize(nil)
	})
}

func (e *TestEnv) SetOnGetClaims(fn func(*authorization.AuthInfo) (*authorization.Claims, error)) {
	e.t.Helper()
	e.cluster.host.SetOnGetClaims(fn)
	e.t.Cleanup(func() {
		e.cluster.host.SetOnGetClaims(nil)
	})
}

func (e *TestEnv) TaskPoller() *taskpoller.TaskPoller {
	return e.taskPoller
}

// NoError asserts that err is nil.
//
// Deprecated: use require.NoError with the parent test or suite instead.
// TODO: remove once all tests are migrated to TestEnv (and no longer use FunctionalTestBase directly).
func (e *TestEnv) NoError(err error, msgAndArgs ...any) {
	e.Assertions.NoError(err, msgAndArgs...)
}

// Error asserts that err is not nil.
//
// Deprecated: use require.Error with the parent test or suite instead.
// TODO: remove once all tests are migrated to TestEnv (and no longer use FunctionalTestBase directly).
func (e *TestEnv) Error(err error, msgAndArgs ...any) {
	e.Assertions.Error(err, msgAndArgs...)
}

// Run executes a subtest.
//
// Deprecated: use the suite's Run method instead.
// TODO: remove once all tests are migrated to TestEnv (and no longer use FunctionalTestBase directly).
func (e *TestEnv) Run(name string, subtest func()) bool {
	return e.FunctionalTestBase.Run(name, subtest)
}

// T returns the *testing.T.
//
// Deprecated: use the suite's T() method instead.
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
//
// Deprecated: use the suite's Context() method instead.
func (e *TestEnv) Context() context.Context {
	return e.ctx
}

// WaitForChannel waits for ch to receive using the TestEnv context.
func (e *TestEnv) WaitForChannel(ch <-chan struct{}) {
	e.t.Helper()
	select {
	case <-ch:
	case <-e.ctx.Done():
		e.FailNow("context timeout while waiting for channel")
	}
}

// SendToChannel sends to ch using the TestEnv context.
func (e *TestEnv) SendToChannel(ch chan<- struct{}) {
	e.t.Helper()
	select {
	case ch <- struct{}{}:
	case <-e.ctx.Done():
		e.FailNow("context timeout while sending to channel")
	}
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

		client, err := sdkclient.Dial(clientOptions)
		if err != nil {
			e.t.Fatalf("Failed to create SDK client: %v", err)
		}
		e.sdkClient = client
		e.t.Cleanup(func() {
			client.Close()
			client = nil
		})
	})
	return e.sdkClient
}

// SdkWorker returns the SDK worker. It is lazily initialized on the first call.
func (e *TestEnv) SdkWorker() sdkworker.Worker {
	e.sdkWorkerOnce.Do(func() {
		client := e.SdkClient() // Ensure client is initialized
		worker := sdkworker.New(client, e.sdkWorkerTQ, sdkworker.Options{})
		if err := worker.Start(); err != nil {
			e.t.Fatalf("Failed to start SDK worker: %v", err)
		}
		e.sdkWorker = worker
		e.t.Cleanup(func() {
			worker.Stop()
			worker = nil
		})
	})
	return e.sdkWorker
}

// WorkerTaskQueue returns the task queue name used by the SDK Worker.
func (e *TestEnv) WorkerTaskQueue() string {
	return e.sdkWorkerTQ
}

// OverrideDynamicConfig overrides a dynamic config setting for the duration of this test.
func (e *TestEnv) OverrideDynamicConfig(setting dynamicconfig.GenericSetting, value any) (cleanup func()) {
	return e.cluster.host.overrideDynamicConfigForTest(e.t, setting.Key(), value)
}

// StartGlobalMetricCapture starts a cluster-global metrics capture for this test and automatically stops it during cleanup.
// Misuse detection is best-effort and only applies to queried metrics that produced recordings.
func (e *TestEnv) StartGlobalMetricCapture() *GlobalMetricCapture {
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

// CloseShard closes the shard that contains the given workflow.
func (e *TestEnv) CloseShard(namespaceID string, workflowID string) {
	shardID := common.WorkflowIDToHistoryShard(namespaceID, workflowID, e.testClusterConfig.HistoryConfig.NumHistoryShards)
	_, err := e.AdminClient().CloseShard(NewContext(), &adminservice.CloseShardRequest{
		ShardId: shardID,
	})
	e.NoError(err)
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
