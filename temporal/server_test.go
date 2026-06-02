package temporal_test

import (
	"context"
	"fmt"
	"math"
	"path"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence/serialization"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite" // needed to register the sqlite plugin
	"go.temporal.io/server/common/testing/testtelemetry"
	"go.temporal.io/server/service/frontend"
	"go.temporal.io/server/temporal"
	"go.temporal.io/server/tests/testutils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestNewServer verifies that NewServer doesn't cause any fx errors, and that there are no unexpected error logs after
// running for a few seconds.
func TestNewServer(t *testing.T) {
	runAndTestServer(t)
}

// TestNewServerWithOTEL verifies that NewServer doesn't cause any fx errors when OTEL is enabled.
func TestNewServerWithOTEL(t *testing.T) {
	t.Setenv("OTEL_TRACES_EXPORTER", "otlp")
	t.Setenv("OTEL_BSP_SCHEDULE_DELAY", "100")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_INSECURE", "true")

	t.Run("with OTEL Collector running", func(t *testing.T) {
		collector, err := testtelemetry.StartMemoryCollector(t)
		require.NoError(t, err)
		t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", collector.Addr())
		runAndTestServer(t)
		require.NotEmpty(t, collector.Spans(), "expected at least one OTEL span")
	})

	t.Run("without OTEL Collector running", func(t *testing.T) {
		runAndTestServer(t)
	})
}

// TestNewServerWithJSONEncoding verifies that NewServer works when JSON encoding is enabled.
func TestNewServerWithJSONEncoding(t *testing.T) {
	t.Setenv(serialization.SerializerDataEncodingEnvVar, enumspb.ENCODING_TYPE_JSON.String())
	runAndTestServer(t)
}

func runAndTestServer(t *testing.T) {
	t.Helper()
	cfg := loadConfig(t)

	logDetector := newErrorLogDetector(t, log.NewTestLogger())
	logDetector.Start()
	t.Cleanup(func() {
		logDetector.Stop()
	})

	// Tweak matching to ensure tasks are written to persistence immediately.
	dcClient := dynamicconfig.StaticClient{
		dynamicconfig.MatchingSyncMatchWaitDuration.Key():       time.Duration(0),
		dynamicconfig.MatchingNumTaskqueueWritePartitions.Key(): 1,
		dynamicconfig.MatchingNumTaskqueueReadPartitions.Key():  1,
	}

	server, err := temporal.NewServer(
		temporal.ForServices(temporal.DefaultServices),
		temporal.WithConfig(cfg),
		temporal.WithLogger(logDetector),
		temporal.WithChainedFrontendGrpcInterceptors(getFrontendInterceptors()),
		temporal.WithDynamicConfigClient(dcClient),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, server.Stop())
	})
	require.NoError(t, server.Start())

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	// Create SDK client
	frontendHostPort := fmt.Sprintf("127.0.0.1:%d", cfg.Services["frontend"].RPC.GRPCPort)
	namespace := "test-" + common.GenerateRandomString(8)
	c, err := client.Dial(client.Options{
		HostPort:  frontendHostPort,
		Namespace: namespace,
	})
	require.NoError(t, err)
	defer c.Close()

	// Register the namespace.
	_, err = c.WorkflowService().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	})
	require.NoError(t, err)

	// Start workflow.
	taskQueue := "test-task-queue"
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: taskQueue}, SimpleWorkflow)
	require.NoError(t, err)

	// Check that the workflow task was backlogged (to test task persistence).
	adminConn, err := grpc.NewClient(frontendHostPort, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = adminConn.Close() }()
	adminClient := adminservice.NewAdminServiceClient(adminConn)
	assert.Eventually(t, func() bool {
		response, err := adminClient.GetTaskQueueTasks(ctx, &adminservice.GetTaskQueueTasksRequest{
			Namespace:     namespace,
			TaskQueue:     taskQueue,
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			MinTaskId:     0,
			MaxTaskId:     math.MaxInt64,
			BatchSize:     10,
		})
		if err != nil {
			return false
		}
		return len(response.Tasks) > 0
	}, 20*time.Second, 100*time.Millisecond)

	// Start worker.
	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterWorkflow(SimpleWorkflow)
	err = w.Start()
	require.NoError(t, err)
	defer w.Stop()

	// Wait for the workflow to complete.
	var result string
	err = run.Get(ctx, &result)
	require.NoError(t, err)
	assert.Equal(t, "Hello World", result)
}

func loadConfig(t *testing.T) *config.Config {
	cfg := loadSQLiteConfig(t)
	setTestPorts(cfg)
	return cfg
}

// loadSQLiteConfig loads the config for the sqlite persistence store. We use sqlite because it doesn't require any
// external dependencies, so it's easy to run this test in isolation.
func loadSQLiteConfig(t *testing.T) *config.Config {
	configDir := path.Join(testutils.GetRepoRootDirectory(), "config")
	cfg, err := config.Load(
		config.WithEnv("development-sqlite"),
		config.WithConfigDir(configDir),
	)
	require.NoError(t, err)

	cfg.DynamicConfigClient.Filepath = path.Join(configDir, "dynamicconfig", "development-sql.yaml")

	return cfg
}

// setTestPorts sets the ports of all services to something different from the default ports.
func setTestPorts(cfg *config.Config) {
	port := 10000

	// The prometheus reporter does not shut down in-between test runs.
	// This will assign a random port to the prometheus reporter,
	// so that it doesn't conflict with other tests.
	cfg.Global.Metrics.Prometheus.ListenAddress = ":0"

	for k, v := range cfg.Services {
		v.RPC.GRPCPort = port
		port++

		v.RPC.MembershipPort = port
		port++

		v.RPC.HTTPPort = port
		port++

		cfg.Services[k] = v
	}

	cfg.Global.PProf.Port = port
}

func getFrontendInterceptors() func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		switch info.Server.(type) {
		case *frontend.Handler, *frontend.OperatorHandler, *frontend.AdminHandler, *frontend.WorkflowHandler:
			return handler(ctx, req)
		default:
			panic("Frontend gRPC interceptor provided to non-frontend handler")
		}
	}
}

type errorLogDetector struct {
	t      testing.TB
	on     atomic.Bool
	logger log.Logger
}

func (d *errorLogDetector) logUnexpected(operation string, msg string, tags []tag.Tag) {
	if !d.on.Load() {
		return
	}

	msg = fmt.Sprintf("unexpected %v log: %v", operation, msg)
	d.t.Error(msg)
	d.logger.Error(msg, tags...)
}

func (d *errorLogDetector) Debug(msg string, tags ...tag.Tag) {
	d.logger.Debug(msg, tags...)
}

func (d *errorLogDetector) Info(msg string, tags ...tag.Tag) {
	d.logger.Info(msg, tags...)
}

func (d *errorLogDetector) DPanic(msg string, tags ...tag.Tag) {
	d.logUnexpected("DPanic", msg, tags)
}

func (d *errorLogDetector) Panic(msg string, tags ...tag.Tag) {
	d.logUnexpected("Panic", msg, tags)
}

func (d *errorLogDetector) Fatal(msg string, tags ...tag.Tag) {
	d.logUnexpected("Fatal", msg, tags)
}

func (d *errorLogDetector) Start() {
	d.on.Store(true)
}

func (d *errorLogDetector) Stop() {
	d.on.Store(false)
}

func (d *errorLogDetector) Warn(msg string, tags ...tag.Tag) {
	d.logger.Warn(msg, tags...)
	for _, s := range []string{
		"error creating sdk client",
		"Failed to poll for task",
		"Fail to process task", // transient startup error
		"OTEL error",           // logged when OTEL collector is not running
		"network dial error",   // transient shutdown error
	} {
		if strings.Contains(msg, s) {
			return
		}
	}

	d.logUnexpected("Warn", msg, tags)
}

func (d *errorLogDetector) Error(msg string, tags ...tag.Tag) {
	d.logger.Error(msg, tags...)
	for _, s := range []string{
		"Unable to process new range",
		"Unable to call",
		"service failures",
		"Queue reader unable to retrieve tasks",        // transient startup error
		"error from matching when initializing",        // transient startup error
		"error fetching user data from parent",         // transient startup error
		"Failed to check Nexus endpoints ownership",    // transient startup error
		"Failed to force load non-root partition",      // transient shutdown error
		"error refreshing endpoints by background job", // transient shutdown error
	} {
		if strings.Contains(msg, s) {
			return
		}
	}

	d.logUnexpected("Error", msg, tags)
}

// newErrorLogDetector returns a logger that fails the test if it logs any errors or warnings, except for the ones that
// are expected. Ideally, there are no "expected" errors or warnings, but we still want this test to avoid introducing
// any new ones while we are working on removing the existing ones.
func newErrorLogDetector(t testing.TB, baseLogger log.Logger) *errorLogDetector {
	return &errorLogDetector{
		t:      t,
		logger: baseLogger,
	}
}

type fakeTest struct {
	testing.TB
	errorLogs []string
}

func (f *fakeTest) Error(args ...any) {
	require.Len(f.TB, args, 1)
	msg, ok := args[0].(string)
	require.True(f.TB, ok)
	f.errorLogs = append(f.errorLogs, msg)
}

func TestErrorLogDetector(t *testing.T) {
	t.Parallel()

	f := &fakeTest{TB: t}
	d := newErrorLogDetector(f, log.NewNoopLogger())
	d.Start()
	d.Debug("debug")
	d.Info("info")
	d.Warn("error creating sdk client")
	d.Warn("unexpected warning")
	d.Warn("Failed to poll for task")
	d.Error("Unable to process new range")
	d.Error("Unable to call matching.PollActivityTaskQueue")
	d.Error("service failures")
	d.Error("unexpected error")
	d.DPanic("dpanic")
	d.Panic("panic")
	d.Fatal("fatal")

	expectedMsgs := []string{
		"unexpected Warn log: unexpected warning",
		"unexpected Error log: unexpected error",
		"unexpected DPanic log: dpanic",
		"unexpected Panic log: panic",
		"unexpected Fatal log: fatal",
	}
	assert.Equal(t, expectedMsgs, f.errorLogs)

	d.Stop()

	f.errorLogs = nil

	d.Warn("unexpected warning")
	d.Error("unexpected error")
	d.DPanic("dpanic")
	d.Panic("panic")
	d.Fatal("fatal")
	assert.Empty(t, f.errorLogs, "should not fail the test if the detector is stopped")
}

func SimpleWorkflow(ctx workflow.Context) (string, error) {
	return "Hello World", nil
}
