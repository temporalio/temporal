package testcore

import (
	"cmp"
	"context"
	"fmt"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/testing/testtelemetry"
	"go.temporal.io/server/components/nexusoperations"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// TestFixture provides test infrastructure without testify suite dependencies.
// It manages a shared test cluster (one per test group) and per-test SDK resources.
type TestFixture struct {
	t *testing.T

	// Shared cluster (created once per test group)
	testCluster *TestCluster
	logger      log.Logger

	// Per-test resources
	namespace  namespace.Name
	sdkClient  sdkclient.Client
	worker     sdkworker.Worker
	taskQueue  string
	otelExport *testtelemetry.MemoryExporter
}

// NewTestClusterForTestGroup creates a test cluster that will be shared across all subtests in a test group.
// This should be called once at the beginning of a parent test function, typically with t.Run().
//
// Example:
//
//	func TestMyTestGroup(t *testing.T) {
//	    cluster, logger := testcore.NewTestClusterForTestGroup(t)
//
//	    t.Run("Subtest1", func(t *testing.T) {
//	        t.Parallel()
//	        f := testcore.NewTestFixture(t, cluster, logger)
//	        // ... test code
//	    })
//	}
func NewTestClusterForTestGroup(t *testing.T, options ...TestClusterOption) (*TestCluster, log.Logger) {
	params := ApplyTestClusterOptions(options)

	testLogger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	testlogger.DontFailOnError(testLogger)
	testLogger.Expect(testlogger.Error, ".*", tag.FailedAssertion)

	clusterConfig := &TestClusterConfig{
		FaultInjection: params.FaultInjectionConfig,
		HistoryConfig: HistoryConfig{
			NumHistoryShards: cmp.Or(params.NumHistoryShards, 4),
		},
		DynamicConfigOverrides: params.DynamicConfigOverrides,
		ServiceFxOptions:       params.ServiceOptions,
		EnableMetricsCapture:   true,
		EnableArchival:         params.ArchivalEnabled,
		EnableMTLS:             params.EnableMTLS,
	}

	// Initialize OTEL if enabled
	if otelOutputDir := os.Getenv("TEMPORAL_TEST_OTEL_OUTPUT"); otelOutputDir != "" {
		otelExporter := testtelemetry.NewFileExporter(otelOutputDir)
		clusterConfig.SpanExporters = map[telemetry.SpanExporterType]sdktrace.SpanExporter{
			telemetry.OtelTracesOtlpExporterType: otelExporter,
		}
	}

	factory := NewTestClusterFactory()
	cluster, err := factory.NewCluster(t, clusterConfig, testLogger)
	require.NoError(t, err)

	// Register cleanup
	t.Cleanup(func() {
		// Close logger before tearing down cluster to prevent teardown errors from failing the test
		testLogger.Close()

		require.NoError(t, cluster.TearDownCluster())
	})

	// Return as log.Logger interface
	return cluster, testLogger
}

// NewTestFixture creates a new test fixture for a single test.
// This should be called once per test (or subtest) to get isolated test resources.
//
// The fixture automatically registers cleanup handlers via t.Cleanup().
func NewTestFixture(t *testing.T, cluster *TestCluster, logger log.Logger) *TestFixture {
	require.NotNil(t, cluster, "test cluster must not be nil")
	require.NotNil(t, logger, "logger must not be nil")

	f := &TestFixture{
		t:           t,
		testCluster: cluster,
		logger:      logger,
		taskQueue:   RandomizeStr("tq"),
	}

	// Create test-specific namespace
	f.namespace = namespace.Name(RandomizeStr("namespace"))
	_, err := f.registerNamespace(f.namespace, 1, enumspb.ARCHIVAL_STATE_DISABLED, "", "")
	require.NoError(t, err)

	// Setup SDK client
	clientOptions := sdkclient.Options{
		HostPort:  cluster.Host().FrontendGRPCAddress(),
		Namespace: f.namespace.String(),
		Logger:    log.NewSdkLogger(logger),
	}

	if provider := cluster.Host().TlsConfigProvider(); provider != nil {
		clientOptions.ConnectionOptions.TLS = provider.FrontendClientConfig
	}

	if interceptor := cluster.Host().GetGrpcClientInterceptor(); interceptor != nil {
		clientOptions.ConnectionOptions.DialOptions = []grpc.DialOption{
			grpc.WithUnaryInterceptor(interceptor.Unary()),
			grpc.WithStreamInterceptor(interceptor.Stream()),
		}
	}

	f.sdkClient, err = sdkclient.Dial(clientOptions)
	require.NoError(t, err)

	// Setup worker
	f.worker = sdkworker.New(f.sdkClient, f.taskQueue, sdkworker.Options{})
	err = f.worker.Start()
	require.NoError(t, err)

	// Override dynamic config for Nexus callback URL
	f.OverrideDynamicConfig(
		nexusoperations.CallbackURLTemplate,
		"http://"+cluster.Host().FrontendHTTPAddress()+"/namespaces/{{.NamespaceName}}/nexus/callback")

	// Annotate gRPC requests with test name for OTEL tracing
	cluster.host.grpcClientInterceptor.Set(func(ctx context.Context) context.Context {
		return metadata.AppendToOutgoingContext(ctx, "temporal-test-name", t.Name())
	})

	// Initialize OTEL exporter if enabled
	if otelOutputDir := os.Getenv("TEMPORAL_TEST_OTEL_OUTPUT"); otelOutputDir != "" {
		f.otelExport = testtelemetry.NewFileExporter(otelOutputDir)
	}

	// Register cleanup
	t.Cleanup(func() {
		f.cleanup()
	})

	return f
}

// cleanup tears down per-test resources
func (f *TestFixture) cleanup() {
	// Export OTEL traces if test failed
	if f.otelExport != nil && f.t.Failed() {
		var validFilenameChars = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)
		fileName := f.t.Name()
		fileName = validFilenameChars.ReplaceAllString(fileName, "-")
		fileName = fmt.Sprintf("traces.%s_%d.json", fileName, time.Now().Unix())
		if filePath, err := f.otelExport.Write(fileName); err != nil {
			f.t.Logf("unable to write OTEL traces: %v", err)
		} else {
			f.t.Logf("wrote OTEL traces to %s", filePath)
		}
		_ = f.otelExport.Shutdown(NewContext())
	}

	// Stop worker
	if f.worker != nil {
		f.worker.Stop()
	}

	// Close SDK client
	if f.sdkClient != nil {
		f.sdkClient.Close()
	}

	// Clear gRPC interceptor
	if f.testCluster != nil {
		f.testCluster.host.grpcClientInterceptor.Set(nil)
	}

	// Mark namespace as deleted
	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(10000 * time.Second)
	defer cancel()
	_, _ = f.FrontendClient().UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: f.namespace.String(),
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{
			State: enumspb.NAMESPACE_STATE_DELETED,
		},
	})
}

// Accessor methods

func (f *TestFixture) FrontendClient() workflowservice.WorkflowServiceClient {
	return f.testCluster.FrontendClient()
}

func (f *TestFixture) AdminClient() adminservice.AdminServiceClient {
	return f.testCluster.AdminClient()
}

func (f *TestFixture) OperatorClient() operatorservice.OperatorServiceClient {
	return f.testCluster.OperatorClient()
}

func (f *TestFixture) SdkClient() sdkclient.Client {
	return f.sdkClient
}

func (f *TestFixture) Worker() sdkworker.Worker {
	return f.worker
}

func (f *TestFixture) TaskQueue() string {
	return f.taskQueue
}

func (f *TestFixture) Namespace() namespace.Name {
	return f.namespace
}

func (f *TestFixture) Logger() log.Logger {
	return f.logger
}

func (f *TestFixture) TestCluster() *TestCluster {
	return f.testCluster
}

func (f *TestFixture) HTTPAPIAddress() string {
	return f.testCluster.Host().FrontendHTTPAddress()
}

func (f *TestFixture) FrontendGRPCAddress() string {
	return f.testCluster.Host().FrontendGRPCAddress()
}

// Helper methods

// OverrideDynamicConfig overrides one dynamic config setting for the duration of this test.
// The change will automatically be reverted at the end of the test (using t.Cleanup).
func (f *TestFixture) OverrideDynamicConfig(setting dynamicconfig.GenericSetting, value any) func() {
	return f.testCluster.host.overrideDynamicConfig(f.t, setting.Key(), value)
}

// registerNamespace registers a namespace using persistence API.
// This is used internally by NewTestFixture and allows:
//  1. Setting retention period to 0 for archival tests
//  2. Avoiding extra API calls for search attributes
//  3. Avoiding extra API call to get namespace.ID
func (f *TestFixture) registerNamespace(
	nsName namespace.Name,
	retentionDays int32,
	archivalState enumspb.ArchivalState,
	historyArchivalURI string,
	visibilityArchivalURI string,
) (namespace.ID, error) {
	currentClusterName := f.testCluster.testBase.ClusterMetadata.GetCurrentClusterName()
	nsID := namespace.ID(uuid.New())
	namespaceRequest := &persistence.CreateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:          nsID.String(),
				Name:        nsName.String(),
				State:       enumspb.NAMESPACE_STATE_REGISTERED,
				Description: "namespace for functional tests",
			},
			Config: &persistencespb.NamespaceConfig{
				Retention:               timestamp.DurationFromDays(retentionDays),
				HistoryArchivalState:    archivalState,
				HistoryArchivalUri:      historyArchivalURI,
				VisibilityArchivalState: archivalState,
				VisibilityArchivalUri:   visibilityArchivalURI,
				BadBinaries:             &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
				CustomSearchAttributeAliases: map[string]string{
					"Bool01":     "CustomBoolField",
					"Datetime01": "CustomDatetimeField",
					"Double01":   "CustomDoubleField",
					"Int01":      "CustomIntField",
					"Keyword01":  "CustomKeywordField",
					"Text01":     "CustomTextField",
				},
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: currentClusterName,
				Clusters: []string{
					currentClusterName,
				},
			},
			FailoverVersion: common.EmptyVersion,
		},
		IsGlobalNamespace: false,
	}
	_, err := f.testCluster.testBase.MetadataManager.CreateNamespace(context.Background(), namespaceRequest)

	if err != nil {
		return namespace.EmptyID, err
	}

	f.logger.Info("Register namespace succeeded",
		tag.WorkflowNamespace(nsName.String()),
		tag.WorkflowNamespaceID(nsID.String()),
	)
	return nsID, nil
}
