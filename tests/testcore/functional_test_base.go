package testcore

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"fmt"
	"maps"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/historyrequire"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/testing/testtelemetry"
	"go.temporal.io/server/common/testing/updateutils"
	"go.temporal.io/server/components/nexusoperations"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type (
	FunctionalTestBase struct {
		suite.Suite

		// `suite.Suite` embeds `*assert.Assertions` which, by default, makes all asserts (like `s.NoError(err)`)
		// only log the error, continue test execution, and only then fail the test.
		// This is not desired behavior in most cases. The idiomatic way to change this behavior
		// is to replace `*assert.Assertions` with `*require.Assertions` by embedding it in every test suite
		// (or base struct of every test suite).
		*require.Assertions

		protorequire.ProtoAssertions
		historyrequire.HistoryRequire
		updateutils.UpdateUtils

		Logger       log.Logger
		otelExporter *testtelemetry.MemoryExporter

		testCluster *TestCluster
		// TODO (alex): this doesn't have to be a separate field. All usages can be replaced with values from testCluster itself.
		testClusterConfig *TestClusterConfig

		namespace         namespace.Name
		namespaceID       namespace.ID
		externalNamespace namespace.Name

		// Fields used by SDK based tests.
		sdkClient sdkclient.Client
		worker    sdkworker.Worker
		taskQueue string

		// TODO (alex): replace with v2
		taskPoller *taskpoller.TaskPoller
	}
	// TestClusterParams contains the variables which are used to configure test cluster via the TestClusterOption type.
	TestClusterParams struct {
		ServiceOptions         map[primitives.ServiceName][]fx.Option
		DynamicConfigOverrides map[dynamicconfig.Key]any
		ArchivalEnabled        bool
		EnableMTLS             bool
		FaultInjectionConfig   *config.FaultInjection
		NumHistoryShards       int32
	}
	TestClusterOption func(params *TestClusterParams)
)

func init() {
	// By default, the SDK worker will calculate a checksum of the binary and use that as an identifier.
	// But given the size of the test binary, that has a significant performance impact (100 ms or more).
	// By specifying a checksum here, we can avoid that overhead.
	sdkworker.SetBinaryChecksum("oss-server-test")
}

// WithFxOptionsForService returns an Option which, when passed as an argument to setupSuite, will append the given list
// of fx options to the end of the arguments to the fx.New call for the given service. For example, if you want to
// obtain the shard controller for the history service, you can do this:
//
//	var shardController shard.Controller
//	s.setupSuite(t, tests.WithFxOptionsForService(primitives.HistoryService, fx.Populate(&shardController)))
//	// now you can use shardController during your test
//
// This is similar to the pattern of plumbing dependencies through the TestClusterConfig, but it's much more convenient,
// scalable and flexible. The reason we need to do this on a per-service basis is that there are separate fx apps for
// each one.
func WithFxOptionsForService(serviceName primitives.ServiceName, options ...fx.Option) TestClusterOption {
	return func(params *TestClusterParams) {
		params.ServiceOptions[serviceName] = append(params.ServiceOptions[serviceName], options...)
	}
}

func WithDynamicConfigOverrides(overrides map[dynamicconfig.Key]any) TestClusterOption {
	return func(params *TestClusterParams) {
		if params.DynamicConfigOverrides == nil {
			params.DynamicConfigOverrides = overrides
		} else {
			maps.Copy(params.DynamicConfigOverrides, overrides)
		}
	}
}

func WithArchivalEnabled() TestClusterOption {
	return func(params *TestClusterParams) {
		params.ArchivalEnabled = true
	}
}

func WithMTLS() TestClusterOption {
	return func(params *TestClusterParams) {
		params.EnableMTLS = true
	}
}

func WithFaultInjectionConfig(cfg *config.FaultInjection) TestClusterOption {
	return func(params *TestClusterParams) {
		params.FaultInjectionConfig = cfg
	}
}

func WithNumHistoryShards(n int32) TestClusterOption {
	return func(params *TestClusterParams) {
		params.NumHistoryShards = n
	}
}

func (s *FunctionalTestBase) GetTestCluster() *TestCluster {
	return s.testCluster
}

func (s *FunctionalTestBase) GetTestClusterConfig() *TestClusterConfig {
	return s.testClusterConfig
}

func (s *FunctionalTestBase) FrontendClient() workflowservice.WorkflowServiceClient {
	return s.testCluster.FrontendClient()
}

func (s *FunctionalTestBase) AdminClient() adminservice.AdminServiceClient {
	return s.testCluster.AdminClient()
}

func (s *FunctionalTestBase) OperatorClient() operatorservice.OperatorServiceClient {
	return s.testCluster.OperatorClient()
}

func (s *FunctionalTestBase) HttpAPIAddress() string {
	return s.testCluster.Host().FrontendHTTPAddress()
}

func (s *FunctionalTestBase) Namespace() namespace.Name {
	return s.namespace
}

func (s *FunctionalTestBase) NamespaceID() namespace.ID {
	return s.namespaceID
}

func (s *FunctionalTestBase) ExternalNamespace() namespace.Name {
	return s.externalNamespace
}

func (s *FunctionalTestBase) FrontendGRPCAddress() string {
	return s.GetTestCluster().Host().FrontendGRPCAddress()
}

func (s *FunctionalTestBase) Worker() sdkworker.Worker {
	return s.worker
}

func (s *FunctionalTestBase) SdkClient() sdkclient.Client {
	return s.sdkClient
}

func (s *FunctionalTestBase) TaskQueue() string {
	return s.taskQueue
}

func (s *FunctionalTestBase) TaskPoller() *taskpoller.TaskPoller {
	return s.taskPoller
}

func (s *FunctionalTestBase) SetupSuite() {
	s.SetupSuiteWithCluster()
}

func (s *FunctionalTestBase) TearDownSuite() {
	// NOTE: We can't make s.Logger a testlogger.TestLogger because of AcquireShardSuiteBase.
	if tl, ok := s.Logger.(*testlogger.TestLogger); ok {
		// Before we tear down the cluster, we disable the test logger.
		// This prevents cluster teardown errors from failing the test; and log spam.
		tl.Close()
	}

	s.TearDownCluster()
}

func (s *FunctionalTestBase) SetupSuiteWithCluster(options ...TestClusterOption) {
	params := ApplyTestClusterOptions(options)

	// NOTE: A suite might set its own logger. Example: AcquireShardSuiteBase.
	if s.Logger == nil {
		tl := testlogger.NewTestLogger(s.T(), testlogger.FailOnExpectedErrorOnly)
		// Instead of panic'ing immediately, TearDownTest will check if the test logger failed
		// after each test completed. This is better since otherwise is would fail inside
		// the server and not the test, creating a lot of noise and possibly stuck tests.
		testlogger.DontFailOnError(tl)
		// Fail test when an assertion fails (see `softassert` package).
		tl.Expect(testlogger.Error, ".*", tag.FailedAssertion)
		s.Logger = tl
	}

	s.testClusterConfig = &TestClusterConfig{
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

	// Initialize the OTEL collector if OTEL is enabled.
	// Must be done before the test cluster is created, so that the collector can be used by the test cluster.
	if otelOutputDir := os.Getenv("TEMPORAL_TEST_OTEL_OUTPUT"); otelOutputDir != "" {
		// Create an OTEL exporter.
		s.otelExporter = testtelemetry.NewFileExporter(otelOutputDir)

		// Direct the OTEL exporter to the collector.
		s.testClusterConfig.SpanExporters = map[telemetry.SpanExporterType]sdktrace.SpanExporter{
			telemetry.OtelTracesOtlpExporterType: s.otelExporter,
		}
	}

	var err error
	testClusterFactory := NewTestClusterFactory()
	s.testCluster, err = testClusterFactory.NewCluster(s.T(), s.testClusterConfig, s.Logger)
	s.Require().NoError(err)

	// Setup test cluster namespaces.
	s.namespace = namespace.Name(RandomizeStr("namespace"))
	s.namespaceID, err = s.RegisterNamespace(s.Namespace(), 1, enumspb.ARCHIVAL_STATE_DISABLED, "", "")
	s.Require().NoError(err)

	s.externalNamespace = namespace.Name(RandomizeStr("external-namespace"))
	_, err = s.RegisterNamespace(s.ExternalNamespace(), 1, enumspb.ARCHIVAL_STATE_DISABLED, "", "")
	s.Require().NoError(err)
}

// All test suites that inherit FunctionalTestBase and overwrite SetupTest must
// call this testcore FunctionalTestBase.SetupTest function to distribute the tests
// into partitions. Otherwise, the test suite will be executed multiple times
// in each partition.
func (s *FunctionalTestBase) SetupTest() {
	s.checkTestShard()
	s.initAssertions()
	s.setupSdk()
	s.taskPoller = taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())

	// Annotate gRPC requests with the test name for OTEL tracing.
	s.testCluster.host.grpcClientInterceptor.Set(func(ctx context.Context) context.Context {
		return metadata.AppendToOutgoingContext(ctx, "temporal-test-name", s.T().Name())
	})
}

func (s *FunctionalTestBase) SetupSubTest() {
	s.initAssertions()
}

func (s *FunctionalTestBase) initAssertions() {
	// `s.Assertions` (as well as other test helpers which depends on `s.T()`) must be initialized on
	// both test and subtest levels (but not suite level, where `s.T()` is `nil`).
	//
	// If these helpers are not reinitialized on subtest level, any failed `assert` in
	// subtest will fail the entire test (not subtest) immediately without running other subtests.

	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())
	s.HistoryRequire = historyrequire.New(s.T())
	s.UpdateUtils = updateutils.New(s.T())
}

// checkTestShard supports test sharding based on environment variables.
func (s *FunctionalTestBase) checkTestShard() {
	totalStr := os.Getenv("TEST_TOTAL_SHARDS")
	indexStr := os.Getenv("TEST_SHARD_INDEX")
	if totalStr == "" || indexStr == "" {
		return
	}
	total, err := strconv.Atoi(totalStr)
	if err != nil || total < 1 {
		s.T().Fatal("Couldn't convert TEST_TOTAL_SHARDS")
	}
	index, err := strconv.Atoi(indexStr)
	if err != nil || index < 0 || index >= total {
		s.T().Fatal("Couldn't convert TEST_SHARD_INDEX")
	}

	// This was determined empirically to distribute our existing test names
	// reasonably well. This can be adjusted from time to time.
	// For parallelism 4, use 11. For 3, use 26. For 2, use 20.
	const salt = "-salt-26"

	nameToHash := s.T().Name() + salt
	testIndex := int(farm.Fingerprint32([]byte(nameToHash))) % total
	if testIndex != index {
		s.T().Skipf("Skipping %s in test shard %d/%d (it runs in %d)", s.T().Name(), index+1, total, testIndex+1)
	}
	s.T().Logf("Running %s in test shard %d/%d", s.T().Name(), index+1, total)
}

func ApplyTestClusterOptions(options []TestClusterOption) TestClusterParams {
	params := TestClusterParams{
		ServiceOptions: make(map[primitives.ServiceName][]fx.Option),
	}
	for _, opt := range options {
		opt(&params)
	}
	return params
}

func (s *FunctionalTestBase) setupSdk() {
	// Set URL template after httpAPAddress is set, see commonnexus.RouteCompletionCallback
	s.OverrideDynamicConfig(
		nexusoperations.CallbackURLTemplate,
		"http://"+s.HttpAPIAddress()+"/namespaces/{{.NamespaceName}}/nexus/callback")

	clientOptions := sdkclient.Options{
		HostPort:  s.FrontendGRPCAddress(),
		Namespace: s.Namespace().String(),
		Logger:    log.NewSdkLogger(s.Logger),
	}

	if provider := s.testCluster.host.tlsConfigProvider; provider != nil {
		clientOptions.ConnectionOptions.TLS = provider.FrontendClientConfig
	}

	if interceptor := s.testCluster.host.grpcClientInterceptor; interceptor != nil {
		clientOptions.ConnectionOptions.DialOptions = []grpc.DialOption{
			grpc.WithUnaryInterceptor(interceptor.Unary()),
			grpc.WithStreamInterceptor(interceptor.Stream()),
		}
	}

	var err error
	s.sdkClient, err = sdkclient.Dial(clientOptions)
	s.NoError(err)
	// TODO(alex): move initialization to suite level?
	s.taskQueue = RandomizeStr("tq")

	workerOptions := sdkworker.Options{}
	s.worker = sdkworker.New(s.sdkClient, s.taskQueue, workerOptions)
	err = s.worker.Start()
	s.NoError(err)
}

func (s *FunctionalTestBase) exportOTELTraces() {
	if s.otelExporter == nil {
		return
	}
	if s.T().Failed() {
		var validFilenameChars = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)
		fileName := s.T().Name()
		fileName = validFilenameChars.ReplaceAllString(fileName, "-") // remove invalid characters
		fileName = fmt.Sprintf("traces.%s_%d.json", fileName, time.Now().Unix())
		if filePath, err := s.otelExporter.Write(fileName); err != nil {
			s.T().Logf("unable to write OTEL traces: %v", err)
		} else {
			s.T().Logf("wrote OTEL traces to %s", filePath)
		}
	}
	_ = s.otelExporter.Shutdown(NewContext())
}

func (s *FunctionalTestBase) TearDownCluster() {
	s.Require().NoError(s.MarkNamespaceAsDeleted(s.Namespace()))
	s.Require().NoError(s.MarkNamespaceAsDeleted(s.ExternalNamespace()))

	if s.testCluster != nil {
		s.Require().NoError(s.testCluster.TearDownCluster())
	}
}

// **IMPORTANT**: When overridding this, make sure to invoke `s.FunctionalTestBase.TearDownTest()`.
func (s *FunctionalTestBase) TearDownTest() {
	s.exportOTELTraces()
	s.tearDownSdk()
	s.testCluster.host.grpcClientInterceptor.Set(nil)
}

// **IMPORTANT**: When overridding this, make sure to invoke `s.FunctionalTestBase.TearDownSubTest()`.
func (s *FunctionalTestBase) TearDownSubTest() {
	s.exportOTELTraces()
}

func (s *FunctionalTestBase) tearDownSdk() {
	if s.worker != nil {
		s.worker.Stop()
	}
	if s.sdkClient != nil {
		s.sdkClient.Close()
	}
}

// Register namespace using persistence API because:
//  1. The Retention period is set to 0 for archival tests, and this can't be done through FE,
//  2. Update search attributes would require an extra API call,
//  3. One more extra API call would be necessary to get namespace.ID.
func (s *FunctionalTestBase) RegisterNamespace(
	nsName namespace.Name,
	retentionDays int32,
	archivalState enumspb.ArchivalState,
	historyArchivalURI string,
	visibilityArchivalURI string,
) (namespace.ID, error) {
	currentClusterName := s.testCluster.testBase.ClusterMetadata.GetCurrentClusterName()
	nsID := namespace.ID(uuid.NewString())
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
	_, err := s.testCluster.testBase.MetadataManager.CreateNamespace(context.Background(), namespaceRequest)

	if err != nil {
		return namespace.EmptyID, err
	}

	s.Logger.Info("Register namespace succeeded",
		tag.WorkflowNamespace(nsName.String()),
		tag.WorkflowNamespaceID(nsID.String()),
	)
	return nsID, nil
}

func (s *FunctionalTestBase) MarkNamespaceAsDeleted(
	nsName namespace.Name,
) error {
	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(10000 * time.Second)
	defer cancel()
	_, err := s.FrontendClient().UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: nsName.String(),
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{
			State: enumspb.NAMESPACE_STATE_DELETED,
		},
	})

	return err
}

func (s *FunctionalTestBase) GetHistoryFunc(namespace string, execution *commonpb.WorkflowExecution) func() []*historypb.HistoryEvent {
	return func() []*historypb.HistoryEvent {
		historyResponse, err := s.FrontendClient().GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:       namespace,
			Execution:       execution,
			MaximumPageSize: 5, // Use small page size to force pagination code path
		})
		require.NoError(s.T(), err)

		events := historyResponse.History.Events
		for historyResponse.NextPageToken != nil {
			historyResponse, err = s.FrontendClient().GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
				Namespace:     namespace,
				Execution:     execution,
				NextPageToken: historyResponse.NextPageToken,
			})
			require.NoError(s.T(), err)
			events = append(events, historyResponse.History.Events...)
		}

		return events
	}
}

func (s *FunctionalTestBase) GetHistory(namespace string, execution *commonpb.WorkflowExecution) []*historypb.HistoryEvent {
	return s.GetHistoryFunc(namespace, execution)()
}

func (s *FunctionalTestBase) DecodePayloadsString(ps *commonpb.Payloads) string {
	s.T().Helper()
	var r string
	s.NoError(payloads.Decode(ps, &r))
	return r
}

func (s *FunctionalTestBase) DecodePayloadsInt(ps *commonpb.Payloads) int {
	s.T().Helper()
	var r int
	s.NoError(payloads.Decode(ps, &r))
	return r
}

func (s *FunctionalTestBase) DecodePayloadsByteSliceInt32(ps *commonpb.Payloads) (r int32) {
	s.T().Helper()
	var buf []byte
	s.NoError(payloads.Decode(ps, &buf))
	s.NoError(binary.Read(bytes.NewReader(buf), binary.LittleEndian, &r))
	return
}

func (s *FunctionalTestBase) DurationNear(value, target, tolerance time.Duration) {
	s.T().Helper()
	s.Greater(value, target-tolerance)
	s.Less(value, target+tolerance)
}

// Overrides one dynamic config setting for the duration of this test (or sub-test). The change
// will automatically be reverted at the end of the test (using t.Cleanup). The cleanup
// function is also returned if you want to revert the change before the end of the test.
func (s *FunctionalTestBase) OverrideDynamicConfig(setting dynamicconfig.GenericSetting, value any) (cleanup func()) {
	return s.testCluster.host.overrideDynamicConfig(s.T(), setting.Key(), value)
}

func (s *FunctionalTestBase) InjectHook(key testhooks.Key, value any) (cleanup func()) {
	return s.testCluster.host.injectHook(s.T(), key, value)
}

func (s *FunctionalTestBase) GetNamespaceID(namespace string) string {
	namespaceResp, err := s.FrontendClient().DescribeNamespace(NewContext(), &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	})
	s.NoError(err)
	return namespaceResp.NamespaceInfo.GetId()
}

func (s *FunctionalTestBase) RunTestWithMatchingBehavior(subtest func()) {
	for _, forcePollForward := range []bool{false, true} {
		for _, forceTaskForward := range []bool{false, true} {
			for _, forceAsync := range []bool{false, true} {
				name := "NoTaskForward"
				if forceTaskForward {
					// force two levels of forwarding
					name = "ForceTaskForward"
				}
				if forcePollForward {
					name += "ForcePollForward"
				} else {
					name += "NoPollForward"
				}
				if forceAsync {
					name += "ForceAsync"
				} else {
					name += "AllowSync"
				}

				s.Run(
					name, func() {
						if forceTaskForward {
							s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 13)
							s.InjectHook(testhooks.MatchingLBForceWritePartition, 11)
						} else {
							s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)
						}
						if forcePollForward {
							s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 13)
							s.InjectHook(testhooks.MatchingLBForceReadPartition, 5)
						} else {
							s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
						}
						if forceAsync {
							s.InjectHook(testhooks.MatchingDisableSyncMatch, true)
						} else {
							s.InjectHook(testhooks.MatchingDisableSyncMatch, false)
						}

						subtest()
					},
				)
			}
		}
	}
}

func (s *FunctionalTestBase) WaitForChannel(ctx context.Context, ch chan struct{}) {
	s.T().Helper()
	select {
	case <-ch:
	case <-ctx.Done():
		s.FailNow("context timeout while waiting for channel")
	}
}

// TODO (alex): change to nsName namespace.Name
func (s *FunctionalTestBase) SendSignal(nsName string, execution *commonpb.WorkflowExecution, signalName string,
	input *commonpb.Payloads, identity string) error {
	_, err := s.FrontendClient().SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         nsName,
		WorkflowExecution: execution,
		SignalName:        signalName,
		Input:             input,
		Identity:          identity,
	})

	return err
}
