// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package testcore

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"maps"
	"os"
	"strconv"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/testing/historyrequire"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/updateutils"
	"go.temporal.io/server/environment"
	"go.uber.org/fx"
	"gopkg.in/yaml.v3"
)

type (
	// FunctionalTestBase is a testcore struct for functional tests
	FunctionalTestBase struct {
		suite.Suite

		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		protorequire.ProtoAssertions
		historyrequire.HistoryRequire
		updateutils.UpdateUtils

		testClusterFactory  TestClusterFactory
		testCluster         *TestCluster
		testClusterConfig   *TestClusterConfig
		frontendClient      workflowservice.WorkflowServiceClient
		adminClient         adminservice.AdminServiceClient
		operatorClient      operatorservice.OperatorServiceClient
		httpAPIAddress      string
		Logger              log.Logger
		namespace           namespace.Name
		namespaceID         namespace.ID
		foreignNamespace    namespace.Name
		archivalNamespace   namespace.Name
		archivalNamespaceID namespace.ID
	}
	// TestClusterParams contains the variables which are used to configure test suites via the TestClusterOption type.
	TestClusterParams struct {
		ServiceOptions         map[primitives.ServiceName][]fx.Option
		DynamicConfigOverrides map[dynamicconfig.Key]any
	}
	TestClusterOption func(params *TestClusterParams)
)

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
		params.DynamicConfigOverrides = overrides
	}
}

func (s *FunctionalTestBase) GetTestCluster() *TestCluster {
	return s.testCluster
}

func (s *FunctionalTestBase) GetTestClusterConfig() *TestClusterConfig {
	return s.testClusterConfig
}

func (s *FunctionalTestBase) FrontendClient() workflowservice.WorkflowServiceClient {
	return s.frontendClient
}

func (s *FunctionalTestBase) AdminClient() adminservice.AdminServiceClient {
	return s.adminClient
}

func (s *FunctionalTestBase) OperatorClient() operatorservice.OperatorServiceClient {
	return s.operatorClient
}

func (s *FunctionalTestBase) HttpAPIAddress() string {
	return s.httpAPIAddress
}

func (s *FunctionalTestBase) Namespace() namespace.Name {
	return s.namespace
}

func (s *FunctionalTestBase) NamespaceID() namespace.ID {
	return s.namespaceID
}

func (s *FunctionalTestBase) ArchivalNamespace() namespace.Name {
	return s.archivalNamespace
}

func (s *FunctionalTestBase) ArchivalNamespaceID() namespace.ID {
	return s.archivalNamespaceID
}

func (s *FunctionalTestBase) ForeignNamespace() namespace.Name {
	return s.foreignNamespace
}

func (s *FunctionalTestBase) FrontendGRPCAddress() string {
	return s.GetTestCluster().Host().FrontendGRPCAddress()
}

func (s *FunctionalTestBase) SetupSuite() {
	s.SetupDefaultTestCluster()
}

func (s *FunctionalTestBase) TearDownSuite() {
	s.TearDownCluster()
}

func (s *FunctionalTestBase) SetupDefaultTestCluster(options ...TestClusterOption) {
	// TODO: rename es_cluster.yaml to default_cluster.yaml
	s.SetupTestCluster("testdata/es_cluster.yaml", options...)
}
func (s *FunctionalTestBase) SetupTestCluster(clusterConfigFile string, options ...TestClusterOption) {
	s.testClusterFactory = NewTestClusterFactory()

	params := ApplyTestClusterOptions(options)

	s.setupLogger()

	clusterConfig, err := ReadTestClusterConfig(clusterConfigFile)
	s.Require().NoError(err)
	s.Require().Empty(clusterConfig.DeprecatedFrontendAddress, "Functional tests against external frontends are not supported")
	s.Require().Empty(clusterConfig.DeprecatedClusterNo, "ClusterNo should not be present in cluster config files")

	if clusterConfig.DynamicConfigOverrides == nil {
		clusterConfig.DynamicConfigOverrides = make(map[dynamicconfig.Key]any)
	}

	// TODO: clusterConfig shouldn't have DC at all.
	maps.Copy(clusterConfig.DynamicConfigOverrides, map[dynamicconfig.Key]any{
		dynamicconfig.HistoryScannerEnabled.Key():    false,
		dynamicconfig.TaskQueueScannerEnabled.Key():  false,
		dynamicconfig.ExecutionsScannerEnabled.Key(): false,
		dynamicconfig.BuildIdScavengerEnabled.Key():  false,
		// Better to read through in tests than add artificial sleeps (which is what we previously had).
		dynamicconfig.ForceSearchAttributesCacheRefreshOnRead.Key(): true,
		dynamicconfig.RetentionTimerJitterDuration.Key():            time.Second,
		dynamicconfig.EnableEagerWorkflowStart.Key():                true,
		dynamicconfig.FrontendEnableExecuteMultiOperation.Key():     true,
	})
	maps.Copy(clusterConfig.DynamicConfigOverrides, params.DynamicConfigOverrides)

	clusterConfig.ServiceFxOptions = params.ServiceOptions
	clusterConfig.EnableMetricsCapture = true
	s.testClusterConfig = clusterConfig

	cluster, err := s.testClusterFactory.NewCluster(s.T(), clusterConfig, s.Logger)
	s.Require().NoError(err)
	s.testCluster = cluster
	s.frontendClient = s.testCluster.FrontendClient()
	s.adminClient = s.testCluster.AdminClient()
	s.operatorClient = s.testCluster.OperatorClient()
	s.httpAPIAddress = cluster.Host().FrontendHTTPAddress()

	s.namespace = namespace.Name(RandomizeStr("namespace"))
	s.namespaceID, err = s.registerNamespace(s.Namespace(), 1, enumspb.ARCHIVAL_STATE_DISABLED, "", "")
	s.Require().NoError(err)

	s.foreignNamespace = namespace.Name(RandomizeStr("foreign-namespace"))
	_, err = s.registerNamespace(s.ForeignNamespace(), 1, enumspb.ARCHIVAL_STATE_DISABLED, "", "")
	s.Require().NoError(err)

	if clusterConfig.EnableArchival {
		s.archivalNamespace = namespace.Name(RandomizeStr("archival-enabled-namespace"))
		s.archivalNamespaceID, err = s.registerNamespace(
			s.ArchivalNamespace(),
			0, // Archive right away.
			enumspb.ARCHIVAL_STATE_ENABLED,
			s.testCluster.archiverBase.historyURI,
			s.testCluster.archiverBase.visibilityURI,
		)
		s.Require().NoError(err)
	}
}

// All test suites that inherit FunctionalTestBase and overwrite SetupTest must
// call this testcore FunctionalTestBase.SetupTest function to distribute the tests
// into partitions. Otherwise, the test suite will be executed multiple times
// in each partition.
// Furthermore, all test suites in the "tests/" directory that don't inherit
// from FunctionalTestBase must implement SetupTest that calls checkTestShard.
func (s *FunctionalTestBase) SetupTest() {
	s.checkTestShard()
	s.initAssertions()
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

// setupLogger sets the Logger for the test suite.
// If the Logger is already set, this method does nothing.
// If the Logger is not set, this method creates a new log.TestLogger which logs to stdout and stderr.
func (s *FunctionalTestBase) setupLogger() {
	if s.Logger == nil {
		s.Logger = log.NewTestLogger()
	}
}

// ReadTestClusterConfig return test cluster config
func ReadTestClusterConfig(configFile string) (*TestClusterConfig, error) {
	environment.SetupEnv()

	configLocation := configFile
	if TestFlags.TestClusterConfigFile != "" {
		configLocation = TestFlags.TestClusterConfigFile
	}
	if _, err := os.Stat(configLocation); err != nil {
		if os.IsNotExist(err) {
			configLocation = "../" + configLocation
		}
	}

	// This is just reading a config, so it's less of a security concern
	// #nosec
	confContent, err := os.ReadFile(configLocation)
	if err != nil {
		return nil, fmt.Errorf("failed to read test cluster config file %s: %w", configLocation, err)
	}
	confContent = []byte(os.ExpandEnv(string(confContent)))
	var clusterConfig TestClusterConfig
	if err = yaml.Unmarshal(confContent, &clusterConfig); err != nil {
		return nil, fmt.Errorf("failed to decode test cluster config %s: %w", configLocation, err)
	}

	// If -FaultInjectionConfigFile is passed to the test runner,
	// then fault injection config will be added to the test cluster config.
	if TestFlags.FaultInjectionConfigFile != "" {
		fiConfigContent, err := os.ReadFile(TestFlags.FaultInjectionConfigFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read test cluster fault injection config file %s: %v", TestFlags.FaultInjectionConfigFile, err)
		}

		var fiOptions TestClusterConfig
		if err = yaml.Unmarshal(fiConfigContent, &fiOptions); err != nil {
			return nil, fmt.Errorf("failed to decode test cluster fault injection config %s: %w", TestFlags.FaultInjectionConfigFile, err)
		}
		clusterConfig.FaultInjection = fiOptions.FaultInjection
	}

	return &clusterConfig, nil
}

func (s *FunctionalTestBase) TearDownCluster() {
	s.Require().NoError(s.markNamespaceAsDeleted(s.Namespace()))
	s.Require().NoError(s.markNamespaceAsDeleted(s.ForeignNamespace()))
	if s.ArchivalNamespace() != namespace.EmptyName {
		s.Require().NoError(s.markNamespaceAsDeleted(s.ArchivalNamespace()))
	}

	if s.testCluster != nil {
		s.Require().NoError(s.testCluster.TearDownCluster())
	}
}

// Register namespace using persistence API because:
//  1. The Retention period is set to 0 for archival tests, and this can't be done through FE,
//  2. Update search attributes would require an extra API call,
//  3. One more extra API call would be necessary to get namespace.ID.
func (s *FunctionalTestBase) registerNamespace(
	nsName namespace.Name,
	retentionDays int32,
	archivalState enumspb.ArchivalState,
	historyArchivalURI string,
	visibilityArchivalURI string,
) (namespace.ID, error) {
	currentClusterName := s.testCluster.testBase.ClusterMetadata.GetCurrentClusterName()
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

func (s *FunctionalTestBase) markNamespaceAsDeleted(
	nsName namespace.Name,
) error {
	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(10000 * time.Second)
	defer cancel()
	_, err := s.frontendClient.UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: nsName.String(),
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{
			State: enumspb.NAMESPACE_STATE_DELETED,
		},
	})

	return err
}

func (s *FunctionalTestBase) GetHistoryFunc(namespace string, execution *commonpb.WorkflowExecution) func() []*historypb.HistoryEvent {
	return func() []*historypb.HistoryEvent {
		historyResponse, err := s.frontendClient.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:       namespace,
			Execution:       execution,
			MaximumPageSize: 5, // Use small page size to force pagination code path
		})
		require.NoError(s.T(), err)

		events := historyResponse.History.Events
		for historyResponse.NextPageToken != nil {
			historyResponse, err = s.frontendClient.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
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
							s.OverrideDynamicConfig(dynamicconfig.TestMatchingLBForceWritePartition, 11)
						} else {
							s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)
						}
						if forcePollForward {
							s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 13)
							s.OverrideDynamicConfig(dynamicconfig.TestMatchingLBForceReadPartition, 5)
						} else {
							s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
						}
						if forceAsync {
							s.OverrideDynamicConfig(dynamicconfig.TestMatchingDisableSyncMatch, true)
						} else {
							s.OverrideDynamicConfig(dynamicconfig.TestMatchingDisableSyncMatch, false)
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

// TODO: change to nsName namespace.Name
func (s *FunctionalTestBase) SendSignal(nsName string, execution *commonpb.WorkflowExecution, signalName string,
	input *commonpb.Payloads, identity string) error {
	_, err := s.frontendClient.SignalWorkflowExecution(NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         nsName,
		WorkflowExecution: execution,
		SignalName:        signalName,
		Input:             input,
		Identity:          identity,
	})

	return err
}
