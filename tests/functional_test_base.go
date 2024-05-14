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

package tests

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"maps"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"google.golang.org/protobuf/types/known/durationpb"
	"gopkg.in/yaml.v3"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/environment"
)

type (
	// FunctionalTestBase is a base struct for functional tests
	FunctionalTestBase struct {
		suite.Suite

		testClusterFactory     TestClusterFactory
		testCluster            *TestCluster
		testClusterConfig      *TestClusterConfig
		engine                 FrontendClient
		adminClient            AdminClient
		operatorClient         operatorservice.OperatorServiceClient
		httpAPIAddress         string
		Logger                 log.Logger
		namespace              string
		foreignNamespace       string
		archivalNamespace      string
		dynamicConfigOverrides map[dynamicconfig.Key]interface{}
		hostPort               string
	}
	// TestClusterParams contains the variables which are used to configure test suites via the Option type.
	TestClusterParams struct {
		ServiceOptions map[primitives.ServiceName][]fx.Option
	}
	Option func(params *TestClusterParams)
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
func WithFxOptionsForService(serviceName primitives.ServiceName, options ...fx.Option) Option {
	return func(params *TestClusterParams) {
		params.ServiceOptions[serviceName] = append(params.ServiceOptions[serviceName], options...)
	}
}

func (s *FunctionalTestBase) setupSuite(defaultClusterConfigFile string, options ...Option) {
	checkTestShard(s.T())

	s.testClusterFactory = NewTestClusterFactory()

	params := ApplyTestClusterParams(options)

	s.hostPort = "127.0.0.1:7134"
	if TestFlags.FrontendAddr != "" {
		s.hostPort = TestFlags.FrontendAddr
	}
	s.setupLogger()

	clusterConfig, err := GetTestClusterConfig(defaultClusterConfigFile)
	s.Require().NoError(err)
	if clusterConfig.DynamicConfigOverrides == nil {
		clusterConfig.DynamicConfigOverrides = make(map[dynamicconfig.Key]interface{})
	}
	maps.Copy(clusterConfig.DynamicConfigOverrides, map[dynamicconfig.Key]any{
		dynamicconfig.HistoryScannerEnabled.Key():                        false,
		dynamicconfig.TaskQueueScannerEnabled.Key():                      false,
		dynamicconfig.ExecutionsScannerEnabled.Key():                     false,
		dynamicconfig.BuildIdScavengerEnabled.Key():                      false,
		dynamicconfig.FrontendEnableNexusAPIs.Key():                      true,
		dynamicconfig.EnableNexusEndpointRegistryBackgroundRefresh.Key(): true,
	})
	maps.Copy(clusterConfig.DynamicConfigOverrides, s.dynamicConfigOverrides)
	clusterConfig.ServiceFxOptions = params.ServiceOptions
	clusterConfig.EnableMetricsCapture = true
	s.testClusterConfig = clusterConfig

	if clusterConfig.FrontendAddress != "" {
		s.Logger.Info("Running functional test against specified frontend", tag.Address(TestFlags.FrontendAddr))

		connection, err := rpc.Dial(TestFlags.FrontendAddr, nil, s.Logger)
		if err != nil {
			s.Require().NoError(err)
		}

		s.engine = NewFrontendClient(connection)
		s.adminClient = NewAdminClient(connection)
		s.operatorClient = operatorservice.NewOperatorServiceClient(connection)
		s.httpAPIAddress = TestFlags.FrontendHTTPAddr
	} else {
		s.Logger.Info("Running functional test against test cluster")
		cluster, err := s.testClusterFactory.NewCluster(s.T(), clusterConfig, s.Logger)
		s.Require().NoError(err)
		s.testCluster = cluster
		s.engine = s.testCluster.GetFrontendClient()
		s.adminClient = s.testCluster.GetAdminClient()
		s.operatorClient = s.testCluster.GetOperatorClient()
		s.httpAPIAddress = cluster.host.FrontendHTTPAddress()
	}

	s.namespace = s.randomizeStr("functional-test-namespace")
	s.Require().NoError(s.registerNamespaceWithDefaults(s.namespace))

	s.foreignNamespace = s.randomizeStr("functional-foreign-test-namespace")
	s.Require().NoError(s.registerNamespaceWithDefaults(s.foreignNamespace))

	if clusterConfig.EnableArchival {
		s.archivalNamespace = s.randomizeStr("functional-archival-enabled-namespace")
		s.Require().NoError(s.registerArchivalNamespace(s.archivalNamespace))
	}

	// For tests using SQL visibility, we need to wait for search attributes to be available as part of the ns config
	// TODO: remove after https://github.com/temporalio/temporal/issues/4017 is resolved
	time.Sleep(2 * NamespaceCacheRefreshInterval)
}

func (s *FunctionalTestBase) registerNamespaceWithDefaults(name string) error {
	return s.registerNamespace(name, 24*time.Hour, enumspb.ARCHIVAL_STATE_DISABLED, "", enumspb.ARCHIVAL_STATE_DISABLED, "")
}

func ApplyTestClusterParams(options []Option) TestClusterParams {
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
		s.Logger = log.NewNoopLogger()
	}
}

// checkTestShard supports test sharding based on environment variables.
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

	// This was determined empirically to distribute our existing test names + run times
	// reasonably well. This can be adjusted from time to time.
	// For parallelism 4, use 11. For 3, use 26. For 2, use 20.
	const salt = "-salt-26"

	nameToHash := t.Name() + salt
	testIndex := int(farm.Fingerprint32([]byte(nameToHash))) % total
	if testIndex != index {
		t.Skipf("Skipping %s in test shard %d/%d (it runs in %d)", t.Name(), index+1, total, testIndex+1)
	}
	t.Logf("Running %s in test shard %d/%d", t.Name(), index+1, total)
}

// GetTestClusterConfig return test cluster config
func GetTestClusterConfig(configFile string) (*TestClusterConfig, error) {
	environment.SetupEnv()

	configLocation := configFile
	if TestFlags.TestClusterConfigFile != "" {
		configLocation = TestFlags.TestClusterConfigFile
	}
	// This is just reading a config so it's less of a security concern
	// #nosec
	confContent, err := os.ReadFile(configLocation)
	if err != nil {
		return nil, fmt.Errorf("failed to read test cluster config file %s: %w", configLocation, err)
	}
	confContent = []byte(os.ExpandEnv(string(confContent)))
	var options TestClusterConfig
	if err := yaml.Unmarshal(confContent, &options); err != nil {
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
		if err := yaml.Unmarshal(fiConfigContent, &fiOptions); err != nil {
			return nil, fmt.Errorf("failed to decode test cluster fault injection config %s: %w", TestFlags.FaultInjectionConfigFile, err)
		}
		options.FaultInjection = fiOptions.FaultInjection
	}

	options.FrontendAddress = TestFlags.FrontendAddr
	return &options, nil
}

func (s *FunctionalTestBase) tearDownSuite() {
	s.Require().NoError(s.markNamespaceAsDeleted(s.namespace))
	s.Require().NoError(s.markNamespaceAsDeleted(s.foreignNamespace))
	if s.archivalNamespace != "" {
		s.Require().NoError(s.markNamespaceAsDeleted(s.archivalNamespace))
	}

	if s.testCluster != nil {
		s.NoError(s.testCluster.TearDownCluster())
		s.testCluster = nil
	}

	s.engine = nil
	s.adminClient = nil
}

func (s *FunctionalTestBase) registerNamespace(
	namespace string,
	retention time.Duration,
	historyArchivalState enumspb.ArchivalState,
	historyArchivalURI string,
	visibilityArchivalState enumspb.ArchivalState,
	visibilityArchivalURI string,
) error {
	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(10000 * time.Second)
	defer cancel()
	_, err := s.engine.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		Description:                      namespace,
		WorkflowExecutionRetentionPeriod: durationpb.New(retention),
		HistoryArchivalState:             historyArchivalState,
		HistoryArchivalUri:               historyArchivalURI,
		VisibilityArchivalState:          visibilityArchivalState,
		VisibilityArchivalUri:            visibilityArchivalURI,
	})

	if err != nil {
		return err
	}

	// Set up default alias for custom search attributes.
	_, err = s.engine.UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		Config: &namespacepb.NamespaceConfig{
			CustomSearchAttributeAliases: map[string]string{
				"Bool01":     "CustomBoolField",
				"Datetime01": "CustomDatetimeField",
				"Double01":   "CustomDoubleField",
				"Int01":      "CustomIntField",
				"Keyword01":  "CustomKeywordField",
				"Text01":     "CustomTextField",
			},
		},
	})

	return err
}

func (s *FunctionalTestBase) markNamespaceAsDeleted(
	namespace string,
) error {
	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(10000 * time.Second)
	defer cancel()
	_, err := s.engine.UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{
			State: enumspb.NAMESPACE_STATE_DELETED,
		},
	})

	return err
}

func (s *FunctionalTestBase) randomizeStr(id string) string {
	return fmt.Sprintf("%v-%v", id, uuid.New())
}

func (s *FunctionalTestBase) getHistory(namespace string, execution *commonpb.WorkflowExecution) []*historypb.HistoryEvent {
	historyResponse, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:       namespace,
		Execution:       execution,
		MaximumPageSize: 5, // Use small page size to force pagination code path
	})
	s.Require().NoError(err)

	events := historyResponse.History.Events
	for historyResponse.NextPageToken != nil {
		historyResponse, err = s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:     namespace,
			Execution:     execution,
			NextPageToken: historyResponse.NextPageToken,
		})
		s.Require().NoError(err)
		events = append(events, historyResponse.History.Events...)
	}

	return events
}

func (s *FunctionalTestBase) decodePayloadsString(ps *commonpb.Payloads) string {
	s.T().Helper()
	var r string
	s.NoError(payloads.Decode(ps, &r))
	return r
}

func (s *FunctionalTestBase) decodePayloadsInt(ps *commonpb.Payloads) int {
	s.T().Helper()
	var r int
	s.NoError(payloads.Decode(ps, &r))
	return r
}

func (s *FunctionalTestBase) decodePayloadsByteSliceInt32(ps *commonpb.Payloads) (r int32) {
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

// To register archival namespace we can't use frontend API as the retention period is set to 0 for testing,
// and request will be rejected by frontend. Here we make a call directly to persistence to register
// the namespace.
func (s *FunctionalTestBase) registerArchivalNamespace(archivalNamespace string) error {
	currentClusterName := s.testCluster.testBase.ClusterMetadata.GetCurrentClusterName()
	namespaceRequest := &persistence.CreateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:    uuid.New(),
				Name:  archivalNamespace,
				State: enumspb.NAMESPACE_STATE_REGISTERED,
			},
			Config: &persistencespb.NamespaceConfig{
				Retention:               timestamp.DurationFromDays(0),
				HistoryArchivalState:    enumspb.ARCHIVAL_STATE_ENABLED,
				HistoryArchivalUri:      s.testCluster.archiverBase.historyURI,
				VisibilityArchivalState: enumspb.ARCHIVAL_STATE_ENABLED,
				VisibilityArchivalUri:   s.testCluster.archiverBase.visibilityURI,
				BadBinaries:             &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
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
	response, err := s.testCluster.testBase.MetadataManager.CreateNamespace(context.Background(), namespaceRequest)

	s.Logger.Info("Register namespace succeeded",
		tag.WorkflowNamespace(archivalNamespace),
		tag.WorkflowNamespaceID(response.ID),
	)
	return err
}
