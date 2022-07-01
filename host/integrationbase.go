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

package host

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
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
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/environment"
)

type (
	// IntegrationBase is a base struct for integration tests
	IntegrationBase struct {
		suite.Suite

		testCluster                 *TestCluster
		testClusterConfig           *TestClusterConfig
		engine                      FrontendClient
		adminClient                 AdminClient
		operatorClient              operatorservice.OperatorServiceClient
		Logger                      log.Logger
		namespace                   string
		testRawHistoryNamespaceName string
		foreignNamespace            string
		archivalNamespace           string
		dynamicConfigOverrides      map[dynamicconfig.Key]interface{}
	}
)

func (s *IntegrationBase) setupSuite(defaultClusterConfigFile string) {
	s.setupLogger()

	clusterConfig, err := GetTestClusterConfig(defaultClusterConfigFile)
	s.Require().NoError(err)
	clusterConfig.DynamicConfigOverrides = s.dynamicConfigOverrides
	s.testClusterConfig = clusterConfig

	if clusterConfig.FrontendAddress != "" {
		s.Logger.Info("Running integration test against specified frontend", tag.Address(TestFlags.FrontendAddr))

		connection, err := rpc.Dial(TestFlags.FrontendAddr, nil, s.Logger)
		if err != nil {
			s.Require().NoError(err)
		}

		s.engine = NewFrontendClient(connection)
		s.adminClient = NewAdminClient(connection)
		s.operatorClient = operatorservice.NewOperatorServiceClient(connection)
	} else {
		s.Logger.Info("Running integration test against test cluster")
		cluster, err := NewCluster(clusterConfig, s.Logger)
		s.Require().NoError(err)
		s.testCluster = cluster
		s.engine = s.testCluster.GetFrontendClient()
		s.adminClient = s.testCluster.GetAdminClient()
		s.operatorClient = s.testCluster.GetOperatorClient()
	}

	s.namespace = s.randomizeStr("integration-test-namespace")
	s.Require().NoError(s.registerNamespace(s.namespace, 24*time.Hour, enumspb.ARCHIVAL_STATE_DISABLED, "", enumspb.ARCHIVAL_STATE_DISABLED, ""))

	s.testRawHistoryNamespaceName = "TestRawHistoryNamespace"
	s.Require().NoError(s.registerNamespace(s.testRawHistoryNamespaceName, 24*time.Hour, enumspb.ARCHIVAL_STATE_DISABLED, "", enumspb.ARCHIVAL_STATE_DISABLED, ""))

	s.foreignNamespace = s.randomizeStr("integration-foreign-test-namespace")
	s.Require().NoError(s.registerNamespace(s.foreignNamespace, 24*time.Hour, enumspb.ARCHIVAL_STATE_DISABLED, "", enumspb.ARCHIVAL_STATE_DISABLED, ""))

	if clusterConfig.EnableArchival {
		s.archivalNamespace = s.randomizeStr("integration-archival-enabled-namespace")
		s.Require().NoError(s.registerArchivalNamespace(s.archivalNamespace))
	}

	if clusterConfig.FrontendAddress == "" {
		// Poke all the in-process namespace caches to refresh without waiting for the usual refresh interval.
		s.testCluster.RefreshNamespaceCache()
	} else {
		// Wait for one whole cycle of the namespace cache v2 refresh interval to be sure that our namespaces are loaded.
		// We are using real server so we don't know what cache refresh interval it uses. Fall back to the 10s old value.
		serverCacheRefreshInterval := 10 * time.Second
		time.Sleep(serverCacheRefreshInterval + time.Second)
	}
}

func (s *IntegrationBase) setupLogger() {
	s.Logger = log.NewTestLogger()
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
		return nil, fmt.Errorf("failed to read test cluster config file %v: %v", configLocation, err)
	}
	confContent = []byte(os.ExpandEnv(string(confContent)))
	var options TestClusterConfig
	if err := yaml.Unmarshal(confContent, &options); err != nil {
		return nil, fmt.Errorf("failed to decode test cluster config %v", err)
	}

	options.FrontendAddress = TestFlags.FrontendAddr
	if options.ESConfig != nil {
		options.ESConfig.Indices[client.VisibilityAppName] += uuid.New()
	}
	return &options, nil
}

func (s *IntegrationBase) tearDownSuite() {
	s.Require().NoError(s.deleteNamespace(s.namespace))
	s.Require().NoError(s.deleteNamespace(s.testRawHistoryNamespaceName))
	s.Require().NoError(s.deleteNamespace(s.foreignNamespace))
	if s.archivalNamespace != "" {
		s.Require().NoError(s.deleteNamespace(s.archivalNamespace))
	}

	if s.testCluster != nil {
		s.testCluster.TearDownCluster()
		s.testCluster = nil
	}

	s.engine = nil
	s.adminClient = nil
}

func (s *IntegrationBase) registerNamespace(
	namespace string,
	retention time.Duration,
	historyArchivalState enumspb.ArchivalState,
	historyArchivalURI string,
	visibilityArchivalState enumspb.ArchivalState,
	visibilityArchivalURI string,
) error {
	ctx, cancel := rpc.NewContextWithTimeoutAndHeaders(10000 * time.Second)
	defer cancel()
	_, err := s.engine.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		Description:                      namespace,
		WorkflowExecutionRetentionPeriod: &retention,
		HistoryArchivalState:             historyArchivalState,
		HistoryArchivalUri:               historyArchivalURI,
		VisibilityArchivalState:          visibilityArchivalState,
		VisibilityArchivalUri:            visibilityArchivalURI,
	})

	return err
}

func (s *IntegrationBase) deleteNamespace(
	namespace string,
) error {
	ctx, cancel := rpc.NewContextWithTimeoutAndHeaders(10000 * time.Second)
	defer cancel()
	_, err := s.engine.UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{
			State: enumspb.NAMESPACE_STATE_DELETED,
		},
	})

	return err
}

func (s *IntegrationBase) randomizeStr(id string) string {
	return fmt.Sprintf("%v-%v", id, uuid.New())
}

func (s *IntegrationBase) printWorkflowHistory(namespace string, execution *commonpb.WorkflowExecution) {
	events := s.getHistory(namespace, execution)
	history := &historypb.History{
		Events: events,
	}
	common.PrettyPrintHistory(history, s.Logger)
}

func (s *IntegrationBase) getHistory(namespace string, execution *commonpb.WorkflowExecution) []*historypb.HistoryEvent {
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

func (s *IntegrationBase) getLastEvent(namespace string, execution *commonpb.WorkflowExecution) *historypb.HistoryEvent {
	events := s.getHistory(namespace, execution)
	s.Require().NotEmpty(events)
	return events[len(events)-1]
}

func (s *IntegrationBase) decodePayloadsString(ps *commonpb.Payloads) string {
	s.T().Helper()
	var r string
	s.NoError(payloads.Decode(ps, &r))
	return r
}

func (s *IntegrationBase) decodePayloadsInt(ps *commonpb.Payloads) int {
	s.T().Helper()
	var r int
	s.NoError(payloads.Decode(ps, &r))
	return r
}

func (s *IntegrationBase) decodePayloadsByteSliceInt32(ps *commonpb.Payloads) (r int32) {
	s.T().Helper()
	var buf []byte
	s.NoError(payloads.Decode(ps, &buf))
	s.NoError(binary.Read(bytes.NewReader(buf), binary.LittleEndian, &r))
	return
}

func (s *IntegrationBase) DurationNear(value, target, tolerance time.Duration) {
	s.T().Helper()
	s.Greater(value, target-tolerance)
	s.Less(value, target+tolerance)
}

// To register archival namespace we can't use frontend API as the retention period is set to 0 for testing,
// and request will be rejected by frontend. Here we make a call directly to persistence to register
// the namespace.
func (s *IntegrationBase) registerArchivalNamespace(archivalNamespace string) error {
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
