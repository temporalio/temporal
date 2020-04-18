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
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	eventpb "go.temporal.io/temporal-proto/event"
	executionpb "go.temporal.io/temporal-proto/execution"
	namespacepb "go.temporal.io/temporal-proto/namespace"
	"go.temporal.io/temporal-proto/workflowservice"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/rpc"
	"github.com/temporalio/temporal/environment"
)

type (
	// IntegrationBase is a base struct for integration tests
	IntegrationBase struct {
		suite.Suite

		testCluster                 *TestCluster
		testClusterConfig           *TestClusterConfig
		engine                      FrontendClient
		adminClient                 AdminClient
		Logger                      log.Logger
		namespace                   string
		testRawHistoryNamespaceName string
		foreignNamespace            string
		archivalNamespace           string
	}
)

func (s *IntegrationBase) setupSuite(defaultClusterConfigFile string) {
	s.setupLogger()

	clusterConfig, err := GetTestClusterConfig(defaultClusterConfigFile)
	s.Require().NoError(err)
	s.testClusterConfig = clusterConfig

	if clusterConfig.FrontendAddress != "" {
		s.Logger.Info("Running integration test against specified frontend", tag.Address(TestFlags.FrontendAddr))

		connection, err := rpc.Dial(TestFlags.FrontendAddrGRPC)
		if err != nil {
			s.Require().NoError(err)
		}

		s.engine = NewFrontendClient(connection)
		s.adminClient = NewAdminClient(connection)
	} else {
		s.Logger.Info("Running integration test against test cluster")
		cluster, err := NewCluster(clusterConfig, s.Logger)
		s.Require().NoError(err)
		s.testCluster = cluster
		s.engine = s.testCluster.GetFrontendClient()
		s.adminClient = s.testCluster.GetAdminClient()
	}

	s.testRawHistoryNamespaceName = "TestRawHistoryNamespace"
	s.namespace = s.randomizeStr("integration-test-namespace")
	s.Require().NoError(
		s.registerNamespace(s.namespace, 1, namespacepb.ArchivalStatus_Disabled, "", namespacepb.ArchivalStatus_Disabled, ""))
	s.Require().NoError(
		s.registerNamespace(s.testRawHistoryNamespaceName, 1, namespacepb.ArchivalStatus_Disabled, "", namespacepb.ArchivalStatus_Disabled, ""))

	s.foreignNamespace = s.randomizeStr("integration-foreign-test-namespace")
	s.Require().NoError(
		s.registerNamespace(s.foreignNamespace, 1, namespacepb.ArchivalStatus_Disabled, "", namespacepb.ArchivalStatus_Disabled, ""))

	s.Require().NoError(s.registerArchivalNamespace())

	// this sleep is necessary because namespacev2 cache gets refreshed in the
	// background only every namespaceCacheRefreshInterval period
	time.Sleep(cache.NamespaceCacheRefreshInterval + time.Second)
}

func (s *IntegrationBase) setupLogger() {
	zapLogger, err := zap.NewDevelopment()
	s.Require().NoError(err)
	s.Logger = loggerimpl.NewLogger(zapLogger)
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
	confContent, err := ioutil.ReadFile(configLocation)
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
		options.ESConfig.Indices[common.VisibilityAppName] += uuid.New()
	}
	return &options, nil
}

func (s *IntegrationBase) tearDownSuite() {
	if s.testCluster != nil {
		s.testCluster.TearDownCluster()
		s.testCluster = nil
		s.engine = nil
		s.adminClient = nil
	}
}

func (s *IntegrationBase) registerNamespace(
	namespace string,
	retentionDays int,
	historyArchivalStatus namespacepb.ArchivalStatus,
	historyArchivalURI string,
	visibilityArchivalStatus namespacepb.ArchivalStatus,
	visibilityArchivalURI string,
) error {
	ctx, cancel := rpc.NewContextWithTimeoutAndHeaders(10000 * time.Second)
	defer cancel()
	_, err := s.engine.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Name:                                   namespace,
		Description:                            namespace,
		WorkflowExecutionRetentionPeriodInDays: int32(retentionDays),
		HistoryArchivalStatus:                  historyArchivalStatus,
		HistoryArchivalURI:                     historyArchivalURI,
		VisibilityArchivalStatus:               visibilityArchivalStatus,
		VisibilityArchivalURI:                  visibilityArchivalURI,
	})

	return err
}

func (s *IntegrationBase) randomizeStr(id string) string {
	return fmt.Sprintf("%v-%v", id, uuid.New())
}

func (s *IntegrationBase) printWorkflowHistory(namespace string, execution *executionpb.WorkflowExecution) {
	events := s.getHistory(namespace, execution)
	history := &eventpb.History{
		Events: events,
	}
	common.PrettyPrintHistory(history, s.Logger)
}

func (s *IntegrationBase) getHistory(namespace string, execution *executionpb.WorkflowExecution) []*eventpb.HistoryEvent {
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

// To register archival namespace we can't use frontend API as the retention period is set to 0 for testing,
// and request will be rejected by frontend. Here we make a call directly to persistence to register
// the namespace.
func (s *IntegrationBase) registerArchivalNamespace() error {
	s.archivalNamespace = s.randomizeStr("integration-archival-enabled-namespace")
	currentClusterName := s.testCluster.testBase.ClusterMetadata.GetCurrentClusterName()
	namespaceRequest := &persistence.CreateNamespaceRequest{
		Namespace: &persistenceblobs.NamespaceDetail{
			Info: &persistenceblobs.NamespaceInfo{
				Id:     uuid.NewRandom(),
				Name:   s.archivalNamespace,
				Status: namespacepb.NamespaceStatus_Registered,
			},
			Config: &persistenceblobs.NamespaceConfig{
				RetentionDays:            0,
				HistoryArchivalStatus:    namespacepb.ArchivalStatus_Enabled,
				HistoryArchivalURI:       s.testCluster.archiverBase.historyURI,
				VisibilityArchivalStatus: namespacepb.ArchivalStatus_Enabled,
				VisibilityArchivalURI:    s.testCluster.archiverBase.visibilityURI,
				BadBinaries:              &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
			},
			ReplicationConfig: &persistenceblobs.NamespaceReplicationConfig{
				ActiveClusterName: currentClusterName,
				Clusters: []string{
					currentClusterName,
				},
			},

			FailoverVersion: common.EmptyVersion,
		},
		IsGlobalNamespace: false,
	}
	response, err := s.testCluster.testBase.MetadataManager.CreateNamespace(namespaceRequest)

	s.Logger.Info("Register namespace succeeded",
		tag.WorkflowNamespace(s.archivalNamespace),
		tag.WorkflowNamespaceIDBytes(response.ID),
	)
	return err
}
