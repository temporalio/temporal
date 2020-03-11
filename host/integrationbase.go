// Copyright (c) 2016 Uber Technologies, Inc.
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
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/workflowservice"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"

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

		testCluster        *TestCluster
		testClusterConfig  *TestClusterConfig
		engine             FrontendClient
		adminClient        AdminClient
		Logger             log.Logger
		domainName         string
		foreignDomainName  string
		archivalDomainName string
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

	s.domainName = s.randomizeStr("integration-test-domain")
	s.Require().NoError(
		s.registerDomain(s.domainName, 1, enums.ArchivalStatusDisabled, "", enums.ArchivalStatusDisabled, ""))

	s.foreignDomainName = s.randomizeStr("integration-foreign-test-domain")
	s.Require().NoError(
		s.registerDomain(s.foreignDomainName, 1, enums.ArchivalStatusDisabled, "", enums.ArchivalStatusDisabled, ""))

	s.Require().NoError(s.registerArchivalDomain())

	// this sleep is necessary because domainv2 cache gets refreshed in the
	// background only every domainCacheRefreshInterval period
	time.Sleep(cache.DomainCacheRefreshInterval + time.Second)
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

func (s *IntegrationBase) registerDomain(
	domain string,
	retentionDays int,
	historyArchivalStatus enums.ArchivalStatus,
	historyArchivalURI string,
	visibilityArchivalStatus enums.ArchivalStatus,
	visibilityArchivalURI string,
) error {
	ctx, cancel := rpc.NewContextWithTimeoutAndHeaders(10000 * time.Second)
	defer cancel()
	_, err := s.engine.RegisterDomain(ctx, &workflowservice.RegisterDomainRequest{
		Name:                                   domain,
		Description:                            domain,
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

func (s *IntegrationBase) printWorkflowHistory(domain string, execution *commonproto.WorkflowExecution) {
	events := s.getHistory(domain, execution)
	history := &commonproto.History{
		Events: events,
	}
	common.PrettyPrintHistory(history, s.Logger)
}

func (s *IntegrationBase) getHistory(domain string, execution *commonproto.WorkflowExecution) []*commonproto.HistoryEvent {
	historyResponse, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Domain:          domain,
		Execution:       execution,
		MaximumPageSize: 5, // Use small page size to force pagination code path
	})
	s.Require().NoError(err)

	events := historyResponse.History.Events
	for historyResponse.NextPageToken != nil {
		historyResponse, err = s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Domain:        domain,
			Execution:     execution,
			NextPageToken: historyResponse.NextPageToken,
		})
		s.Require().NoError(err)
		events = append(events, historyResponse.History.Events...)
	}

	return events
}

// To register archival domain we can't use frontend API as the retention period is set to 0 for testing,
// and request will be rejected by frontend. Here we make a call directly to persistence to register
// the domain.
func (s *IntegrationBase) registerArchivalDomain() error {
	s.archivalDomainName = s.randomizeStr("integration-archival-enabled-domain")
	currentClusterName := s.testCluster.testBase.ClusterMetadata.GetCurrentClusterName()
	domainRequest := &persistence.CreateDomainRequest{
		Info: &persistence.DomainInfo{
			ID:     uuid.New(),
			Name:   s.archivalDomainName,
			Status: persistence.DomainStatusRegistered,
		},
		Config: &persistence.DomainConfig{
			Retention:                0,
			HistoryArchivalStatus:    enums.ArchivalStatusEnabled,
			HistoryArchivalURI:       s.testCluster.archiverBase.historyURI,
			VisibilityArchivalStatus: enums.ArchivalStatusEnabled,
			VisibilityArchivalURI:    s.testCluster.archiverBase.visibilityURI,
			BadBinaries:              commonproto.BadBinaries{Binaries: map[string]*commonproto.BadBinaryInfo{}},
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: currentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: currentClusterName},
			},
		},
		IsGlobalDomain:  false,
		FailoverVersion: common.EmptyVersion,
	}
	response, err := s.testCluster.testBase.MetadataManager.CreateDomain(domainRequest)

	s.Logger.Info("Register domain succeeded",
		tag.WorkflowDomainName(s.archivalDomainName),
		tag.WorkflowDomainID(response.ID),
	)
	return err
}
