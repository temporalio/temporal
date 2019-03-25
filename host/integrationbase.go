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
	"os"
	"testing"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/uber-common/bark"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/persistence-tests"
)

type (
	// IntegrationBase is a base struct for integration tests
	IntegrationBase struct {
		TestCluster
		domainName        string
		foreignDomainName string
	}
)

func (s *IntegrationBase) setupSuite(enableGlobalDomain bool, isMasterCluster bool, enableWorker bool, enableArchival bool) {
	s.setupLogger()
	s.setupCadenceHost(enableGlobalDomain, isMasterCluster, enableWorker, enableArchival)

	s.domainName = "integration-test-domain"
	s.MetadataManager.CreateDomain(&persistence.CreateDomainRequest{
		Info: &persistence.DomainInfo{
			ID:          uuid.New(),
			Name:        s.domainName,
			Status:      persistence.DomainStatusRegistered,
			Description: "Test domain for integration test",
		},
		Config: &persistence.DomainConfig{
			Retention:  1,
			EmitMetric: false,
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{},
	})

	s.foreignDomainName = "integration-foreign-test-domain"
	s.MetadataManager.CreateDomain(&persistence.CreateDomainRequest{
		Info: &persistence.DomainInfo{
			ID:          uuid.New(),
			Name:        s.foreignDomainName,
			Status:      persistence.DomainStatusRegistered,
			Description: "Test foreign domain for integration test",
		},
		Config: &persistence.DomainConfig{
			Retention:  1,
			EmitMetric: false,
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{},
	})
}

func (s *IntegrationBase) setupLogger() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

	logger := log.New()
	formatter := &log.TextFormatter{}
	formatter.FullTimestamp = true
	logger.Formatter = formatter
	s.Logger = bark.NewLoggerFromLogrus(logger)
}

func (s *IntegrationBase) setupCadenceHost(enableGlobalDomain bool, isMasterCluster bool, enableWorker bool, enableArchival bool) {
	persistOptions := &persistencetests.TestBaseOptions{
		EnableGlobalDomain: enableGlobalDomain,
		IsMasterCluster:    isMasterCluster,
		EnableArchival:     enableArchival,
	}

	options := &TestClusterOptions{
		PersistOptions:   persistOptions,
		EnableWorker:     enableWorker,
		MessagingClient:  mocks.NewMockMessagingClient(&mocks.KafkaProducer{}, nil),
		NumHistoryShards: testNumberOfHistoryShards,
		EnableEventsV2:   *EnableEventsV2,
	}
	s.SetupCluster(options)
}

func (s *IntegrationBase) tearDownSuite() {
	s.TearDownCluster()
}

func (s *IntegrationBase) printWorkflowHistory(domain string, execution *workflow.WorkflowExecution) {
	events := s.getHistory(domain, execution)
	history := &workflow.History{}
	history.Events = events
	common.PrettyPrintHistory(history, s.Logger)
}

func (s *IntegrationBase) getHistory(domain string, execution *workflow.WorkflowExecution) []*workflow.HistoryEvent {
	historyResponse, err := s.engine.GetWorkflowExecutionHistory(createContext(), &workflow.GetWorkflowExecutionHistoryRequest{
		Domain:          common.StringPtr(domain),
		Execution:       execution,
		MaximumPageSize: common.Int32Ptr(5), // Use small page size to force pagination code path
	})
	s.Require().NoError(err)

	events := historyResponse.History.Events
	for historyResponse.NextPageToken != nil {
		historyResponse, err = s.engine.GetWorkflowExecutionHistory(createContext(), &workflow.GetWorkflowExecutionHistoryRequest{
			Domain:        common.StringPtr(domain),
			Execution:     execution,
			NextPageToken: historyResponse.NextPageToken,
		})
		s.Require().NoError(err)
		events = append(events, historyResponse.History.Events...)
	}

	return events
}
