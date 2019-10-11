// Copyright (c) 2018 Uber Technologies, Inc.
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

// +build !race
// +build esintegration

// to run locally, make sure kafka and es is running,
// then run cmd `go test -v ./host/xdc -run TestESCrossDCTestSuite -tags esintegration`
package xdc

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/olivere/elastic"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/environment"
	"github.com/uber/cadence/host"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

const (
	numOfRetry        = 100
	waitTimeInMs      = 400
	waitForESToSettle = 4 * time.Second // wait es shards for some time ensure data consistent
)

type esCrossDCTestSuite struct {
	// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
	// not merely log an error
	*require.Assertions
	suite.Suite
	cluster1       *host.TestCluster
	cluster2       *host.TestCluster
	logger         log.Logger
	clusterConfigs []*host.TestClusterConfig
	esClient       *elastic.Client

	testSearchAttributeKey string
	testSearchAttributeVal string
}

func TestESCrossDCTestSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(esCrossDCTestSuite))
}

var (
	clusterNameES              = []string{"active-es", "standby-es"}
	clusterReplicationConfigES = []*workflow.ClusterReplicationConfiguration{
		{
			ClusterName: common.StringPtr(clusterNameES[0]),
		},
		{
			ClusterName: common.StringPtr(clusterNameES[1]),
		},
	}
)

func (s *esCrossDCTestSuite) SetupSuite() {
	zapLogger, err := zap.NewDevelopment()
	// cannot use s.Nil since it is not initialized
	s.Require().NoError(err)
	s.logger = loggerimpl.NewLogger(zapLogger)

	fileName := "../testdata/xdc_integration_es_clusters.yaml"
	if host.TestFlags.TestClusterConfigFile != "" {
		fileName = host.TestFlags.TestClusterConfigFile
	}
	environment.SetupEnv()

	confContent, err := ioutil.ReadFile(fileName)
	s.Require().NoError(err)
	confContent = []byte(os.ExpandEnv(string(confContent)))

	var clusterConfigs []*host.TestClusterConfig
	s.Require().NoError(yaml.Unmarshal(confContent, &clusterConfigs))
	s.clusterConfigs = clusterConfigs

	c, err := host.NewCluster(clusterConfigs[0], s.logger.WithTags(tag.ClusterName(clusterNameES[0])))
	s.Require().NoError(err)
	s.cluster1 = c

	c, err = host.NewCluster(clusterConfigs[1], s.logger.WithTags(tag.ClusterName(clusterNameES[1])))
	s.Require().NoError(err)
	s.cluster2 = c

	s.esClient = host.CreateESClient(s.Suite, s.clusterConfigs[0].ESConfig.URL.String())
	host.PutIndexTemplate(s.Suite, s.esClient, "../testdata/es_index_template.json", "test-visibility-template")
	host.CreateIndex(s.Suite, s.esClient, s.clusterConfigs[0].ESConfig.Indices[common.VisibilityAppName])
	host.CreateIndex(s.Suite, s.esClient, s.clusterConfigs[1].ESConfig.Indices[common.VisibilityAppName])

	s.testSearchAttributeKey = definition.CustomStringField
	s.testSearchAttributeVal = "test value"
}

func (s *esCrossDCTestSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

func (s *esCrossDCTestSuite) TearDownSuite() {
	s.cluster1.TearDownCluster()
	s.cluster2.TearDownCluster()
	host.DeleteIndex(s.Suite, s.esClient, s.clusterConfigs[0].ESConfig.Indices[common.VisibilityAppName])
	host.DeleteIndex(s.Suite, s.esClient, s.clusterConfigs[1].ESConfig.Indices[common.VisibilityAppName])
}

func (s *esCrossDCTestSuite) TestSearchAttributes() {
	domainName := "test-xdc-search-attr-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflow.RegisterDomainRequest{
		Name:                                   common.StringPtr(domainName),
		Clusters:                               clusterReplicationConfigES,
		ActiveClusterName:                      common.StringPtr(clusterNameES[0]),
		IsGlobalDomain:                         common.BoolPtr(true),
		WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(1),
	}
	err := client1.RegisterDomain(createContext(), regReq)
	s.NoError(err)

	descReq := &workflow.DescribeDomainRequest{
		Name: common.StringPtr(domainName),
	}
	resp, err := client1.DescribeDomain(createContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)
	// Wait for domain cache to pick the change
	time.Sleep(cacheRefreshInterval)

	client2 := s.cluster2.GetFrontendClient() // standby
	resp2, err := client2.DescribeDomain(createContext(), descReq)
	s.NoError(err)
	s.NotNil(resp2)
	s.Equal(resp, resp2)

	// start a workflow
	id := "xdc-search-attr-test-" + uuid.New()
	wt := "xdc-search-attr-test-type"
	tl := "xdc-search-attr-test-tasklist"
	identity := "worker1"
	workflowType := &workflow.WorkflowType{Name: common.StringPtr(wt)}
	taskList := &workflow.TaskList{Name: common.StringPtr(tl)}
	attrValBytes, _ := json.Marshal(s.testSearchAttributeVal)
	searchAttr := &workflow.SearchAttributes{
		IndexedFields: map[string][]byte{
			s.testSearchAttributeKey: attrValBytes,
		},
	}
	startReq := &workflow.StartWorkflowExecutionRequest{
		RequestId:                           common.StringPtr(uuid.New()),
		Domain:                              common.StringPtr(domainName),
		WorkflowId:                          common.StringPtr(id),
		WorkflowType:                        workflowType,
		TaskList:                            taskList,
		Input:                               nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
		SearchAttributes:                    searchAttr,
	}
	startTime := time.Now().UnixNano()
	we, err := client1.StartWorkflowExecution(createContext(), startReq)
	s.Nil(err)
	s.NotNil(we.GetRunId())

	s.logger.Info("StartWorkflowExecution \n", tag.WorkflowRunID(we.GetRunId()))

	startFilter := &workflow.StartTimeFilter{}
	startFilter.EarliestTime = common.Int64Ptr(startTime)
	query := fmt.Sprintf(`WorkflowID = "%s" and %s = "%s"`, id, s.testSearchAttributeKey, s.testSearchAttributeVal)
	listRequest := &workflow.ListWorkflowExecutionsRequest{
		Domain:   common.StringPtr(domainName),
		PageSize: common.Int32Ptr(100),
		Query:    common.StringPtr(query),
	}

	testListResult := func(client host.FrontendClient) {
		var openExecution *workflow.WorkflowExecutionInfo
		for i := 0; i < numOfRetry; i++ {
			startFilter.LatestTime = common.Int64Ptr(time.Now().UnixNano())

			resp, err := client.ListWorkflowExecutions(createContext(), listRequest)
			s.Nil(err)
			if len(resp.GetExecutions()) == 1 {
				openExecution = resp.GetExecutions()[0]
				break
			}
			time.Sleep(waitTimeInMs * time.Millisecond)
		}
		s.NotNil(openExecution)
		s.Equal(we.GetRunId(), openExecution.GetExecution().GetRunId())
		searchValBytes := openExecution.SearchAttributes.GetIndexedFields()[s.testSearchAttributeKey]
		var searchVal string
		json.Unmarshal(searchValBytes, &searchVal)
		s.Equal(s.testSearchAttributeVal, searchVal)
	}

	// List workflow in active
	engine1 := s.cluster1.GetFrontendClient()
	testListResult(engine1)

	// List workflow in standby
	engine2 := s.cluster2.GetFrontendClient()
	testListResult(engine2)

	// upsert search attributes
	dtHandler := func(execution *workflow.WorkflowExecution, wt *workflow.WorkflowType,
		previousStartedEventID, startedEventID int64, history *workflow.History) ([]byte, []*workflow.Decision, error) {

		upsertDecision := &workflow.Decision{
			DecisionType: common.DecisionTypePtr(workflow.DecisionTypeUpsertWorkflowSearchAttributes),
			UpsertWorkflowSearchAttributesDecisionAttributes: &workflow.UpsertWorkflowSearchAttributesDecisionAttributes{
				SearchAttributes: getUpsertSearchAttributes(),
			}}

		return nil, []*workflow.Decision{upsertDecision}, nil
	}

	poller := host.TaskPoller{
		Engine:          client1,
		Domain:          domainName,
		TaskList:        taskList,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	_, err = poller.PollAndProcessDecisionTask(false, false)
	s.logger.Info("PollAndProcessDecisionTask", tag.Error(err))
	s.Nil(err)

	time.Sleep(waitForESToSettle)

	listRequest = &workflow.ListWorkflowExecutionsRequest{
		Domain:   common.StringPtr(domainName),
		PageSize: common.Int32Ptr(int32(2)),
		Query:    common.StringPtr(fmt.Sprintf(`WorkflowType = '%s' and CloseTime = missing`, wt)),
	}

	testListResult = func(client host.FrontendClient) {
		verified := false
		for i := 0; i < numOfRetry; i++ {
			resp, err := client.ListWorkflowExecutions(createContext(), listRequest)
			s.Nil(err)
			if len(resp.GetExecutions()) == 1 {
				execution := resp.GetExecutions()[0]
				retrievedSearchAttr := execution.SearchAttributes
				if retrievedSearchAttr != nil && len(retrievedSearchAttr.GetIndexedFields()) == 2 {
					fields := retrievedSearchAttr.GetIndexedFields()
					searchValBytes := fields[s.testSearchAttributeKey]
					var searchVal string
					json.Unmarshal(searchValBytes, &searchVal)
					s.Equal("another string", searchVal)

					searchValBytes2 := fields[definition.CustomIntField]
					var searchVal2 int
					json.Unmarshal(searchValBytes2, &searchVal2)
					s.Equal(123, searchVal2)

					verified = true
					break
				}
			}
			time.Sleep(waitTimeInMs * time.Millisecond)
		}
		s.True(verified)
	}

	// test upsert result in active
	testListResult(engine1)

	// terminate workflow
	terminateReason := "force terminate to make sure standby process tasks"
	terminateDetails := []byte("terminate details.")
	err = client1.TerminateWorkflowExecution(createContext(), &workflow.TerminateWorkflowExecutionRequest{
		Domain: common.StringPtr(domainName),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(id),
		},
		Reason:   common.StringPtr(terminateReason),
		Details:  terminateDetails,
		Identity: common.StringPtr(identity),
	})
	s.Nil(err)

	// check terminate done
	executionTerminated := false
	getHistoryReq := &workflow.GetWorkflowExecutionHistoryRequest{
		Domain: common.StringPtr(domainName),
		Execution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(id),
		},
	}
GetHistoryLoop:
	for i := 0; i < 10; i++ {
		historyResponse, err := client1.GetWorkflowExecutionHistory(createContext(), getHistoryReq)
		s.Nil(err)
		history := historyResponse.History

		lastEvent := history.Events[len(history.Events)-1]
		if *lastEvent.EventType != workflow.EventTypeWorkflowExecutionTerminated {
			s.logger.Warn("Execution not terminated yet.")
			time.Sleep(100 * time.Millisecond)
			continue GetHistoryLoop
		}

		terminateEventAttributes := lastEvent.WorkflowExecutionTerminatedEventAttributes
		s.Equal(terminateReason, *terminateEventAttributes.Reason)
		s.Equal(terminateDetails, terminateEventAttributes.Details)
		s.Equal(identity, *terminateEventAttributes.Identity)
		executionTerminated = true
		break GetHistoryLoop
	}
	s.True(executionTerminated)

	// check history replicated to the other cluster
	var historyResponse *workflow.GetWorkflowExecutionHistoryResponse
	eventsReplicated := false
GetHistoryLoop2:
	for i := 0; i < numOfRetry; i++ {
		historyResponse, err = client2.GetWorkflowExecutionHistory(createContext(), getHistoryReq)
		if err == nil {
			history := historyResponse.History
			lastEvent := history.Events[len(history.Events)-1]
			if *lastEvent.EventType == workflow.EventTypeWorkflowExecutionTerminated {
				terminateEventAttributes := lastEvent.WorkflowExecutionTerminatedEventAttributes
				s.Equal(terminateReason, *terminateEventAttributes.Reason)
				s.Equal(terminateDetails, terminateEventAttributes.Details)
				s.Equal(identity, *terminateEventAttributes.Identity)
				eventsReplicated = true
				break GetHistoryLoop2
			}
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.Nil(err)
	s.True(eventsReplicated)

	// test upsert result in standby
	testListResult(engine2)
}

func getUpsertSearchAttributes() *workflow.SearchAttributes {
	attrValBytes1, _ := json.Marshal("another string")
	attrValBytes2, _ := json.Marshal(123)
	upsertSearchAttr := &workflow.SearchAttributes{
		IndexedFields: map[string][]byte{
			definition.CustomStringField: attrValBytes1,
			definition.CustomIntField:    attrValBytes2,
		},
	}
	return upsertSearchAttr
}
