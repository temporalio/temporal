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

// +build !race
// +build esintegration

// to run locally, make sure kafka and es is running,
// then run cmd `go test -v ./host/xdc -run TestESCrossDCTestSuite -tags esintegration`
package xdc

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	filterpb "go.temporal.io/api/filter/v1"
	historypb "go.temporal.io/api/history/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/elasticsearch"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/loggerimpl"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/environment"
	"go.temporal.io/server/host"
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
	esClient       elasticsearch.IntegrationTestsClient

	testSearchAttributeKey string
	testSearchAttributeVal string
}

func TestESCrossDCTestSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(esCrossDCTestSuite))
}

var (
	clusterNameES              = []string{"active-es", "standby-es"}
	clusterReplicationConfigES = []*replicationpb.ClusterReplicationConfig{
		{
			ClusterName: clusterNameES[0],
		},
		{
			ClusterName: clusterNameES[1],
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

	s.esClient = host.CreateESClient(s.Suite, s.clusterConfigs[0].ESConfig.URL.String(), s.clusterConfigs[0].ESConfig.Version)
	host.PutIndexTemplate(s.Suite, s.esClient, fmt.Sprintf("../testdata/es_%s_index_template.json", s.clusterConfigs[0].ESConfig.Version), "test-visibility-template")
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
	namespace := "test-xdc-search-attr-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		Clusters:                         clusterReplicationConfigES,
		ActiveClusterName:                clusterNameES[0],
		IsGlobalNamespace:                true,
		WorkflowExecutionRetentionPeriod: timestamp.DurationPtr(1 * time.Hour * 24),
	}
	_, err := client1.RegisterNamespace(host.NewContext(), regReq)
	s.NoError(err)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	}
	resp, err := client1.DescribeNamespace(host.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	client2 := s.cluster2.GetFrontendClient() // standby
	resp2, err := client2.DescribeNamespace(host.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp2)
	s.Equal(resp, resp2)

	// start a workflow
	id := "xdc-search-attr-test-" + uuid.New()
	wt := "xdc-search-attr-test-type"
	tl := "xdc-search-attr-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl}
	attrValBytes, _ := payload.Encode(s.testSearchAttributeVal)
	searchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			s.testSearchAttributeKey: attrValBytes,
		},
	}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
		SearchAttributes:    searchAttr,
	}
	startTime := time.Now().UTC()
	we, err := client1.StartWorkflowExecution(host.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	s.logger.Info("StartWorkflowExecution \n", tag.WorkflowRunID(we.GetRunId()))

	startFilter := &filterpb.StartTimeFilter{}
	startFilter.EarliestTime = &startTime
	query := fmt.Sprintf(`WorkflowId = "%s" and %s = "%s"`, id, s.testSearchAttributeKey, s.testSearchAttributeVal)
	listRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: namespace,
		PageSize:  5,
		Query:     query,
	}

	testListResult := func(client host.FrontendClient) {
		var openExecution *workflowpb.WorkflowExecutionInfo
		for i := 0; i < numOfRetry; i++ {
			startFilter.LatestTime = timestamp.TimePtr(time.Now().UTC())

			resp, err := client.ListWorkflowExecutions(host.NewContext(), listRequest)
			s.NoError(err)
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
		payload.Decode(searchValBytes, &searchVal)
		s.Equal(s.testSearchAttributeVal, searchVal)
	}

	// List workflow in active
	engine1 := s.cluster1.GetFrontendClient()
	testListResult(engine1)

	// List workflow in standby
	engine2 := s.cluster2.GetFrontendClient()
	testListResult(engine2)

	// upsert search attributes
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		upsertCommand := &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES,
			Attributes: &commandpb.Command_UpsertWorkflowSearchAttributesCommandAttributes{UpsertWorkflowSearchAttributesCommandAttributes: &commandpb.UpsertWorkflowSearchAttributesCommandAttributes{
				SearchAttributes: getUpsertSearchAttributes(),
			}}}

		return []*commandpb.Command{upsertCommand}, nil
	}

	poller := host.TaskPoller{
		Engine:              client1,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	time.Sleep(waitForESToSettle)

	listRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: namespace,
		PageSize:  int32(2),
		Query:     fmt.Sprintf(`WorkflowType = '%s' and CloseTime = missing`, wt),
	}

	testListResult = func(client host.FrontendClient) {
		verified := false
		for i := 0; i < numOfRetry; i++ {
			resp, err := client.ListWorkflowExecutions(host.NewContext(), listRequest)
			s.NoError(err)
			if len(resp.GetExecutions()) == 1 {
				execution := resp.GetExecutions()[0]
				retrievedSearchAttr := execution.SearchAttributes
				if retrievedSearchAttr != nil && len(retrievedSearchAttr.GetIndexedFields()) == 2 {
					fields := retrievedSearchAttr.GetIndexedFields()
					searchValBytes := fields[s.testSearchAttributeKey]
					var searchVal string
					payload.Decode(searchValBytes, &searchVal)
					s.Equal("another string", searchVal)

					searchValBytes2 := fields[definition.CustomIntField]
					var searchVal2 int
					payload.Decode(searchValBytes2, &searchVal2)
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
	terminateDetails := payloads.EncodeString("terminate details")
	_, err = client1.TerminateWorkflowExecution(host.NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
		Reason:   terminateReason,
		Details:  terminateDetails,
		Identity: identity,
	})
	s.NoError(err)

	// check terminate done
	executionTerminated := false
	getHistoryReq := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
	}
GetHistoryLoop:
	for i := 0; i < 10; i++ {
		historyResponse, err := client1.GetWorkflowExecutionHistory(host.NewContext(), getHistoryReq)
		s.NoError(err)
		history := historyResponse.History

		lastEvent := history.Events[len(history.Events)-1]
		if lastEvent.EventType != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED {
			s.logger.Warn("Execution not terminated yet")
			time.Sleep(100 * time.Millisecond)
			continue GetHistoryLoop
		}

		terminateEventAttributes := lastEvent.GetWorkflowExecutionTerminatedEventAttributes()
		s.Equal(terminateReason, terminateEventAttributes.Reason)
		s.Equal(terminateDetails, terminateEventAttributes.Details)
		s.Equal(identity, terminateEventAttributes.Identity)
		executionTerminated = true
		break GetHistoryLoop
	}
	s.True(executionTerminated)

	// check history replicated to the other cluster
	var historyResponse *workflowservice.GetWorkflowExecutionHistoryResponse
	eventsReplicated := false
GetHistoryLoop2:
	for i := 0; i < numOfRetry; i++ {
		historyResponse, err = client2.GetWorkflowExecutionHistory(host.NewContext(), getHistoryReq)
		if err == nil {
			history := historyResponse.History
			lastEvent := history.Events[len(history.Events)-1]
			if lastEvent.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED {
				terminateEventAttributes := lastEvent.GetWorkflowExecutionTerminatedEventAttributes()
				s.Equal(terminateReason, terminateEventAttributes.Reason)
				s.Equal(terminateDetails, terminateEventAttributes.Details)
				s.Equal(identity, terminateEventAttributes.Identity)
				eventsReplicated = true
				break GetHistoryLoop2
			}
		}
		time.Sleep(waitTimeInMs * time.Millisecond)
	}
	s.NoError(err)
	s.True(eventsReplicated)

	// test upsert result in standby
	testListResult(engine2)
}

func getUpsertSearchAttributes() *commonpb.SearchAttributes {
	attrValBytes1, _ := payload.Encode("another string")
	attrValBytes2, _ := payload.Encode(123)
	upsertSearchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			definition.CustomStringField: attrValBytes1,
			definition.CustomIntField:    attrValBytes2,
		},
	}
	return upsertSearchAttr
}
