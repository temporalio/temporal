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

//go:build !race && esintegration
// +build !race,esintegration

// to run locally, make sure Elasticsearch is running,
// then run cmd `go test -v ./host/xdc -run TestESCrossDCTestSuite -tags esintegration`
package xdc

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"go.temporal.io/server/api/adminservice/v1"

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
	"gopkg.in/yaml.v3"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
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
	esClient       esclient.IntegrationTestsClient

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
	s.logger = log.NewTestLogger()

	fileName := "../testdata/xdc_integration_es_clusters.yaml"
	if host.TestFlags.TestClusterConfigFile != "" {
		fileName = host.TestFlags.TestClusterConfigFile
	}
	environment.SetupEnv()

	confContent, err := os.ReadFile(fileName)
	s.Require().NoError(err)
	confContent = []byte(os.ExpandEnv(string(confContent)))

	var clusterConfigs []*host.TestClusterConfig
	s.Require().NoError(yaml.Unmarshal(confContent, &clusterConfigs))
	s.clusterConfigs = clusterConfigs

	c, err := host.NewCluster(clusterConfigs[0], log.With(s.logger, tag.ClusterName(clusterNameES[0])))
	s.Require().NoError(err)
	s.cluster1 = c

	c, err = host.NewCluster(clusterConfigs[1], log.With(s.logger, tag.ClusterName(clusterNameES[1])))
	s.Require().NoError(err)
	s.cluster2 = c

	cluster1Address := clusterConfigs[0].ClusterMetadata.ClusterInformation[clusterConfigs[0].ClusterMetadata.CurrentClusterName].RPCAddress
	cluster2Address := clusterConfigs[1].ClusterMetadata.ClusterInformation[clusterConfigs[1].ClusterMetadata.CurrentClusterName].RPCAddress
	_, err = s.cluster1.GetAdminClient().AddOrUpdateRemoteCluster(host.NewContext(), &adminservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress:               cluster2Address,
		EnableRemoteClusterConnection: true,
	})
	s.Require().NoError(err)

	_, err = s.cluster2.GetAdminClient().AddOrUpdateRemoteCluster(host.NewContext(), &adminservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress:               cluster1Address,
		EnableRemoteClusterConnection: true,
	})
	s.Require().NoError(err)
	// Wait for cluster metadata to refresh new added clusters
	time.Sleep(time.Millisecond * 200)

	s.esClient = host.CreateESClient(s.Suite, s.clusterConfigs[0].ESConfig, s.logger)
	host.PutIndexTemplate(s.Suite, s.esClient, fmt.Sprintf("../testdata/es_%s_index_template.json", s.clusterConfigs[0].ESConfig.Version), "test-visibility-template")
	host.CreateIndex(s.Suite, s.esClient, s.clusterConfigs[0].ESConfig.GetVisibilityIndex())
	host.CreateIndex(s.Suite, s.esClient, s.clusterConfigs[1].ESConfig.GetVisibilityIndex())

	s.testSearchAttributeKey = "CustomTextField"
	s.testSearchAttributeVal = "test value"
}

func (s *esCrossDCTestSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

func (s *esCrossDCTestSuite) TearDownSuite() {
	s.cluster1.TearDownCluster()
	s.cluster2.TearDownCluster()
	host.DeleteIndex(s.Suite, s.esClient, s.clusterConfigs[0].ESConfig.GetVisibilityIndex())
	host.DeleteIndex(s.Suite, s.esClient, s.clusterConfigs[1].ESConfig.GetVisibilityIndex())
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

	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)
	descReq := &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	}
	resp, err := client1.DescribeNamespace(host.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)

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
	attrValPayload, _ := payload.Encode(s.testSearchAttributeVal)
	searchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			s.testSearchAttributeKey: attrValPayload,
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
		searchValPayload := openExecution.GetSearchAttributes().GetIndexedFields()[s.testSearchAttributeKey]
		var searchVal string
		err = payload.Decode(searchValPayload, &searchVal)
		s.NoError(err)
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
		Query:     fmt.Sprintf(`WorkflowType = '%s' and ExecutionStatus = 'Running'`, wt),
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

					searchValBytes2 := fields["CustomIntField"]
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
	attrValPayload1, _ := payload.Encode("another string")
	attrValPayload2, _ := payload.Encode(123)
	upsertSearchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"CustomTextField": attrValPayload1,
			"CustomIntField":  attrValPayload2,
		},
	}
	return upsertSearchAttr
}
