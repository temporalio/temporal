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

package xdc

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	filterpb "go.temporal.io/api/filter/v1"
	historypb "go.temporal.io/api/history/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/historyrequire"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/environment"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"
)

type AdvVisCrossDCTestSuite struct {
	// TODO (alex): use FunctionalTestSuite
	// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
	// not merely log an error
	*require.Assertions
	protorequire.ProtoAssertions
	historyrequire.HistoryRequire
	suite.Suite

	testClusterFactory testcore.TestClusterFactory

	cluster1               *testcore.TestCluster
	cluster2               *testcore.TestCluster
	logger                 log.Logger
	clusterConfigs         []*testcore.TestClusterConfig
	isElasticsearchEnabled bool

	dynamicConfigOverrides  map[dynamicconfig.Key]interface{}
	enableTransitionHistory bool

	testSearchAttributeKey string
	testSearchAttributeVal string

	startTime          time.Time
	onceClusterConnect sync.Once
}

func TestAdvVisCrossDCTestSuite(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                    string
		enableTransitionHistory bool
	}{
		{
			name:                    "EnableTransitionHistory",
			enableTransitionHistory: true,
		},
		{
			name:                    "DisableTransitionHistory",
			enableTransitionHistory: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &AdvVisCrossDCTestSuite{
				enableTransitionHistory: tc.enableTransitionHistory,
			}
			suite.Run(t, s)
		})
	}
}

var (
	clusterNameAdvVis              = []string{"active-adv-vis", "standby-adv-vis"}
	clusterReplicationConfigAdvVis = []*replicationpb.ClusterReplicationConfig{
		{
			ClusterName: clusterNameAdvVis[0],
		},
		{
			ClusterName: clusterNameAdvVis[1],
		},
	}
)

func (s *AdvVisCrossDCTestSuite) SetupSuite() {
	s.logger = log.NewTestLogger()
	s.testClusterFactory = testcore.NewTestClusterFactory()

	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.EnableTransitionHistory.Key(): s.enableTransitionHistory,
	}

	var fileName string
	if testcore.UsingSQLAdvancedVisibility() {
		// NOTE: can't use xdc_clusters.yaml here because it somehow interferes with the other xDC tests.
		fileName = "../testdata/xdc_adv_vis_clusters.yaml"
		s.isElasticsearchEnabled = false
		s.logger.Info(fmt.Sprintf("Running xDC advanced visibility test with %s/%s persistence", testcore.TestFlags.PersistenceType, testcore.TestFlags.PersistenceDriver))
	} else {
		fileName = "../testdata/xdc_adv_vis_es_clusters.yaml"
		s.isElasticsearchEnabled = true
		s.logger.Info("Running xDC advanced visibility test with Elasticsearch persistence")
	}

	if testcore.TestFlags.TestClusterConfigFile != "" {
		fileName = testcore.TestFlags.TestClusterConfigFile
	}
	environment.SetupEnv()

	confContent, err := os.ReadFile(fileName)
	s.Require().NoError(err)
	confContent = []byte(os.ExpandEnv(string(confContent)))

	var clusterConfigs []*testcore.TestClusterConfig
	s.Require().NoError(yaml.Unmarshal(confContent, &clusterConfigs))
	s.clusterConfigs = clusterConfigs

	for _, config := range clusterConfigs {
		config.DynamicConfigOverrides = s.dynamicConfigOverrides
	}

	c, err := s.testClusterFactory.NewCluster(s.T(), clusterConfigs[0], log.With(s.logger, tag.ClusterName(clusterNameAdvVis[0])))
	s.Require().NoError(err)
	s.cluster1 = c

	c, err = s.testClusterFactory.NewCluster(s.T(), clusterConfigs[1], log.With(s.logger, tag.ClusterName(clusterNameAdvVis[1])))
	s.Require().NoError(err)
	s.cluster2 = c

	s.startTime = time.Now()

	cluster1Address := clusterConfigs[0].ClusterMetadata.ClusterInformation[clusterConfigs[0].ClusterMetadata.CurrentClusterName].RPCAddress
	cluster2Address := clusterConfigs[1].ClusterMetadata.ClusterInformation[clusterConfigs[1].ClusterMetadata.CurrentClusterName].RPCAddress
	_, err = s.cluster1.AdminClient().AddOrUpdateRemoteCluster(testcore.NewContext(), &adminservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress:               cluster2Address,
		EnableRemoteClusterConnection: true,
	})
	s.Require().NoError(err)

	_, err = s.cluster2.AdminClient().AddOrUpdateRemoteCluster(testcore.NewContext(), &adminservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress:               cluster1Address,
		EnableRemoteClusterConnection: true,
	})
	s.Require().NoError(err)
	// Wait for cluster metadata to refresh new added clusters
	time.Sleep(time.Millisecond * 200) // nolint:forbidigo

	s.testSearchAttributeKey = "CustomTextField"
	s.testSearchAttributeVal = "test value"
}

func (s *AdvVisCrossDCTestSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())
	s.HistoryRequire = historyrequire.New(s.T())

	s.onceClusterConnect.Do(func() {
		waitForClusterConnected(s.Assertions, s.logger, s.cluster1, clusterNameAdvVis[0], clusterNameAdvVis[1], s.startTime)
		waitForClusterConnected(s.Assertions, s.logger, s.cluster2, clusterNameAdvVis[1], clusterNameAdvVis[0], s.startTime)
	})
}

func (s *AdvVisCrossDCTestSuite) TearDownSuite() {
	s.NoError(s.cluster1.TearDownCluster())
	s.NoError(s.cluster2.TearDownCluster())
}

func (s *AdvVisCrossDCTestSuite) TestSearchAttributes() {
	ns := "test-xdc-search-attr-" + common.GenerateRandomString(5)
	client1 := s.cluster1.FrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        ns,
		Clusters:                         clusterReplicationConfigAdvVis,
		ActiveClusterName:                clusterNameAdvVis[0],
		IsGlobalNamespace:                true,
		WorkflowExecutionRetentionPeriod: durationpb.New(1 * time.Hour * 24),
	}
	_, err := client1.RegisterNamespace(testcore.NewContext(), regReq)
	s.NoError(err)

	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval) // nolint:forbidigo
	if !s.isElasticsearchEnabled {
		// When Elasticsearch is enabled, the search attribute aliases are not used.
		_, err = client1.UpdateNamespace(testcore.NewContext(), &workflowservice.UpdateNamespaceRequest{
			Namespace: ns,
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
		s.NoError(err)
		// Wait for namespace cache to pick the UpdateNamespace changes.
		time.Sleep(cacheRefreshInterval) // nolint:forbidigo
	}

	s.EventuallyWithT(func(t *assert.CollectT) {
		// Wait for namespace record to be replicated and loaded into memory.
		for _, r := range s.cluster2.Host().FrontendNamespaceRegistries() {
			_, err := r.GetNamespace(namespace.Name(ns))
			assert.NoError(t, err)
		}
	}, 15*time.Second, 500*time.Millisecond)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Namespace: ns,
	}
	resp, err := client1.DescribeNamespace(testcore.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)

	client2 := s.cluster2.FrontendClient() // standby
	resp2, err := client2.DescribeNamespace(testcore.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp2)
	s.Equal(resp, resp2)

	// start a workflow
	id := "xdc-search-attr-test-" + uuid.New()
	wt := "xdc-search-attr-test-type"
	tl := "xdc-search-attr-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	searchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			s.testSearchAttributeKey: payload.EncodeString(s.testSearchAttributeVal),
		},
	}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           ns,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
		SearchAttributes:    searchAttr,
	}
	startTime := time.Now().UTC()
	we, err := client1.StartWorkflowExecution(testcore.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	s.logger.Info("StartWorkflowExecution \n", tag.WorkflowRunID(we.GetRunId()))

	startFilter := &filterpb.StartTimeFilter{}
	startFilter.EarliestTime = timestamppb.New(startTime)
	saListRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: ns,
		PageSize:  5,
		Query:     fmt.Sprintf(`WorkflowId = "%s" and %s = "%s"`, id, s.testSearchAttributeKey, s.testSearchAttributeVal),
	}

	testListResult := func(client workflowservice.WorkflowServiceClient, lr *workflowservice.ListWorkflowExecutionsRequest) {
		var openExecution *workflowpb.WorkflowExecutionInfo
		for i := 0; i < numOfRetry; i++ {
			startFilter.LatestTime = timestamppb.New(time.Now().UTC())

			resp, err := client.ListWorkflowExecutions(testcore.NewContext(), lr)
			s.NoError(err)
			if len(resp.GetExecutions()) == 1 {
				openExecution = resp.GetExecutions()[0]
				break
			}
			time.Sleep(waitTimeInMs * time.Millisecond) // nolint:forbidigo
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
	engine1 := s.cluster1.FrontendClient()
	testListResult(engine1, saListRequest)

	// List workflow in standby
	engine2 := s.cluster2.FrontendClient()
	testListResult(engine2, saListRequest)

	// upsert search attributes
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		upsertCommand := &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES,
			Attributes: &commandpb.Command_UpsertWorkflowSearchAttributesCommandAttributes{UpsertWorkflowSearchAttributesCommandAttributes: &commandpb.UpsertWorkflowSearchAttributesCommandAttributes{
				SearchAttributes: getUpsertSearchAttributes(),
			}}}

		return []*commandpb.Command{upsertCommand}, nil
	}

	poller := testcore.TaskPoller{
		Client:              client1,
		Namespace:           ns,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	_, err = poller.PollAndProcessWorkflowTask()
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	time.Sleep(waitForESToSettle) // nolint:forbidigo

	testListResult = func(client workflowservice.WorkflowServiceClient, lr *workflowservice.ListWorkflowExecutionsRequest) {
		s.Eventually(func() bool {
			resp, err := client.ListWorkflowExecutions(testcore.NewContext(), lr)
			s.NoError(err)
			if len(resp.GetExecutions()) != 1 {
				return false
			}
			fields := resp.GetExecutions()[0].SearchAttributes.GetIndexedFields()
			if len(fields) != 3 {
				return false
			}

			searchValBytes := fields[s.testSearchAttributeKey]
			var searchVal string
			payload.Decode(searchValBytes, &searchVal)
			s.Equal("another string", searchVal)

			searchValBytes2 := fields["CustomIntField"]
			var searchVal2 int
			payload.Decode(searchValBytes2, &searchVal2)
			s.Equal(123, searchVal2)

			buildIdsBytes := fields[searchattribute.BuildIds]
			var buildIds []string
			err = payload.Decode(buildIdsBytes, &buildIds)
			s.NoError(err)
			s.Equal([]string{worker_versioning.UnversionedSearchAttribute}, buildIds)

			return true
		}, waitTimeInMs*time.Millisecond*numOfRetry, waitTimeInMs*time.Millisecond)
	}

	saListRequest = &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: ns,
		PageSize:  int32(2),
		Query:     fmt.Sprintf(`WorkflowId = "%s" and %s = "another string"`, id, s.testSearchAttributeKey),
	}

	// test upsert result in active
	testListResult(engine1, saListRequest)
	// test upsert result in standby
	testListResult(engine2, saListRequest)

	runningListRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: ns,
		PageSize:  int32(2),
		Query:     fmt.Sprintf(`WorkflowType = '%s' and ExecutionStatus = 'Running'`, wt),
	}
	// test upsert result in active
	testListResult(engine1, runningListRequest)
	// test upsert result in standby
	testListResult(engine2, runningListRequest)

	// terminate workflow
	terminateReason := "force terminate to make sure standby process tasks"
	terminateDetails := payloads.EncodeString("terminate details")
	_, err = client1.TerminateWorkflowExecution(testcore.NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: ns,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
		Reason:   terminateReason,
		Details:  terminateDetails,
		Identity: identity,
	})
	s.NoError(err)

	// check terminate done
	getHistoryReq := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
	}

	s.WaitForHistory(`
  1 v1 WorkflowExecutionStarted
  2 v1 WorkflowTaskScheduled
  3 v1 WorkflowTaskStarted
  4 v1 WorkflowTaskCompleted
  5 v1 UpsertWorkflowSearchAttributes
  6 v1 WorkflowExecutionTerminated {"Details":{"Payloads":[{"Data":"\"terminate details\""}]},"Identity":"worker1","Reason":"force terminate to make sure standby process tasks"}`,
		func() *historypb.History {
			historyResponse, err := client1.GetWorkflowExecutionHistory(testcore.NewContext(), getHistoryReq)
			s.NoError(err)
			return historyResponse.History
		}, 1*time.Second, 100*time.Millisecond)

	// check history replicated to the other cluster
	s.WaitForHistory(`
  1 v1 WorkflowExecutionStarted
  2 v1 WorkflowTaskScheduled
  3 v1 WorkflowTaskStarted
  4 v1 WorkflowTaskCompleted
  5 v1 UpsertWorkflowSearchAttributes
  6 v1 WorkflowExecutionTerminated {"Details":{"Payloads":[{"Data":"\"terminate details\""}]},"Identity":"worker1","Reason":"force terminate to make sure standby process tasks"}`,
		func() *historypb.History {
			historyResponse, err := client2.GetWorkflowExecutionHistory(testcore.NewContext(), getHistoryReq)
			if err != nil {
				return nil
			}
			return historyResponse.History
		}, waitTimeInMs*numOfRetry*time.Millisecond, waitTimeInMs*time.Millisecond)

	terminatedListRequest := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: ns,
		PageSize:  int32(2),
		Query:     fmt.Sprintf(`WorkflowType = '%s' and ExecutionStatus = 'Terminated'`, wt),
	}
	// test upsert result in active
	testListResult(engine1, terminatedListRequest)
	// test upsert result in standby
	testListResult(engine2, terminatedListRequest)
}

func getUpsertSearchAttributes() *commonpb.SearchAttributes {
	attrValPayload2, _ := payload.Encode(123)
	upsertSearchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"CustomTextField": payload.EncodeString("another string"),
			"CustomIntField":  attrValPayload2,
		},
	}
	return upsertSearchAttr
}
