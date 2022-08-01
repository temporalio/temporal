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

//go:build !race
// +build !race

// need to run xdc tests with race detector off because of ringpop bug causing data race issue

package xdc

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	querypb "go.temporal.io/api/query/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"gopkg.in/yaml.v3"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/environment"
	"go.temporal.io/server/host"
	sw "go.temporal.io/server/service/worker"
	"go.temporal.io/server/service/worker/migration"
)

type (
	integrationClustersTestSuite struct {
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		suite.Suite
		cluster1 *host.TestCluster
		cluster2 *host.TestCluster
		logger   log.Logger
	}
)

const (
	cacheRefreshInterval = host.NamespaceCacheRefreshInterval + 5*time.Second
)

var (
	clusterName              = []string{"active", "standby"}
	clusterReplicationConfig = []*replicationpb.ClusterReplicationConfig{
		{
			ClusterName: clusterName[0],
		},
		{
			ClusterName: clusterName[1],
		},
	}
)

func TestIntegrationClustersTestSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(integrationClustersTestSuite))
}

func (s *integrationClustersTestSuite) SetupSuite() {
	s.logger = log.NewTestLogger()

	fileName := "../testdata/xdc_integration_test_clusters.yaml"
	if host.TestFlags.TestClusterConfigFile != "" {
		fileName = host.TestFlags.TestClusterConfigFile
	}
	environment.SetupEnv()

	confContent, err := os.ReadFile(fileName)
	s.Require().NoError(err)
	confContent = []byte(os.ExpandEnv(string(confContent)))

	var clusterConfigs []*host.TestClusterConfig
	s.Require().NoError(yaml.Unmarshal(confContent, &clusterConfigs))

	c, err := host.NewCluster(clusterConfigs[0], log.With(s.logger, tag.ClusterName(clusterName[0])))
	s.Require().NoError(err)
	s.cluster1 = c

	c, err = host.NewCluster(clusterConfigs[1], log.With(s.logger, tag.ClusterName(clusterName[1])))
	s.Require().NoError(err)
	s.cluster2 = c

	cluster1Address := clusterConfigs[0].ClusterMetadata.ClusterInformation[clusterConfigs[0].ClusterMetadata.CurrentClusterName].RPCAddress
	cluster2Address := clusterConfigs[1].ClusterMetadata.ClusterInformation[clusterConfigs[1].ClusterMetadata.CurrentClusterName].RPCAddress
	_, err = s.cluster1.GetAdminClient().AddOrUpdateRemoteCluster(
		host.NewContext(),
		&adminservice.AddOrUpdateRemoteClusterRequest{
			FrontendAddress:               cluster2Address,
			EnableRemoteClusterConnection: true,
		})
	s.Require().NoError(err)

	_, err = s.cluster2.GetAdminClient().AddOrUpdateRemoteCluster(
		host.NewContext(),
		&adminservice.AddOrUpdateRemoteClusterRequest{
			FrontendAddress:               cluster1Address,
			EnableRemoteClusterConnection: true,
		})
	s.Require().NoError(err)
	// Wait for cluster metadata to refresh new added clusters
	time.Sleep(time.Millisecond * 200)
}

func (s *integrationClustersTestSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

func (s *integrationClustersTestSuite) TearDownSuite() {
	s.cluster1.TearDownCluster()
	s.cluster2.TearDownCluster()
}

func (s *integrationClustersTestSuite) decodePayloadsString(ps *commonpb.Payloads) (r string) {
	s.NoError(payloads.Decode(ps, &r))
	return
}

func (s *integrationClustersTestSuite) TestNamespaceFailover() {
	namespace := "test-namespace-for-fail-over-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         clusterReplicationConfig,
		ActiveClusterName:                clusterName[0],
		WorkflowExecutionRetentionPeriod: timestamp.DurationPtr(7 * time.Hour * 24),
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

	s.failover(namespace, clusterName[1], int64(2), client1)

	updated := false
	var resp3 *workflowservice.DescribeNamespaceResponse
	for i := 0; i < 30; i++ {
		resp3, err = client2.DescribeNamespace(host.NewContext(), descReq)
		s.NoError(err)
		if resp3.ReplicationConfig.GetActiveClusterName() == clusterName[1] {
			updated = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	s.True(updated)
	s.NotNil(resp3)
	s.Equal(int64(2), resp3.GetFailoverVersion())

	// start workflow in new cluster
	id := "integration-namespace-failover-test"
	wt := "integration-namespace-failover-test-type"
	tq := "integration-namespace-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tq}
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
	}
	var we *workflowservice.StartWorkflowExecutionResponse
	for i := 0; i < 30; i++ {
		we, err = client2.StartWorkflowExecution(host.NewContext(), startReq)
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	s.NoError(err)
	s.NotNil(we.GetRunId())
}

func (s *integrationClustersTestSuite) TestSimpleWorkflowFailover() {
	namespaceName := "test-simple-workflow-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespaceName,
		IsGlobalNamespace:                true,
		Clusters:                         clusterReplicationConfig,
		ActiveClusterName:                clusterName[0],
		WorkflowExecutionRetentionPeriod: timestamp.DurationPtr(1 * time.Hour * 24),
	}
	_, err := client1.RegisterNamespace(host.NewContext(), regReq)
	s.NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Namespace: namespaceName,
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
	id := "integration-simple-workflow-failover-test"
	wt := "integration-simple-workflow-failover-test-type"
	tq := "integration-simple-workflow-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tq}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           namespaceName,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	}
	we, err := client1.StartWorkflowExecution(host.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())
	rid := we.GetRunId()

	s.logger.Info("StartWorkflowExecution \n", tag.WorkflowRunID(we.GetRunId()))

	workflowComplete := false
	activityName := "activity_type1"
	activityCount := int32(1)
	activityCounter := int32(0)
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tq},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(30 * time.Second),
					StartToCloseTimeout:    timestamp.DurationPtr(50 * time.Second),
					HeartbeatTimeout:       timestamp.DurationPtr(20 * time.Second),
				}},
			}}, nil
		}

		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	atHandler := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {

		return payloads.EncodeString("Activity Result"), false, nil
	}

	queryType := "test-query"
	queryHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*commonpb.Payloads, error) {
		s.NotNil(task.Query)
		s.NotNil(task.Query.QueryType)
		if task.Query.QueryType == queryType {
			return payloads.EncodeString("query-result"), nil
		}

		return nil, errors.New("unknown-query-type")
	}

	poller := host.TaskPoller{
		Engine:              client1,
		Namespace:           namespaceName,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		QueryHandler:        queryHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	poller2 := host.TaskPoller{
		Engine:              client2,
		Namespace:           namespaceName,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		QueryHandler:        queryHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// make some progress in cluster 1
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	type QueryResult struct {
		Resp *workflowservice.QueryWorkflowResponse
		Err  error
	}
	queryResultCh := make(chan QueryResult)
	queryWorkflowFn := func(client workflowservice.WorkflowServiceClient, queryType string) {
		queryResp, err := client.QueryWorkflow(host.NewContext(), &workflowservice.QueryWorkflowRequest{
			Namespace: namespaceName,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
			Query: &querypb.WorkflowQuery{
				QueryType: queryType,
			},
		})
		queryResultCh <- QueryResult{Resp: queryResp, Err: err}
	}

	// call QueryWorkflow in separate goroutinue (because it is blocking). That will generate a query task
	go queryWorkflowFn(client1, queryType)
	// process that query task, which should respond via RespondQueryTaskCompleted
	for {
		// loop until process the query task
		isQueryTask, errInner := poller.PollAndProcessWorkflowTask(false, false)
		s.logger.Info("PollAndProcessQueryTask", tag.Error(err))
		s.NoError(errInner)
		if isQueryTask {
			break
		}
	}
	// wait until query result is ready
	queryResult := <-queryResultCh
	s.NoError(queryResult.Err)
	s.NotNil(queryResult.Resp)
	s.NotNil(queryResult.Resp.QueryResult)
	s.Equal("query-result", s.decodePayloadsString(queryResult.Resp.GetQueryResult()))

	// Wait a while so the events are replicated.
	time.Sleep(5 * time.Second)

	// call QueryWorkflow in separate goroutinue (because it is blocking). That will generate a query task
	go queryWorkflowFn(client2, queryType)
	// process that query task, which should respond via RespondQueryTaskCompleted
	for {
		// loop until process the query task
		isQueryTask, errInner := poller2.PollAndProcessWorkflowTask(false, false)
		s.logger.Info("PollAndProcessQueryTask", tag.Error(err))
		s.NoError(errInner)
		if isQueryTask {
			break
		}
	}
	// wait until query result is ready
	queryResult = <-queryResultCh
	s.NoError(queryResult.Err)
	s.NotNil(queryResult.Resp)
	s.NotNil(queryResult.Resp.QueryResult)
	s.Equal("query-result", s.decodePayloadsString(queryResult.Resp.GetQueryResult()))

	s.failover(namespaceName, clusterName[1], int64(2), client1)

	// check history matched
	getHistoryReq := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: namespaceName,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      rid,
		},
	}
	var historyResponse *workflowservice.GetWorkflowExecutionHistoryResponse
	eventsReplicated := false
	for i := 0; i < 15; i++ {
		historyResponse, err = client2.GetWorkflowExecutionHistory(host.NewContext(), getHistoryReq)
		if err == nil && len(historyResponse.History.Events) == 5 {
			eventsReplicated = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	s.NoError(err)
	s.True(eventsReplicated)

	// Make sure query is still working after failover
	// call QueryWorkflow in separate goroutinue (because it is blocking). That will generate a query task
	go queryWorkflowFn(client1, queryType)
	// process that query task, which should respond via RespondQueryTaskCompleted
	for {
		// loop until process the query task
		isQueryTask, errInner := poller.PollAndProcessWorkflowTask(false, false)
		s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(errInner)
		if isQueryTask {
			break
		}
	}
	// wait until query result is ready
	queryResult = <-queryResultCh
	s.NoError(queryResult.Err)
	s.NotNil(queryResult.Resp)
	s.NotNil(queryResult.Resp.QueryResult)
	s.Equal("query-result", s.decodePayloadsString(queryResult.Resp.GetQueryResult()))

	// call QueryWorkflow in separate goroutinue (because it is blocking). That will generate a query task
	go queryWorkflowFn(client2, queryType)
	// process that query task, which should respond via RespondQueryTaskCompleted
	for {
		// loop until process the query task
		isQueryTask, errInner := poller2.PollAndProcessWorkflowTask(false, false)
		s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(errInner)
		if isQueryTask {
			break
		}
	}
	// wait until query result is ready
	queryResult = <-queryResultCh
	s.NoError(queryResult.Err)
	s.NotNil(queryResult.Resp)
	s.NotNil(queryResult.Resp.QueryResult)
	s.Equal("query-result", s.decodePayloadsString(queryResult.Resp.GetQueryResult()))

	// make process in cluster 2
	err = poller2.PollAndProcessActivityTask(false)
	s.logger.Info("PollAndProcessActivityTask 2", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	_, err = poller2.PollAndProcessWorkflowTask(false, false)
	s.logger.Info("PollAndProcessWorkflowTask 2", tag.Error(err))
	s.NoError(err)
	s.True(workflowComplete)

	// check history replicated in cluster 1
	eventsReplicated = false
	for i := 0; i < 15; i++ {
		historyResponse, err = client1.GetWorkflowExecutionHistory(host.NewContext(), getHistoryReq)
		if err == nil && len(historyResponse.History.Events) == 11 {
			eventsReplicated = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	s.NoError(err)
	s.True(eventsReplicated)
}

func (s *integrationClustersTestSuite) TestStickyWorkflowTaskFailover() {
	namespace := "test-sticky-workflow-task-workflow-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         clusterReplicationConfig,
		ActiveClusterName:                clusterName[0],
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

	// Start a workflow
	id := "integration-sticky-workflow-task-workflow-failover-test"
	wt := "integration-sticky-workflow-task-workflow-failover-test-type"
	tq := "integration-sticky-workflow-task-workflow-failover-test-taskqueue"
	stq1 := "integration-sticky-workflow-task-workflow-failover-test-taskqueue-sticky1"
	stq2 := "integration-sticky-workflow-task-workflow-failover-test-taskqueue-sticky2"
	identity1 := "worker1"
	identity2 := "worker2"

	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tq}
	stickyTaskQueue1 := &taskqueuepb.TaskQueue{Name: stq1}
	stickyTaskQueue2 := &taskqueuepb.TaskQueue{Name: stq2}
	stickyTaskTimeout := 100 * time.Second
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(2592000 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(60 * time.Second),
		Identity:            identity1,
	}
	we, err := client1.StartWorkflowExecution(host.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	s.logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

	firstCommandMade := false
	secondCommandMade := false
	workflowCompleted := false
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		if !firstCommandMade {
			firstCommandMade = true
			return []*commandpb.Command{}, nil
		}

		if !secondCommandMade {
			secondCommandMade = true
			return []*commandpb.Command{}, nil
		}

		workflowCompleted = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller1 := &host.TaskPoller{
		Engine:                       client1,
		Namespace:                    namespace,
		TaskQueue:                    taskQueue,
		StickyTaskQueue:              stickyTaskQueue1,
		StickyScheduleToStartTimeout: stickyTaskTimeout,
		Identity:                     identity1,
		WorkflowTaskHandler:          wtHandler,
		Logger:                       s.logger,
		T:                            s.T(),
	}

	poller2 := &host.TaskPoller{
		Engine:                       client2,
		Namespace:                    namespace,
		TaskQueue:                    taskQueue,
		StickyTaskQueue:              stickyTaskQueue2,
		StickyScheduleToStartTimeout: stickyTaskTimeout,
		Identity:                     identity2,
		WorkflowTaskHandler:          wtHandler,
		Logger:                       s.logger,
		T:                            s.T(),
	}

	_, err = poller1.PollAndProcessWorkflowTaskWithAttemptAndRetry(false, false, false, true, 1, 5)
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(firstCommandMade)

	// Send a signal in cluster
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	_, err = client1.SignalWorkflowExecution(host.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.GetRunId(),
		},
		SignalName: signalName,
		Input:      signalInput,
		Identity:   identity1,
	})
	s.NoError(err)

	s.failover(namespace, clusterName[1], int64(2), client1)

	_, err = poller2.PollAndProcessWorkflowTaskWithAttemptAndRetry(false, false, false, true, 1, 5)
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(secondCommandMade)

	_, err = client2.SignalWorkflowExecution(host.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.GetRunId(),
		},
		SignalName: signalName,
		Input:      signalInput,
		Identity:   identity2,
	})
	s.NoError(err)

	s.failover(namespace, clusterName[0], int64(11), client2)

	_, err = poller1.PollAndProcessWorkflowTask(true, false)
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(workflowCompleted)
}

func (s *integrationClustersTestSuite) TestStartWorkflowExecution_Failover_WorkflowIDReusePolicy() {
	namespaceName := "test-start-workflow-failover-ID-reuse-policy" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespaceName,
		IsGlobalNamespace:                true,
		Clusters:                         clusterReplicationConfig,
		ActiveClusterName:                clusterName[0],
		WorkflowExecutionRetentionPeriod: timestamp.DurationPtr(1 * time.Hour * 24),
	}
	_, err := client1.RegisterNamespace(host.NewContext(), regReq)
	s.NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Namespace: namespaceName,
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
	id := "integration-start-workflow-failover-ID-reuse-policy-test"
	wt := "integration-start-workflow-failover-ID-reuse-policy-test-type"
	tl := "integration-start-workflow-failover-ID-reuse-policy-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:             uuid.New(),
		Namespace:             namespaceName,
		WorkflowId:            id,
		WorkflowType:          workflowType,
		TaskQueue:             taskQueue,
		Input:                 nil,
		WorkflowRunTimeout:    timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout:   timestamp.DurationPtr(1 * time.Second),
		Identity:              identity,
		WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}
	we, err := client1.StartWorkflowExecution(host.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())
	s.logger.Info("StartWorkflowExecution in cluster 1: ", tag.WorkflowRunID(we.GetRunId()))

	workflowCompleteTimes := 0
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		workflowCompleteTimes++
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller := host.TaskPoller{
		Engine:              client1,
		Namespace:           namespaceName,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.logger,
		T:                   s.T(),
	}

	poller2 := host.TaskPoller{
		Engine:              client2,
		Namespace:           namespaceName,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// Complete the workflow in cluster 1
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.Equal(1, workflowCompleteTimes)

	s.failover(namespaceName, clusterName[1], int64(2), client1)

	// start the same workflow in cluster 2 is not allowed if policy is AllowDuplicateFailedOnly
	startReq.RequestId = uuid.New()
	startReq.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY
	we, err = client2.StartWorkflowExecution(host.NewContext(), startReq)
	s.IsType(&serviceerror.WorkflowExecutionAlreadyStarted{}, err)
	s.Nil(we)

	// start the same workflow in cluster 2 is not allowed if policy is RejectDuplicate
	startReq.RequestId = uuid.New()
	startReq.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
	we, err = client2.StartWorkflowExecution(host.NewContext(), startReq)
	s.IsType(&serviceerror.WorkflowExecutionAlreadyStarted{}, err)
	s.Nil(we)

	// start the workflow in cluster 2
	startReq.RequestId = uuid.New()
	startReq.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	we, err = client2.StartWorkflowExecution(host.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())
	s.logger.Info("StartWorkflowExecution in cluster 2: ", tag.WorkflowRunID(we.GetRunId()))

	_, err = poller2.PollAndProcessWorkflowTask(false, false)
	s.logger.Info("PollAndProcessWorkflowTask 2", tag.Error(err))
	s.NoError(err)
	s.Equal(2, workflowCompleteTimes)
}

func (s *integrationClustersTestSuite) TestTerminateFailover() {
	namespace := "test-terminate-workflow-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         clusterReplicationConfig,
		ActiveClusterName:                clusterName[0],
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

	// start a workflow
	id := "integration-terminate-workflow-failover-test"
	wt := "integration-terminate-workflow-failover-test-type"
	tl := "integration-terminate-workflow-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl}
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
	}
	we, err := client1.StartWorkflowExecution(host.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	activityName := "activity_type1"
	activityCount := int32(1)
	activityCounter := int32(0)
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
					ScheduleToStartTimeout: timestamp.DurationPtr(10 * time.Second),
					StartToCloseTimeout:    timestamp.DurationPtr(50 * time.Second),
					HeartbeatTimeout:       timestamp.DurationPtr(5 * time.Second),
				}},
			}}, nil
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	atHandler := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {

		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &host.TaskPoller{
		Engine:              client1,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// make some progress in cluster 1
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.failover(namespace, clusterName[1], int64(2), client1)

	// terminate workflow at cluster 2
	terminateReason := "terminate reason"
	terminateDetails := payloads.EncodeString("terminate details")
	_, err = client2.TerminateWorkflowExecution(host.NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
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
		historyResponse, err := client2.GetWorkflowExecutionHistory(host.NewContext(), getHistoryReq)
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
	for i := 0; i < 15; i++ {
		historyResponse, err = client1.GetWorkflowExecutionHistory(host.NewContext(), getHistoryReq)
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
		time.Sleep(1 * time.Second)
	}
	s.NoError(err)
	s.True(eventsReplicated)
}

func (s *integrationClustersTestSuite) TestResetWorkflowFailover() {
	namespace := "test-reset-workflow-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         clusterReplicationConfig,
		ActiveClusterName:                clusterName[0],
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

	// start a workflow
	id := "integration-reset-workflow-failover-test"
	wt := "integration-reset-workflow-failover-test-type"
	tl := "integration-reset-workflow-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl}
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
	}
	we, err := client1.StartWorkflowExecution(host.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	_, err = client1.SignalWorkflowExecution(host.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		SignalName: "random signal name",
		Input: &commonpb.Payloads{Payloads: []*commonpb.Payload{
			{Data: []byte("random signal payload")},
		}},
		Identity: identity,
	})
	s.NoError(err)

	// workflow logic
	workflowComplete := false
	isWorkflowTaskProcessed := false
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if !isWorkflowTaskProcessed {
			isWorkflowTaskProcessed = true
			return []*commandpb.Command{}, nil
		}

		// Complete workflow after reset
		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				}},
		}}, nil

	}

	poller := host.TaskPoller{
		Engine:              client1,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.logger,
		T:                   s.T(),
	}

	poller2 := host.TaskPoller{
		Engine:              client2,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.logger,
		T:                   s.T(),
	}

	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// events layout
	//  1. WorkflowExecutionStarted
	//  2. WorkflowTaskScheduled
	//  3. WorkflowExecutionSignaled
	//  4. WorkflowTaskStarted
	//  5. WorkflowTaskCompleted

	// Reset workflow execution
	resetResp, err := client1.ResetWorkflowExecution(host.NewContext(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		Reason:                    "reset execution from test",
		WorkflowTaskFinishEventId: 4, // before WorkflowTaskStarted
		RequestId:                 uuid.New(),
	})
	s.NoError(err)

	s.failover(namespace, clusterName[1], int64(2), client1)

	_, err = poller2.PollAndProcessWorkflowTask(false, false)
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(workflowComplete)

	time.Sleep(cacheRefreshInterval)

	getHistoryReq := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      resetResp.RunId,
		},
	}

	getHistoryResp, err := client1.GetWorkflowExecutionHistory(host.NewContext(), getHistoryReq)
	s.NoError(err)
	events := getHistoryResp.History.Events
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, events[len(events)-1].GetEventType())

	getHistoryResp, err = client2.GetWorkflowExecutionHistory(host.NewContext(), getHistoryReq)
	s.NoError(err)
	events = getHistoryResp.History.Events
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, events[len(events)-1].GetEventType())
}

func (s *integrationClustersTestSuite) TestContinueAsNewFailover() {
	namespace := "test-continueAsNew-workflow-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         clusterReplicationConfig,
		ActiveClusterName:                clusterName[0],
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

	// start a workflow
	id := "integration-continueAsNew-workflow-failover-test"
	wt := "integration-continueAsNew-workflow-failover-test-type"
	tl := "integration-continueAsNew-workflow-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl}
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
	}
	we, err := client1.StartWorkflowExecution(host.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	workflowComplete := false
	continueAsNewCount := int32(5)
	continueAsNewCounter := int32(0)
	var previousRunID string
	var lastRunStartedEvent *historypb.HistoryEvent
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		if continueAsNewCounter < continueAsNewCount {
			previousRunID = execution.GetRunId()
			continueAsNewCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, continueAsNewCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
					WorkflowType:        workflowType,
					TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
					Input:               payloads.EncodeBytes(buf.Bytes()),
					WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
					WorkflowTaskTimeout: timestamp.DurationPtr(10 * time.Second),
				}},
			}}, nil
		}

		lastRunStartedEvent = history.Events[0]
		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller := &host.TaskPoller{
		Engine:              client1,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	poller2 := host.TaskPoller{
		Engine:              client2,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// make some progress in cluster 1 and did some continueAsNew
	for i := 0; i < 3; i++ {
		_, err := poller.PollAndProcessWorkflowTask(false, false)
		s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err, strconv.Itoa(i))
	}

	s.failover(namespace, clusterName[1], int64(2), client1)

	// finish the rest in cluster 2
	for i := 0; i < 2; i++ {
		_, err := poller2.PollAndProcessWorkflowTask(false, false)
		s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err, strconv.Itoa(i))
	}

	s.False(workflowComplete)
	_, err = poller2.PollAndProcessWorkflowTask(false, false)
	s.NoError(err)
	s.True(workflowComplete)
	s.Equal(previousRunID, lastRunStartedEvent.GetWorkflowExecutionStartedEventAttributes().GetContinuedExecutionRunId())
}

func (s *integrationClustersTestSuite) TestSignalFailover() {
	namespace := "test-signal-workflow-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         clusterReplicationConfig,
		ActiveClusterName:                clusterName[0],
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

	// Start a workflow
	id := "integration-signal-workflow-failover-test"
	wt := "integration-signal-workflow-failover-test-type"
	tl := "integration-signal-workflow-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(300 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	}
	we, err := client1.StartWorkflowExecution(host.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	s.logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

	eventSignaled := false
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		if !eventSignaled {
			for _, event := range history.Events[previousStartedEventID:] {
				if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					eventSignaled = true
					return []*commandpb.Command{}, nil
				}
			}
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller := &host.TaskPoller{
		Engine:              client1,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	poller2 := &host.TaskPoller{
		Engine:              client2,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// Send a signal in cluster 1
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	_, err = client1.SignalWorkflowExecution(host.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.GetRunId(),
		},
		SignalName: signalName,
		Input:      signalInput,
		Identity:   identity,
	})
	s.NoError(err)

	// Process signal in cluster 1
	s.False(eventSignaled)
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(eventSignaled)

	s.failover(namespace, clusterName[1], int64(2), client1)

	// check history matched
	getHistoryReq := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
	}
	var historyResponse *workflowservice.GetWorkflowExecutionHistoryResponse
	eventsReplicated := false
	for i := 0; i < 15; i++ {
		historyResponse, err = client2.GetWorkflowExecutionHistory(host.NewContext(), getHistoryReq)
		if err == nil && len(historyResponse.History.Events) == 5 {
			eventsReplicated = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	s.NoError(err)
	s.True(eventsReplicated)

	// Send another signal in cluster 2
	signalName2 := "my signal 2"
	signalInput2 := payloads.EncodeString("my signal input 2")
	_, err = client2.SignalWorkflowExecution(host.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
		SignalName: signalName2,
		Input:      signalInput2,
		Identity:   identity,
	})
	s.NoError(err)

	// Process signal in cluster 2
	eventSignaled = false
	_, err = poller2.PollAndProcessWorkflowTask(false, false)
	s.logger.Info("PollAndProcessWorkflowTask 2", tag.Error(err))
	s.NoError(err)
	s.True(eventSignaled)

	// check history matched
	eventsReplicated = false
	for i := 0; i < 15; i++ {
		historyResponse, err = client2.GetWorkflowExecutionHistory(host.NewContext(), getHistoryReq)
		if err == nil && len(historyResponse.History.Events) == 9 {
			eventsReplicated = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	s.NoError(err)
	s.True(eventsReplicated)
}

func (s *integrationClustersTestSuite) TestUserTimerFailover() {
	namespace := "test-user-timer-workflow-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         clusterReplicationConfig,
		ActiveClusterName:                clusterName[0],
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

	// Start a workflow
	id := "integration-user-timer-workflow-failover-test"
	wt := "integration-user-timer-workflow-failover-test-type"
	tl := "integration-user-timer-workflow-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(300 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(10 * time.Second),
		Identity:            identity,
	}
	var we *workflowservice.StartWorkflowExecutionResponse
	for i := 0; i < 10; i++ {
		we, err = client1.StartWorkflowExecution(host.NewContext(), startReq)
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	s.NoError(err)
	s.NotNil(we.GetRunId())

	s.logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

	timerCreated := false
	timerFired := false
	workflowCompleted := false
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {

		if !timerCreated {
			timerCreated = true

			// Send a signal in cluster
			signalName := "my signal"
			signalInput := payloads.EncodeString("my signal input")
			_, err = client1.SignalWorkflowExecution(host.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
				Namespace: namespace,
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					RunId:      we.GetRunId(),
				},
				SignalName: signalName,
				Input:      signalInput,
				Identity:   "",
			})
			s.NoError(err)
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_TIMER,
				Attributes: &commandpb.Command_StartTimerCommandAttributes{StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
					TimerId:            "timer-id",
					StartToFireTimeout: timestamp.DurationPtr(2 * time.Second),
				}},
			}}, nil
		}

		if !timerFired {
			resp, err := client2.GetWorkflowExecutionHistory(host.NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
				Namespace: namespace,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: id,
					RunId:      we.GetRunId(),
				},
			})
			s.NoError(err)
			for _, event := range resp.History.Events {
				if event.GetEventType() == enumspb.EVENT_TYPE_TIMER_FIRED {
					timerFired = true
				}
			}
			if !timerFired {
				return []*commandpb.Command{}, nil
			}
		}

		workflowCompleted = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller1 := &host.TaskPoller{
		Engine:              client1,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	poller2 := &host.TaskPoller{
		Engine:              client2,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	for i := 0; i < 2; i++ {
		_, err = poller1.PollAndProcessWorkflowTask(false, false)
		if err != nil {
			timerCreated = false
			continue
		}
		if timerCreated {
			break
		}
	}
	s.True(timerCreated)

	s.failover(namespace, clusterName[1], int64(2), client1)

	for i := 1; i < 20; i++ {
		if !workflowCompleted {
			_, err = poller2.PollAndProcessWorkflowTask(true, false)
			s.NoError(err)
			time.Sleep(time.Second)
		}
	}
}

func (s *integrationClustersTestSuite) TestTransientWorkflowTaskFailover() {
	namespace := "test-transient-workflow-task-workflow-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         clusterReplicationConfig,
		ActiveClusterName:                clusterName[0],
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

	// Start a workflow
	id := "integration-transient-workflow-task-workflow-failover-test"
	wt := "integration-transient-workflow-task-workflow-failover-test-type"
	tl := "integration-transient-workflow-task-workflow-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(300 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(8 * time.Second),
		Identity:            identity,
	}
	var we *workflowservice.StartWorkflowExecutionResponse
	for i := 0; i < 10; i++ {
		we, err = client1.StartWorkflowExecution(host.NewContext(), startReq)
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	s.NoError(err)
	s.NotNil(we.GetRunId())

	s.logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

	workflowTaskFailed := false
	workflowFinished := false
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		if !workflowTaskFailed {
			workflowTaskFailed = true
			return nil, errors.New("random fail workflow task reason")
		}

		workflowFinished = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller1 := &host.TaskPoller{
		Engine:              client1,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	poller2 := &host.TaskPoller{
		Engine:              client2,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// this will fail the workflow task
	_, err = poller1.PollAndProcessWorkflowTask(false, false)
	s.NoError(err)

	s.failover(namespace, clusterName[1], int64(2), client1)

	// for failover transient workflow task, it is guaranteed that the transient workflow task
	// after the failover has attempt 1
	// for details see ReplicateTransientWorkflowTaskScheduled
	_, err = poller2.PollAndProcessWorkflowTaskWithAttempt(false, false, false, false, 1)
	s.NoError(err)
	s.True(workflowFinished)
}

func (s *integrationClustersTestSuite) TestCronWorkflowFailover() {
	namespace := "test-cron-workflow-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         clusterReplicationConfig,
		ActiveClusterName:                clusterName[0],
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

	// start a workflow
	id := "integration-cron-workflow-failover-test"
	wt := "integration-cron-workflow-failover-test-type"
	tl := "integration-cron-workflow-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl}
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
		CronSchedule:        "@every 5s",
	}
	we, err := client1.StartWorkflowExecution(host.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		return []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("cron-test-result"),
				}},
			}}, nil
	}

	poller2 := host.TaskPoller{
		Engine:              client2,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	s.failover(namespace, clusterName[1], int64(2), client1)

	// Run twice to make sure cron schedule is passed to standby.
	for i := 0; i < 2; i++ {
		_, err = poller2.PollAndProcessWorkflowTask(false, false)
		s.NoError(err)
	}

	_, err = client2.TerminateWorkflowExecution(host.NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
	})
	s.NoError(err)
}

func (s *integrationClustersTestSuite) TestWorkflowRetryFailover() {
	namespace := "test-workflow-retry-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         clusterReplicationConfig,
		ActiveClusterName:                clusterName[0],
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

	// start a workflow
	id := "integration-workflow-retry-failover-test"
	wt := "integration-workflow-retry-failover-test-type"
	tl := "integration-workflow-retry-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl}
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
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:        timestamp.DurationPtr(1 * time.Second),
			MaximumAttempts:        3,
			MaximumInterval:        timestamp.DurationPtr(1 * time.Second),
			NonRetryableErrorTypes: []string{"bad-bug"},
			BackoffCoefficient:     1,
		},
	}
	we, err := client1.StartWorkflowExecution(host.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	var executions []*commonpb.WorkflowExecution
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		executions = append(executions, execution)
		return []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
					Failure: failure.NewServerFailure("retryable-error", false),
				}},
			}}, nil
	}

	poller2 := host.TaskPoller{
		Engine:              client2,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	s.failover(namespace, clusterName[1], int64(2), client1)

	// First attempt
	_, err = poller2.PollAndProcessWorkflowTask(false, false)
	s.NoError(err)
	events := s.getHistory(client2, namespace, executions[0])
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, events[len(events)-1].GetEventType())
	s.Equal(int32(1), events[0].GetWorkflowExecutionStartedEventAttributes().GetAttempt())

	// second attempt
	_, err = poller2.PollAndProcessWorkflowTask(false, false)
	s.NoError(err)
	events = s.getHistory(client2, namespace, executions[1])
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, events[len(events)-1].GetEventType())
	s.Equal(int32(2), events[0].GetWorkflowExecutionStartedEventAttributes().GetAttempt())

	// third attempt. Still failing, should stop retry.
	_, err = poller2.PollAndProcessWorkflowTask(false, false)
	s.NoError(err)
	events = s.getHistory(client2, namespace, executions[2])
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, events[len(events)-1].GetEventType())
	s.Equal(int32(3), events[0].GetWorkflowExecutionStartedEventAttributes().GetAttempt())
}

func (s *integrationClustersTestSuite) TestActivityHeartbeatFailover() {
	namespace := "test-activity-heartbeat-workflow-failover-" + common.GenerateRandomString(5)
	s.registerNamespace(namespace, true)

	taskqueue := "integration-activity-heartbeat-workflow-failover-test-taskqueue"
	client1, worker1 := s.newClientAndWorker(s.cluster1.GetHost().FrontendGRPCAddress(), namespace, taskqueue, "worker1")
	client2, worker2 := s.newClientAndWorker(s.cluster2.GetHost().FrontendGRPCAddress(), namespace, taskqueue, "worker2")

	lastAttemptCount := 0
	expectedHeartbeatValue := 100
	activityWithHB := func(ctx context.Context) error {
		lastAttemptCount = int(activity.GetInfo(ctx).Attempt)
		if activity.HasHeartbeatDetails(ctx) {
			var retrievedHeartbeatValue int
			if err := activity.GetHeartbeatDetails(ctx, &retrievedHeartbeatValue); err == nil {
				s.Equal(expectedHeartbeatValue, retrievedHeartbeatValue)
				return nil
			}
		}
		activity.RecordHeartbeat(ctx, expectedHeartbeatValue)
		time.Sleep(time.Second * 10)
		return errors.New("no heartbeat progress found")
	}
	testWorkflowFn := func(ctx workflow.Context) error {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: time.Second * 1000,
			HeartbeatTimeout:    time.Second * 3,
		}
		ctx = workflow.WithActivityOptions(ctx, ao)
		err := workflow.ExecuteActivity(ctx, activityWithHB).Get(ctx, nil)
		return err
	}
	worker1.RegisterWorkflow(testWorkflowFn)
	worker1.RegisterActivity(activityWithHB)
	worker1.Start()

	// Start a workflow
	startTime := time.Now()
	workflowID := "integration-activity-heartbeat-workflow-failover-test"
	run1, err := client1.ExecuteWorkflow(host.NewContext(), sdkclient.StartWorkflowOptions{
		ID:                 workflowID,
		TaskQueue:          taskqueue,
		WorkflowRunTimeout: time.Second * 300,
	}, testWorkflowFn)

	s.NoError(err)
	s.NotEmpty(run1.GetRunID())

	s.logger.Info("StartWorkflowExecution", tag.WorkflowRunID(run1.GetRunID()))
	time.Sleep(time.Second * 4) // wait for heartbeat from activity to be reported and activity timed out on heartbeat

	worker1.Stop() // stop worker1 so cluster 1 won't make any progress
	s.failover(namespace, clusterName[1], int64(2), s.cluster1.GetFrontendClient())

	// verify things are replicated over
	resp, err := s.cluster1.GetHistoryClient().GetReplicationStatus(context.Background(), &historyservice.GetReplicationStatusRequest{})
	s.NoError(err)
	s.Equal(1, len(resp.Shards)) // test cluster has only one history shard
	shard := resp.Shards[0]
	s.True(shard.MaxReplicationTaskId > 0)
	s.NotNil(shard.ShardLocalTime)
	s.True(shard.ShardLocalTime.Before(time.Now()))
	s.True(shard.ShardLocalTime.After(startTime))
	s.NotNil(shard.RemoteClusters)
	standbyAckInfo, ok := shard.RemoteClusters[clusterName[1]]
	s.True(ok)
	s.Equal(shard.MaxReplicationTaskId, standbyAckInfo.AckedTaskId)
	s.NotNil(standbyAckInfo.AckedTaskVisibilityTime)
	s.True(standbyAckInfo.AckedTaskVisibilityTime.Before(time.Now()))
	s.True(standbyAckInfo.AckedTaskVisibilityTime.After(startTime))

	// Make sure the heartbeat details are sent to cluster2 even when the activity at cluster1
	// has heartbeat timeout. Also make sure the information is recorded when the activity state
	// is "Scheduled"
	dweResponse, err := client2.DescribeWorkflowExecution(host.NewContext(), workflowID, "")
	s.NoError(err)
	pendingActivities := dweResponse.GetPendingActivities()
	s.Equal(1, len(pendingActivities))
	s.Equal(enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, pendingActivities[0].GetState())
	heartbeatPayload := pendingActivities[0].GetHeartbeatDetails()
	var heartbeatValue int
	s.NoError(payloads.Decode(heartbeatPayload, &heartbeatValue))
	s.Equal(expectedHeartbeatValue, heartbeatValue)
	s.Equal(enumspb.TIMEOUT_TYPE_HEARTBEAT, pendingActivities[0].GetLastFailure().GetTimeoutFailureInfo().GetTimeoutType())
	s.Equal("worker1", pendingActivities[0].GetLastWorkerIdentity())

	// start worker2
	worker2.RegisterWorkflow(testWorkflowFn)
	worker2.RegisterActivity(activityWithHB)
	worker2.Start()
	defer worker2.Stop()

	// ExecuteWorkflow return existing running workflow if it already started
	run2, err := client2.ExecuteWorkflow(host.NewContext(), sdkclient.StartWorkflowOptions{
		ID:                 workflowID,
		TaskQueue:          taskqueue,
		WorkflowRunTimeout: time.Second * 300,
	}, testWorkflowFn)
	s.NoError(err)
	// verify we get the same execution as in cluster1
	s.Equal(run1.GetRunID(), run2.GetRunID())

	err = run2.Get(host.NewContext(), nil)
	s.NoError(err) // workflow succeed
	s.Equal(2, lastAttemptCount)
}

// Uncomment if you need to debug history.
// func (s *integrationClustersTestSuite) printHistory(frontendClient workflowservice.WorkflowServiceClient, namespace, workflowID, runID string) {
// 	events := s.getHistory(frontendClient, namespace, &commonpb.WorkflowExecution{
// 		WorkflowId: workflowID,
// 		RunId:      runID,
// 	})
// 	history := &historypb.History{Events: events}
// 	common.PrettyPrintHistory(history, s.logger)
// }

func (s *integrationClustersTestSuite) TestLocalNamespaceMigration() {
	testCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	namespace := "local-ns-to-be-promote-" + common.GenerateRandomString(5)
	s.registerNamespace(namespace, false)

	taskqueue := "integration-local-ns-to-be-promote-taskqueue"
	client1, worker1 := s.newClientAndWorker(s.cluster1.GetHost().FrontendGRPCAddress(), namespace, taskqueue, "worker1")

	testWorkflowFn := func(ctx workflow.Context, sleepInterval time.Duration) error {
		err := workflow.Sleep(ctx, sleepInterval)
		return err
	}

	worker1.RegisterWorkflow(testWorkflowFn)
	worker1.Start()

	// Start wf1 (in local ns)
	workflowID := "local-ns-wf-1"
	run1, err := client1.ExecuteWorkflow(testCtx, sdkclient.StartWorkflowOptions{
		ID:                 workflowID,
		TaskQueue:          taskqueue,
		WorkflowRunTimeout: time.Second * 30,
	}, testWorkflowFn, time.Millisecond*10)

	s.NoError(err)
	s.NotEmpty(run1.GetRunID())
	s.logger.Info("start wf1", tag.WorkflowRunID(run1.GetRunID()))
	// wait until wf1 complete
	err = run1.Get(testCtx, nil)
	s.NoError(err)

	// Start wf2 (start in local ns, and then promote to global ns, wf2 close in global ns)
	workflowID2 := "local-ns-wf-2"
	run2, err := client1.ExecuteWorkflow(testCtx, sdkclient.StartWorkflowOptions{
		ID:                 workflowID2,
		TaskQueue:          taskqueue,
		WorkflowRunTimeout: time.Second * 30,
	}, testWorkflowFn, time.Second*15 /* longer than ns refresh */)
	s.NoError(err)
	s.NotEmpty(run2.GetRunID())
	s.logger.Info("start wf2", tag.WorkflowRunID(run2.GetRunID()))

	// Start wf6 (start in local ns, with buffered event when ns is promoted, close in global ns)
	workflowID6 := "local-ns-wf-buffered-events"
	sigReadyToSendChan := make(chan struct{}, 1)
	sigSendDoneChan := make(chan struct{})
	localActivityFn := func(ctx context.Context) error {
		// to unblock signal sending, so signal is send after first workflow task started.
		select {
		case sigReadyToSendChan <- struct{}{}:
		default:
		}

		// this will block workflow task and cause the signal to become buffered event
		select {
		case <-sigSendDoneChan:
		case <-ctx.Done():
		}

		return nil
	}

	var receivedSig string
	wfWithBufferedEvents := func(ctx workflow.Context) error {
		ctx1 := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
			StartToCloseTimeout: 40 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		})
		f1 := workflow.ExecuteLocalActivity(ctx1, localActivityFn)
		err1 := f1.Get(ctx1, nil)
		if err1 != nil {
			return err1
		}

		sigCh := workflow.GetSignalChannel(ctx, "signal-name")

		for {
			var sigVal string
			ok := sigCh.ReceiveAsync(&sigVal)
			if !ok {
				break
			}
			receivedSig = sigVal
		}

		return nil
	}
	worker1.RegisterWorkflow(wfWithBufferedEvents)

	// Start wf7 (start in local ns, then ns promote and buffer events, close in global ns)
	workflowID7 := "local-ns-promoted-buffered-events-wf7"
	sigReadyToSendChan2 := make(chan struct{}, 1)
	sigSendDoneChan2 := make(chan struct{})
	localActivityFn2 := func(ctx context.Context) error {
		// to unblock signal sending, so signal is send after first workflow task started.
		select {
		case sigReadyToSendChan2 <- struct{}{}:
		default:
		}

		// this will block workflow task and cause the signal to become buffered event
		select {
		case <-sigSendDoneChan2:
		case <-ctx.Done():
		}

		return nil
	}

	var receivedSig2 string
	wfWithBufferedEvents2 := func(ctx workflow.Context) error {
		ctx1 := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
			StartToCloseTimeout: 40 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		})
		f1 := workflow.ExecuteLocalActivity(ctx1, localActivityFn2)
		err1 := f1.Get(ctx1, nil)
		if err1 != nil {
			return err1
		}

		sigCh := workflow.GetSignalChannel(ctx, "signal-name")

		for {
			var sigVal string
			ok := sigCh.ReceiveAsync(&sigVal)
			if !ok {
				break
			}
			receivedSig2 = sigVal
		}

		return nil
	}
	worker1.RegisterWorkflow(wfWithBufferedEvents2)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        workflowID6,
		TaskQueue: taskqueue,
		// Intentionally use same timeout for WorkflowTaskTimeout and WorkflowRunTimeout so if workflow task is not
		// correctly dispatched, it would time out which would fail the workflow and cause test to fail.
		WorkflowTaskTimeout: 40 * time.Second,
		WorkflowRunTimeout:  40 * time.Second,
	}
	run6, err := client1.ExecuteWorkflow(testCtx, workflowOptions, wfWithBufferedEvents)
	s.NoError(err)
	s.NotNil(run6)
	s.True(run6.GetRunID() != "")

	workflowOptions2 := sdkclient.StartWorkflowOptions{
		ID:        workflowID7,
		TaskQueue: taskqueue,
		// Intentionally use same timeout for WorkflowTaskTimeout and WorkflowRunTimeout so if workflow task is not
		// correctly dispatched, it would time out which would fail the workflow and cause test to fail.
		WorkflowTaskTimeout: 40 * time.Second,
		WorkflowRunTimeout:  40 * time.Second,
	}
	run7, err := client1.ExecuteWorkflow(testCtx, workflowOptions2, wfWithBufferedEvents2)
	s.NoError(err)
	s.NotNil(run7)
	s.True(run7.GetRunID() != "")

	// block until first workflow task started
	select {
	case <-sigReadyToSendChan:
	case <-testCtx.Done():
	}

	select {
	case <-sigReadyToSendChan2:
	case <-testCtx.Done():
	}

	// this signal will become buffered event
	err = client1.SignalWorkflow(testCtx, workflowID6, run6.GetRunID(), "signal-name", "signal-value")
	s.NoError(err)

	// promote ns
	frontendClient1 := s.cluster1.GetFrontendClient()
	_, err = frontendClient1.UpdateNamespace(context.Background(), &workflowservice.UpdateNamespaceRequest{
		Namespace:        namespace,
		PromoteNamespace: true,
	})
	s.NoError(err)
	nsResp, err := frontendClient1.DescribeNamespace(context.Background(), &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	})
	s.NoError(err)
	s.True(nsResp.IsGlobalNamespace)
	s.Equal(1, len(nsResp.ReplicationConfig.Clusters))
	time.Sleep(cacheRefreshInterval)

	// this will buffer after ns promotion
	err = client1.SignalWorkflow(testCtx, workflowID7, run7.GetRunID(), "signal-name", "signal-value")
	s.NoError(err)
	// send 2 signals to wf7, both would be buffered.
	err = client1.SignalWorkflow(testCtx, workflowID7, run7.GetRunID(), "signal-name", "signal-value2")
	s.NoError(err)

	// update ns to have 2 clusters
	_, err = frontendClient1.UpdateNamespace(testCtx, &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			Clusters: clusterReplicationConfig,
		},
	})
	s.NoError(err)

	// wait for ns cache to pick up the change
	time.Sleep(cacheRefreshInterval)

	nsResp, err = frontendClient1.DescribeNamespace(testCtx, &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	})
	s.NoError(err)
	s.True(nsResp.IsGlobalNamespace)
	s.Equal(2, len(nsResp.ReplicationConfig.Clusters))

	// namespace update completed, now resume wf6 (bufferedEvent workflow)
	close(sigSendDoneChan)
	close(sigSendDoneChan2)

	// wait until wf2 complete
	err = run2.Get(testCtx, nil)
	s.NoError(err)

	// wait until wf6 complete
	err = run6.Get(testCtx, nil)
	s.NoError(err) // if new workflow task is not correctly dispatched, it would cause timeout error here
	s.Equal("signal-value", receivedSig)

	err = run7.Get(testCtx, nil)
	s.NoError(err) // if new workflow task is not correctly dispatched, it would cause timeout error here
	s.Equal("signal-value2", receivedSig2)

	// start wf3 (start in global ns)
	workflowID3 := "local-ns-wf-3"
	run3, err := client1.ExecuteWorkflow(testCtx, sdkclient.StartWorkflowOptions{
		ID:                 workflowID3,
		TaskQueue:          taskqueue,
		WorkflowRunTimeout: time.Second * 30,
	}, testWorkflowFn, time.Millisecond*10)
	s.NoError(err)
	s.NotEmpty(run3.GetRunID())
	s.logger.Info("start wf3", tag.WorkflowRunID(run3.GetRunID()))
	// wait until wf3 complete
	err = run3.Get(testCtx, nil)
	s.NoError(err)

	// start force-replicate wf
	sysClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.cluster1.GetHost().FrontendGRPCAddress(),
		Namespace: "temporal-system",
	})
	s.NoError(err)
	workflowID4 := "force-replication-wf-4"
	run4, err := sysClient.ExecuteWorkflow(testCtx, sdkclient.StartWorkflowOptions{
		ID:                 workflowID4,
		TaskQueue:          sw.DefaultWorkerTaskQueue,
		WorkflowRunTimeout: time.Second * 30,
	}, "force-replication", migration.ForceReplicationParams{
		Namespace:  namespace,
		OverallRps: 10,
	})

	s.NoError(err)
	err = run4.Get(testCtx, nil)
	s.NoError(err)

	// start namespace-handover wf
	workflowID5 := "namespace-handover-wf-5"
	run5, err := sysClient.ExecuteWorkflow(testCtx, sdkclient.StartWorkflowOptions{
		ID:                 workflowID5,
		TaskQueue:          sw.DefaultWorkerTaskQueue,
		WorkflowRunTimeout: time.Second * 30,
	}, "namespace-handover", migration.NamespaceHandoverParams{
		Namespace:              namespace,
		RemoteCluster:          clusterName[1],
		AllowedLaggingSeconds:  10,
		HandoverTimeoutSeconds: 30,
	})
	s.NoError(err)
	err = run5.Get(testCtx, nil)
	s.NoError(err)

	// at this point ns migration is done.
	// verify namespace is now active in cluster2
	nsResp2, err := frontendClient1.DescribeNamespace(testCtx, &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	})
	s.NoError(err)
	s.True(nsResp2.IsGlobalNamespace)
	s.Equal(2, len(nsResp2.ReplicationConfig.Clusters))
	s.Equal(clusterName[1], nsResp2.ReplicationConfig.ActiveClusterName)

	// verify all wf in ns is now available in cluster2
	client2, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.cluster2.GetHost().FrontendGRPCAddress(),
		Namespace: namespace,
	})
	s.NoError(err)
	verify := func(wfID string, expectedRunID string) {
		desc1, err := client2.DescribeWorkflowExecution(testCtx, wfID, "")
		s.NoError(err)
		s.Equal(expectedRunID, desc1.WorkflowExecutionInfo.Execution.RunId)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, desc1.WorkflowExecutionInfo.Status)
	}
	verify(workflowID, run1.GetRunID())
	verify(workflowID2, run2.GetRunID())
	verify(workflowID3, run3.GetRunID())
	verify(workflowID6, run6.GetRunID())
	verify(workflowID7, run7.GetRunID())
}

func (s *integrationClustersTestSuite) TestForceMigration_ClosedWorkflow() {
	testCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	namespace := "force-replication" + common.GenerateRandomString(5)
	s.registerNamespace(namespace, true)

	taskqueue := "integration-local-force-replication-task-queue"
	client1, worker1 := s.newClientAndWorker(s.cluster1.GetHost().FrontendGRPCAddress(), namespace, taskqueue, "worker1")

	testWorkflowFn := func(ctx workflow.Context) error {
		return nil
	}

	worker1.RegisterWorkflow(testWorkflowFn)
	worker1.Start()

	// Start wf1
	workflowID := "force-replication-test-wf-1"
	run1, err := client1.ExecuteWorkflow(testCtx, sdkclient.StartWorkflowOptions{
		ID:                 workflowID,
		TaskQueue:          taskqueue,
		WorkflowRunTimeout: time.Second * 30,
	}, testWorkflowFn)

	s.NoError(err)
	s.NotEmpty(run1.GetRunID())
	s.logger.Info("start wf1", tag.WorkflowRunID(run1.GetRunID()))
	// wait until wf1 complete
	err = run1.Get(testCtx, nil)
	s.NoError(err)

	frontendClient1 := s.cluster1.GetFrontendClient()
	// Update ns to have 2 clusters
	_, err = frontendClient1.UpdateNamespace(testCtx, &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			Clusters: clusterReplicationConfig,
		},
	})
	s.NoError(err)

	// Wait for ns cache to pick up the change
	time.Sleep(cacheRefreshInterval)

	nsResp, err := frontendClient1.DescribeNamespace(testCtx, &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	})
	s.NoError(err)
	s.True(nsResp.IsGlobalNamespace)
	s.Equal(2, len(nsResp.ReplicationConfig.Clusters))

	// Start force-replicate wf
	sysClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.cluster1.GetHost().FrontendGRPCAddress(),
		Namespace: "temporal-system",
	})
	s.NoError(err)
	forceReplicationWorkflowID := "force-replication-wf"
	sysWfRun, err := sysClient.ExecuteWorkflow(testCtx, sdkclient.StartWorkflowOptions{
		ID:                 forceReplicationWorkflowID,
		TaskQueue:          sw.DefaultWorkerTaskQueue,
		WorkflowRunTimeout: time.Second * 30,
	}, "force-replication", migration.ForceReplicationParams{
		Namespace:  namespace,
		OverallRps: 10,
	})
	s.NoError(err)
	err = sysWfRun.Get(testCtx, nil)
	s.NoError(err)

	// Verify all wf in ns is now available in cluster2
	client2, worker2 := s.newClientAndWorker(s.cluster2.GetHost().FrontendGRPCAddress(), namespace, taskqueue, "worker2")
	verify := func(wfID string, expectedRunID string) {
		desc1, err := client2.DescribeWorkflowExecution(testCtx, wfID, "")
		s.NoError(err)
		s.Equal(expectedRunID, desc1.WorkflowExecutionInfo.Execution.RunId)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, desc1.WorkflowExecutionInfo.Status)
	}
	verify(workflowID, run1.GetRunID())

	frontendClient2 := s.cluster2.GetFrontendClient()
	// Failover ns
	_, err = frontendClient2.UpdateNamespace(testCtx, &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterName[1],
		},
	})
	s.NoError(err)

	time.Sleep(cacheRefreshInterval)

	nsResp, err = frontendClient2.DescribeNamespace(testCtx, &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	})
	s.NoError(err)
	s.Equal(clusterName[1], nsResp.ReplicationConfig.ActiveClusterName)

	worker2.RegisterWorkflow(testWorkflowFn)
	worker2.Start()

	// Test reset workflow in cluster 2
	resetResp, err := client2.ResetWorkflowExecution(testCtx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      run1.GetRunID(),
		},
		Reason:                    "force-replication-test",
		WorkflowTaskFinishEventId: 3,
		RequestId:                 uuid.New(),
	})
	s.NoError(err)

	resetRun := client2.GetWorkflow(testCtx, workflowID, resetResp.GetRunId())
	err = resetRun.Get(testCtx, nil)
	s.NoError(err)

	descResp, err := client2.DescribeWorkflowExecution(testCtx, workflowID, resetResp.GetRunId())
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, descResp.GetWorkflowExecutionInfo().Status)
}

func (s *integrationClustersTestSuite) TestForceMigration_ResetWorkflow() {
	testCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	namespace := "force-replication" + common.GenerateRandomString(5)
	s.registerNamespace(namespace, true)

	taskqueue := "integration-force-replication-reset-task-queue"
	client1, worker1 := s.newClientAndWorker(s.cluster1.GetHost().FrontendGRPCAddress(), namespace, taskqueue, "worker1")

	testWorkflowFn := func(ctx workflow.Context) error {
		return nil
	}

	worker1.RegisterWorkflow(testWorkflowFn)
	worker1.Start()

	// Start wf1
	workflowID := "force-replication-test-reset-wf-1"
	run1, err := client1.ExecuteWorkflow(testCtx, sdkclient.StartWorkflowOptions{
		ID:                 workflowID,
		TaskQueue:          taskqueue,
		WorkflowRunTimeout: time.Second * 30,
	}, testWorkflowFn)

	s.NoError(err)
	s.NotEmpty(run1.GetRunID())
	s.logger.Info("start wf1", tag.WorkflowRunID(run1.GetRunID()))
	// wait until wf1 complete
	err = run1.Get(testCtx, nil)
	s.NoError(err)

	resp, err := client1.ResetWorkflowExecution(testCtx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      run1.GetRunID(),
		},
		Reason:                    "test",
		WorkflowTaskFinishEventId: 3,
		RequestId:                 uuid.New(),
	})
	s.NoError(err)
	resetRun := client1.GetWorkflow(testCtx, workflowID, resp.GetRunId())
	err = resetRun.Get(testCtx, nil)
	s.NoError(err)

	frontendClient1 := s.cluster1.GetFrontendClient()
	// Update ns to have 2 clusters
	_, err = frontendClient1.UpdateNamespace(testCtx, &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			Clusters: clusterReplicationConfig,
		},
	})
	s.NoError(err)

	// Wait for ns cache to pick up the change
	time.Sleep(cacheRefreshInterval)

	nsResp, err := frontendClient1.DescribeNamespace(testCtx, &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	})
	s.NoError(err)
	s.True(nsResp.IsGlobalNamespace)
	s.Equal(2, len(nsResp.ReplicationConfig.Clusters))

	// Start force-replicate wf
	sysClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.cluster1.GetHost().FrontendGRPCAddress(),
		Namespace: "temporal-system",
	})
	s.NoError(err)
	forceReplicationWorkflowID := "force-replication-wf"
	sysWfRun, err := sysClient.ExecuteWorkflow(testCtx, sdkclient.StartWorkflowOptions{
		ID:                 forceReplicationWorkflowID,
		TaskQueue:          sw.DefaultWorkerTaskQueue,
		WorkflowRunTimeout: time.Second * 30,
	}, "force-replication", migration.ForceReplicationParams{
		Namespace:  namespace,
		OverallRps: 10,
	})
	s.NoError(err)
	err = sysWfRun.Get(testCtx, nil)
	s.NoError(err)

	// Verify all wf in ns is now available in cluster2
	client2, _ := s.newClientAndWorker(s.cluster2.GetHost().FrontendGRPCAddress(), namespace, taskqueue, "worker2")
	verifyHistory := func(wfID string, runID string) {
		iter1 := client1.GetWorkflowHistory(testCtx, wfID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		iter2 := client2.GetWorkflowHistory(testCtx, wfID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for iter1.HasNext() && iter2.HasNext() {
			event1, err := iter1.Next()
			s.NoError(err)
			event2, err := iter2.Next()
			s.NoError(err)
			s.Equal(event1, event2)
		}
		s.False(iter1.HasNext())
		s.False(iter2.HasNext())
	}
	verifyHistory(workflowID, run1.GetRunID())
	verifyHistory(workflowID, resp.GetRunId())
}

func (s *integrationClustersTestSuite) getHistory(client host.FrontendClient, namespace string, execution *commonpb.WorkflowExecution) []*historypb.HistoryEvent {
	historyResponse, err := client.GetWorkflowExecutionHistory(host.NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:       namespace,
		Execution:       execution,
		MaximumPageSize: 5, // Use small page size to force pagination code path
	})
	s.NoError(err)

	events := historyResponse.History.Events
	for historyResponse.NextPageToken != nil {
		historyResponse, err = client.GetWorkflowExecutionHistory(host.NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:     namespace,
			Execution:     execution,
			NextPageToken: historyResponse.NextPageToken,
		})
		s.NoError(err)
		events = append(events, historyResponse.History.Events...)
	}

	return events
}

func (s *integrationClustersTestSuite) failover(
	namespace string,
	targetCluster string,
	targetFailoverVersion int64,
	client host.FrontendClient,
) {
	// wait for replication task propagation
	time.Sleep(4 * time.Second)

	// update namespace to fail over
	updateReq := &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: targetCluster,
		},
	}
	updateResp, err := client.UpdateNamespace(host.NewContext(), updateReq)
	s.NoError(err)
	s.Equal(targetCluster, updateResp.ReplicationConfig.GetActiveClusterName())
	s.Equal(targetFailoverVersion, updateResp.GetFailoverVersion())

	// wait till failover completed
	time.Sleep(cacheRefreshInterval)
}

func (s *integrationClustersTestSuite) registerNamespace(namespace string, isGlobalNamespace bool) {
	clusters := clusterReplicationConfig
	if !isGlobalNamespace {
		clusters = clusterReplicationConfig[0:1]
	}
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                isGlobalNamespace,
		Clusters:                         clusters,
		ActiveClusterName:                clusterName[0],
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
	s.Equal(namespace, resp.NamespaceInfo.Name)
	s.Equal(isGlobalNamespace, resp.IsGlobalNamespace)
}

func (s *integrationClustersTestSuite) newClientAndWorker(hostport, namespace, taskqueue, identity string) (sdkclient.Client, sdkworker.Worker) {
	sdkClient1, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  hostport,
		Namespace: namespace,
	})
	s.NoError(err)

	worker1 := sdkworker.New(sdkClient1, taskqueue, sdkworker.Options{
		Identity: identity,
	})

	return sdkClient1, worker1
}
