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
// need to run xdc tests with race detector off because of ringpop bug causing data race issue

package xdc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"io/ioutil"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	decisionpb "go.temporal.io/api/decision/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	querypb "go.temporal.io/api/query/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/loggerimpl"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/environment"
	"go.temporal.io/server/host"
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
	cacheRefreshInterval = cache.NamespaceCacheRefreshInterval + 5*time.Second
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
	zapLogger, err := zap.NewDevelopment()
	// cannot use s.Nil since it is not initialized
	s.Require().NoError(err)
	s.logger = loggerimpl.NewLogger(zapLogger)

	fileName := "../testdata/xdc_integration_test_clusters.yaml"
	if host.TestFlags.TestClusterConfigFile != "" {
		fileName = host.TestFlags.TestClusterConfigFile
	}
	environment.SetupEnv()

	confContent, err := ioutil.ReadFile(fileName)
	s.Require().NoError(err)
	confContent = []byte(os.ExpandEnv(string(confContent)))

	var clusterConfigs []*host.TestClusterConfig
	s.Require().NoError(yaml.Unmarshal(confContent, &clusterConfigs))

	c, err := host.NewCluster(clusterConfigs[0], s.logger.WithTags(tag.ClusterName(clusterName[0])))
	s.Require().NoError(err)
	s.cluster1 = c

	c, err = host.NewCluster(clusterConfigs[1], s.logger.WithTags(tag.ClusterName(clusterName[1])))
	s.Require().NoError(err)
	s.cluster2 = c
}

func (s *integrationClustersTestSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

func (s *integrationClustersTestSuite) TearDownSuite() {
	s.cluster1.TearDownCluster()
	s.cluster2.TearDownCluster()
}

func (s *integrationClustersTestSuite) TestNamespaceFailover() {
	namespace := "test-namespace-for-fail-over-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Name:                                 namespace,
		IsGlobalNamespace:                    true,
		Clusters:                             clusterReplicationConfig,
		ActiveClusterName:                    clusterName[0],
		WorkflowExecutionRetentionPeriodDays: 7,
	}
	_, err := client1.RegisterNamespace(host.NewContext(), regReq)
	s.NoError(err)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Name: namespace,
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

	// update namespace to fail over
	updateReq := &workflowservice.UpdateNamespaceRequest{
		Name: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterName[1],
		},
	}
	updateResp, err := client1.UpdateNamespace(host.NewContext(), updateReq)
	s.NoError(err)
	s.NotNil(updateResp)
	s.Equal(clusterName[1], updateResp.ReplicationConfig.GetActiveClusterName())
	s.Equal(int64(1), updateResp.GetFailoverVersion())

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
	s.Equal(int64(1), resp3.GetFailoverVersion())

	// start workflow in new cluster
	id := "integration-namespace-failover-test"
	wt := "integration-namespace-failover-test-type"
	tq := "integration-namespace-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tq}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		Identity:                   identity,
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
	namespace := "test-simple-workflow-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Name:                                 namespace,
		IsGlobalNamespace:                    true,
		Clusters:                             clusterReplicationConfig,
		ActiveClusterName:                    clusterName[0],
		WorkflowExecutionRetentionPeriodDays: 1,
	}
	_, err := client1.RegisterNamespace(host.NewContext(), regReq)
	s.NoError(err)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Name: namespace,
	}
	resp, err := client1.DescribeNamespace(host.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)
	// Wait for namespace cache to pick the change
	time.Sleep(cache.NamespaceCacheRefreshInterval)

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
		RequestId:                  uuid.New(),
		Namespace:                  namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		Identity:                   identity,
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
	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(activityCounter)),
					ActivityType:                  &commonpb.ActivityType{Name: activityName},
					TaskQueue:                     &taskqueuepb.TaskQueue{Name: tq},
					Input:                         payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 30,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       20,
				}},
			}}, nil
		}

		workflowComplete = true
		return []*decisionpb.Decision{{
			DecisionType: enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
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
		Engine:          client1,
		Namespace:       namespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		QueryHandler:    queryHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	poller2 := host.TaskPoller{
		Engine:          client2,
		Namespace:       namespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		QueryHandler:    queryHandler,
		Logger:          s.logger,
		T:               s.T(),
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
			Namespace: namespace,
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
	var queryResultString string
	err = payloads.Decode(queryResult.Resp.QueryResult, &queryResultString)
	s.NoError(err)
	s.Equal("query-result", queryResultString)

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
	err = payloads.Decode(queryResult.Resp.QueryResult, &queryResultString)
	s.NoError(err)
	s.Equal("query-result", queryResultString)

	// update namespace to fail over
	updateReq := &workflowservice.UpdateNamespaceRequest{
		Name: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterName[1],
		},
	}
	updateResp, err := client1.UpdateNamespace(host.NewContext(), updateReq)
	s.NoError(err)
	s.NotNil(updateResp)
	s.Equal(clusterName[1], updateResp.ReplicationConfig.GetActiveClusterName())
	s.Equal(int64(1), updateResp.GetFailoverVersion())

	// wait till failover completed
	time.Sleep(cacheRefreshInterval)

	// check history matched
	getHistoryReq := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: namespace,
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
	err = payloads.Decode(queryResult.Resp.QueryResult, &queryResultString)
	s.NoError(err)
	s.Equal("query-result", queryResultString)

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
	err = payloads.Decode(queryResult.Resp.QueryResult, &queryResultString)
	s.NoError(err)
	s.Equal("query-result", queryResultString)

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

func (s *integrationClustersTestSuite) TestStickyDecisionFailover() {
	namespace := "test-sticky-decision-workflow-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Name:                                 namespace,
		IsGlobalNamespace:                    true,
		Clusters:                             clusterReplicationConfig,
		ActiveClusterName:                    clusterName[0],
		WorkflowExecutionRetentionPeriodDays: 1,
	}
	_, err := client1.RegisterNamespace(host.NewContext(), regReq)
	s.NoError(err)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Name: namespace,
	}
	resp, err := client1.DescribeNamespace(host.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	client2 := s.cluster2.GetFrontendClient() // standby

	// Start a workflow
	id := "integration-sticky-decision-workflow-failover-test"
	wt := "integration-sticky-decision-workflow-failover-test-type"
	tq := "integration-sticky-decision-workflow-failover-test-taskqueue"
	stq1 := "integration-sticky-decision-workflow-failover-test-taskqueue-sticky1"
	stq2 := "integration-sticky-decision-workflow-failover-test-taskqueue-sticky2"
	identity1 := "worker1"
	identity2 := "worker2"

	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tq}
	stickyTaskQueue1 := &taskqueuepb.TaskQueue{Name: stq1}
	stickyTaskQueue2 := &taskqueuepb.TaskQueue{Name: stq2}
	stickyTaskTimeout := 100
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  2592000,
		WorkflowTaskTimeoutSeconds: 60,
		Identity:                   identity1,
	}
	we, err := client1.StartWorkflowExecution(host.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	s.logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

	firstDecisionMade := false
	secondDecisionMade := false
	workflowCompleted := false
	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {
		if !firstDecisionMade {
			firstDecisionMade = true
			return []*decisionpb.Decision{}, nil
		}

		if !secondDecisionMade {
			secondDecisionMade = true
			return []*decisionpb.Decision{}, nil
		}

		workflowCompleted = true
		return []*decisionpb.Decision{{
			DecisionType: enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller1 := &host.TaskPoller{
		Engine:                              client1,
		Namespace:                           namespace,
		TaskQueue:                           taskQueue,
		StickyTaskQueue:                     stickyTaskQueue1,
		StickyScheduleToStartTimeoutSeconds: int32(stickyTaskTimeout),
		Identity:                            identity1,
		DecisionHandler:                     dtHandler,
		Logger:                              s.logger,
		T:                                   s.T(),
	}

	poller2 := &host.TaskPoller{
		Engine:                              client2,
		Namespace:                           namespace,
		TaskQueue:                           taskQueue,
		StickyTaskQueue:                     stickyTaskQueue2,
		StickyScheduleToStartTimeoutSeconds: int32(stickyTaskTimeout),
		Identity:                            identity2,
		DecisionHandler:                     dtHandler,
		Logger:                              s.logger,
		T:                                   s.T(),
	}

	_, err = poller1.PollAndProcessWorkflowTaskWithAttemptAndRetry(false, false, false, true, 0, 5)
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(firstDecisionMade)

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

	// Update namespace to fail over
	updateReq := &workflowservice.UpdateNamespaceRequest{
		Name: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterName[1],
		},
	}
	updateResp, err := client1.UpdateNamespace(host.NewContext(), updateReq)
	s.NoError(err)
	s.NotNil(updateResp)
	s.Equal(clusterName[1], updateResp.ReplicationConfig.GetActiveClusterName())
	s.Equal(int64(1), updateResp.GetFailoverVersion())

	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	_, err = poller2.PollAndProcessWorkflowTaskWithAttemptAndRetry(false, false, false, true, 0, 5)
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(secondDecisionMade)

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

	// Update namespace to fail over back
	updateReq = &workflowservice.UpdateNamespaceRequest{
		Name: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterName[0],
		},
	}
	updateResp, err = client2.UpdateNamespace(host.NewContext(), updateReq)
	s.NoError(err)
	s.NotNil(updateResp)
	s.Equal(clusterName[0], updateResp.ReplicationConfig.GetActiveClusterName())
	s.Equal(int64(10), updateResp.GetFailoverVersion())

	_, err = poller1.PollAndProcessWorkflowTask(true, false)
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(workflowCompleted)
}

func (s *integrationClustersTestSuite) TestStartWorkflowExecution_Failover_WorkflowIDReusePolicy() {
	namespace := "test-start-workflow-failover-ID-reuse-policy" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Name:                                 namespace,
		IsGlobalNamespace:                    true,
		Clusters:                             clusterReplicationConfig,
		ActiveClusterName:                    clusterName[0],
		WorkflowExecutionRetentionPeriodDays: 1,
	}
	_, err := client1.RegisterNamespace(host.NewContext(), regReq)
	s.NoError(err)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Name: namespace,
	}
	resp, err := client1.DescribeNamespace(host.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)
	// Wait for namespace cache to pick the change
	time.Sleep(cache.NamespaceCacheRefreshInterval)

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
		RequestId:                  uuid.New(),
		Namespace:                  namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		Identity:                   identity,
		WorkflowIdReusePolicy:      enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}
	we, err := client1.StartWorkflowExecution(host.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())
	s.logger.Info("StartWorkflowExecution in cluster 1: ", tag.WorkflowRunID(we.GetRunId()))

	workflowCompleteTimes := 0
	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {

		workflowCompleteTimes++
		return []*decisionpb.Decision{{
			DecisionType: enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller := host.TaskPoller{
		Engine:          client1,
		Namespace:       namespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: nil,
		Logger:          s.logger,
		T:               s.T(),
	}

	poller2 := host.TaskPoller{
		Engine:          client2,
		Namespace:       namespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: nil,
		Logger:          s.logger,
		T:               s.T(),
	}

	// Complete the workflow in cluster 1
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.Equal(1, workflowCompleteTimes)

	// update namespace to fail over
	updateReq := &workflowservice.UpdateNamespaceRequest{
		Name: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterName[1],
		},
	}
	updateResp, err := client1.UpdateNamespace(host.NewContext(), updateReq)
	s.NoError(err)
	s.NotNil(updateResp)
	s.Equal(clusterName[1], updateResp.ReplicationConfig.GetActiveClusterName())
	s.Equal(int64(1), updateResp.GetFailoverVersion())

	// wait till failover completed
	time.Sleep(cacheRefreshInterval)

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
		Name:                                 namespace,
		IsGlobalNamespace:                    true,
		Clusters:                             clusterReplicationConfig,
		ActiveClusterName:                    clusterName[0],
		WorkflowExecutionRetentionPeriodDays: 1,
	}
	_, err := client1.RegisterNamespace(host.NewContext(), regReq)
	s.NoError(err)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Name: namespace,
	}
	resp, err := client1.DescribeNamespace(host.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	client2 := s.cluster2.GetFrontendClient() // standby

	// start a workflow
	id := "integration-terminate-workflow-failover-test"
	wt := "integration-terminate-workflow-failover-test-type"
	tl := "integration-terminate-workflow-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		Identity:                   identity,
	}
	we, err := client1.StartWorkflowExecution(host.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	activityName := "activity_type1"
	activityCount := int32(1)
	activityCounter := int32(0)
	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(int(activityCounter)),
					ActivityType:                  &commonpb.ActivityType{Name: activityName},
					TaskQueue:                     &taskqueuepb.TaskQueue{Name: tl},
					Input:                         payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeoutSeconds: 100,
					ScheduleToStartTimeoutSeconds: 10,
					StartToCloseTimeoutSeconds:    50,
					HeartbeatTimeoutSeconds:       5,
				}},
			}}, nil
		}

		return []*decisionpb.Decision{{
			DecisionType: enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	atHandler := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {

		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &host.TaskPoller{
		Engine:          client1,
		Namespace:       namespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	// make some progress in cluster 1
	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// update namespace to fail over
	updateReq := &workflowservice.UpdateNamespaceRequest{
		Name: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterName[1],
		},
	}
	updateResp, err := client1.UpdateNamespace(host.NewContext(), updateReq)
	s.NoError(err)
	s.NotNil(updateResp)
	s.Equal(clusterName[1], updateResp.ReplicationConfig.GetActiveClusterName())
	s.Equal(int64(1), updateResp.GetFailoverVersion())

	// wait till failover completed
	time.Sleep(cacheRefreshInterval)

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

func (s *integrationClustersTestSuite) TestContinueAsNewFailover() {
	namespace := "test-continueAsNew-workflow-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Name:                                 namespace,
		IsGlobalNamespace:                    true,
		Clusters:                             clusterReplicationConfig,
		ActiveClusterName:                    clusterName[0],
		WorkflowExecutionRetentionPeriodDays: 1,
	}
	_, err := client1.RegisterNamespace(host.NewContext(), regReq)
	s.NoError(err)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Name: namespace,
	}
	resp, err := client1.DescribeNamespace(host.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	client2 := s.cluster2.GetFrontendClient() // standby

	// start a workflow
	id := "integration-continueAsNew-workflow-failover-test"
	wt := "integration-continueAsNew-workflow-failover-test-type"
	tl := "integration-continueAsNew-workflow-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		Identity:                   identity,
	}
	we, err := client1.StartWorkflowExecution(host.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	workflowComplete := false
	continueAsNewCount := int32(5)
	continueAsNewCounter := int32(0)
	var previousRunID string
	var lastRunStartedEvent *historypb.HistoryEvent
	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {
		if continueAsNewCounter < continueAsNewCount {
			previousRunID = execution.GetRunId()
			continueAsNewCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, continueAsNewCounter))

			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
				Attributes: &decisionpb.Decision_ContinueAsNewWorkflowExecutionDecisionAttributes{ContinueAsNewWorkflowExecutionDecisionAttributes: &decisionpb.ContinueAsNewWorkflowExecutionDecisionAttributes{
					WorkflowType:               workflowType,
					TaskQueue:                  &taskqueuepb.TaskQueue{Name: tl},
					Input:                      payloads.EncodeBytes(buf.Bytes()),
					WorkflowRunTimeoutSeconds:  100,
					WorkflowTaskTimeoutSeconds: 10,
				}},
			}}, nil
		}

		lastRunStartedEvent = history.Events[0]
		workflowComplete = true
		return []*decisionpb.Decision{{
			DecisionType: enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller := &host.TaskPoller{
		Engine:          client1,
		Namespace:       namespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	poller2 := host.TaskPoller{
		Engine:          client2,
		Namespace:       namespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	// make some progress in cluster 1 and did some continueAsNew
	for i := 0; i < 3; i++ {
		_, err := poller.PollAndProcessWorkflowTask(false, false)
		s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err, strconv.Itoa(i))
	}

	// update namespace to fail over
	updateReq := &workflowservice.UpdateNamespaceRequest{
		Name: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterName[1],
		},
	}
	updateResp, err := client1.UpdateNamespace(host.NewContext(), updateReq)
	s.NoError(err)
	s.NotNil(updateResp)
	s.Equal(clusterName[1], updateResp.ReplicationConfig.GetActiveClusterName())
	s.Equal(int64(1), updateResp.GetFailoverVersion())

	// wait till failover completed
	time.Sleep(cacheRefreshInterval)

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
		Name:                                 namespace,
		IsGlobalNamespace:                    true,
		Clusters:                             clusterReplicationConfig,
		ActiveClusterName:                    clusterName[0],
		WorkflowExecutionRetentionPeriodDays: 1,
	}
	_, err := client1.RegisterNamespace(host.NewContext(), regReq)
	s.NoError(err)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Name: namespace,
	}
	resp, err := client1.DescribeNamespace(host.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	client2 := s.cluster2.GetFrontendClient() // standby

	// Start a workflow
	id := "integration-signal-workflow-failover-test"
	wt := "integration-signal-workflow-failover-test-type"
	tl := "integration-signal-workflow-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  300,
		WorkflowTaskTimeoutSeconds: 1,
		Identity:                   identity,
	}
	we, err := client1.StartWorkflowExecution(host.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	s.logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

	eventSignaled := false
	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {
		if !eventSignaled {
			for _, event := range history.Events[previousStartedEventID:] {
				if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					eventSignaled = true
					return []*decisionpb.Decision{}, nil
				}
			}
		}

		return []*decisionpb.Decision{{
			DecisionType: enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller := &host.TaskPoller{
		Engine:          client1,
		Namespace:       namespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	poller2 := &host.TaskPoller{
		Engine:          client2,
		Namespace:       namespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.logger,
		T:               s.T(),
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

	// Update namespace to fail over
	updateReq := &workflowservice.UpdateNamespaceRequest{
		Name: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterName[1],
		},
	}
	updateResp, err := client1.UpdateNamespace(host.NewContext(), updateReq)
	s.NoError(err)
	s.NotNil(updateResp)
	s.Equal(clusterName[1], updateResp.ReplicationConfig.GetActiveClusterName())
	s.Equal(int64(1), updateResp.GetFailoverVersion())

	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

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
		Name:                                 namespace,
		IsGlobalNamespace:                    true,
		Clusters:                             clusterReplicationConfig,
		ActiveClusterName:                    clusterName[0],
		WorkflowExecutionRetentionPeriodDays: 1,
	}
	_, err := client1.RegisterNamespace(host.NewContext(), regReq)
	s.NoError(err)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Name: namespace,
	}
	resp, err := client1.DescribeNamespace(host.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	client2 := s.cluster2.GetFrontendClient() // standby

	// Start a workflow
	id := "integration-user-timer-workflow-failover-test"
	wt := "integration-user-timer-workflow-failover-test-type"
	tl := "integration-user-timer-workflow-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  300,
		WorkflowTaskTimeoutSeconds: 10,
		Identity:                   identity,
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
	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {

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
			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_START_TIMER,
				Attributes: &decisionpb.Decision_StartTimerDecisionAttributes{StartTimerDecisionAttributes: &decisionpb.StartTimerDecisionAttributes{
					TimerId:                   "timer-id",
					StartToFireTimeoutSeconds: 2,
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
				return []*decisionpb.Decision{}, nil
			}
		}

		workflowCompleted = true
		return []*decisionpb.Decision{{
			DecisionType: enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller1 := &host.TaskPoller{
		Engine:          client1,
		Namespace:       namespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	poller2 := &host.TaskPoller{
		Engine:          client2,
		Namespace:       namespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.logger,
		T:               s.T(),
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

	// Update namespace to fail over
	updateReq := &workflowservice.UpdateNamespaceRequest{
		Name: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterName[1],
		},
	}
	updateResp, err := client1.UpdateNamespace(host.NewContext(), updateReq)
	s.NoError(err)
	s.NotNil(updateResp)
	s.Equal(clusterName[1], updateResp.ReplicationConfig.GetActiveClusterName())
	s.Equal(int64(1), updateResp.GetFailoverVersion())

	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	for i := 1; i < 20; i++ {
		if !workflowCompleted {
			_, err = poller2.PollAndProcessWorkflowTask(true, false)
			s.NoError(err)
			time.Sleep(time.Second)
		}
	}
}

func (s *integrationClustersTestSuite) TestActivityHeartbeatFailover() {
	namespace := "test-activity-heartbeat-workflow-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Name:                                 namespace,
		IsGlobalNamespace:                    true,
		Clusters:                             clusterReplicationConfig,
		ActiveClusterName:                    clusterName[0],
		WorkflowExecutionRetentionPeriodDays: 1,
	}
	_, err := client1.RegisterNamespace(host.NewContext(), regReq)
	s.NoError(err)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Name: namespace,
	}
	resp, err := client1.DescribeNamespace(host.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	client2 := s.cluster2.GetFrontendClient() // standby

	// Start a workflow
	id := "integration-activity-heartbeat-workflow-failover-test"
	wt := "integration-activity-heartbeat-workflow-failover-test-type"
	tl := "integration-activity-heartbeat-workflow-failover-test-taskqueue"
	identity1 := "worker1"
	identity2 := "worker2"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  300,
		WorkflowTaskTimeoutSeconds: 10,
		Identity:                   identity1,
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

	activitySent := false
	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {
		if !activitySent {
			activitySent = true
			return []*decisionpb.Decision{{
				DecisionType: enumspb.DECISION_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &decisionpb.Decision_ScheduleActivityTaskDecisionAttributes{ScheduleActivityTaskDecisionAttributes: &decisionpb.ScheduleActivityTaskDecisionAttributes{
					ActivityId:                    strconv.Itoa(1),
					ActivityType:                  &commonpb.ActivityType{Name: "some random activity type"},
					TaskQueue:                     &taskqueuepb.TaskQueue{Name: tl},
					Input:                         payloads.EncodeString("some random input"),
					ScheduleToCloseTimeoutSeconds: 1000,
					ScheduleToStartTimeoutSeconds: 1000,
					StartToCloseTimeoutSeconds:    1000,
					HeartbeatTimeoutSeconds:       3,
					RetryPolicy: &commonpb.RetryPolicy{
						InitialIntervalInSeconds: 1,
						MaximumAttempts:          3,
						MaximumIntervalInSeconds: 1,
						NonRetryableErrorTypes:   []string{"bad-bug"},
						BackoffCoefficient:       1,
					},
				}},
			}}, nil
		}

		return []*decisionpb.Decision{{
			DecisionType: enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	// activity handler
	activity1Called := false
	heartbeatDetails := payloads.EncodeString("details")
	atHandler1 := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {
		activity1Called = true
		_, err = client1.RecordActivityTaskHeartbeat(host.NewContext(), &workflowservice.RecordActivityTaskHeartbeatRequest{
			TaskToken: taskToken, Details: heartbeatDetails})
		s.NoError(err)
		time.Sleep(5 * time.Second)
		return payloads.EncodeString("Activity Result"), false, nil
	}

	// activity handler
	activity2Called := false
	atHandler2 := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType,
		activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {
		activity2Called = true
		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller1 := &host.TaskPoller{
		Engine:          client1,
		Namespace:       namespace,
		TaskQueue:       taskQueue,
		Identity:        identity1,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler1,
		Logger:          s.logger,
		T:               s.T(),
	}

	poller2 := &host.TaskPoller{
		Engine:          client2,
		Namespace:       namespace,
		TaskQueue:       taskQueue,
		Identity:        identity2,
		DecisionHandler: dtHandler,
		ActivityHandler: atHandler2,
		Logger:          s.logger,
		T:               s.T(),
	}

	describeWorkflowExecution := func(client workflowservice.WorkflowServiceClient) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
		return client.DescribeWorkflowExecution(host.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
		})
	}

	_, err = poller1.PollAndProcessWorkflowTask(false, false)
	s.NoError(err)
	err = poller1.PollAndProcessActivityTask(false)
	s.IsType(&serviceerror.NotFound{}, err)

	// Update namespace to fail over
	updateReq := &workflowservice.UpdateNamespaceRequest{
		Name: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterName[1],
		},
	}
	updateResp, err := client1.UpdateNamespace(host.NewContext(), updateReq)
	s.NoError(err)
	s.NotNil(updateResp)
	s.Equal(clusterName[1], updateResp.ReplicationConfig.GetActiveClusterName())
	s.Equal(int64(1), updateResp.GetFailoverVersion())

	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	// Make sure the heartbeat details are sent to cluster2 even when the activity at cluster1
	// has heartbeat timeout. Also make sure the information is recorded when the activity state
	// is "Scheduled"
	dweResponse, err := describeWorkflowExecution(client2)
	s.NoError(err)
	pendingActivities := dweResponse.GetPendingActivities()
	s.Equal(1, len(pendingActivities))
	s.Equal(enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, pendingActivities[0].GetState())
	s.Equal(heartbeatDetails, pendingActivities[0].GetHeartbeatDetails())
	s.Equal(enumspb.TIMEOUT_TYPE_HEARTBEAT, pendingActivities[0].GetLastFailure().GetTimeoutFailureInfo().GetTimeoutType())
	s.Equal(identity1, pendingActivities[0].GetLastWorkerIdentity())

	for i := 0; i < 10; i++ {
		poller2.PollAndProcessActivityTask(false)
		if activity2Called {
			break
		} else {
			time.Sleep(1 * time.Second)
		}
	}

	s.True(activity1Called)
	s.True(activity2Called)

	historyResponse, err := client2.GetWorkflowExecutionHistory(host.NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
	})
	s.NoError(err)
	history := historyResponse.History

	activityRetryFound := false
	for _, event := range history.Events {
		if event.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED {
			attribute := event.GetActivityTaskStartedEventAttributes()
			s.True(attribute.GetAttempt() > 0)
			activityRetryFound = true
		}
	}
	s.True(activityRetryFound)
}

func (s *integrationClustersTestSuite) TestTransientDecisionFailover() {
	namespace := "test-transient-decision-workflow-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Name:                                 namespace,
		IsGlobalNamespace:                    true,
		Clusters:                             clusterReplicationConfig,
		ActiveClusterName:                    clusterName[0],
		WorkflowExecutionRetentionPeriodDays: 1,
	}
	_, err := client1.RegisterNamespace(host.NewContext(), regReq)
	s.NoError(err)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Name: namespace,
	}
	resp, err := client1.DescribeNamespace(host.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	client2 := s.cluster2.GetFrontendClient() // standby

	// Start a workflow
	id := "integration-transient-decision-workflow-failover-test"
	wt := "integration-transient-decision-workflow-failover-test-type"
	tl := "integration-transient-decision-workflow-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  300,
		WorkflowTaskTimeoutSeconds: 8,
		Identity:                   identity,
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

	decisionFailed := false
	workflowFinished := false
	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {
		if !decisionFailed {
			decisionFailed = true
			return nil, errors.New("random fail decision reason")
		}

		workflowFinished = true
		return []*decisionpb.Decision{{
			DecisionType: enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller1 := &host.TaskPoller{
		Engine:          client1,
		Namespace:       namespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	poller2 := &host.TaskPoller{
		Engine:          client2,
		Namespace:       namespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	// this will fail the decision
	_, err = poller1.PollAndProcessWorkflowTask(false, false)
	s.NoError(err)

	// Update namespace to fail over
	updateReq := &workflowservice.UpdateNamespaceRequest{
		Name: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterName[1],
		},
	}
	updateResp, err := client1.UpdateNamespace(host.NewContext(), updateReq)
	s.NoError(err)
	s.NotNil(updateResp)
	s.Equal(clusterName[1], updateResp.ReplicationConfig.GetActiveClusterName())
	s.Equal(int64(1), updateResp.GetFailoverVersion())

	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	// for failover transient decision, it is guaranteed that the transient decision
	// after the failover has attempt 0
	// for details see ReplicateTransientWorkflowTaskScheduled
	_, err = poller2.PollAndProcessWorkflowTaskWithAttempt(false, false, false, false, 0)
	s.NoError(err)
	s.True(workflowFinished)
}

func (s *integrationClustersTestSuite) TestCronWorkflowFailover() {
	namespace := "test-cron-workflow-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Name:                                 namespace,
		IsGlobalNamespace:                    true,
		Clusters:                             clusterReplicationConfig,
		ActiveClusterName:                    clusterName[0],
		WorkflowExecutionRetentionPeriodDays: 1,
	}
	_, err := client1.RegisterNamespace(host.NewContext(), regReq)
	s.NoError(err)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Name: namespace,
	}
	resp, err := client1.DescribeNamespace(host.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	client2 := s.cluster2.GetFrontendClient() // standby

	// start a workflow
	id := "integration-cron-workflow-failover-test"
	wt := "integration-cron-workflow-failover-test-type"
	tl := "integration-cron-workflow-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		Identity:                   identity,
		CronSchedule:               "@every 5s",
	}
	we, err := client1.StartWorkflowExecution(host.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {
		return []*decisionpb.Decision{
			{
				DecisionType: enumspb.DECISION_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &decisionpb.Decision_CompleteWorkflowExecutionDecisionAttributes{CompleteWorkflowExecutionDecisionAttributes: &decisionpb.CompleteWorkflowExecutionDecisionAttributes{
					Result: payloads.EncodeString("cron-test-result"),
				}},
			}}, nil
	}

	poller2 := host.TaskPoller{
		Engine:          client2,
		Namespace:       namespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	// Failover during backoff
	// Update namespace to fail over
	updateReq := &workflowservice.UpdateNamespaceRequest{
		Name: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterName[1],
		},
	}
	updateResp, err := client1.UpdateNamespace(host.NewContext(), updateReq)
	s.NoError(err)
	s.NotNil(updateResp)
	s.Equal(clusterName[1], updateResp.ReplicationConfig.GetActiveClusterName())
	s.Equal(int64(1), updateResp.GetFailoverVersion())

	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

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
		Name:                                 namespace,
		IsGlobalNamespace:                    true,
		Clusters:                             clusterReplicationConfig,
		ActiveClusterName:                    clusterName[0],
		WorkflowExecutionRetentionPeriodDays: 1,
	}
	_, err := client1.RegisterNamespace(host.NewContext(), regReq)
	s.NoError(err)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Name: namespace,
	}
	resp, err := client1.DescribeNamespace(host.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	client2 := s.cluster2.GetFrontendClient() // standby

	// start a workflow
	id := "integration-workflow-retry-failover-test"
	wt := "integration-workflow-retry-failover-test-type"
	tl := "integration-workflow-retry-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                  uuid.New(),
		Namespace:                  namespace,
		WorkflowId:                 id,
		WorkflowType:               workflowType,
		TaskQueue:                  taskQueue,
		Input:                      nil,
		WorkflowRunTimeoutSeconds:  100,
		WorkflowTaskTimeoutSeconds: 1,
		Identity:                   identity,
		RetryPolicy: &commonpb.RetryPolicy{
			InitialIntervalInSeconds: 1,
			MaximumAttempts:          3,
			MaximumIntervalInSeconds: 1,
			NonRetryableErrorTypes:   []string{"bad-bug"},
			BackoffCoefficient:       1,
		},
	}
	we, err := client1.StartWorkflowExecution(host.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	var executions []*commonpb.WorkflowExecution
	dtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*decisionpb.Decision, error) {
		executions = append(executions, execution)
		return []*decisionpb.Decision{
			{
				DecisionType: enumspb.DECISION_TYPE_FAIL_WORKFLOW_EXECUTION,
				Attributes: &decisionpb.Decision_FailWorkflowExecutionDecisionAttributes{FailWorkflowExecutionDecisionAttributes: &decisionpb.FailWorkflowExecutionDecisionAttributes{
					Failure: failure.NewServerFailure("retryable-error", false),
				}},
			}}, nil
	}

	poller2 := host.TaskPoller{
		Engine:          client2,
		Namespace:       namespace,
		TaskQueue:       taskQueue,
		Identity:        identity,
		DecisionHandler: dtHandler,
		Logger:          s.logger,
		T:               s.T(),
	}

	// Update namespace to fail over
	updateReq := &workflowservice.UpdateNamespaceRequest{
		Name: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterName[1],
		},
	}
	updateResp, err := client1.UpdateNamespace(host.NewContext(), updateReq)
	s.NoError(err)
	s.NotNil(updateResp)
	s.Equal(clusterName[1], updateResp.ReplicationConfig.GetActiveClusterName())
	s.Equal(int64(1), updateResp.GetFailoverVersion())

	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	// First attempt
	_, err = poller2.PollAndProcessWorkflowTask(false, false)
	s.NoError(err)
	events := s.getHistory(client2, namespace, executions[0])
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW, events[len(events)-1].GetEventType())
	s.Equal(int32(0), events[0].GetWorkflowExecutionStartedEventAttributes().GetAttempt())

	// second attempt
	_, err = poller2.PollAndProcessWorkflowTask(false, false)
	s.NoError(err)
	events = s.getHistory(client2, namespace, executions[1])
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW, events[len(events)-1].GetEventType())
	s.Equal(int32(1), events[0].GetWorkflowExecutionStartedEventAttributes().GetAttempt())

	// third attempt. Still failing, should stop retry.
	_, err = poller2.PollAndProcessWorkflowTask(false, false)
	s.NoError(err)
	events = s.getHistory(client2, namespace, executions[2])
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, events[len(events)-1].GetEventType())
	s.Equal(int32(2), events[0].GetWorkflowExecutionStartedEventAttributes().GetAttempt())
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
