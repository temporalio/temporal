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

// need to run xdc tests with race detector off because of ringpop bug causing data race issue

package xdc

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"strconv"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	filterpb "go.temporal.io/api/filter/v1"
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
	"google.golang.org/protobuf/types/known/durationpb"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/worker/migration"
	"go.temporal.io/server/tests"
)

type (
	FunctionalClustersTestSuite struct {
		xdcBaseSuite
	}
)

func TestFuncClustersTestSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(FunctionalClustersTestSuite))
}

func (s *FunctionalClustersTestSuite) SetupSuite() {
	s.setupSuite([]string{"integ_active", "integ_standby"})
}

func (s *FunctionalClustersTestSuite) SetupTest() {
	s.setupTest()
}

func (s *FunctionalClustersTestSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *FunctionalClustersTestSuite) decodePayloadsString(ps *commonpb.Payloads) (r string) {
	s.NoError(payloads.Decode(ps, &r))
	return
}

func (s *FunctionalClustersTestSuite) TestNamespaceFailover() {
	namespace := "test-namespace-for-fail-over-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: durationpb.New(7 * time.Hour * 24),
	}
	_, err := client1.RegisterNamespace(tests.NewContext(), regReq)
	s.NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	}
	resp, err := client1.DescribeNamespace(tests.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)

	client2 := s.cluster2.GetFrontendClient() // standby
	resp2, err := client2.DescribeNamespace(tests.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp2)
	s.Equal(resp, resp2)

	s.failover(namespace, s.clusterNames[1], int64(2), client1)

	updated := false
	var resp3 *workflowservice.DescribeNamespaceResponse
	for i := 0; i < 30; i++ {
		resp3, err = client2.DescribeNamespace(tests.NewContext(), descReq)
		s.NoError(err)
		if resp3.ReplicationConfig.GetActiveClusterName() == s.clusterNames[1] {
			updated = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	s.True(updated)
	s.NotNil(resp3)
	s.Equal(int64(2), resp3.GetFailoverVersion())

	// start workflow in new cluster
	id := "functional-namespace-failover-test"
	wt := "functional-namespace-failover-test-type"
	tq := "functional-namespace-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}
	var we *workflowservice.StartWorkflowExecutionResponse
	for i := 0; i < 30; i++ {
		we, err = client2.StartWorkflowExecution(tests.NewContext(), startReq)
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	s.NoError(err)
	s.NotNil(we.GetRunId())
}

func (s *FunctionalClustersTestSuite) TestSimpleWorkflowFailover() {
	namespaceName := "test-simple-workflow-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespaceName,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: durationpb.New(1 * time.Hour * 24),
	}
	_, err := client1.RegisterNamespace(tests.NewContext(), regReq)
	s.NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Namespace: namespaceName,
	}
	resp, err := client1.DescribeNamespace(tests.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)

	client2 := s.cluster2.GetFrontendClient() // standby
	resp2, err := client2.DescribeNamespace(tests.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp2)
	s.Equal(resp, resp2)

	// start a workflow
	id := "functional-simple-workflow-failover-test"
	wt := "functional-simple-workflow-failover-test-type"
	tq := "functional-simple-workflow-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           namespaceName,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}
	we, err := client1.StartWorkflowExecution(tests.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())
	rid := we.GetRunId()

	s.logger.Info("StartWorkflowExecution \n", tag.WorkflowRunID(we.GetRunId()))

	workflowComplete := false
	activityName := "activity_type1"
	activityCount := int32(1)
	activityCounter := int32(0)
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(30 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(20 * time.Second),
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

	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {

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

	poller := tests.TaskPoller{
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

	poller2 := tests.TaskPoller{
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
	_, err = poller.PollAndProcessWorkflowTask()
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	type QueryResult struct {
		Resp *workflowservice.QueryWorkflowResponse
		Err  error
	}
	queryResultCh := make(chan QueryResult)
	queryWorkflowFn := func(client workflowservice.WorkflowServiceClient, queryType string) {
		queryResp, err := client.QueryWorkflow(tests.NewContext(), &workflowservice.QueryWorkflowRequest{
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
		res, errInner := poller.PollAndProcessWorkflowTask()
		s.logger.Info("PollAndProcessQueryTask", tag.Error(err))
		s.NoError(errInner)
		if res.IsQueryTask {
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
		res, errInner := poller2.PollAndProcessWorkflowTask()
		s.logger.Info("PollAndProcessQueryTask", tag.Error(err))
		s.NoError(errInner)
		if res.IsQueryTask {
			break
		}
	}
	// wait until query result is ready
	queryResult = <-queryResultCh
	s.NoError(queryResult.Err)
	s.NotNil(queryResult.Resp)
	s.NotNil(queryResult.Resp.QueryResult)
	s.Equal("query-result", s.decodePayloadsString(queryResult.Resp.GetQueryResult()))

	s.failover(namespaceName, s.clusterNames[1], int64(2), client1)

	// check history matched
	getHistoryReq := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: namespaceName,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      rid,
		},
	}
	eventsReplicated := false
	for i := 0; i < 15; i++ {
		var historyResponse *workflowservice.GetWorkflowExecutionHistoryResponse
		historyResponse, err = client2.GetWorkflowExecutionHistory(tests.NewContext(), getHistoryReq)
		if err == nil && len(historyResponse.History.Events) == 5 {
			eventsReplicated = true
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled`, historyResponse.History)
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
		res, errInner := poller.PollAndProcessWorkflowTask()
		s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(errInner)
		if res.IsQueryTask {
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
		res, errInner := poller2.PollAndProcessWorkflowTask()
		s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(errInner)
		if res.IsQueryTask {
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
	_, err = poller2.PollAndProcessWorkflowTask()
	s.logger.Info("PollAndProcessWorkflowTask 2", tag.Error(err))
	s.NoError(err)
	s.True(workflowComplete)

	// check history replicated in cluster 1
	eventsReplicated = false
	for i := 0; i < 15; i++ {
		var historyResponse *workflowservice.GetWorkflowExecutionHistoryResponse
		historyResponse, err = client1.GetWorkflowExecutionHistory(tests.NewContext(), getHistoryReq)
		if err == nil && len(historyResponse.History.Events) == 11 {
			eventsReplicated = true
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 ActivityTaskStarted
  7 ActivityTaskCompleted
  8 WorkflowTaskScheduled
  9 WorkflowTaskStarted
 10 WorkflowTaskCompleted
 11 WorkflowExecutionCompleted`, historyResponse.History)
			break
		}
		time.Sleep(1 * time.Second)
	}
	s.NoError(err)
	s.True(eventsReplicated)
}

func (s *FunctionalClustersTestSuite) TestStickyWorkflowTaskFailover() {
	namespace := "test-sticky-workflow-task-workflow-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: durationpb.New(1 * time.Hour * 24),
	}
	_, err := client1.RegisterNamespace(tests.NewContext(), regReq)
	s.NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	}
	resp, err := client1.DescribeNamespace(tests.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)

	client2 := s.cluster2.GetFrontendClient() // standby

	// Start a workflow
	id := "functional-sticky-workflow-task-workflow-failover-test"
	wt := "functional-sticky-workflow-task-workflow-failover-test-type"
	tq := "functional-sticky-workflow-task-workflow-failover-test-taskqueue"
	stq1 := "functional-sticky-workflow-task-workflow-failover-test-taskqueue-sticky1"
	stq2 := "functional-sticky-workflow-task-workflow-failover-test-taskqueue-sticky2"
	identity1 := "worker1"
	identity2 := "worker2"

	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	stickyTaskQueue1 := &taskqueuepb.TaskQueue{Name: stq1, Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: tq}
	stickyTaskQueue2 := &taskqueuepb.TaskQueue{Name: stq2, Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: tq}
	stickyTaskTimeout := 100 * time.Second
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(2592000 * time.Second),
		WorkflowTaskTimeout: durationpb.New(60 * time.Second),
		Identity:            identity1,
	}
	we, err := client1.StartWorkflowExecution(tests.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	s.logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

	firstCommandMade := false
	secondCommandMade := false
	workflowCompleted := false
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
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

	poller1 := &tests.TaskPoller{
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

	poller2 := &tests.TaskPoller{
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

	_, err = poller1.PollAndProcessWorkflowTask(tests.WithRespondSticky)
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(firstCommandMade)

	// Send a signal in cluster
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	_, err = client1.SignalWorkflowExecution(tests.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
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

	s.failover(namespace, s.clusterNames[1], int64(2), client1)

	_, err = poller2.PollAndProcessWorkflowTask(tests.WithRespondSticky)
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(secondCommandMade)

	_, err = client2.SignalWorkflowExecution(tests.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
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

	s.failover(namespace, s.clusterNames[0], int64(11), client2)

	_, err = poller1.PollAndProcessWorkflowTask()
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(workflowCompleted)
}

func (s *FunctionalClustersTestSuite) TestStartWorkflowExecution_Failover_WorkflowIDReusePolicy() {
	namespaceName := "test-start-workflow-failover-ID-reuse-policy" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespaceName,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: durationpb.New(1 * time.Hour * 24),
	}
	_, err := client1.RegisterNamespace(tests.NewContext(), regReq)
	s.NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Namespace: namespaceName,
	}
	resp, err := client1.DescribeNamespace(tests.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)

	client2 := s.cluster2.GetFrontendClient() // standby
	resp2, err := client2.DescribeNamespace(tests.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp2)
	s.Equal(resp, resp2)

	// start a workflow
	id := "functional-start-workflow-failover-ID-reuse-policy-test"
	wt := "functional-start-workflow-failover-ID-reuse-policy-test-type"
	tl := "functional-start-workflow-failover-ID-reuse-policy-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:             uuid.New(),
		Namespace:             namespaceName,
		WorkflowId:            id,
		WorkflowType:          workflowType,
		TaskQueue:             taskQueue,
		Input:                 nil,
		WorkflowRunTimeout:    durationpb.New(100 * time.Second),
		WorkflowTaskTimeout:   durationpb.New(1 * time.Second),
		Identity:              identity,
		WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}
	we, err := client1.StartWorkflowExecution(tests.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())
	s.logger.Info("StartWorkflowExecution in cluster 1: ", tag.WorkflowRunID(we.GetRunId()))

	workflowCompleteTimes := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		workflowCompleteTimes++
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller := tests.TaskPoller{
		Engine:              client1,
		Namespace:           namespaceName,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.logger,
		T:                   s.T(),
	}

	poller2 := tests.TaskPoller{
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
	_, err = poller.PollAndProcessWorkflowTask()
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.Equal(1, workflowCompleteTimes)

	s.failover(namespaceName, s.clusterNames[1], int64(2), client1)

	// start the same workflow in cluster 2 is not allowed if policy is AllowDuplicateFailedOnly
	startReq.RequestId = uuid.New()
	startReq.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY
	we, err = client2.StartWorkflowExecution(tests.NewContext(), startReq)
	s.IsType(&serviceerror.WorkflowExecutionAlreadyStarted{}, err)
	s.Nil(we)

	// start the same workflow in cluster 2 is not allowed if policy is RejectDuplicate
	startReq.RequestId = uuid.New()
	startReq.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
	we, err = client2.StartWorkflowExecution(tests.NewContext(), startReq)
	s.IsType(&serviceerror.WorkflowExecutionAlreadyStarted{}, err)
	s.Nil(we)

	// start the workflow in cluster 2
	startReq.RequestId = uuid.New()
	startReq.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	we, err = client2.StartWorkflowExecution(tests.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())
	s.logger.Info("StartWorkflowExecution in cluster 2: ", tag.WorkflowRunID(we.GetRunId()))

	_, err = poller2.PollAndProcessWorkflowTask()
	s.logger.Info("PollAndProcessWorkflowTask 2", tag.Error(err))
	s.NoError(err)
	s.Equal(2, workflowCompleteTimes)
}

func (s *FunctionalClustersTestSuite) TestTerminateFailover() {
	namespace := "test-terminate-workflow-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: durationpb.New(1 * time.Hour * 24),
	}
	_, err := client1.RegisterNamespace(tests.NewContext(), regReq)
	s.NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	}
	resp, err := client1.DescribeNamespace(tests.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)

	client2 := s.cluster2.GetFrontendClient() // standby

	// start a workflow
	id := "functional-terminate-workflow-failover-test"
	wt := "functional-terminate-workflow-failover-test-type"
	tl := "functional-terminate-workflow-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}
	we, err := client1.StartWorkflowExecution(tests.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	activityName := "activity_type1"
	activityCount := int32(1)
	activityCounter := int32(0)
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
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

	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {

		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &tests.TaskPoller{
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
	_, err = poller.PollAndProcessWorkflowTask()
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.failover(namespace, s.clusterNames[1], int64(2), client1)

	// terminate workflow at cluster 2
	terminateReason := "terminate reason"
	terminateDetails := payloads.EncodeString("terminate details")
	_, err = client2.TerminateWorkflowExecution(tests.NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
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
		historyResponse, err := client2.GetWorkflowExecutionHistory(tests.NewContext(), getHistoryReq)
		s.NoError(err)
		history := historyResponse.History

		lastEvent := history.Events[len(history.Events)-1]
		if lastEvent.EventType != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED {
			s.logger.Warn("Execution not terminated yet")
			time.Sleep(100 * time.Millisecond)
			continue GetHistoryLoop
		}

		s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 ActivityTaskTimedOut
  7 WorkflowTaskScheduled
  8 WorkflowExecutionTerminated  {"Details":{"Payloads":[{"Data":"\"terminate details\""}]},"Identity":"worker1","Reason":"terminate reason"}`, history)

		executionTerminated = true
		break GetHistoryLoop
	}
	s.True(executionTerminated)

	// check history replicated to the other cluster
	var historyResponse *workflowservice.GetWorkflowExecutionHistoryResponse
	eventsReplicated := false
GetHistoryLoop2:
	for i := 0; i < 15; i++ {
		historyResponse, err = client1.GetWorkflowExecutionHistory(tests.NewContext(), getHistoryReq)
		if err == nil {
			history := historyResponse.History
			lastEvent := history.Events[len(history.Events)-1]
			if lastEvent.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED {
				s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 ActivityTaskTimedOut
  7 WorkflowTaskScheduled
  8 WorkflowExecutionTerminated  {"Details":{"Payloads":[{"Data":"\"terminate details\""}]},"Identity":"worker1","Reason":"terminate reason"}`, history)
				eventsReplicated = true
				break GetHistoryLoop2
			}
		}
		time.Sleep(1 * time.Second)
	}
	s.NoError(err)
	s.True(eventsReplicated)
}

func (s *FunctionalClustersTestSuite) TestResetWorkflowFailover() {
	namespace := "test-reset-workflow-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: durationpb.New(1 * time.Hour * 24),
	}
	_, err := client1.RegisterNamespace(tests.NewContext(), regReq)
	s.NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	}
	resp, err := client1.DescribeNamespace(tests.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)

	client2 := s.cluster2.GetFrontendClient() // standby

	// start a workflow
	id := "functional-reset-workflow-failover-test"
	wt := "functional-reset-workflow-failover-test-type"
	tl := "functional-reset-workflow-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}
	we, err := client1.StartWorkflowExecution(tests.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	_, err = client1.SignalWorkflowExecution(tests.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
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
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

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

	poller := tests.TaskPoller{
		Engine:              client1,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.logger,
		T:                   s.T(),
	}

	poller2 := tests.TaskPoller{
		Engine:              client2,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.logger,
		T:                   s.T(),
	}

	_, err = poller.PollAndProcessWorkflowTask()
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// events layout
	//  1. WorkflowExecutionStarted
	//  2. WorkflowTaskScheduled
	//  3. WorkflowExecutionSignaled
	//  4. WorkflowTaskStarted
	//  5. WorkflowTaskCompleted

	// Reset workflow execution
	resetResp, err := client1.ResetWorkflowExecution(tests.NewContext(), &workflowservice.ResetWorkflowExecutionRequest{
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

	s.failover(namespace, s.clusterNames[1], int64(2), client1)

	_, err = poller2.PollAndProcessWorkflowTask()
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

	getHistoryResp, err := client1.GetWorkflowExecutionHistory(tests.NewContext(), getHistoryReq)
	s.NoError(err)
	s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowExecutionSignaled
  4 WorkflowTaskStarted
  5 WorkflowTaskFailed
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted
  9 WorkflowExecutionCompleted`, getHistoryResp.History)

	getHistoryResp, err = client2.GetWorkflowExecutionHistory(tests.NewContext(), getHistoryReq)
	s.NoError(err)
	s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowExecutionSignaled
  4 WorkflowTaskStarted
  5 WorkflowTaskFailed
  6 WorkflowTaskScheduled
  7 WorkflowTaskStarted
  8 WorkflowTaskCompleted
  9 WorkflowExecutionCompleted`, getHistoryResp.History)
}

func (s *FunctionalClustersTestSuite) TestContinueAsNewFailover() {
	namespace := "test-continueAsNew-workflow-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: durationpb.New(1 * time.Hour * 24),
	}
	_, err := client1.RegisterNamespace(tests.NewContext(), regReq)
	s.NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	}
	resp, err := client1.DescribeNamespace(tests.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)

	client2 := s.cluster2.GetFrontendClient() // standby

	// start a workflow
	id := "functional-continueAsNew-workflow-failover-test"
	wt := "functional-continueAsNew-workflow-failover-test-type"
	tl := "functional-continueAsNew-workflow-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}
	we, err := client1.StartWorkflowExecution(tests.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	workflowComplete := false
	continueAsNewCount := int32(5)
	continueAsNewCounter := int32(0)
	var previousRunID string
	var lastRunStartedEvent *historypb.HistoryEvent
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if continueAsNewCounter < continueAsNewCount {
			previousRunID = task.WorkflowExecution.GetRunId()
			continueAsNewCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, continueAsNewCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
					WorkflowType:        workflowType,
					TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:               payloads.EncodeBytes(buf.Bytes()),
					WorkflowRunTimeout:  durationpb.New(100 * time.Second),
					WorkflowTaskTimeout: durationpb.New(10 * time.Second),
				}},
			}}, nil
		}

		lastRunStartedEvent = task.History.Events[0]
		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller := &tests.TaskPoller{
		Engine:              client1,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	poller2 := tests.TaskPoller{
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
		_, err := poller.PollAndProcessWorkflowTask()
		s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err, strconv.Itoa(i))
	}

	s.failover(namespace, s.clusterNames[1], int64(2), client1)

	// finish the rest in cluster 2
	for i := 0; i < 2; i++ {
		_, err := poller2.PollAndProcessWorkflowTask()
		s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err, strconv.Itoa(i))
	}

	s.False(workflowComplete)
	_, err = poller2.PollAndProcessWorkflowTask()
	s.NoError(err)
	s.True(workflowComplete)
	s.Equal(previousRunID, lastRunStartedEvent.GetWorkflowExecutionStartedEventAttributes().GetContinuedExecutionRunId())
}

func (s *FunctionalClustersTestSuite) TestSignalFailover() {
	namespace := "test-signal-workflow-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: durationpb.New(1 * time.Hour * 24),
	}
	_, err := client1.RegisterNamespace(tests.NewContext(), regReq)
	s.NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	}
	resp, err := client1.DescribeNamespace(tests.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)

	client2 := s.cluster2.GetFrontendClient() // standby

	// Start a workflow
	id := "functional-signal-workflow-failover-test"
	wt := "functional-signal-workflow-failover-test-type"
	tl := "functional-signal-workflow-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(300 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}
	we, err := client1.StartWorkflowExecution(tests.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	s.logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

	eventSignaled := false
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if task.PreviousStartedEventId == 0 {
			return []*commandpb.Command{}, nil
		}
		if !eventSignaled {
			for _, event := range task.History.Events[task.PreviousStartedEventId:] {
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

	poller := &tests.TaskPoller{
		Engine:              client1,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	poller2 := &tests.TaskPoller{
		Engine:              client2,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// Process start event in cluster 1
	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	s.False(eventSignaled)

	// Send a signal without a task in cluster 1
	_, err = client1.SignalWorkflowExecution(tests.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.GetRunId(),
		},
		SignalName:               "signal without task",
		Input:                    payloads.EncodeString("my signal input without task"),
		Identity:                 identity,
		SkipGenerateWorkflowTask: true,
	})
	s.NoError(err)

	// Send a signal in cluster 1
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	_, err = client1.SignalWorkflowExecution(tests.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
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
	_, err = poller.PollAndProcessWorkflowTask()
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(eventSignaled)

	s.failover(namespace, s.clusterNames[1], int64(2), client1)

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
		historyResponse, err = client2.GetWorkflowExecutionHistory(tests.NewContext(), getHistoryReq)
		if err == nil && len(historyResponse.History.Events) == 9 {
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionSignaled
  6 WorkflowExecutionSignaled
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted`, historyResponse.History)
			eventsReplicated = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	s.NoError(err)
	s.True(eventsReplicated)

	// Send another signal without a task in cluster 2
	_, err = client2.SignalWorkflowExecution(tests.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.GetRunId(),
		},
		SignalName:               "signal without task",
		Input:                    payloads.EncodeString("my signal input without task"),
		Identity:                 identity,
		SkipGenerateWorkflowTask: true,
	})
	s.NoError(err)

	// Send another signal in cluster 2
	signalName2 := "my signal 2"
	signalInput2 := payloads.EncodeString("my signal input 2")
	_, err = client2.SignalWorkflowExecution(tests.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
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
	_, err = poller2.PollAndProcessWorkflowTask()
	s.logger.Info("PollAndProcessWorkflowTask 2", tag.Error(err))
	s.NoError(err)
	s.True(eventSignaled)

	// check history matched
	eventsReplicated = false
	for i := 0; i < 15; i++ {
		historyResponse, err = client2.GetWorkflowExecutionHistory(tests.NewContext(), getHistoryReq)
		if err == nil && len(historyResponse.History.Events) == 14 {
			s.EqualHistory(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionSignaled
  6 WorkflowExecutionSignaled
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted
 10 WorkflowExecutionSignaled
 11 WorkflowExecutionSignaled
 12 WorkflowTaskScheduled
 13 WorkflowTaskStarted
 14 WorkflowTaskCompleted`, historyResponse.History)
			eventsReplicated = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	s.NoError(err)
	s.True(eventsReplicated)
}

func (s *FunctionalClustersTestSuite) TestUserTimerFailover() {
	namespace := "test-user-timer-workflow-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: durationpb.New(1 * time.Hour * 24),
	}
	_, err := client1.RegisterNamespace(tests.NewContext(), regReq)
	s.NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	}
	resp, err := client1.DescribeNamespace(tests.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)

	client2 := s.cluster2.GetFrontendClient() // standby

	// Start a workflow
	id := "functional-user-timer-workflow-failover-test"
	wt := "functional-user-timer-workflow-failover-test-type"
	tl := "functional-user-timer-workflow-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(300 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}
	var we *workflowservice.StartWorkflowExecutionResponse
	for i := 0; i < 10; i++ {
		we, err = client1.StartWorkflowExecution(tests.NewContext(), startReq)
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
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		if !timerCreated {
			timerCreated = true

			// Send a signal in cluster
			signalName := "my signal"
			signalInput := payloads.EncodeString("my signal input")
			_, err = client1.SignalWorkflowExecution(tests.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
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
					StartToFireTimeout: durationpb.New(2 * time.Second),
				}},
			}}, nil
		}

		if !timerFired {
			resp, err := client2.GetWorkflowExecutionHistory(tests.NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
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

	poller1 := &tests.TaskPoller{
		Engine:              client1,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	poller2 := &tests.TaskPoller{
		Engine:              client2,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	for i := 0; i < 2; i++ {
		_, err = poller1.PollAndProcessWorkflowTask()
		if err != nil {
			timerCreated = false
			continue
		}
		if timerCreated {
			break
		}
	}
	s.True(timerCreated)

	s.failover(namespace, s.clusterNames[1], int64(2), client1)

	for i := 1; i < 20; i++ {
		if !workflowCompleted {
			_, err = poller2.PollAndProcessWorkflowTask()
			s.NoError(err)
			time.Sleep(time.Second)
		}
	}
}

func (s *FunctionalClustersTestSuite) TestForceWorkflowTaskClose_WithClusterReconnect() {
	namespace := "test-force-workflow-task-close-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: durationpb.New(1 * time.Hour * 24),
	}
	_, err := client1.RegisterNamespace(tests.NewContext(), regReq)
	s.NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	}
	resp, err := client1.DescribeNamespace(tests.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)

	client2 := s.cluster2.GetFrontendClient() // standby

	// Start a workflow
	id := "test-force-workflow-task-close-test"
	wt := "test-force-workflow-task-close-test-type"
	tl := "test-force-workflow-task-close-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(300 * time.Second),
		WorkflowTaskTimeout: durationpb.New(60 * time.Second),
		Identity:            identity,
	}
	var we *workflowservice.StartWorkflowExecutionResponse
	for i := 0; i < 10; i++ {
		we, err = client1.StartWorkflowExecution(tests.NewContext(), startReq)
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	s.NoError(err)
	s.NotNil(we.GetRunId())

	s.logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller1 := &tests.TaskPoller{
		Engine:              client1,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// this will fail the workflow task
	_, err = poller1.PollAndProcessWorkflowTask(tests.WithDropTask)
	s.NoError(err)

	s.failover(namespace, s.clusterNames[1], int64(2), client1)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	// Update the namespace in cluster 2 to be a single cluster namespace
	upReq := &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			Clusters: []*replicationpb.ClusterReplicationConfig{
				{
					ClusterName: s.clusterNames[1],
				},
			},
		},
	}
	_, err = client2.UpdateNamespace(tests.NewContext(), upReq)
	s.NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	// Send a signal to cluster 2, namespace contains one cluster
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	_, err = client2.SignalWorkflowExecution(tests.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.GetRunId(),
		},
		SignalName: signalName,
		Input:      signalInput,
	})
	s.NoError(err)

	// No error is expected with single cluster namespace.
	_, err = client2.DescribeWorkflowExecution(tests.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
	})
	s.NoError(err)

	// Update the namespace in cluster 2 to be a multi cluster namespace
	upReq2 := &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			Clusters: []*replicationpb.ClusterReplicationConfig{
				{
					ClusterName: s.clusterNames[1],
				},
				{
					ClusterName: s.clusterNames[0],
				},
			},
		},
	}
	_, err = client2.UpdateNamespace(tests.NewContext(), upReq2)
	s.NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	// No error is expected with multi cluster namespace.
	_, err = client2.DescribeWorkflowExecution(tests.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
	})
	s.NoError(err)
}

func (s *FunctionalClustersTestSuite) TestTransientWorkflowTaskFailover() {
	namespace := "test-transient-workflow-task-workflow-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: durationpb.New(1 * time.Hour * 24),
	}
	_, err := client1.RegisterNamespace(tests.NewContext(), regReq)
	s.NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	}
	resp, err := client1.DescribeNamespace(tests.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)

	client2 := s.cluster2.GetFrontendClient() // standby

	// Start a workflow
	id := "functional-transient-workflow-task-workflow-failover-test"
	wt := "functional-transient-workflow-task-workflow-failover-test-type"
	tl := "functional-transient-workflow-task-workflow-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(300 * time.Second),
		WorkflowTaskTimeout: durationpb.New(8 * time.Second),
		Identity:            identity,
	}
	var we *workflowservice.StartWorkflowExecutionResponse
	for i := 0; i < 10; i++ {
		we, err = client1.StartWorkflowExecution(tests.NewContext(), startReq)
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
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
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

	poller1 := &tests.TaskPoller{
		Engine:              client1,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	poller2 := &tests.TaskPoller{
		Engine:              client2,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// this will fail the workflow task
	_, err = poller1.PollAndProcessWorkflowTask()
	s.NoError(err)

	s.failover(namespace, s.clusterNames[1], int64(2), client1)

	// for failover transient workflow task, it is guaranteed that the transient workflow task
	// after the failover has attempt 1
	// for details see ApplyTransientWorkflowTaskScheduled
	_, err = poller2.PollAndProcessWorkflowTask(tests.WithExpectedAttemptCount(1))
	s.NoError(err)
	s.True(workflowFinished)
}

func (s *FunctionalClustersTestSuite) TestCronWorkflowStartAndFailover() {
	namespace := "test-cron-workflow-start-and-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: durationpb.New(1 * time.Hour * 24),
	}
	_, err := client1.RegisterNamespace(tests.NewContext(), regReq)
	s.NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	}
	resp, err := client1.DescribeNamespace(tests.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)

	client2 := s.cluster2.GetFrontendClient() // standby

	// start a workflow
	id := "functional-cron-workflow-start-and-failover-test"
	wt := "functional-cron-workflow-start-and-failover-test-type"
	tl := "functional-cron-workflow-start-and-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
		CronSchedule:        "@every 5s",
	}
	we, err := client1.StartWorkflowExecution(tests.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	wfCompleted := false
	var executions []*commonpb.WorkflowExecution
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		executions = append(executions, task.WorkflowExecution)
		wfCompleted = true
		return []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("cron-test-result"),
				}},
			}}, nil
	}

	poller2 := tests.TaskPoller{
		Engine:              client2,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	s.failover(namespace, s.clusterNames[1], int64(2), client1)

	_, err = poller2.PollAndProcessWorkflowTask()
	s.NoError(err)
	s.True(wfCompleted)
	events := s.getHistory(client2, namespace, executions[0])
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionCompleted`, events)
	s.Equal(int64(2), events[len(events)-1].GetVersion())

	// terminate the remaining cron
	_, err = client2.TerminateWorkflowExecution(tests.NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
	})
	s.NoError(err)
}

func (s *FunctionalClustersTestSuite) TestCronWorkflowCompleteAndFailover() {
	namespace := "test-cron-workflow-complete-and-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: durationpb.New(1 * time.Hour * 24),
	}
	_, err := client1.RegisterNamespace(tests.NewContext(), regReq)
	s.NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	}
	resp, err := client1.DescribeNamespace(tests.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)

	client2 := s.cluster2.GetFrontendClient() // standby

	// start a workflow
	id := "functional-cron-workflow-complete-andfailover-test"
	wt := "functional-cron-workflow-complete-andfailover-test-type"
	tl := "functional-cron-workflow-complete-andfailover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
		CronSchedule:        "@every 5s",
	}
	we, err := client1.StartWorkflowExecution(tests.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	wfCompletionCount := 0
	var executions []*commonpb.WorkflowExecution
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wfCompletionCount += 1
		executions = append(executions, task.WorkflowExecution)
		return []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("cron-test-result"),
				}},
			}}, nil
	}

	poller1 := tests.TaskPoller{
		Engine:              client1,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	poller2 := tests.TaskPoller{
		Engine:              client2,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	_, err = poller1.PollAndProcessWorkflowTask()
	s.NoError(err)
	s.Equal(1, wfCompletionCount)
	events := s.getHistory(client1, namespace, executions[0])
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionCompleted`, events)

	s.Equal(int64(1), events[0].GetVersion())
	s.Equal(int64(1), events[len(events)-1].GetVersion())

	s.failover(namespace, s.clusterNames[1], int64(2), client1)

	_, err = poller2.PollAndProcessWorkflowTask()
	s.NoError(err)
	s.Equal(2, wfCompletionCount)
	events = s.getHistory(client2, namespace, executions[1])
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionCompleted`, events)
	s.Equal(int64(1), events[0].GetVersion())
	s.Equal(int64(2), events[len(events)-1].GetVersion())

	_, err = client2.TerminateWorkflowExecution(tests.NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
	})
	s.NoError(err)
}

func (s *FunctionalClustersTestSuite) TestWorkflowRetryStartAndFailover() {
	namespace := "test-workflow-retry-start-and-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: durationpb.New(1 * time.Hour * 24),
	}
	_, err := client1.RegisterNamespace(tests.NewContext(), regReq)
	s.NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	}
	resp, err := client1.DescribeNamespace(tests.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)

	client2 := s.cluster2.GetFrontendClient() // standby

	// start a workflow
	id := "functional-workflow-retry-start-and-failover-test"
	wt := "functional-workflow-retry-start-and-failover-test-type"
	tl := "functional-workflow-retry-start-and-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:        durationpb.New(1 * time.Second),
			MaximumAttempts:        3,
			MaximumInterval:        durationpb.New(1 * time.Second),
			NonRetryableErrorTypes: []string{"bad-bug"},
			BackoffCoefficient:     1,
		},
	}
	we, err := client1.StartWorkflowExecution(tests.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	var executions []*commonpb.WorkflowExecution
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		executions = append(executions, task.WorkflowExecution)
		return []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
					Failure: failure.NewServerFailure("retryable-error", false),
				}},
			}}, nil
	}

	poller2 := tests.TaskPoller{
		Engine:              client2,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	s.failover(namespace, s.clusterNames[1], int64(2), client1)

	// First attempt
	_, err = poller2.PollAndProcessWorkflowTask()
	s.NoError(err)
	events := s.getHistory(client2, namespace, executions[0])
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted {"Attempt":1}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionFailed`, events)
	s.Equal(int64(1), events[0].GetVersion())
	s.Equal(int64(2), events[len(events)-1].GetVersion())

	// second attempt
	_, err = poller2.PollAndProcessWorkflowTask()
	s.NoError(err)
	events = s.getHistory(client2, namespace, executions[1])
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted {"Attempt":2}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionFailed`, events)
	s.Equal(int64(2), events[0].GetVersion())
	s.Equal(int64(2), events[len(events)-1].GetVersion())
}

func (s *FunctionalClustersTestSuite) TestWorkflowRetryFailAndFailover() {
	namespace := "test-workflow-retry-fail-and-failover-" + common.GenerateRandomString(5)
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: durationpb.New(1 * time.Hour * 24),
	}
	_, err := client1.RegisterNamespace(tests.NewContext(), regReq)
	s.NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	}
	resp, err := client1.DescribeNamespace(tests.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)

	client2 := s.cluster2.GetFrontendClient() // standby

	// start a workflow
	id := "functional-workflow-retry-fail-and-failover-test"
	wt := "functional-workflow-retry-fail-and-failover-test-type"
	tl := "functional-workflow-retry-fail-and-failover-test-taskqueue"
	identity := "worker1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:        durationpb.New(1 * time.Second),
			MaximumAttempts:        3,
			MaximumInterval:        durationpb.New(1 * time.Second),
			NonRetryableErrorTypes: []string{"bad-bug"},
			BackoffCoefficient:     1,
		},
	}
	we, err := client1.StartWorkflowExecution(tests.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	var executions []*commonpb.WorkflowExecution
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		executions = append(executions, task.WorkflowExecution)
		return []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
					Failure: failure.NewServerFailure("retryable-error", false),
				}},
			}}, nil
	}

	poller1 := tests.TaskPoller{
		Engine:              client1,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	poller2 := tests.TaskPoller{
		Engine:              client2,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	_, err = poller1.PollAndProcessWorkflowTask()
	s.NoError(err)
	events := s.getHistory(client1, namespace, executions[0])
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted {"Attempt":1}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionFailed`, events)

	s.Equal(int64(1), events[0].GetVersion())
	s.Equal(int64(1), events[len(events)-1].GetVersion())

	s.failover(namespace, s.clusterNames[1], int64(2), client1)

	_, err = poller2.PollAndProcessWorkflowTask()
	s.NoError(err)
	events = s.getHistory(client2, namespace, executions[1])
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted {"Attempt":2}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionFailed`, events)
	s.Equal(int64(1), events[0].GetVersion())
	s.Equal(int64(2), events[len(events)-1].GetVersion())
}

func (s *FunctionalClustersTestSuite) TestActivityHeartbeatFailover() {
	namespace := "test-activity-heartbeat-workflow-failover-" + common.GenerateRandomString(5)
	s.registerNamespace(namespace, true)

	taskqueue := "functional-activity-heartbeat-workflow-failover-test-taskqueue"
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
		return workflow.ExecuteActivity(ctx, activityWithHB).Get(ctx, nil)
	}
	worker1.RegisterWorkflow(testWorkflowFn)
	worker1.RegisterActivity(activityWithHB)
	s.NoError(worker1.Start())

	// Start a workflow
	startTime := time.Now()
	workflowID := "functional-activity-heartbeat-workflow-failover-test"
	run1, err := client1.ExecuteWorkflow(tests.NewContext(), sdkclient.StartWorkflowOptions{
		ID:                 workflowID,
		TaskQueue:          taskqueue,
		WorkflowRunTimeout: time.Second * 300,
	}, testWorkflowFn)

	s.NoError(err)
	s.NotEmpty(run1.GetRunID())

	s.logger.Info("StartWorkflowExecution", tag.WorkflowRunID(run1.GetRunID()))
	time.Sleep(time.Second * 4) // wait for heartbeat from activity to be reported and activity timed out on heartbeat

	worker1.Stop() // stop worker1 so cluster 1 won't make any progress
	s.failover(namespace, s.clusterNames[1], int64(2), s.cluster1.GetFrontendClient())

	// verify things are replicated over
	resp, err := s.cluster1.GetHistoryClient().GetReplicationStatus(context.Background(), &historyservice.GetReplicationStatusRequest{})
	s.NoError(err)
	s.Equal(1, len(resp.Shards)) // test cluster has only one history shard
	shard := resp.Shards[0]
	s.True(shard.MaxReplicationTaskId > 0)
	s.NotNil(shard.ShardLocalTime)
	s.True(shard.ShardLocalTime.AsTime().Before(time.Now()))
	s.True(shard.ShardLocalTime.AsTime().After(startTime))
	s.NotNil(shard.RemoteClusters)
	standbyAckInfo, ok := shard.RemoteClusters[s.clusterNames[1]]
	s.True(ok)
	s.LessOrEqual(shard.MaxReplicationTaskId, standbyAckInfo.AckedTaskId)
	s.NotNil(standbyAckInfo.AckedTaskVisibilityTime)
	s.True(standbyAckInfo.AckedTaskVisibilityTime.AsTime().Before(time.Now()))
	s.True(standbyAckInfo.AckedTaskVisibilityTime.AsTime().After(startTime))

	// Make sure the heartbeat details are sent to cluster2 even when the activity at cluster1
	// has heartbeat timeout. Also make sure the information is recorded when the activity state
	// is "Scheduled"
	dweResponse, err := client2.DescribeWorkflowExecution(tests.NewContext(), workflowID, "")
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
	s.NoError(worker2.Start())
	defer worker2.Stop()

	// ExecuteWorkflow return existing running workflow if it already started
	run2, err := client2.ExecuteWorkflow(tests.NewContext(), sdkclient.StartWorkflowOptions{
		ID:                 workflowID,
		TaskQueue:          taskqueue,
		WorkflowRunTimeout: time.Second * 300,
	}, testWorkflowFn)
	s.NoError(err)
	// verify we get the same execution as in cluster1
	s.Equal(run1.GetRunID(), run2.GetRunID())

	err = run2.Get(tests.NewContext(), nil)
	s.NoError(err) // workflow succeed
	s.Equal(2, lastAttemptCount)
}

// Uncomment if you need to debug history.
// func (s *funcClustersTestSuite) printHistory(frontendClient workflowservice.WorkflowServiceClient, namespace, workflowID, runID string) {
// 	events := s.getHistory(frontendClient, namespace, &commonpb.WorkflowExecution{
// 		WorkflowId: workflowID,
// 		RunId:      runID,
// 	})
// 	history := &historypb.History{Events: events}
// 	common.PrettyPrintHistory(history, s.logger)
// }

func (s *FunctionalClustersTestSuite) TestLocalNamespaceMigration() {
	if !tests.UsingSQLAdvancedVisibility() {
		s.T().Skip("Test requires advanced visibility")
	}

	testCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	namespace := "local-ns-to-be-promote-" + common.GenerateRandomString(5)
	s.registerNamespace(namespace, false)

	taskqueue := "functional-local-ns-to-be-promote-taskqueue"
	client1, worker1 := s.newClientAndWorker(s.cluster1.GetHost().FrontendGRPCAddress(), namespace, taskqueue, "worker1")

	testWorkflowFn := func(ctx workflow.Context, sleepInterval time.Duration) error {
		return workflow.Sleep(ctx, sleepInterval)
	}

	worker1.RegisterWorkflow(testWorkflowFn)
	s.NoError(worker1.Start())
	defer worker1.Stop()

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

	// Start wf1 (in local ns)
	workflowID8 := "global-ns-wf-1"
	run8, err := client1.ExecuteWorkflow(testCtx, sdkclient.StartWorkflowOptions{
		ID:                 workflowID8,
		TaskQueue:          taskqueue,
		WorkflowRunTimeout: time.Second * 30,
	}, testWorkflowFn, time.Millisecond*10)

	s.NoError(err)
	s.NotEmpty(run8.GetRunID())
	s.logger.Info("start wf8", tag.WorkflowRunID(run8.GetRunID()))
	// wait until wf1 complete
	err = run8.Get(testCtx, nil)
	s.NoError(err)

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
			Clusters: s.clusterReplicationConfig(),
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
		TaskQueue:          primitives.DefaultWorkerTaskQueue,
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
		TaskQueue:          primitives.DefaultWorkerTaskQueue,
		WorkflowRunTimeout: time.Second * 30,
	}, "namespace-handover", migration.NamespaceHandoverParams{
		Namespace:              namespace,
		RemoteCluster:          s.clusterNames[1],
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
	s.Equal(s.clusterNames[1], nsResp2.ReplicationConfig.ActiveClusterName)

	// verify all wf in ns is now available in cluster2
	client2, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.cluster2.GetHost().FrontendGRPCAddress(),
		Namespace: namespace,
	})
	s.NoError(err)
	feClient2 := s.cluster2.GetFrontendClient()
	verify := func(wfID string, expectedRunID string) {
		desc1, err := client2.DescribeWorkflowExecution(testCtx, wfID, "")
		s.NoError(err)
		s.Equal(expectedRunID, desc1.WorkflowExecutionInfo.Execution.RunId)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, desc1.WorkflowExecutionInfo.Status)
		resp, err := feClient2.GetWorkflowExecutionHistoryReverse(testCtx, &workflowservice.GetWorkflowExecutionHistoryReverseRequest{
			Namespace: namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: wfID,
				RunId:      expectedRunID,
			},
			MaximumPageSize: 1,
			NextPageToken:   nil,
		})
		s.NoError(err)
		s.True(len(resp.GetHistory().GetEvents()) > 0)
		listWorkflowResp, err := feClient2.ListClosedWorkflowExecutions(
			testCtx,
			&workflowservice.ListClosedWorkflowExecutionsRequest{
				Namespace:       namespace,
				MaximumPageSize: 1000,
				Filters: &workflowservice.ListClosedWorkflowExecutionsRequest_ExecutionFilter{
					ExecutionFilter: &filterpb.WorkflowExecutionFilter{
						WorkflowId: wfID,
					}},
			},
		)
		s.NoError(err)
		s.True(len(listWorkflowResp.GetExecutions()) > 0)
	}
	verify(workflowID, run1.GetRunID())
	verify(workflowID2, run2.GetRunID())
	verify(workflowID3, run3.GetRunID())
	verify(workflowID6, run6.GetRunID())
	verify(workflowID7, run7.GetRunID())
}

func (s *FunctionalClustersTestSuite) TestForceMigration_ClosedWorkflow() {
	if !tests.UsingSQLAdvancedVisibility() {
		s.T().Skip("Test requires advanced visibility")
	}

	testCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	namespace := "force-replication" + common.GenerateRandomString(5)
	s.registerNamespace(namespace, true)

	taskqueue := "functional-local-force-replication-task-queue"
	client1, worker1 := s.newClientAndWorker(s.cluster1.GetHost().FrontendGRPCAddress(), namespace, taskqueue, "worker1")

	testWorkflowFn := func(ctx workflow.Context) error {
		return nil
	}

	worker1.RegisterWorkflow(testWorkflowFn)
	s.NoError(worker1.Start())
	defer worker1.Stop()

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
			Clusters: s.clusterReplicationConfig(),
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
		TaskQueue:          primitives.DefaultWorkerTaskQueue,
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
			ActiveClusterName: s.clusterNames[1],
		},
	})
	s.NoError(err)

	time.Sleep(cacheRefreshInterval)

	nsResp, err = frontendClient2.DescribeNamespace(testCtx, &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	})
	s.NoError(err)
	s.Equal(s.clusterNames[1], nsResp.ReplicationConfig.ActiveClusterName)

	worker2.RegisterWorkflow(testWorkflowFn)
	s.NoError(worker2.Start())
	defer worker2.Stop()

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

func (s *FunctionalClustersTestSuite) TestForceMigration_ResetWorkflow() {
	if !tests.UsingSQLAdvancedVisibility() {
		s.T().Skip("Test requires advanced visibility")
	}

	testCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	namespace := "force-replication" + common.GenerateRandomString(5)
	s.registerNamespace(namespace, true)

	taskqueue := "functional-force-replication-reset-task-queue"
	client1, worker1 := s.newClientAndWorker(s.cluster1.GetHost().FrontendGRPCAddress(), namespace, taskqueue, "worker1")

	testWorkflowFn := func(ctx workflow.Context) error {
		return nil
	}

	worker1.RegisterWorkflow(testWorkflowFn)
	s.NoError(worker1.Start())
	defer worker1.Stop()

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
			Clusters: s.clusterReplicationConfig(),
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
		TaskQueue:          primitives.DefaultWorkerTaskQueue,
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

func (s *FunctionalClustersTestSuite) getHistory(client tests.FrontendClient, namespace string, execution *commonpb.WorkflowExecution) []*historypb.HistoryEvent {
	historyResponse, err := client.GetWorkflowExecutionHistory(tests.NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:       namespace,
		Execution:       execution,
		MaximumPageSize: 5, // Use small page size to force pagination code path
	})
	s.NoError(err)

	events := historyResponse.History.Events
	for historyResponse.NextPageToken != nil {
		historyResponse, err = client.GetWorkflowExecutionHistory(tests.NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:     namespace,
			Execution:     execution,
			NextPageToken: historyResponse.NextPageToken,
		})
		s.NoError(err)
		events = append(events, historyResponse.History.Events...)
	}

	return events
}

func (s *FunctionalClustersTestSuite) failover(
	namespace string,
	targetCluster string,
	targetFailoverVersion int64,
	client tests.FrontendClient,
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
	updateResp, err := client.UpdateNamespace(tests.NewContext(), updateReq)
	s.NoError(err)
	s.Equal(targetCluster, updateResp.ReplicationConfig.GetActiveClusterName())
	s.Equal(targetFailoverVersion, updateResp.GetFailoverVersion())

	// wait till failover completed
	time.Sleep(cacheRefreshInterval)
}

func (s *FunctionalClustersTestSuite) registerNamespace(namespace string, isGlobalNamespace bool) {
	clusters := s.clusterReplicationConfig()
	if !isGlobalNamespace {
		clusters = s.clusterReplicationConfig()[0:1]
	}
	client1 := s.cluster1.GetFrontendClient() // active
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                isGlobalNamespace,
		Clusters:                         clusters,
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: durationpb.New(1 * time.Hour * 24),
	}
	_, err := client1.RegisterNamespace(tests.NewContext(), regReq)
	s.NoError(err)
	// Wait for namespace cache to pick the change
	time.Sleep(cacheRefreshInterval)

	descReq := &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	}
	resp, err := client1.DescribeNamespace(tests.NewContext(), descReq)
	s.NoError(err)
	s.NotNil(resp)
	s.Equal(namespace, resp.NamespaceInfo.Name)
	s.Equal(isGlobalNamespace, resp.IsGlobalNamespace)
}

func (s *FunctionalClustersTestSuite) newClientAndWorker(hostport, namespace, taskqueue, identity string) (sdkclient.Client, sdkworker.Worker) {
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
