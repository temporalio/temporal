package xdc

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	filterpb "go.temporal.io/api/filter/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/operatorservice/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/worker/migration"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/fx"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	FunctionalClustersTestSuite struct {
		xdcBaseSuite
	}
	FunctionalClustersWithRedirectionTestSuite struct {
		xdcBaseSuite
	}
)

func TestFuncClustersTestSuite(t *testing.T) {
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
			s := &FunctionalClustersTestSuite{}
			s.enableTransitionHistory = tc.enableTransitionHistory
			suite.Run(t, s)
		})
	}
}

func (s *FunctionalClustersTestSuite) SetupSuite() {
	s.setupSuite()
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
	namespace := s.createGlobalNamespace()

	s.failover(namespace, 0, s.clusters[1].ClusterName(), 2)

	// start workflow in new cluster
	id := "functional-namespace-failover-test"
	wt := "functional-namespace-failover-test-type"
	tq := "functional-namespace-failover-test-taskqueue"
	identity := "worker1"
	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()
	taskQueue := taskqueuepb.TaskQueue_builder{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()
	startReq := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}.Build()
	we, err := s.clusters[1].FrontendClient().StartWorkflowExecution(testcore.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())
}

func (s *FunctionalClustersTestSuite) TestSimpleWorkflowFailover() {
	namespaceName := s.createGlobalNamespace()
	client0 := s.clusters[0].FrontendClient() // active
	client1 := s.clusters[1].FrontendClient() // standby

	// start a workflow
	id := "functional-simple-workflow-failover-test"
	wt := "functional-simple-workflow-failover-test-type"
	tq := "functional-simple-workflow-failover-test-taskqueue"
	identity := "worker1"
	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()
	taskQueue := taskqueuepb.TaskQueue_builder{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()
	startReq := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           namespaceName,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}.Build()
	we, err := client0.StartWorkflowExecution(testcore.NewContext(), startReq)
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

			return []*commandpb.Command{commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				ScheduleActivityTaskCommandAttributes: commandpb.ScheduleActivityTaskCommandAttributes_builder{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           commonpb.ActivityType_builder{Name: activityName}.Build(),
					TaskQueue:              taskqueuepb.TaskQueue_builder{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(30 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(20 * time.Second),
				}.Build(),
			}.Build()}, nil
		}

		workflowComplete = true
		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
				Result: payloads.EncodeString("Done"),
			}.Build(),
		}.Build()}, nil
	}

	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {

		return payloads.EncodeString("Activity Result"), false, nil
	}

	queryType := "test-query"
	queryHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*commonpb.Payloads, error) {
		s.NotNil(task.GetQuery())
		s.NotNil(task.GetQuery().GetQueryType())
		if task.GetQuery().GetQueryType() == queryType {
			return payloads.EncodeString("query-result"), nil
		}

		return nil, errors.New("unknown-query-type")
	}

	// nolint
	poller0 := testcore.TaskPoller{
		Client:              client0,
		Namespace:           namespaceName,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		QueryHandler:        queryHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// nolint
	poller1 := testcore.TaskPoller{
		Client:              client1,
		Namespace:           namespaceName,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		QueryHandler:        queryHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// make some progress in cluster0
	_, err = poller0.PollAndProcessWorkflowTask()
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	type QueryResult struct {
		Resp *workflowservice.QueryWorkflowResponse
		Err  error
	}
	queryResultCh := make(chan QueryResult)
	queryWorkflowFn := func(client workflowservice.WorkflowServiceClient, queryType string) {
		queryResp, err := client.QueryWorkflow(testcore.NewContext(), workflowservice.QueryWorkflowRequest_builder{
			Namespace: namespaceName,
			Execution: commonpb.WorkflowExecution_builder{
				WorkflowId: id,
				RunId:      we.GetRunId(),
			}.Build(),
			Query: querypb.WorkflowQuery_builder{
				QueryType: queryType,
			}.Build(),
		}.Build())
		queryResultCh <- QueryResult{Resp: queryResp, Err: err}
	}

	// call QueryWorkflow in separate goroutinue (because it is blocking). That will generate a query task
	go queryWorkflowFn(client0, queryType)
	// process that query task, which should respond via RespondQueryTaskCompleted
	for {
		// loop until process the query task
		res, errInner := poller0.PollAndProcessWorkflowTask()
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
	s.NotNil(queryResult.Resp.GetQueryResult())
	s.Equal("query-result", s.decodePayloadsString(queryResult.Resp.GetQueryResult()))

	// Wait a while so the events are replicated.
	time.Sleep(5 * time.Second) // nolint:forbidigo

	// call QueryWorkflow in separate goroutinue (because it is blocking). That will generate a query task
	go queryWorkflowFn(client1, queryType)
	// process that query task, which should respond via RespondQueryTaskCompleted
	for {
		// loop until process the query task
		res, errInner := poller1.PollAndProcessWorkflowTask()
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
	s.NotNil(queryResult.Resp.GetQueryResult())
	s.Equal("query-result", s.decodePayloadsString(queryResult.Resp.GetQueryResult()))

	s.failover(namespaceName, 0, s.clusters[1].ClusterName(), 2)

	// check history matched
	getHistoryReq := workflowservice.GetWorkflowExecutionHistoryRequest_builder{
		Namespace: namespaceName,
		Execution: commonpb.WorkflowExecution_builder{
			WorkflowId: id,
			RunId:      rid,
		}.Build(),
	}.Build()
	// TODO (alex): this shouldn't be WaitForHistory anymore (just EqualHistory)
	s.WaitForHistory(`
  1 v1 WorkflowExecutionStarted
  2 v1 WorkflowTaskScheduled
  3 v1 WorkflowTaskStarted
  4 v1 WorkflowTaskCompleted
  5 v1 ActivityTaskScheduled`,
		func() *historypb.History {
			historyResponse, err := client1.GetWorkflowExecutionHistory(testcore.NewContext(), getHistoryReq)
			if err != nil {
				return nil
			}
			return historyResponse.GetHistory()
		}, replicationWaitTime, replicationCheckInterval,
	)

	// Make sure query is still working after failover
	// call QueryWorkflow in separate goroutinue (because it is blocking). That will generate a query task
	go queryWorkflowFn(client0, queryType)
	// process that query task, which should respond via RespondQueryTaskCompleted
	for {
		// loop until process the query task
		res, errInner := poller0.PollAndProcessWorkflowTask()
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
	s.NotNil(queryResult.Resp.GetQueryResult())
	s.Equal("query-result", s.decodePayloadsString(queryResult.Resp.GetQueryResult()))

	// call QueryWorkflow in separate goroutinue (because it is blocking). That will generate a query task
	go queryWorkflowFn(client1, queryType)
	// process that query task, which should respond via RespondQueryTaskCompleted
	for {
		// loop until process the query task
		res, errInner := poller1.PollAndProcessWorkflowTask()
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
	s.NotNil(queryResult.Resp.GetQueryResult())
	s.Equal("query-result", s.decodePayloadsString(queryResult.Resp.GetQueryResult()))

	// make process in cluster1
	err = poller1.PollAndProcessActivityTask(false)
	s.logger.Info("PollAndProcessActivityTask 2", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)
	_, err = poller1.PollAndProcessWorkflowTask()
	s.logger.Info("PollAndProcessWorkflowTask 2", tag.Error(err))
	s.NoError(err)
	s.True(workflowComplete)

	// check history replicated in cluster0
	s.WaitForHistory(`
  1 v1 WorkflowExecutionStarted
  2 v1 WorkflowTaskScheduled
  3 v1 WorkflowTaskStarted
  4 v1 WorkflowTaskCompleted
  5 v1 ActivityTaskScheduled
  6 v2 ActivityTaskStarted
  7 v2 ActivityTaskCompleted
  8 v2 WorkflowTaskScheduled
  9 v2 WorkflowTaskStarted
 10 v2 WorkflowTaskCompleted
 11 v2 WorkflowExecutionCompleted`,
		func() *historypb.History {
			historyResponse, err := client0.GetWorkflowExecutionHistory(testcore.NewContext(), getHistoryReq)
			if err != nil {
				return nil
			}
			return historyResponse.GetHistory()
		}, replicationWaitTime, replicationCheckInterval,
	)
}

func (s *FunctionalClustersTestSuite) TestStickyWorkflowTaskFailover() {
	namespace := s.createGlobalNamespace()
	client0 := s.clusters[0].FrontendClient() // active
	client1 := s.clusters[1].FrontendClient() // standby

	// Start a workflow
	id := "functional-sticky-workflow-task-workflow-failover-test-" + "TransitionHistory" + strconv.FormatBool(s.enableTransitionHistory)
	wt := id + "-type"
	tq := id + "-taskqueue"
	stq1 := id + "-taskqueue-sticky1"
	stq2 := id + "-taskqueue-sticky2"
	identity1 := "worker1"
	identity2 := "worker2"

	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()
	taskQueue := taskqueuepb.TaskQueue_builder{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()
	stickyTaskQueue1 := taskqueuepb.TaskQueue_builder{Name: stq1, Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: tq}.Build()
	stickyTaskQueue2 := taskqueuepb.TaskQueue_builder{Name: stq2, Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: tq}.Build()
	stickyTaskTimeout := 100 * time.Second
	startReq := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(2592000 * time.Second),
		WorkflowTaskTimeout: durationpb.New(60 * time.Second),
		Identity:            identity1,
	}.Build()
	we, err := client0.StartWorkflowExecution(testcore.NewContext(), startReq)
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
		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
				Result: payloads.EncodeString("Done"),
			}.Build(),
		}.Build()}, nil
	}

	// nolint
	poller0 := &testcore.TaskPoller{
		Client:                       client0,
		Namespace:                    namespace,
		TaskQueue:                    taskQueue,
		StickyTaskQueue:              stickyTaskQueue1,
		StickyScheduleToStartTimeout: stickyTaskTimeout,
		Identity:                     identity1,
		WorkflowTaskHandler:          wtHandler,
		Logger:                       s.logger,
		T:                            s.T(),
	}

	// nolint
	poller1 := &testcore.TaskPoller{
		Client:                       client1,
		Namespace:                    namespace,
		TaskQueue:                    taskQueue,
		StickyTaskQueue:              stickyTaskQueue2,
		StickyScheduleToStartTimeout: stickyTaskTimeout,
		Identity:                     identity2,
		WorkflowTaskHandler:          wtHandler,
		Logger:                       s.logger,
		T:                            s.T(),
	}

	_, err = poller0.PollAndProcessWorkflowTask(testcore.WithRespondSticky)
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(firstCommandMade)

	// Send a signal in cluster
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	_, err = client0.SignalWorkflowExecution(testcore.NewContext(), workflowservice.SignalWorkflowExecutionRequest_builder{
		Namespace: namespace,
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: id,
			RunId:      we.GetRunId(),
		}.Build(),
		SignalName: signalName,
		Input:      signalInput,
		Identity:   identity1,
	}.Build())
	s.NoError(err)

	s.failover(namespace, 0, s.clusters[1].ClusterName(), 2)

	_, err = poller1.PollAndProcessWorkflowTask(testcore.WithRespondSticky)
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(secondCommandMade)

	_, err = client1.SignalWorkflowExecution(testcore.NewContext(), workflowservice.SignalWorkflowExecutionRequest_builder{
		Namespace: namespace,
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: id,
			RunId:      we.GetRunId(),
		}.Build(),
		SignalName: signalName,
		Input:      signalInput,
		Identity:   identity2,
	}.Build())
	s.NoError(err)

	s.failover(namespace, 1, s.clusters[0].ClusterName(), 11)

	_, err = poller0.PollAndProcessWorkflowTask()
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(workflowCompleted)
}

func (s *FunctionalClustersTestSuite) TestStartWorkflowExecution_Failover_WorkflowIDReusePolicy() {
	namespaceName := s.createGlobalNamespace()
	client0 := s.clusters[0].FrontendClient() // active
	client1 := s.clusters[1].FrontendClient() // standby

	// start a workflow
	id := "functional-start-workflow-failover-ID-reuse-policy-test"
	wt := "functional-start-workflow-failover-ID-reuse-policy-test-type"
	tl := "functional-start-workflow-failover-ID-reuse-policy-test-taskqueue"
	identity := "worker1"
	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()
	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()
	startReq := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:             uuid.NewString(),
		Namespace:             namespaceName,
		WorkflowId:            id,
		WorkflowType:          workflowType,
		TaskQueue:             taskQueue,
		Input:                 nil,
		WorkflowRunTimeout:    durationpb.New(100 * time.Second),
		WorkflowTaskTimeout:   durationpb.New(1 * time.Second),
		Identity:              identity,
		WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}.Build()
	we, err := client0.StartWorkflowExecution(testcore.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())
	s.logger.Info("StartWorkflowExecution in cluster0: ", tag.WorkflowRunID(we.GetRunId()))

	workflowCompleteTimes := 0
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		workflowCompleteTimes++
		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
				Result: payloads.EncodeString("Done"),
			}.Build(),
		}.Build()}, nil
	}

	// nolint
	poller0 := testcore.TaskPoller{
		Client:              client0,
		Namespace:           namespaceName,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// nolint
	poller1 := testcore.TaskPoller{
		Client:              client1,
		Namespace:           namespaceName,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// Complete the workflow in cluster0
	_, err = poller0.PollAndProcessWorkflowTask()
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.Equal(1, workflowCompleteTimes)

	s.failover(namespaceName, 0, s.clusters[1].ClusterName(), 2)

	// start the same workflow in cluster1 is not allowed if policy is AllowDuplicateFailedOnly
	startReq.SetRequestId(uuid.NewString())
	startReq.SetWorkflowIdReusePolicy(enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY)
	we, err = client1.StartWorkflowExecution(testcore.NewContext(), startReq)
	s.IsType(&serviceerror.WorkflowExecutionAlreadyStarted{}, err)
	s.Nil(we)

	// start the same workflow in cluster1 is not allowed if policy is RejectDuplicate
	startReq.SetRequestId(uuid.NewString())
	startReq.SetWorkflowIdReusePolicy(enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
	we, err = client1.StartWorkflowExecution(testcore.NewContext(), startReq)
	s.IsType(&serviceerror.WorkflowExecutionAlreadyStarted{}, err)
	s.Nil(we)

	// start the workflow in cluster1
	startReq.SetRequestId(uuid.NewString())
	startReq.SetWorkflowIdReusePolicy(enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE)
	we, err = client1.StartWorkflowExecution(testcore.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())
	s.logger.Info("StartWorkflowExecution in cluster1: ", tag.WorkflowRunID(we.GetRunId()))

	_, err = poller1.PollAndProcessWorkflowTask()
	s.logger.Info("PollAndProcessWorkflowTask 2", tag.Error(err))
	s.NoError(err)
	s.Equal(2, workflowCompleteTimes)
}

func (s *FunctionalClustersTestSuite) TestStartWorkflowExecution_Failover_WorkflowIDConflictPolicy_TerminateExisting() {
	namespaceName := s.createGlobalNamespace()
	client0 := s.clusters[0].FrontendClient() // active
	client1 := s.clusters[1].FrontendClient() // standby

	// start a workflow
	id := "functional-start-workflow-failover-ID-conflict-policy-test"
	wt := "functional-start-workflow-failover-ID-conflict-policy-test-type"
	tl := "functional-start-workflow-failover-ID-conflict-policy-test-taskqueue"
	identity := "worker1"
	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()
	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()
	startReq := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:             uuid.NewString(),
		Namespace:             namespaceName,
		WorkflowId:            id,
		WorkflowType:          workflowType,
		TaskQueue:             taskQueue,
		Input:                 nil,
		WorkflowRunTimeout:    durationpb.New(100 * time.Second),
		WorkflowTaskTimeout:   durationpb.New(1 * time.Second),
		Identity:              identity,
		WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}.Build()
	we, err := client0.StartWorkflowExecution(testcore.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())
	s.logger.Info("StartWorkflowExecution in cluster0: ", tag.WorkflowRunID(we.GetRunId()))

	workflowCompleteTimes := 0
	firstCommandMade := false
	var executions []*commonpb.WorkflowExecution
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		executions = append(executions, task.GetWorkflowExecution())
		if !firstCommandMade {
			firstCommandMade = true
			return []*commandpb.Command{}, nil
		}

		workflowCompleteTimes++
		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
				Result: payloads.EncodeString("Done"),
			}.Build(),
		}.Build()}, nil
	}

	// nolint
	poller0 := testcore.TaskPoller{
		Client:              client0,
		Namespace:           namespaceName,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// nolint
	poller1 := testcore.TaskPoller{
		Client:              client1,
		Namespace:           namespaceName,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// keep the workflow in cluster0 running
	_, err = poller0.PollAndProcessWorkflowTask()
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// start the same workflow in cluster0 and terminate the existing workflow
	startReq.SetRequestId(uuid.NewString())
	startReq.SetWorkflowIdConflictPolicy(enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING)
	we, err = client0.StartWorkflowExecution(testcore.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())
	s.logger.Info("StartWorkflowExecution in cluster0: ", tag.WorkflowRunID(we.GetRunId()))

	s.failover(namespaceName, 0, s.clusters[1].ClusterName(), 2)

	_, err = poller1.PollAndProcessWorkflowTask()
	s.logger.Info("PollAndProcessWorkflowTask 2", tag.Error(err))
	s.NoError(err)
	s.Equal(1, workflowCompleteTimes)
	s.Equal(2, len(executions))
	s.Equal(executions[1].GetRunId(), we.GetRunId())
}

func (s *FunctionalClustersTestSuite) TestTerminateFailover() {
	namespace := s.createGlobalNamespace()
	client0 := s.clusters[0].FrontendClient() // active
	client1 := s.clusters[1].FrontendClient() // standby

	// start a workflow
	id := "functional-terminate-workflow-failover-test"
	wt := "functional-terminate-workflow-failover-test-type"
	tl := "functional-terminate-workflow-failover-test-taskqueue"
	identity := "worker1"
	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()
	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()
	startReq := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}.Build()
	we, err := client0.StartWorkflowExecution(testcore.NewContext(), startReq)
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

			return []*commandpb.Command{commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				ScheduleActivityTaskCommandAttributes: commandpb.ScheduleActivityTaskCommandAttributes_builder{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           commonpb.ActivityType_builder{Name: activityName}.Build(),
					TaskQueue:              taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(50 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
				}.Build(),
			}.Build()}, nil
		}

		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
				Result: payloads.EncodeString("Done"),
			}.Build(),
		}.Build()}, nil
	}

	// nolint
	poller0 := &testcore.TaskPoller{
		Client:              client0,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// make some progress in cluster0
	_, err = poller0.PollAndProcessWorkflowTask()
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// check terminate done
	getHistoryReq := workflowservice.GetWorkflowExecutionHistoryRequest_builder{
		Namespace: namespace,
		Execution: commonpb.WorkflowExecution_builder{
			WorkflowId: id,
		}.Build(),
	}.Build()

	s.WaitForHistory(`
  1 v1 WorkflowExecutionStarted
  2 v1 WorkflowTaskScheduled
  3 v1 WorkflowTaskStarted
  4 v1 WorkflowTaskCompleted
  5 v1 ActivityTaskScheduled`,
		func() *historypb.History {
			historyResponse, err := client0.GetWorkflowExecutionHistory(testcore.NewContext(), getHistoryReq)
			s.NoError(err)
			return historyResponse.GetHistory()
		}, 1*time.Second, 100*time.Millisecond,
	)

	s.failover(namespace, 0, s.clusters[1].ClusterName(), 2)

	s.WaitForHistory(`
  1 v1 WorkflowExecutionStarted
  2 v1 WorkflowTaskScheduled
  3 v1 WorkflowTaskStarted
  4 v1 WorkflowTaskCompleted
  5 v1 ActivityTaskScheduled`,
		func() *historypb.History {
			historyResponse, err := client1.GetWorkflowExecutionHistory(testcore.NewContext(), getHistoryReq)
			s.NoError(err)
			return historyResponse.GetHistory()
		}, 5*time.Second, 100*time.Millisecond,
	)

	// terminate workflow at cluster1
	terminateReason := "terminate reason"
	terminateDetails := payloads.EncodeString("terminate details")
	_, err = client1.TerminateWorkflowExecution(testcore.NewContext(), workflowservice.TerminateWorkflowExecutionRequest_builder{
		Namespace: namespace,
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: id,
		}.Build(),
		Reason:   terminateReason,
		Details:  terminateDetails,
		Identity: identity,
	}.Build())
	s.NoError(err)

	// check terminate done
	s.WaitForHistory(`
  1 v1 WorkflowExecutionStarted
  2 v1 WorkflowTaskScheduled
  3 v1 WorkflowTaskStarted
  4 v1 WorkflowTaskCompleted
  5 v1 ActivityTaskScheduled
  6 v2 WorkflowExecutionTerminated  {"Details":{"Payloads":[{"Data":"\"terminate details\""}]},"Identity":"worker1","Reason":"terminate reason"}`,
		func() *historypb.History {
			historyResponse, err := client1.GetWorkflowExecutionHistory(testcore.NewContext(), getHistoryReq)
			s.NoError(err)
			return historyResponse.GetHistory()
		}, 1*time.Second, 100*time.Millisecond,
	)

	// check history replicated to the other cluster
	s.WaitForHistory(`
  1 v1 WorkflowExecutionStarted
  2 v1 WorkflowTaskScheduled
  3 v1 WorkflowTaskStarted
  4 v1 WorkflowTaskCompleted
  5 v1 ActivityTaskScheduled
  6 v2 WorkflowExecutionTerminated  {"Details":{"Payloads":[{"Data":"\"terminate details\""}]},"Identity":"worker1","Reason":"terminate reason"}`,
		func() *historypb.History {
			historyResponse, err := client0.GetWorkflowExecutionHistory(testcore.NewContext(), getHistoryReq)
			if err != nil {
				return nil
			}
			return historyResponse.GetHistory()
		}, replicationWaitTime, replicationCheckInterval,
	)
}

func (s *FunctionalClustersTestSuite) TestResetWorkflowFailover() {
	namespace := s.createGlobalNamespace()
	client0 := s.clusters[0].FrontendClient() // active
	client1 := s.clusters[1].FrontendClient() // standby

	// start a workflow
	id := "functional-reset-workflow-failover-test"
	wt := "functional-reset-workflow-failover-test-type"
	tl := "functional-reset-workflow-failover-test-taskqueue"
	identity := "worker1"
	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()
	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()
	startReq := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}.Build()
	we, err := client0.StartWorkflowExecution(testcore.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	_, err = client0.SignalWorkflowExecution(testcore.NewContext(), workflowservice.SignalWorkflowExecutionRequest_builder{
		Namespace: namespace,
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: id,
			RunId:      we.GetRunId(),
		}.Build(),
		SignalName: "random signal name",
		Input: commonpb.Payloads_builder{Payloads: []*commonpb.Payload{
			commonpb.Payload_builder{Data: []byte("random signal payload")}.Build(),
		}}.Build(),
		Identity: identity,
	}.Build())
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
		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
				Result: payloads.EncodeString("Done"),
			}.Build(),
		}.Build()}, nil

	}

	// nolint
	poller0 := testcore.TaskPoller{
		Client:              client0,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// nolint
	poller1 := testcore.TaskPoller{
		Client:              client1,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.logger,
		T:                   s.T(),
	}

	_, err = poller0.PollAndProcessWorkflowTask()
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// TODO (alex): assert on history instead
	// events layout
	//  1. WorkflowExecutionStarted
	//  2. WorkflowTaskScheduled
	//  3. WorkflowExecutionSignaled
	//  4. WorkflowTaskStarted
	//  5. WorkflowTaskCompleted

	// Reset workflow execution
	resetResp, err := client0.ResetWorkflowExecution(testcore.NewContext(), workflowservice.ResetWorkflowExecutionRequest_builder{
		Namespace: namespace,
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: id,
			RunId:      we.GetRunId(),
		}.Build(),
		Reason:                    "reset execution from test",
		WorkflowTaskFinishEventId: 4, // before WorkflowTaskStarted
		RequestId:                 uuid.NewString(),
	}.Build())
	s.NoError(err)

	s.failover(namespace, 0, s.clusters[1].ClusterName(), 2)

	_, err = poller1.PollAndProcessWorkflowTask()
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(workflowComplete)

	s.waitForClusterSynced()

	getHistoryReq := workflowservice.GetWorkflowExecutionHistoryRequest_builder{
		Namespace: namespace,
		Execution: commonpb.WorkflowExecution_builder{
			WorkflowId: id,
			RunId:      resetResp.GetRunId(),
		}.Build(),
	}.Build()

	getHistoryResp, err := client0.GetWorkflowExecutionHistory(testcore.NewContext(), getHistoryReq)
	s.NoError(err)
	s.EqualHistory(`
  1 v1 WorkflowExecutionStarted
  2 v1 WorkflowTaskScheduled
  3 v1 WorkflowExecutionSignaled
  4 v1 WorkflowTaskStarted
  5 v1 WorkflowTaskFailed
  6 v1 WorkflowTaskScheduled
  7 v2 WorkflowTaskStarted
  8 v2 WorkflowTaskCompleted
  9 v2 WorkflowExecutionCompleted`, getHistoryResp.GetHistory())

	getHistoryResp, err = client1.GetWorkflowExecutionHistory(testcore.NewContext(), getHistoryReq)
	s.NoError(err)
	s.EqualHistory(`
  1 v1 WorkflowExecutionStarted
  2 v1 WorkflowTaskScheduled
  3 v1 WorkflowExecutionSignaled
  4 v1 WorkflowTaskStarted
  5 v1 WorkflowTaskFailed
  6 v1 WorkflowTaskScheduled
  7 v2 WorkflowTaskStarted
  8 v2 WorkflowTaskCompleted
  9 v2 WorkflowExecutionCompleted`, getHistoryResp.GetHistory())
}

func (s *FunctionalClustersTestSuite) TestContinueAsNewFailover() {
	namespace := s.createGlobalNamespace()
	client0 := s.clusters[0].FrontendClient() // active
	client1 := s.clusters[1].FrontendClient() // standby

	// start a workflow
	id := "functional-continueAsNew-workflow-failover-test"
	wt := "functional-continueAsNew-workflow-failover-test-type"
	tl := "functional-continueAsNew-workflow-failover-test-taskqueue"
	identity := "worker1"
	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()
	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()
	startReq := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}.Build()
	we, err := client0.StartWorkflowExecution(testcore.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	workflowComplete := false
	continueAsNewCount := int32(5)
	continueAsNewCounter := int32(0)
	var previousRunID string
	var lastRunStartedEvent *historypb.HistoryEvent
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if continueAsNewCounter < continueAsNewCount {
			previousRunID = task.GetWorkflowExecution().GetRunId()
			continueAsNewCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, continueAsNewCounter))

			return []*commandpb.Command{commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
				ContinueAsNewWorkflowExecutionCommandAttributes: commandpb.ContinueAsNewWorkflowExecutionCommandAttributes_builder{
					WorkflowType:        workflowType,
					TaskQueue:           taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
					Input:               payloads.EncodeBytes(buf.Bytes()),
					WorkflowRunTimeout:  durationpb.New(100 * time.Second),
					WorkflowTaskTimeout: durationpb.New(10 * time.Second),
				}.Build(),
			}.Build()}, nil
		}

		lastRunStartedEvent = task.GetHistory().GetEvents()[0]
		workflowComplete = true
		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
				Result: payloads.EncodeString("Done"),
			}.Build(),
		}.Build()}, nil
	}

	// nolint
	poller0 := &testcore.TaskPoller{
		Client:              client0,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// nolint
	poller1 := testcore.TaskPoller{
		Client:              client1,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// make some progress in cluster0 and did some continueAsNew
	for i := 0; i < 3; i++ {
		_, err := poller0.PollAndProcessWorkflowTask()
		s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err, strconv.Itoa(i))
	}

	s.failover(namespace, 0, s.clusters[1].ClusterName(), 2)

	// finish the rest in cluster1
	for i := 0; i < 2; i++ {
		_, err := poller1.PollAndProcessWorkflowTask()
		s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err, strconv.Itoa(i))
	}

	s.False(workflowComplete)
	_, err = poller1.PollAndProcessWorkflowTask()
	s.NoError(err)
	s.True(workflowComplete)
	s.Equal(previousRunID, lastRunStartedEvent.GetWorkflowExecutionStartedEventAttributes().GetContinuedExecutionRunId())
}

func (s *FunctionalClustersTestSuite) TestSignalFailover() {
	namespace := s.createGlobalNamespace()
	client0 := s.clusters[0].FrontendClient() // active
	client1 := s.clusters[1].FrontendClient() // standby

	// Start a workflow
	id := "functional-signal-workflow-failover-test"
	wt := "functional-signal-workflow-failover-test-type"
	tl := "functional-signal-workflow-failover-test-taskqueue"
	identity := "worker1"
	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()
	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()
	startReq := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(300 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}.Build()
	we, err := client0.StartWorkflowExecution(testcore.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	s.logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

	eventSignaled := false
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if task.GetPreviousStartedEventId() == 0 {
			return []*commandpb.Command{}, nil
		}
		if !eventSignaled {
			for _, event := range task.GetHistory().GetEvents()[task.GetPreviousStartedEventId():] {
				if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
					eventSignaled = true
					return []*commandpb.Command{}, nil
				}
			}
		}

		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
				Result: payloads.EncodeString("Done"),
			}.Build(),
		}.Build()}, nil
	}

	// nolint
	poller0 := &testcore.TaskPoller{
		Client:              client0,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// nolint
	poller1 := &testcore.TaskPoller{
		Client:              client1,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// Process start event in cluster0
	_, err = poller0.PollAndProcessWorkflowTask()
	s.NoError(err)
	s.False(eventSignaled)

	// Send a signal in cluster0
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	_, err = client0.SignalWorkflowExecution(testcore.NewContext(), workflowservice.SignalWorkflowExecutionRequest_builder{
		Namespace: namespace,
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: id,
			RunId:      we.GetRunId(),
		}.Build(),
		SignalName: signalName,
		Input:      signalInput,
		Identity:   identity,
	}.Build())
	s.NoError(err)

	// Process signal in cluster0
	s.False(eventSignaled)
	_, err = poller0.PollAndProcessWorkflowTask()
	s.logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(eventSignaled)

	s.failover(namespace, 0, s.clusters[1].ClusterName(), 2)

	// check history matched
	getHistoryReq := workflowservice.GetWorkflowExecutionHistoryRequest_builder{
		Namespace: namespace,
		Execution: commonpb.WorkflowExecution_builder{
			WorkflowId: id,
		}.Build(),
	}.Build()
	// TODO (alex): this shouldn't be WaitForHistory anymore (just EqualHistory)
	s.WaitForHistory(`
  1 v1 WorkflowExecutionStarted
  2 v1 WorkflowTaskScheduled
  3 v1 WorkflowTaskStarted
  4 v1 WorkflowTaskCompleted
  5 v1 WorkflowExecutionSignaled
  6 v1 WorkflowTaskScheduled
  7 v1 WorkflowTaskStarted
  8 v1 WorkflowTaskCompleted`,
		func() *historypb.History {
			historyResponse, err := client1.GetWorkflowExecutionHistory(testcore.NewContext(), getHistoryReq)
			if err != nil {
				return nil
			}
			return historyResponse.GetHistory()
		}, replicationWaitTime, replicationCheckInterval,
	)

	// Send another signal in cluster1
	signalName2 := "my signal 2"
	signalInput2 := payloads.EncodeString("my signal input 2")
	_, err = client1.SignalWorkflowExecution(testcore.NewContext(), workflowservice.SignalWorkflowExecutionRequest_builder{
		Namespace: namespace,
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: id,
		}.Build(),
		SignalName: signalName2,
		Input:      signalInput2,
		Identity:   identity,
	}.Build())
	s.NoError(err)

	// Process signal in cluster1
	eventSignaled = false
	_, err = poller1.PollAndProcessWorkflowTask()
	s.logger.Info("PollAndProcessWorkflowTask 2", tag.Error(err))
	s.NoError(err)
	s.True(eventSignaled)

	// check history matched
	s.WaitForHistory(`
  1 v1 WorkflowExecutionStarted
  2 v1 WorkflowTaskScheduled
  3 v1 WorkflowTaskStarted
  4 v1 WorkflowTaskCompleted
  5 v1 WorkflowExecutionSignaled
  6 v1 WorkflowTaskScheduled
  7 v1 WorkflowTaskStarted
  8 v1 WorkflowTaskCompleted
  9 v2 WorkflowExecutionSignaled
 10 v2 WorkflowTaskScheduled
 11 v2 WorkflowTaskStarted
 12 v2 WorkflowTaskCompleted`,
		func() *historypb.History {
			historyResponse, err := client1.GetWorkflowExecutionHistory(testcore.NewContext(), getHistoryReq)
			if err != nil {
				return nil
			}
			return historyResponse.GetHistory()
		}, replicationWaitTime, replicationCheckInterval)
}

func (s *FunctionalClustersTestSuite) TestUserTimerFailover() {
	namespace := s.createGlobalNamespace()
	client0 := s.clusters[0].FrontendClient() // active
	client1 := s.clusters[1].FrontendClient() // standby

	// Start a workflow
	id := "functional-user-timer-workflow-failover-test"
	wt := "functional-user-timer-workflow-failover-test-type"
	tl := "functional-user-timer-workflow-failover-test-taskqueue"
	identity := "worker1"
	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()
	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()
	startReq := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(300 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}.Build()
	we, err := client0.StartWorkflowExecution(testcore.NewContext(), startReq)
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
			_, err = client0.SignalWorkflowExecution(testcore.NewContext(), workflowservice.SignalWorkflowExecutionRequest_builder{
				Namespace: namespace,
				WorkflowExecution: commonpb.WorkflowExecution_builder{
					WorkflowId: id,
					RunId:      we.GetRunId(),
				}.Build(),
				SignalName: signalName,
				Input:      signalInput,
				Identity:   "",
			}.Build())
			s.NoError(err)
			return []*commandpb.Command{commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_START_TIMER,
				StartTimerCommandAttributes: commandpb.StartTimerCommandAttributes_builder{
					TimerId:            "timer-id",
					StartToFireTimeout: durationpb.New(2 * time.Second),
				}.Build(),
			}.Build()}, nil
		}

		if !timerFired {
			resp, err := client1.GetWorkflowExecutionHistory(testcore.NewContext(), workflowservice.GetWorkflowExecutionHistoryRequest_builder{
				Namespace: namespace,
				Execution: commonpb.WorkflowExecution_builder{
					WorkflowId: id,
					RunId:      we.GetRunId(),
				}.Build(),
			}.Build())
			s.NoError(err)
			for _, event := range resp.GetHistory().GetEvents() {
				if event.GetEventType() == enumspb.EVENT_TYPE_TIMER_FIRED {
					timerFired = true
				}
			}
			if !timerFired {
				return []*commandpb.Command{}, nil
			}
		}

		workflowCompleted = true
		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
				Result: payloads.EncodeString("Done"),
			}.Build(),
		}.Build()}, nil
	}

	// nolint
	poller0 := &testcore.TaskPoller{
		Client:              client0,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// nolint
	poller1 := &testcore.TaskPoller{
		Client:              client1,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	for i := 0; i < 2; i++ {
		_, err = poller0.PollAndProcessWorkflowTask()
		if err != nil {
			timerCreated = false
			continue
		}
		if timerCreated {
			break
		}
	}
	s.True(timerCreated)

	s.failover(namespace, 0, s.clusters[1].ClusterName(), 2)

	for i := 1; i < 20; i++ {
		if !workflowCompleted {
			_, err = poller1.PollAndProcessWorkflowTask()
			s.NoError(err)
			time.Sleep(time.Second) // nolint:forbidigo
		}
	}
}

func (s *FunctionalClustersTestSuite) TestForceWorkflowTaskClose_WithClusterReconnect() {
	namespace := s.createGlobalNamespace()
	client0 := s.clusters[0].FrontendClient() // active
	client1 := s.clusters[1].FrontendClient() // standby

	// Start a workflow
	id := "test-force-workflow-task-close-test"
	wt := "test-force-workflow-task-close-test-type"
	tl := "test-force-workflow-task-close-test-taskqueue"
	identity := "worker1"
	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()
	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()
	startReq := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(300 * time.Second),
		WorkflowTaskTimeout: durationpb.New(60 * time.Second),
		Identity:            identity,
	}.Build()
	we, err := client0.StartWorkflowExecution(testcore.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	s.logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.GetRunId()))

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
				Result: payloads.EncodeString("Done"),
			}.Build(),
		}.Build()}, nil
	}

	// nolint
	poller0 := &testcore.TaskPoller{
		Client:              client0,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// this will fail the workflow task
	_, err = poller0.PollAndProcessWorkflowTask(testcore.WithDropTask)
	s.NoError(err)

	s.failover(namespace, 0, s.clusters[1].ClusterName(), 2)

	// Update the namespace in cluster1 to be a single cluster namespace
	s.updateNamespaceClusters(namespace, 0, s.clusters[1:2])

	// Send a signal to cluster1, namespace contains one cluster
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	_, err = client1.SignalWorkflowExecution(testcore.NewContext(), workflowservice.SignalWorkflowExecutionRequest_builder{
		Namespace: namespace,
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: id,
			RunId:      we.GetRunId(),
		}.Build(),
		SignalName: signalName,
		Input:      signalInput,
	}.Build())
	s.NoError(err)

	// No error is expected with single cluster namespace.
	_, err = client1.DescribeWorkflowExecution(testcore.NewContext(), workflowservice.DescribeWorkflowExecutionRequest_builder{
		Namespace: namespace,
		Execution: commonpb.WorkflowExecution_builder{
			WorkflowId: id,
		}.Build(),
	}.Build())
	s.NoError(err)

	// Update the namespace in cluster1 to be a multi cluster namespace
	s.updateNamespaceClusters(namespace, 1, s.clusters)

	// No error is expected with multi cluster namespace.
	_, err = client1.DescribeWorkflowExecution(testcore.NewContext(), workflowservice.DescribeWorkflowExecutionRequest_builder{
		Namespace: namespace,
		Execution: commonpb.WorkflowExecution_builder{
			WorkflowId: id,
		}.Build(),
	}.Build())
	s.NoError(err)
}

func (s *FunctionalClustersTestSuite) TestTransientWorkflowTaskFailover() {
	namespace := s.createGlobalNamespace()
	client0 := s.clusters[0].FrontendClient() // active
	client1 := s.clusters[1].FrontendClient() // standby

	// Start a workflow
	id := "functional-transient-workflow-task-workflow-failover-test"
	wt := "functional-transient-workflow-task-workflow-failover-test-type"
	tl := "functional-transient-workflow-task-workflow-failover-test-taskqueue"
	identity := "worker1"
	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()
	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()
	startReq := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(300 * time.Second),
		WorkflowTaskTimeout: durationpb.New(8 * time.Second),
		Identity:            identity,
	}.Build()
	we, err := client0.StartWorkflowExecution(testcore.NewContext(), startReq)
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
		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
				Result: payloads.EncodeString("Done"),
			}.Build(),
		}.Build()}, nil
	}

	// nolint
	poller0 := &testcore.TaskPoller{
		Client:              client0,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// nolint
	poller1 := &testcore.TaskPoller{
		Client:              client1,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// this will fail the workflow task
	_, err = poller0.PollAndProcessWorkflowTask()
	s.NoError(err)

	s.failover(namespace, 0, s.clusters[1].ClusterName(), 2)

	// for failover transient workflow task, it is guaranteed that the transient workflow task
	// after the failover has attempt 1
	// for details see ApplyTransientWorkflowTaskScheduled
	_, err = poller1.PollAndProcessWorkflowTask(testcore.WithExpectedAttemptCount(1))
	s.NoError(err)
	s.True(workflowFinished)
}

func (s *FunctionalClustersTestSuite) TestCronWorkflowStartAndFailover() {
	namespace := s.createGlobalNamespace()
	client0 := s.clusters[0].FrontendClient() // active
	client1 := s.clusters[1].FrontendClient() // standby

	// start a workflow
	id := "functional-cron-workflow-start-and-failover-test"
	wt := "functional-cron-workflow-start-and-failover-test-type"
	tl := "functional-cron-workflow-start-and-failover-test-taskqueue"
	identity := "worker1"
	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()
	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()
	startReq := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
		CronSchedule:        "@every 5s",
	}.Build()
	we, err := client0.StartWorkflowExecution(testcore.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	wfCompleted := false
	var executions []*commonpb.WorkflowExecution
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		executions = append(executions, task.GetWorkflowExecution())
		wfCompleted = true
		return []*commandpb.Command{
			commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
					Result: payloads.EncodeString("cron-test-result"),
				}.Build(),
			}.Build()}, nil
	}

	// nolint
	poller1 := testcore.TaskPoller{
		Client:              client1,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	s.failover(namespace, 0, s.clusters[1].ClusterName(), 2)

	_, err = poller1.PollAndProcessWorkflowTask()
	s.NoError(err)
	s.True(wfCompleted)
	events := s.getHistory(client1, namespace, executions[0])
	s.EqualHistoryEvents(`
  1 v1 WorkflowExecutionStarted
  2 v2 WorkflowTaskScheduled
  3 v2 WorkflowTaskStarted
  4 v2 WorkflowTaskCompleted
  5 v2 WorkflowExecutionCompleted`, events)

	// terminate the remaining cron
	_, err = client1.TerminateWorkflowExecution(testcore.NewContext(), workflowservice.TerminateWorkflowExecutionRequest_builder{
		Namespace: namespace,
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: id,
		}.Build(),
	}.Build())
	s.NoError(err)
}

func (s *FunctionalClustersTestSuite) getLastEvent(
	client workflowservice.WorkflowServiceClient,
	namespace string,
	execution *commonpb.WorkflowExecution,
) *historypb.HistoryEvent {

	resp, err := client.GetWorkflowExecutionHistory(testcore.NewContext(), workflowservice.GetWorkflowExecutionHistoryRequest_builder{
		Namespace: namespace,
		Execution: execution,
	}.Build())
	s.NoError(err)
	s.NotNil(resp.GetHistory())
	s.NotEmpty(resp.GetHistory().GetEvents())

	return resp.GetHistory().GetEvents()[len(resp.GetHistory().GetEvents())-1]
}

func (s *FunctionalClustersTestSuite) getNewExecutionRunIdFromLastEvent(
	client workflowservice.WorkflowServiceClient,
	namespace string,
	execution *commonpb.WorkflowExecution,
) string {
	lastEvent := s.getLastEvent(client, namespace, execution)
	s.NotNil(lastEvent)

	if lastEvent.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED {
		attrs := lastEvent.GetWorkflowExecutionCompletedEventAttributes()
		s.NotNil(attrs)
		return attrs.GetNewExecutionRunId()
	}
	return ""
}

func (s *FunctionalClustersTestSuite) waitForNewRunToStart(
	client workflowservice.WorkflowServiceClient,
	namespace string,
	execution *commonpb.WorkflowExecution,
) string {
	var newRunID string
	s.Eventually(func() bool {
		newRunID = s.getNewExecutionRunIdFromLastEvent(client, namespace, execution)
		return newRunID != ""
	}, 10*time.Second, 100*time.Millisecond)

	s.NotEmpty(newRunID, "New run should have started")
	return newRunID
}

func (s *FunctionalClustersTestSuite) TestCronWorkflowCompleteAndFailover() {
	namespace := s.createGlobalNamespace()
	client0 := s.clusters[0].FrontendClient() // active
	client1 := s.clusters[1].FrontendClient() // standby

	// start a workflow
	id := "functional-cron-workflow-complete-andfailover-test"
	wt := "functional-cron-workflow-complete-andfailover-test-type"
	tl := "functional-cron-workflow-complete-andfailover-test-taskqueue"
	identity := "worker1"
	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()
	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()
	startReq := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
		CronSchedule:        "@every 5s",
	}.Build()
	we, err := client0.StartWorkflowExecution(testcore.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	wfCompletionCount := 0
	var executions []*commonpb.WorkflowExecution
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		wfCompletionCount += 1
		executions = append(executions, task.GetWorkflowExecution())
		return []*commandpb.Command{
			commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
					Result: payloads.EncodeString("cron-test-result"),
				}.Build(),
			}.Build()}, nil
	}

	// nolint
	poller0 := testcore.TaskPoller{
		Client:              client0,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// nolint
	poller1 := testcore.TaskPoller{
		Client:              client1,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	_, err = poller0.PollAndProcessWorkflowTask()
	s.NoError(err)
	s.Equal(1, wfCompletionCount)
	events := s.getHistory(client0, namespace, executions[0])
	s.EqualHistoryEvents(`
  1 v1 WorkflowExecutionStarted
  2 v1 WorkflowTaskScheduled
  3 v1 WorkflowTaskStarted
  4 v1 WorkflowTaskCompleted
  5 v1 WorkflowExecutionCompleted`, events)

	_ = s.waitForNewRunToStart(client0, namespace, executions[0])

	s.failover(namespace, 0, s.clusters[1].ClusterName(), 2)

	_, err = poller1.PollAndProcessWorkflowTask()
	s.NoError(err)
	s.Equal(2, wfCompletionCount)
	events = s.getHistory(client1, namespace, executions[1])
	s.EqualHistoryEvents(`
  1 v1 WorkflowExecutionStarted
  2 v2 WorkflowTaskScheduled
  3 v2 WorkflowTaskStarted
  4 v2 WorkflowTaskCompleted
  5 v2 WorkflowExecutionCompleted`, events)

	_, err = client1.TerminateWorkflowExecution(testcore.NewContext(), workflowservice.TerminateWorkflowExecutionRequest_builder{
		Namespace: namespace,
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: id,
		}.Build(),
	}.Build())
	s.NoError(err)
}

func (s *FunctionalClustersTestSuite) TestWorkflowRetryStartAndFailover() {
	namespace := s.createGlobalNamespace()
	client0 := s.clusters[0].FrontendClient() // active
	client1 := s.clusters[1].FrontendClient() // standby

	// start a workflow
	id := "functional-workflow-retry-start-and-failover-test"
	wt := "functional-workflow-retry-start-and-failover-test-type"
	tl := "functional-workflow-retry-start-and-failover-test-taskqueue"
	identity := "worker1"
	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()
	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()
	startReq := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
		RetryPolicy: commonpb.RetryPolicy_builder{
			InitialInterval:        durationpb.New(1 * time.Second),
			MaximumAttempts:        3,
			MaximumInterval:        durationpb.New(1 * time.Second),
			NonRetryableErrorTypes: []string{"bad-bug"},
			BackoffCoefficient:     1,
		}.Build(),
	}.Build()
	we, err := client0.StartWorkflowExecution(testcore.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	var executions []*commonpb.WorkflowExecution
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		executions = append(executions, task.GetWorkflowExecution())
		return []*commandpb.Command{
			commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
				FailWorkflowExecutionCommandAttributes: commandpb.FailWorkflowExecutionCommandAttributes_builder{
					Failure: failure.NewServerFailure("retryable-error", false),
				}.Build(),
			}.Build()}, nil
	}

	// nolint
	poller1 := testcore.TaskPoller{
		Client:              client1,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	s.failover(namespace, 0, s.clusters[1].ClusterName(), 2)

	// First attempt
	_, err = poller1.PollAndProcessWorkflowTask()
	s.NoError(err)
	events := s.getHistory(client1, namespace, executions[0])
	s.EqualHistoryEvents(`
  1 v1 WorkflowExecutionStarted {"Attempt":1}
  2 v1 WorkflowTaskScheduled
  3 v2 WorkflowTaskStarted
  4 v2 WorkflowTaskCompleted
  5 v2 WorkflowExecutionFailed`, events)

	// second attempt
	_, err = poller1.PollAndProcessWorkflowTask()
	s.NoError(err)
	events = s.getHistory(client1, namespace, executions[1])
	s.EqualHistoryEvents(`
  1 v2 WorkflowExecutionStarted {"Attempt":2}
  2 v2 WorkflowTaskScheduled
  3 v2 WorkflowTaskStarted
  4 v2 WorkflowTaskCompleted
  5 v2 WorkflowExecutionFailed`, events)
}

func (s *FunctionalClustersTestSuite) TestWorkflowRetryFailAndFailover() {
	namespace := s.createGlobalNamespace()
	client0 := s.clusters[0].FrontendClient() // active
	client1 := s.clusters[1].FrontendClient() // standby

	// start a workflow
	id := "functional-workflow-retry-fail-and-failover-test"
	wt := "functional-workflow-retry-fail-and-failover-test-type"
	tl := "functional-workflow-retry-fail-and-failover-test-taskqueue"
	identity := "worker1"
	workflowType := commonpb.WorkflowType_builder{Name: wt}.Build()
	taskQueue := taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build()
	startReq := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
		RetryPolicy: commonpb.RetryPolicy_builder{
			InitialInterval:        durationpb.New(1 * time.Second),
			MaximumAttempts:        3,
			MaximumInterval:        durationpb.New(1 * time.Second),
			NonRetryableErrorTypes: []string{"bad-bug"},
			BackoffCoefficient:     1,
		}.Build(),
	}.Build()
	we, err := client0.StartWorkflowExecution(testcore.NewContext(), startReq)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	var executions []*commonpb.WorkflowExecution
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		executions = append(executions, task.GetWorkflowExecution())
		return []*commandpb.Command{
			commandpb.Command_builder{
				CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
				FailWorkflowExecutionCommandAttributes: commandpb.FailWorkflowExecutionCommandAttributes_builder{
					Failure: failure.NewServerFailure("retryable-error", false),
				}.Build(),
			}.Build()}, nil
	}

	// nolint
	poller0 := testcore.TaskPoller{
		Client:              client0,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	// nolint
	poller1 := testcore.TaskPoller{
		Client:              client1,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}

	_, err = poller0.PollAndProcessWorkflowTask()
	s.NoError(err)
	events := s.getHistory(client0, namespace, executions[0])
	s.EqualHistoryEvents(`
  1 v1 WorkflowExecutionStarted {"Attempt":1}
  2 v1 WorkflowTaskScheduled
  3 v1 WorkflowTaskStarted
  4 v1 WorkflowTaskCompleted
  5 v1 WorkflowExecutionFailed`, events)

	s.failover(namespace, 0, s.clusters[1].ClusterName(), 2)

	_, err = poller1.PollAndProcessWorkflowTask()
	s.NoError(err)
	events = s.getHistory(client1, namespace, executions[1])
	s.EqualHistoryEvents(`
  1 v1 WorkflowExecutionStarted {"Attempt":2}
  2 v1 WorkflowTaskScheduled
  3 v2 WorkflowTaskStarted
  4 v2 WorkflowTaskCompleted
  5 v2 WorkflowExecutionFailed`, events)
}

func (s *FunctionalClustersTestSuite) TestActivityHeartbeatFailover() {
	namespace := s.createGlobalNamespace()

	taskqueue := "functional-activity-heartbeat-workflow-failover-test-taskqueue"
	client0, worker0 := s.newClientAndWorker(s.clusters[0].Host().FrontendGRPCAddress(), namespace, taskqueue, "worker0")
	client1, worker1 := s.newClientAndWorker(s.clusters[1].Host().FrontendGRPCAddress(), namespace, taskqueue, "worker1")

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
		time.Sleep(time.Second * 10) // nolint:forbidigo
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
	worker0.RegisterWorkflow(testWorkflowFn)
	worker0.RegisterActivity(activityWithHB)
	s.NoError(worker0.Start())

	// Start a workflow
	workflowID := "functional-activity-heartbeat-workflow-failover-test"
	run1, err := client0.ExecuteWorkflow(testcore.NewContext(), sdkclient.StartWorkflowOptions{
		ID:                 workflowID,
		TaskQueue:          taskqueue,
		WorkflowRunTimeout: time.Second * 300,
	}, testWorkflowFn)

	s.NoError(err)
	s.NotEmpty(run1.GetRunID())

	s.logger.Info("StartWorkflowExecution", tag.WorkflowRunID(run1.GetRunID()))
	// nolint:forbidigo
	time.Sleep(time.Second * 4) // wait for heartbeat from activity to be reported and activity timed out on heartbeat

	worker0.Stop() // stop worker0 so cluster0 won't make any progress
	s.failover(namespace, 0, s.clusters[1].ClusterName(), 2)

	// Make sure the heartbeat details are sent to cluster2 even when the activity at cluster1
	// has heartbeat timeout. Also make sure the information is recorded when the activity state
	// is "Scheduled"
	dweResponse, err := client1.DescribeWorkflowExecution(testcore.NewContext(), workflowID, "")
	s.NoError(err)
	pendingActivities := dweResponse.GetPendingActivities()
	s.Equal(1, len(pendingActivities))
	s.Equal(enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, pendingActivities[0].GetState())
	heartbeatPayload := pendingActivities[0].GetHeartbeatDetails()
	var heartbeatValue int
	s.NoError(payloads.Decode(heartbeatPayload, &heartbeatValue))
	s.Equal(expectedHeartbeatValue, heartbeatValue)
	s.Equal(enumspb.TIMEOUT_TYPE_HEARTBEAT, pendingActivities[0].GetLastFailure().GetTimeoutFailureInfo().GetTimeoutType())
	s.Equal("worker0", pendingActivities[0].GetLastWorkerIdentity())

	// start worker1
	worker1.RegisterWorkflow(testWorkflowFn)
	worker1.RegisterActivity(activityWithHB)
	s.NoError(worker1.Start())
	defer worker1.Stop()

	// ExecuteWorkflow return existing running workflow if it already started
	run2, err := client1.ExecuteWorkflow(testcore.NewContext(), sdkclient.StartWorkflowOptions{
		ID:                 workflowID,
		TaskQueue:          taskqueue,
		WorkflowRunTimeout: time.Second * 300,
	}, testWorkflowFn)
	s.NoError(err)
	// verify we get the same execution as in cluster1
	s.Equal(run1.GetRunID(), run2.GetRunID())

	err = run2.Get(testcore.NewContext(), nil)
	s.NoError(err) // workflow succeed
	s.Equal(2, lastAttemptCount)
}

func (s *FunctionalClustersTestSuite) TestLocalNamespaceMigration() {
	testCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	namespace := s.createNamespaceInCluster0(false)

	taskqueue := "functional-local-ns-to-be-promote-taskqueue"
	client0, worker0 := s.newClientAndWorker(s.clusters[0].Host().FrontendGRPCAddress(), namespace, taskqueue, "worker0")

	testWorkflowFn := func(ctx workflow.Context, sleepInterval time.Duration) error {
		return workflow.Sleep(ctx, sleepInterval)
	}

	worker0.RegisterWorkflow(testWorkflowFn)
	s.NoError(worker0.Start())
	defer worker0.Stop()

	// Start wf1 (in local ns)
	workflowID := "local-ns-wf-1"
	run1, err := client0.ExecuteWorkflow(testCtx, sdkclient.StartWorkflowOptions{
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
	run2, err := client0.ExecuteWorkflow(testCtx, sdkclient.StartWorkflowOptions{
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
	worker0.RegisterWorkflow(wfWithBufferedEvents)

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
	worker0.RegisterWorkflow(wfWithBufferedEvents2)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        workflowID6,
		TaskQueue: taskqueue,
		// Intentionally use same timeout for WorkflowTaskTimeout and WorkflowRunTimeout so if workflow task is not
		// correctly dispatched, it would time out which would fail the workflow and cause test to fail.
		WorkflowTaskTimeout: 40 * time.Second,
		WorkflowRunTimeout:  40 * time.Second,
	}
	run6, err := client0.ExecuteWorkflow(testCtx, workflowOptions, wfWithBufferedEvents)
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
	run7, err := client0.ExecuteWorkflow(testCtx, workflowOptions2, wfWithBufferedEvents2)
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
	err = client0.SignalWorkflow(testCtx, workflowID6, run6.GetRunID(), "signal-name", "signal-value")
	s.NoError(err)

	// promote ns
	s.promoteNamespace(namespace, 0)

	// Start wf1 (in local ns)
	workflowID8 := "global-ns-wf-1"
	run8, err := client0.ExecuteWorkflow(testCtx, sdkclient.StartWorkflowOptions{
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
	err = client0.SignalWorkflow(testCtx, workflowID7, run7.GetRunID(), "signal-name", "signal-value")
	s.NoError(err)
	// send 2 signals to wf7, both would be buffered.
	err = client0.SignalWorkflow(testCtx, workflowID7, run7.GetRunID(), "signal-name", "signal-value2")
	s.NoError(err)

	// update ns to have 2 clusters
	s.updateNamespaceClusters(namespace, 0, s.clusters)

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
	run3, err := client0.ExecuteWorkflow(testCtx, sdkclient.StartWorkflowOptions{
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
		HostPort:  s.clusters[0].Host().FrontendGRPCAddress(),
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
		RemoteCluster:          s.clusters[1].ClusterName(),
		AllowedLaggingSeconds:  10,
		HandoverTimeoutSeconds: 10,
	})
	s.NoError(err)
	err = run5.Get(testCtx, nil)
	s.NoError(err)

	// at this point ns migration is done.
	// verify namespace is now active in cluster2
	nsResp2, err := s.clusters[0].FrontendClient().DescribeNamespace(testCtx, workflowservice.DescribeNamespaceRequest_builder{
		Namespace: namespace,
	}.Build())
	s.NoError(err)
	s.True(nsResp2.GetIsGlobalNamespace())
	s.Equal(2, len(nsResp2.GetReplicationConfig().GetClusters()))
	s.Equal(s.clusters[1].ClusterName(), nsResp2.GetReplicationConfig().GetActiveClusterName())

	// verify all wf in ns is now available in cluster2
	feClient1 := s.clusters[1].FrontendClient()
	adminClient1 := s.clusters[1].AdminClient()
	verify := func(wfID string, expectedRunID string) {
		desc1, err := adminClient1.DescribeMutableState(testCtx, adminservice.DescribeMutableStateRequest_builder{
			Namespace: namespace,
			Execution: commonpb.WorkflowExecution_builder{
				WorkflowId: wfID,
			}.Build(),
			Archetype: chasm.WorkflowArchetype,
		}.Build())
		s.NoError(err)
		s.Equal(expectedRunID, desc1.GetDatabaseMutableState().GetExecutionState().GetRunId())
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, desc1.GetDatabaseMutableState().GetExecutionState().GetStatus())
		expectedEventId := desc1.GetDatabaseMutableState().GetNextEventId() - 1
		var nextPageToken []byte
		for {
			resp, err := feClient1.GetWorkflowExecutionHistoryReverse(testCtx, workflowservice.GetWorkflowExecutionHistoryReverseRequest_builder{
				Namespace: namespace,
				Execution: commonpb.WorkflowExecution_builder{
					WorkflowId: wfID,
					RunId:      expectedRunID,
				}.Build(),
				MaximumPageSize: 256,
				NextPageToken:   nil,
			}.Build())
			s.NoError(err)
			for _, event := range resp.GetHistory().GetEvents() {
				s.Equal(expectedEventId, event.GetEventId())
				expectedEventId--
			}
			if len(nextPageToken) <= 0 {
				break
			}
			nextPageToken = resp.GetNextPageToken()
		}
		s.Equal(int64(0), expectedEventId)
		s.NoError(err)

		listWorkflowResp, err := feClient1.ListClosedWorkflowExecutions(
			testCtx,
			workflowservice.ListClosedWorkflowExecutionsRequest_builder{
				Namespace:       namespace,
				MaximumPageSize: 1000,
				ExecutionFilter: filterpb.WorkflowExecutionFilter_builder{
					WorkflowId: wfID,
				}.Build(),
			}.Build(),
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
	testCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	namespace := s.createNamespaceInCluster0(true)

	taskqueue := "functional-local-force-replication-task-queue"
	client0, worker0 := s.newClientAndWorker(s.clusters[0].Host().FrontendGRPCAddress(), namespace, taskqueue, "worker0")

	testWorkflowFn := func(ctx workflow.Context) error {
		return nil
	}

	worker0.RegisterWorkflow(testWorkflowFn)
	s.NoError(worker0.Start())
	defer worker0.Stop()

	// Start wf1
	workflowID := "force-replication-test-wf-1"
	run1, err := client0.ExecuteWorkflow(testCtx, sdkclient.StartWorkflowOptions{
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

	// Update ns to have 2 clusters
	s.updateNamespaceClusters(namespace, 0, s.clusters)

	// Start force-replicate wf
	sysClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.clusters[0].Host().FrontendGRPCAddress(),
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
	client1, worker1 := s.newClientAndWorker(s.clusters[1].Host().FrontendGRPCAddress(), namespace, taskqueue, "worker1")
	verify := func(wfID string, expectedRunID string) {
		desc1, err := client1.DescribeWorkflowExecution(testCtx, wfID, "")
		s.NoError(err)
		s.Equal(expectedRunID, desc1.GetWorkflowExecutionInfo().GetExecution().GetRunId())
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, desc1.GetWorkflowExecutionInfo().GetStatus())
	}
	verify(workflowID, run1.GetRunID())

	s.failover(namespace, 0, s.clusters[1].ClusterName(), 2)

	worker1.RegisterWorkflow(testWorkflowFn)
	s.NoError(worker1.Start())
	defer worker1.Stop()

	// Test reset workflow in cluster1
	resetResp, err := client1.ResetWorkflowExecution(testCtx, workflowservice.ResetWorkflowExecutionRequest_builder{
		Namespace: namespace,
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: workflowID,
			RunId:      run1.GetRunID(),
		}.Build(),
		Reason:                    "force-replication-test",
		WorkflowTaskFinishEventId: 3,
		RequestId:                 uuid.NewString(),
	}.Build())
	s.NoError(err)

	resetRun := client1.GetWorkflow(testCtx, workflowID, resetResp.GetRunId())
	err = resetRun.Get(testCtx, nil)
	s.NoError(err)

	descResp, err := client1.DescribeWorkflowExecution(testCtx, workflowID, resetResp.GetRunId())
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, descResp.GetWorkflowExecutionInfo().GetStatus())
}

func (s *FunctionalClustersTestSuite) TestForceMigration_ResetWorkflow() {
	testCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	namespace := s.createNamespaceInCluster0(true)

	taskqueue := "functional-force-replication-reset-task-queue"
	client0, worker0 := s.newClientAndWorker(s.clusters[0].Host().FrontendGRPCAddress(), namespace, taskqueue, "worker0")

	testWorkflowFn := func(ctx workflow.Context) error {
		return nil
	}

	worker0.RegisterWorkflow(testWorkflowFn)
	s.NoError(worker0.Start())
	defer worker0.Stop()

	// Start wf1
	workflowID := "force-replication-test-reset-wf-1"
	run1, err := client0.ExecuteWorkflow(testCtx, sdkclient.StartWorkflowOptions{
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

	resp, err := client0.ResetWorkflowExecution(testCtx, workflowservice.ResetWorkflowExecutionRequest_builder{
		Namespace: namespace,
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: workflowID,
			RunId:      run1.GetRunID(),
		}.Build(),
		Reason:                    "test",
		WorkflowTaskFinishEventId: 3,
		RequestId:                 uuid.NewString(),
	}.Build())
	s.NoError(err)
	resetRun := client0.GetWorkflow(testCtx, workflowID, resp.GetRunId())
	err = resetRun.Get(testCtx, nil)
	s.NoError(err)

	// Update ns to have 2 clusters
	s.updateNamespaceClusters(namespace, 0, s.clusters)

	// Start force-replicate wf
	sysClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.clusters[0].Host().FrontendGRPCAddress(),
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

	s.waitForClusterSynced()

	// Verify all wf in ns is now available in cluster2
	client1, _ := s.newClientAndWorker(s.clusters[1].Host().FrontendGRPCAddress(), namespace, taskqueue, "worker1")
	verifyHistory := func(wfID string, runID string) {
		iter1 := client0.GetWorkflowHistory(testCtx, wfID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		iter2 := client1.GetWorkflowHistory(testCtx, wfID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
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

func (s *FunctionalClustersTestSuite) TestBlockNamespaceDeleteInPassiveCluster() {
	namespace := s.createGlobalNamespace()

	// cluster2 is passive.
	resp, err := s.clusters[1].OperatorClient().DeleteNamespace(
		testcore.NewContext(),
		operatorservice.DeleteNamespaceRequest_builder{
			Namespace: namespace,
		}.Build())
	s.Error(err)
	s.Nil(resp)
	s.Contains(err.Error(), "is passive in current cluster")
	s.Contains(err.Error(), "make namespace active in this cluster and retry")
}

func (s *FunctionalClustersTestSuite) getHistory(client workflowservice.WorkflowServiceClient, namespace string, execution *commonpb.WorkflowExecution) []*historypb.HistoryEvent {
	historyResponse, err := client.GetWorkflowExecutionHistory(testcore.NewContext(), workflowservice.GetWorkflowExecutionHistoryRequest_builder{
		Namespace:       namespace,
		Execution:       execution,
		MaximumPageSize: 5, // Use small page size to force pagination code path
	}.Build())
	s.NoError(err)

	events := historyResponse.GetHistory().GetEvents()
	for len(historyResponse.GetNextPageToken()) != 0 {
		historyResponse, err = client.GetWorkflowExecutionHistory(testcore.NewContext(), workflowservice.GetWorkflowExecutionHistoryRequest_builder{
			Namespace:     namespace,
			Execution:     execution,
			NextPageToken: historyResponse.GetNextPageToken(),
		}.Build())
		s.NoError(err)
		events = append(events, historyResponse.GetHistory().GetEvents()...)
	}

	return events
}

func TestFuncClustersWithRedirectionTestSuite(t *testing.T) {
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
			s := &FunctionalClustersWithRedirectionTestSuite{}
			s.enableTransitionHistory = tc.enableTransitionHistory
			suite.Run(t, s)
		})
	}
}

func (s *FunctionalClustersWithRedirectionTestSuite) SetupSuite() {
	s.setupSuite(
		testcore.WithFxOptionsForService(primitives.FrontendService,
			fx.Decorate(func(_ config.DCRedirectionPolicy) config.DCRedirectionPolicy {
				return config.DCRedirectionPolicy{Policy: "all-apis-forwarding"}
			}),
		),
	)
}

func (s *FunctionalClustersWithRedirectionTestSuite) SetupTest() {
	s.setupTest()
}

func (s *FunctionalClustersWithRedirectionTestSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *FunctionalClustersWithRedirectionTestSuite) TestActivityMultipleHeartbeatsAcrossFailover() {
	namespace := s.createGlobalNamespace()

	taskqueue := "functional-activity-multi-heartbeat-failover-test-taskqueue"
	client0, worker0 := s.newClientAndWorker(s.clusters[0].Host().FrontendGRPCAddress(), namespace, taskqueue, "worker0")

	// Orchestration channels
	hb1Ch := make(chan struct{}, 1)
	hb2Ch := make(chan struct{}, 1)
	hb3Ch := make(chan struct{}, 1)
	allowFailover := make(chan struct{})
	allowComplete := make(chan struct{})

	// Values to heartbeat in sequence
	hb1Val := 1
	hb2Val := 2
	hb3Val := 3

	activityWithMultipleHB := func(ctx context.Context) error {
		// Heartbeat before failover
		activity.RecordHeartbeat(ctx, hb1Val)
		select {
		case hb1Ch <- struct{}{}:
		default:
		}
		// wait for failover
		<-allowFailover

		// After failover, verify we can still heartbeat and complete
		if activity.HasHeartbeatDetails(ctx) {
			var v int
			_ = activity.GetHeartbeatDetails(ctx, &v)
		}
		activity.RecordHeartbeat(ctx, hb2Val)
		select {
		case hb2Ch <- struct{}{}:
		default:
		}
		activity.RecordHeartbeat(ctx, hb3Val)
		select {
		case hb3Ch <- struct{}{}:
		default:
		}
		<-allowComplete
		return nil
	}

	testWorkflowFn := func(ctx workflow.Context) error {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: time.Second * 120,
			HeartbeatTimeout:    time.Second * 10,
		}
		ctx = workflow.WithActivityOptions(ctx, ao)
		return workflow.ExecuteActivity(ctx, activityWithMultipleHB).Get(ctx, nil)
	}

	worker0.RegisterWorkflow(testWorkflowFn)
	worker0.RegisterActivity(activityWithMultipleHB)
	s.NoError(worker0.Start())
	defer worker0.Stop()

	// Start a workflow
	workflowID := "functional-activity-multi-heartbeat-failover-test"
	run, err := client0.ExecuteWorkflow(testcore.NewContext(), sdkclient.StartWorkflowOptions{
		ID:                 workflowID,
		TaskQueue:          taskqueue,
		WorkflowRunTimeout: time.Second * 300,
	}, testWorkflowFn)
	s.NoError(err)
	s.NotEmpty(run.GetRunID())

	// Wait for first heartbeat to be sent
	<-hb1Ch

	// Validate heartbeat1 is visible before failover (eventually)
	var hbVal int
	s.Eventually(func() bool {
		desc0, err := client0.DescribeWorkflowExecution(testcore.NewContext(), workflowID, "")
		if err != nil || len(desc0.GetPendingActivities()) != 1 {
			return false
		}
		hbVal = 0
		if err := payloads.Decode(desc0.GetPendingActivities()[0].GetHeartbeatDetails(), &hbVal); err != nil {
			return false
		}
		return hbVal == hb1Val
	}, 10*time.Second, 200*time.Millisecond)

	s.failover(namespace, 0, s.clusters[1].ClusterName(), 2)
	// nolint:forbidigo
	time.Sleep(time.Second * 4)

	close(allowFailover)
	// Wait for heartbeats from second attempt
	<-hb2Ch
	<-hb3Ch

	// Validate latest heartbeat is visible in new active cluster (eventually)
	s.Eventually(func() bool {
		desc1, err := client0.DescribeWorkflowExecution(testcore.NewContext(), workflowID, "")
		if err != nil || len(desc1.GetPendingActivities()) != 1 {
			return false
		}
		hbVal = 0
		if err := payloads.Decode(desc1.GetPendingActivities()[0].GetHeartbeatDetails(), &hbVal); err != nil {
			return false
		}
		return hbVal == hb3Val
	}, 10*time.Second, 200*time.Millisecond)

	// Complete the activity and workflow
	close(allowComplete)

	s.NoError(run.Get(testcore.NewContext(), nil))
}
