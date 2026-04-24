package tests

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/tests/testcore"
)

type QueryWorkflowSuite struct {
	parallelsuite.Suite[*QueryWorkflowSuite]
}

func TestQueryWorkflowSuite(t *testing.T) {
	parallelsuite.Run(t, &QueryWorkflowSuite{})
}

func (s *QueryWorkflowSuite) TestQueryWorkflow_Sticky() {
	env := testcore.NewEnv(s.T())
	var replayCount int32
	workflowFn := func(ctx workflow.Context) (string, error) {
		// every replay will start from here
		atomic.AddInt32(&replayCount, 1)

		_ = workflow.SetQueryHandler(ctx, "test", func() (string, error) {
			return "query works", nil
		})

		signalCh := workflow.GetSignalChannel(ctx, "test")
		var msg string
		signalCh.Receive(ctx, &msg)
		return msg, nil
	}

	env.SdkWorker().RegisterWorkflow(workflowFn)

	id := "test-query-sticky"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 id,
		TaskQueue:          env.WorkerTaskQueue(),
		WorkflowRunTimeout: 20 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	s.NotNil(workflowRun)
	s.NotEmpty(workflowRun.GetRunID())

	queryResult, err := env.SdkClient().QueryWorkflow(ctx, id, "", "test", "test")
	s.NoError(err)

	var queryResultStr string
	err = queryResult.Get(&queryResultStr)
	s.NoError(err)
	s.Equal("query works", queryResultStr)

	// verify query is handed by sticky worker (no replay)
	s.Equal(int32(1), replayCount)
}

//nolint:forbidigo
func (s *QueryWorkflowSuite) TestQueryWorkflow_Consistent_PiggybackQuery() {
	env := testcore.NewEnv(s.T())
	workflowFn := func(ctx workflow.Context) (string, error) {
		var receivedMsgs string
		_ = workflow.SetQueryHandler(ctx, "test", func() (string, error) {
			return receivedMsgs, nil
		})

		signalCh := workflow.GetSignalChannel(ctx, "test")
		for {
			var msg string
			signalCh.Receive(ctx, &msg)
			receivedMsgs += msg
			if msg == "pause" {
				// block workflow task for 3s.
				_ = workflow.ExecuteLocalActivity(ctx, func() {
					time.Sleep(time.Second * 3)
				}).Get(ctx, nil)
			}
		}
	}

	env.SdkWorker().RegisterWorkflow(workflowFn)

	id := "test-query-consistent"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 id,
		TaskQueue:          env.WorkerTaskQueue(),
		WorkflowRunTimeout: 20 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	s.NotNil(workflowRun)
	s.NotEmpty(workflowRun.GetRunID())

	err = env.SdkClient().SignalWorkflow(ctx, id, "", "test", "pause")
	s.NoError(err)

	err = env.SdkClient().SignalWorkflow(ctx, id, "", "test", "abc")
	s.NoError(err)

	queryResult, err := env.SdkClient().QueryWorkflow(ctx, id, "", "test")
	s.NoError(err)

	var queryResultStr string
	err = queryResult.Get(&queryResultStr)
	s.NoError(err)

	// verify query sees all signals before it
	s.Equal("pauseabc", queryResultStr)
}

func (s *QueryWorkflowSuite) TestQueryWorkflow_QueryWhileBackoff() {
	env := testcore.NewEnv(s.T())
	tv := testvars.New(s.T())
	workflowFn := func(ctx workflow.Context) error {
		_ = workflow.SetQueryHandler(ctx, tv.QueryType(), func() (string, error) {
			return tv.Any().String(), nil
		})
		return nil
	}
	env.SdkWorker().RegisterWorkflow(workflowFn)

	testCases := []struct {
		testName       string
		contextTimeout time.Duration
		startDelay     time.Duration
		err            error
	}{
		{
			testName:       "backoff query will fail",
			contextTimeout: 10 * time.Second,
			startDelay:     5 * time.Second,
			err:            nil,
		},
		{
			testName:       "backoff query will pass",
			contextTimeout: 8 * time.Second,
			startDelay:     10 * time.Second,
			err:            consts.ErrWorkflowTaskNotScheduled,
		},
	}

	for tci, tc := range testCases {
		tvi := tv.WithWorkflowIDNumber(tci)
		s.Run(tc.testName, func(s *QueryWorkflowSuite) {
			workflowOptions := sdkclient.StartWorkflowOptions{
				ID:         tvi.WorkflowID(),
				TaskQueue:  env.WorkerTaskQueue(),
				StartDelay: tc.startDelay,
			}

			// contextTimeout is not going to be the provided timeout.
			// It is going to be timeout / 2. See newGRPCContext implementation.
			ctx, cancel := context.WithTimeout(context.Background(), 2*tc.contextTimeout)
			defer cancel()

			env.Logger.Info(fmt.Sprintf("Start workflow with delay %v", tc.startDelay))
			workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
			s.NoError(err, "Start workflow failed")
			s.NotNil(workflowRun)
			s.NotEmpty(workflowRun.GetRunID())

			queryResp, err := env.SdkClient().QueryWorkflow(ctx, tvi.WorkflowID(), workflowRun.GetRunID(), tvi.QueryType())

			if tc.err != nil {
				s.Error(err)
				s.ErrorContains(err, tc.err.Error())
				s.Nil(queryResp)
			} else {
				s.NoError(err)
				s.NotNil(queryResp)
			}
		})
	}
}

func (s *QueryWorkflowSuite) TestQueryWorkflow_QueryBeforeStart() {
	env := testcore.NewEnv(s.T())
	// stop the worker, so the workflow won't be started before query
	env.SdkWorker().Stop()

	workflowFn := func(ctx workflow.Context) (string, error) {
		status := "initialized"
		_ = workflow.SetQueryHandler(ctx, "test", func() (string, error) {
			return status, nil
		})

		status = "started"
		_ = workflow.Sleep(ctx, time.Hour)
		return "", nil
	}

	id := "test-query-before-start"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 id,
		TaskQueue:          env.WorkerTaskQueue(),
		WorkflowRunTimeout: 20 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	s.NotNil(workflowRun)
	s.NotEmpty(workflowRun.GetRunID())

	var queryErr, getErr error
	var queryResultStr string
	var queryDuration time.Duration
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		startTime := time.Now()
		queryResult, err := env.SdkClient().QueryWorkflow(ctx, id, "", "test")
		queryDuration = time.Since(startTime)
		queryErr = err
		if err == nil {
			getErr = queryResult.Get(&queryResultStr)
		}
	}()

	// delay 2s to start worker, this will block query for 2s
	time.Sleep(time.Second * 2) //nolint:forbidigo
	var queryWorker worker.Worker

	queryWorker = worker.New(env.SdkClient(), env.WorkerTaskQueue(), worker.Options{})
	queryWorker.RegisterWorkflow(workflowFn)
	err = queryWorker.Start()
	s.NoError(err)

	// wait query
	wg.Wait()

	s.NoError(queryErr)
	s.NoError(getErr)
	// verify query sees all signals before it
	s.Equal("started", queryResultStr)
	s.Greater(queryDuration, time.Second)
}

func (s *QueryWorkflowSuite) TestQueryWorkflow_QueryFailedWorkflowTask() {
	env := testcore.NewEnv(s.T())
	testname := s.T().Name()
	var failures int32
	workflowFn := func(ctx workflow.Context) (string, error) {
		err := workflow.SetQueryHandler(ctx, testname, func() (string, error) {
			return "", nil
		})

		if err != nil {
			s.T().Fatalf("SetQueryHandler failed: %s", err.Error())
		}
		atomic.AddInt32(&failures, 1)
		// force workflow task to fail
		panic("Workflow failed")
	}

	env.SdkWorker().RegisterWorkflow(workflowFn)

	id := "test-query-failed-workflow-task"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                  id,
		TaskQueue:           env.WorkerTaskQueue(),
		WorkflowTaskTimeout: time.Second * 1, // use shorter wft timeout to make this test faster
		WorkflowRunTimeout:  20 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	s.NotNil(workflowRun)
	s.NotEmpty(workflowRun.GetRunID())

	s.Eventually(func() bool {
		// wait for workflow task to fail 3 times
		return atomic.LoadInt32(&failures) >= 3
	}, 10*time.Second, 50*time.Millisecond)

	_, err = env.SdkClient().QueryWorkflow(ctx, id, "", testname)
	s.Error(err)
	s.IsType(&serviceerror.WorkflowNotReady{}, err)

}

func (s *QueryWorkflowSuite) TestQueryWorkflow_ClosedWithoutWorkflowTaskStarted() {
	env := testcore.NewEnv(s.T())
	testname := s.T().Name()
	workflowFn := func(ctx workflow.Context) (string, error) {
		return "", errors.New("workflow should never execute") //nolint:err113
	}
	id := "test-query-after-terminate"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        id,
		TaskQueue: env.WorkerTaskQueue(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)
	s.NotNil(workflowRun)
	s.NotEmpty(workflowRun.GetRunID())

	err = env.SdkClient().TerminateWorkflow(ctx, id, "", "terminating to make sure query fails")
	s.NoError(err)

	_, err = env.SdkClient().QueryWorkflow(ctx, id, "", testname)
	s.Error(err)
	s.ErrorContains(err, consts.ErrWorkflowClosedBeforeWorkflowTaskStarted.Error())
}

// TestQueryWorkflow_NonStickyMultiPageHistory verifies that the NextPageToken from a
// non-sticky query task poll is a valid HistoryContinuation token usable with
// GetWorkflowExecutionHistory. Fails with "Invalid NextPageToken" if matching service
// returns a RawHistoryContinuation token instead.
// Uses a dedicated cluster with MatchingHistoryMaxPageSize=14 (for query tasks) and
// HistoryMaxPageSize=14 (for regular workflow tasks via RecordWorkflowTaskStarted).
// With 5 activities generating ~16 history blobs, page size 14 ensures any task type
// produces a multi-page history response with a non-empty NextPageToken. 14 is large
// enough that activity-phase WFTs (≤14 blobs) don't need extra pagination roundtrips.
func (s *QueryWorkflowSuite) TestQueryWorkflow_NonStickyMultiPageHistory() {
	env := testcore.NewEnv(s.T(),
		testcore.WithDedicatedCluster(),
		testcore.WithDynamicConfig(dynamicconfig.MatchingHistoryMaxPageSize, 14),
		testcore.WithDynamicConfig(dynamicconfig.HistoryMaxPageSize, 14),
	)

	activityFn := func(ctx context.Context) error { return nil }
	workflowFn := func(ctx workflow.Context) (string, error) {
		_ = workflow.SetQueryHandler(ctx, "test", func() (string, error) {
			return "query works", nil
		})
		// Run activities to generate multiple event batches in history.
		ao := workflow.ActivityOptions{StartToCloseTimeout: 5 * time.Second}
		actCtx := workflow.WithActivityOptions(ctx, ao)
		for range 5 {
			_ = workflow.ExecuteActivity(actCtx, activityFn).Get(ctx, nil)
		}
		// Keep workflow alive for query.
		workflow.GetSignalChannel(ctx, "done").Receive(ctx, nil)
		return "done", nil
	}

	tq := env.WorkerTaskQueue()
	id := "test-query-non-sticky-multi-page"
	ctx, cancel := context.WithTimeout(env.Context(), 30*time.Second)
	defer cancel()

	queryWorker := worker.New(env.SdkClient(), tq, worker.Options{})
	queryWorker.RegisterWorkflow(workflowFn)
	queryWorker.RegisterActivity(activityFn)
	s.NoError(queryWorker.Start())

	workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                 id,
		TaskQueue:          tq,
		WorkflowRunTimeout: 20 * time.Second,
	}, workflowFn)
	s.NoError(err)
	s.NotNil(workflowRun)

	// Wait for all activities to complete, generating many event batches.
	s.Eventually(func() bool {
		resp, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: id},
		})
		return err == nil && resp.GetWorkflowExecutionInfo().GetHistoryLength() > 30
	}, 10*time.Second, 200*time.Millisecond)

	// Stop worker so the query goes through the non-sticky path.
	queryWorker.Stop()

	// Clear stickiness so the query dispatches directly to the normal queue
	// without waiting for StickyScheduleToStartTimeout (~5s).
	_, err = env.FrontendClient().ResetStickyTaskQueue(ctx, &workflowservice.ResetStickyTaskQueueRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: id},
	})
	s.NoError(err)

	// Terminate the workflow to prevent any further workflow tasks from being scheduled.
	err = env.SdkClient().TerminateWorkflow(ctx, id, "", "test cleanup")
	s.NoError(err)

	go func() { _, _ = env.SdkClient().QueryWorkflow(ctx, id, "", "test") }()

	// Poll for the query task on the normal (non-sticky) task queue.
	var pollResp *workflowservice.PollWorkflowTaskQueueResponse
	s.Eventually(func() bool {
		pollCtx, pollCancel := context.WithTimeout(ctx, 3*time.Second)
		defer pollCancel()
		pollResp, err = env.FrontendClient().PollWorkflowTaskQueue(pollCtx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  "test-worker",
		})
		return err == nil && len(pollResp.GetTaskToken()) > 0
	}, 20*time.Second, 100*time.Millisecond)

	s.NotNil(pollResp.GetHistory())
	s.NotEmpty(pollResp.GetNextPageToken(), "multi-page history should have NextPageToken")

	// Use the token with GetWorkflowExecutionHistory — this is what the worker SDK does.
	// Fails with "Invalid NextPageToken" if the token is a RawHistoryContinuation.
	histResp, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:     env.Namespace().String(),
		Execution:     &commonpb.WorkflowExecution{WorkflowId: id},
		NextPageToken: pollResp.GetNextPageToken(),
	})
	s.NoError(err)
	s.NotNil(histResp)
}

func (s *QueryWorkflowSuite) TestQueryWorkflow_FailurePropagated() {
	env := testcore.NewEnv(s.T())
	ctx := env.Context()
	taskQueue := testcore.RandomizeStr(s.T().Name())

	workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: taskQueue}, "workflow")
	s.NoError(err)

	// Create a channel for errors generated in background goroutines.
	errChan := make(chan error, 1)

	// First query, should come in the workflow task Queries field and responded to via the RespondWorkflowTaskCompleted
	// API.
	// Query the workflow in the background to have the query delivered with the first workflow task in the Queries map.
	go func() {
		_, err := env.FrontendClient().QueryWorkflow(ctx, &workflowservice.QueryWorkflowRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowRun.GetID(),
			},
			Query: &querypb.WorkflowQuery{
				QueryType: "dont-care",
			},
		})
		errChan <- err
	}()

	// Hope that 3 seconds will be enough for history to record the query and attach it to the pending workflow task.
	// There's really no other way to ensure that the query is included in the task unfortunately.
	util.InterruptibleSleep(ctx, 3*time.Second)

	task, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue},
		Identity:  s.T().Name(),
	})
	s.NoError(err)
	s.Len(task.Queries, 1)
	qKey := slices.Collect(maps.Keys(task.Queries))[0]

	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken: task.TaskToken,
		Identity:  s.T().Name(),
		QueryResults: map[string]*querypb.WorkflowQueryResult{
			qKey: {
				ResultType:   enumspb.QUERY_RESULT_TYPE_FAILED,
				ErrorMessage: "my error message",
				Failure: &failurepb.Failure{
					Message: "my failure error message",
				},
			},
		},
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{},
			},
		},
	})
	s.NoError(err)

	select {
	case err = <-errChan:
	case <-ctx.Done():
		// Abort and fail the test.
		s.NoError(ctx.Err())
	}

	var query1FailedErr *serviceerror.QueryFailed
	s.ErrorAs(err, &query1FailedErr)
	s.Equal("my error message", query1FailedErr.Message)
	s.Equal("my failure error message", query1FailedErr.Failure.Message)

	// Second query, should come in the workflow task Query field and responded to via the RespondQueryTaskCompleted
	// API.
	go func() {
		task, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue},
			Identity:  s.T().Name(),
		})
		if err != nil {
			errChan <- err
			return
		}

		_, err = env.FrontendClient().RespondQueryTaskCompleted(ctx, &workflowservice.RespondQueryTaskCompletedRequest{
			Namespace:     env.Namespace().String(),
			TaskToken:     task.TaskToken,
			CompletedType: enumspb.QUERY_RESULT_TYPE_FAILED,
			ErrorMessage:  "my error message",
			Failure: &failurepb.Failure{
				Message: "my failure error message",
			},
		})

		errChan <- err
	}()

	_, err = env.FrontendClient().QueryWorkflow(ctx, &workflowservice.QueryWorkflowRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Query: &querypb.WorkflowQuery{
			QueryType: "dont-care",
		},
	})

	var query2FailedErr *serviceerror.QueryFailed
	s.ErrorAs(err, &query2FailedErr)
	s.Equal("my error message", query2FailedErr.Message)
	s.Equal("my failure error message", query2FailedErr.Failure.Message)

	select {
	case err = <-errChan:
		s.NoError(err)
	case <-ctx.Done():
		// Abort and fail the test.
		s.NoError(ctx.Err())
	}
}
