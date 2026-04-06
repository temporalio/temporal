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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
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
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/tests/testcore"
)

type QueryWorkflowSuite struct {
	testcore.FunctionalTestBase
}

func TestQueryWorkflowSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(QueryWorkflowSuite))
}

func (s *QueryWorkflowSuite) TestQueryWorkflow_Sticky() {
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

	s.SdkWorker().RegisterWorkflow(workflowFn)

	id := "test-query-sticky"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 id,
		TaskQueue:          s.TaskQueue(),
		WorkflowRunTimeout: 20 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	s.NotNil(workflowRun)
	s.NotEmpty(workflowRun.GetRunID())

	queryResult, err := s.SdkClient().QueryWorkflow(ctx, id, "", "test", "test")
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

	s.SdkWorker().RegisterWorkflow(workflowFn)

	id := "test-query-consistent"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 id,
		TaskQueue:          s.TaskQueue(),
		WorkflowRunTimeout: 20 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	s.NotNil(workflowRun)
	s.NotEmpty(workflowRun.GetRunID())

	err = s.SdkClient().SignalWorkflow(ctx, id, "", "test", "pause")
	s.NoError(err)

	err = s.SdkClient().SignalWorkflow(ctx, id, "", "test", "abc")
	s.NoError(err)

	queryResult, err := s.SdkClient().QueryWorkflow(ctx, id, "", "test")
	s.NoError(err)

	var queryResultStr string
	err = queryResult.Get(&queryResultStr)
	s.NoError(err)

	// verify query sees all signals before it
	s.Equal("pauseabc", queryResultStr)
}

func (s *QueryWorkflowSuite) TestQueryWorkflow_QueryWhileBackoff() {

	tv := testvars.New(s.T())
	workflowFn := func(ctx workflow.Context) error {
		_ = workflow.SetQueryHandler(ctx, tv.QueryType(), func() (string, error) {
			return tv.Any().String(), nil
		})
		return nil
	}
	s.SdkWorker().RegisterWorkflow(workflowFn)

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
		s.T().Run(tc.testName, func(t *testing.T) {
			tv = tv.WithWorkflowIDNumber(tci)
			workflowOptions := sdkclient.StartWorkflowOptions{
				ID:         tv.WorkflowID(),
				TaskQueue:  s.TaskQueue(),
				StartDelay: tc.startDelay,
			}

			// contextTimeout is not going to be the provided timeout.
			// It is going to be timeout / 2. See newGRPCContext implementation.
			ctx, cancel := context.WithTimeout(context.Background(), 2*tc.contextTimeout)
			defer cancel()

			t.Log(fmt.Sprintf("Start workflow with delay %v", tc.startDelay))
			workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
			assert.NoError(t, err, "Start workflow failed")
			assert.NotNil(t, workflowRun)
			assert.NotEmpty(t, workflowRun.GetRunID())

			queryResp, err := s.SdkClient().QueryWorkflow(ctx, tv.WorkflowID(), workflowRun.GetRunID(), tv.QueryType())

			if tc.err != nil {
				assert.Error(t, err)
				assert.ErrorContains(t, err, tc.err.Error())
				assert.Nil(t, queryResp)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, queryResp)
			}
		})
	}
}

func (s *QueryWorkflowSuite) TestQueryWorkflow_QueryBeforeStart() {
	// stop the worker, so the workflow won't be started before query
	s.SdkWorker().Stop()

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
		TaskQueue:          s.TaskQueue(),
		WorkflowRunTimeout: 20 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
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
		queryResult, err := s.SdkClient().QueryWorkflow(ctx, id, "", "test")
		queryDuration = time.Since(startTime)
		queryErr = err
		if err == nil {
			getErr = queryResult.Get(&queryResultStr)
		}
	}()

	// delay 2s to start worker, this will block query for 2s
	time.Sleep(time.Second * 2) //nolint:forbidigo
	var queryWorker worker.Worker

	queryWorker = worker.New(s.SdkClient(), s.TaskQueue(), worker.Options{})
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

	s.SdkWorker().RegisterWorkflow(workflowFn)

	id := "test-query-failed-workflow-task"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                  id,
		TaskQueue:           s.TaskQueue(),
		WorkflowTaskTimeout: time.Second * 1, // use shorter wft timeout to make this test faster
		WorkflowRunTimeout:  20 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	s.NotNil(workflowRun)
	s.NotEmpty(workflowRun.GetRunID())

	s.Eventually(func() bool {
		// wait for workflow task to fail 3 times
		return atomic.LoadInt32(&failures) >= 3
	}, 10*time.Second, 50*time.Millisecond)

	_, err = s.SdkClient().QueryWorkflow(ctx, id, "", testname)
	s.Error(err)
	s.IsType(&serviceerror.WorkflowNotReady{}, err)

}

func (s *QueryWorkflowSuite) TestQueryWorkflow_ClosedWithoutWorkflowTaskStarted() {
	testname := s.T().Name()
	workflowFn := func(ctx workflow.Context) (string, error) {
		return "", errors.New("workflow should never execute") //nolint:err113
	}
	id := "test-query-after-terminate"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        id,
		TaskQueue: s.TaskQueue(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)
	s.NotNil(workflowRun)
	s.NotEmpty(workflowRun.GetRunID())

	err = s.SdkClient().TerminateWorkflow(ctx, id, "", "terminating to make sure query fails")
	s.NoError(err)

	_, err = s.SdkClient().QueryWorkflow(ctx, id, "", testname)
	s.Error(err)
	s.ErrorContains(err, consts.ErrWorkflowClosedBeforeWorkflowTaskStarted.Error())
}

// TestQueryWorkflow_NonStickyMultiPageHistory verifies that the NextPageToken from a
// non-sticky query task poll is a valid HistoryContinuation token usable with
// GetWorkflowExecutionHistory. Fails with "Invalid NextPageToken" if matching service
// returns a RawHistoryContinuation token instead.
func (s *QueryWorkflowSuite) TestQueryWorkflow_NonStickyMultiPageHistory() {
	// Small page size forces pagination in matching service.
	s.OverrideDynamicConfig(dynamicconfig.MatchingHistoryMaxPageSize, 2)

	s.SdkWorker().Stop()

	signalCount := 5
	workflowFn := func(ctx workflow.Context) (string, error) {
		_ = workflow.SetQueryHandler(ctx, "test", func() (string, error) {
			return "query works", nil
		})
		signalCh := workflow.GetSignalChannel(ctx, "done")
		for i := 0; i < signalCount; i++ {
			signalCh.Receive(ctx, nil)
		}
		return "done", nil
	}

	id := "test-query-non-sticky-multi-page"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queryWorker := worker.New(s.SdkClient(), s.TaskQueue(), worker.Options{})
	queryWorker.RegisterWorkflow(workflowFn)
	s.NoError(queryWorker.Start())

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                 id,
		TaskQueue:          s.TaskQueue(),
		WorkflowRunTimeout: 20 * time.Second,
	}, workflowFn)
	s.NoError(err)
	s.NotNil(workflowRun)

	// Send signals to build up history events beyond the page size.
	for i := 0; i < signalCount; i++ {
		s.NoError(s.SdkClient().SignalWorkflow(ctx, id, "", "done", nil))
		time.Sleep(500 * time.Millisecond)
	}

	// Wait for all signals to be processed.
	s.Eventually(func() bool {
		resp, err := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: id},
		})
		return err == nil && resp.GetWorkflowExecutionInfo().GetHistoryLength() > 10
	}, 10*time.Second, 200*time.Millisecond)

	// Stop worker to clear sticky cache so the query goes through non-sticky path.
	queryWorker.Stop()

	// Issue a query in background; we'll poll for the task manually below.
	go func() { _, _ = s.SdkClient().QueryWorkflow(ctx, id, "", "test") }()

	// Poll for the query task on the normal (non-sticky) task queue.
	var pollResp *workflowservice.PollWorkflowTaskQueueResponse
	s.Eventually(func() bool {
		pollCtx, pollCancel := context.WithTimeout(ctx, 3*time.Second)
		defer pollCancel()
		pollResp, err = s.FrontendClient().PollWorkflowTaskQueue(pollCtx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: s.TaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  "test-worker",
		})
		return err == nil && len(pollResp.GetTaskToken()) > 0
	}, 10*time.Second, 100*time.Millisecond)

	s.NotNil(pollResp.GetHistory())
	s.NotEmpty(pollResp.GetNextPageToken(), "multi-page history should have NextPageToken")

	// Use the token with GetWorkflowExecutionHistory — this is what the worker SDK does.
	// Fails with "Invalid NextPageToken" if the token is a RawHistoryContinuation.
	histResp, err := s.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:     s.Namespace().String(),
		Execution:     &commonpb.WorkflowExecution{WorkflowId: id},
		NextPageToken: pollResp.GetNextPageToken(),
	})
	s.NoError(err)
	s.NotNil(histResp)
}

func (s *QueryWorkflowSuite) TestQueryWorkflow_FailurePropagated() {
	ctx := testcore.NewContext()
	taskQueue := testcore.RandomizeStr(s.T().Name())

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: taskQueue}, "workflow")
	s.NoError(err)

	// Create a channel for errors generated in background goroutines.
	errChan := make(chan error, 1)

	// First query, should come in the workflow task Queries field and responded to via the RespondWorkflowTaskCompleted
	// API.
	// Query the workflow in the background to have the query delivered with the first workflow task in the Queries map.
	go func() {
		_, err := s.FrontendClient().QueryWorkflow(ctx, &workflowservice.QueryWorkflowRequest{
			Namespace: s.Namespace().String(),
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

	task, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue},
		Identity:  s.T().Name(),
	})
	s.NoError(err)
	s.Len(task.Queries, 1)
	qKey := slices.Collect(maps.Keys(task.Queries))[0]

	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
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
		task, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue},
			Identity:  s.T().Name(),
		})
		if err != nil {
			errChan <- err
			return
		}

		_, err = s.FrontendClient().RespondQueryTaskCompleted(ctx, &workflowservice.RespondQueryTaskCompletedRequest{
			Namespace:     s.Namespace().String(),
			TaskToken:     task.TaskToken,
			CompletedType: enumspb.QUERY_RESULT_TYPE_FAILED,
			ErrorMessage:  "my error message",
			Failure: &failurepb.Failure{
				Message: "my failure error message",
			},
		})

		errChan <- err
	}()

	_, err = s.FrontendClient().QueryWorkflow(ctx, &workflowservice.QueryWorkflowRequest{
		Namespace: s.Namespace().String(),
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
