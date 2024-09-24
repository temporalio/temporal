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

package tests

import (
	"context"
	"fmt"
	commonpb "go.temporal.io/api/common/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"time"

	"github.com/pborman/uuid"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/multierr"
)

type ClientMiscTestSuite struct {
	testcore.ClientFunctionalSuite
	maxPendingSignals         int
	maxPendingCancelRequests  int
	maxPendingActivities      int
	maxPendingChildExecutions int
}

func (s *ClientMiscTestSuite) SetupSuite() {
	s.ClientFunctionalSuite.SetupSuite()
	s.maxPendingSignals = testcore.ClientSuiteLimit
	s.maxPendingCancelRequests = testcore.ClientSuiteLimit
	s.maxPendingActivities = testcore.ClientSuiteLimit
	s.maxPendingChildExecutions = testcore.ClientSuiteLimit

}
func (s *ClientMiscTestSuite) TestTooManyChildWorkflows() {
	// To ensure that there is one pending child workflow before we try to create the next one,
	// we create a child workflow here that signals the parent when it has started and then blocks forever.
	parentWorkflowId := "client-func-too-many-child-workflows"
	blockingChildWorkflow := func(ctx workflow.Context) error {
		workflow.SignalExternalWorkflow(ctx, parentWorkflowId, "", "blocking-child-started", nil)
		workflow.GetSignalChannel(ctx, "unblock-child").Receive(ctx, nil)
		return nil
	}
	childWorkflow := func(ctx workflow.Context) error {
		return nil
	}

	// define a workflow which creates N blocked children, and then tries to create another, which should fail because
	// it's now past the limit
	maxPendingChildWorkflows := s.maxPendingChildExecutions
	parentWorkflow := func(ctx workflow.Context) error {
		childStarted := workflow.GetSignalChannel(ctx, "blocking-child-started")
		for i := 0; i < maxPendingChildWorkflows; i++ {
			childOptions := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
				WorkflowID: fmt.Sprintf("child-%d", i+1),
			})
			workflow.ExecuteChildWorkflow(childOptions, blockingChildWorkflow)
		}
		for i := 0; i < maxPendingChildWorkflows; i++ {
			childStarted.Receive(ctx, nil)
		}
		return workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID: fmt.Sprintf("child-%d", maxPendingChildWorkflows+1),
		}), childWorkflow).Get(ctx, nil)
	}

	// register all the workflows
	s.Worker().RegisterWorkflow(blockingChildWorkflow)
	s.Worker().RegisterWorkflow(childWorkflow)
	s.Worker().RegisterWorkflow(parentWorkflow)

	// start the parent workflow
	timeout := time.Minute * 5
	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(timeout)
	defer cancel()
	options := sdkclient.StartWorkflowOptions{
		ID:                 parentWorkflowId,
		TaskQueue:          s.TaskQueue(),
		WorkflowRunTimeout: timeout,
	}
	future, err := s.SdkClient().ExecuteWorkflow(ctx, options, parentWorkflow)
	s.NoError(err)

	s.HistoryContainsFailureCausedBy(
		ctx,
		parentWorkflowId,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_PENDING_CHILD_WORKFLOWS_LIMIT_EXCEEDED,
	)

	// unblock the last child, allowing it to complete, which lowers the number of pending child workflows
	s.NoError(s.SdkClient().SignalWorkflow(
		ctx,
		fmt.Sprintf("child-%d", maxPendingChildWorkflows),
		"",
		"unblock-child",
		nil,
	))

	// verify that the parent workflow completes soon after the number of pending child workflows drops
	s.EventuallySucceeds(ctx, func(ctx context.Context) error {
		return future.Get(ctx, nil)
	})
}

// TestTooManyPendingActivities verifies that we don't allow users to schedule new activities when they've already
// reached the limit for pending activities.
func (s *ClientMiscTestSuite) TestTooManyPendingActivities() {
	timeout := time.Minute * 5
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	pendingActivities := make(chan activity.Info, s.maxPendingActivities)
	pendingActivity := func(ctx context.Context) error {
		pendingActivities <- activity.GetInfo(ctx)
		return activity.ErrResultPending
	}
	s.Worker().RegisterActivity(pendingActivity)
	lastActivity := func(ctx context.Context) error {
		return nil
	}
	s.Worker().RegisterActivity(lastActivity)

	readyToScheduleLastActivity := "ready-to-schedule-last-activity"
	myWorkflow := func(ctx workflow.Context) error {
		for i := 0; i < s.maxPendingActivities; i++ {
			workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: time.Minute,
				ActivityID:          fmt.Sprintf("pending-activity-%d", i),
			}), pendingActivity)
		}

		workflow.GetSignalChannel(ctx, readyToScheduleLastActivity).Receive(ctx, nil)

		return workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute,
			ActivityID:          "last-activity",
		}), lastActivity).Get(ctx, nil)
	}
	s.Worker().RegisterWorkflow(myWorkflow)

	workflowId := uuid.New()
	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        workflowId,
		TaskQueue: s.TaskQueue(),
	}, myWorkflow)
	s.NoError(err)

	// wait until all of the activities are started (but not finished) before trying to schedule the last one
	var activityInfo activity.Info
	for i := 0; i < s.maxPendingActivities; i++ {
		activityInfo = <-pendingActivities
	}
	s.NoError(s.SdkClient().SignalWorkflow(ctx, workflowId, "", readyToScheduleLastActivity, nil))

	// verify that we can't finish the workflow yet
	{
		ctx, cancel := context.WithTimeout(ctx, time.Millisecond*100)
		defer cancel()
		err = workflowRun.Get(ctx, nil)
		s.Error(err, "the workflow should not be done while there are too many pending activities")
	}

	// verify that the workflow's history contains a task that failed because it would otherwise exceed the pending
	// child workflow limit
	s.HistoryContainsFailureCausedBy(
		ctx,
		workflowId,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_PENDING_ACTIVITIES_LIMIT_EXCEEDED,
	)

	// mark one of the pending activities as complete and verify that the workflow can now complete
	s.NoError(s.SdkClient().CompleteActivity(ctx, activityInfo.TaskToken, nil, nil))
	s.EventuallySucceeds(ctx, func(ctx context.Context) error {
		return workflowRun.Get(ctx, nil)
	})
}

func (s *ClientMiscTestSuite) TestTooManyCancelRequests() {
	// set a timeout for this whole test
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	// create a large number of blocked workflows
	numTargetWorkflows := 50 // should be much greater than s.maxPendingCancelRequests
	targetWorkflow := func(ctx workflow.Context) error {
		return workflow.Await(ctx, func() bool {
			return false
		})
	}
	s.Worker().RegisterWorkflow(targetWorkflow)
	for i := 0; i < numTargetWorkflows; i++ {
		_, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			ID:        fmt.Sprintf("workflow-%d", i),
			TaskQueue: s.TaskQueue(),
		}, targetWorkflow)
		s.NoError(err)
	}

	// define a workflow that attempts to cancel a given subsequence of the blocked workflows
	cancelWorkflowsInRange := func(ctx workflow.Context, start, stop int) error {
		var futures []workflow.Future
		for i := start; i < stop; i++ {
			future := workflow.RequestCancelExternalWorkflow(ctx, fmt.Sprintf("workflow-%d", i), "")
			futures = append(futures, future)
		}
		for _, future := range futures {
			if err := future.Get(ctx, nil); err != nil {
				return err
			}
		}
		return nil
	}
	s.Worker().RegisterWorkflow(cancelWorkflowsInRange)

	// try to cancel all the workflows at once and verify that we can't because of the limit violation
	s.Run("CancelAllWorkflowsAtOnce", func() {
		cancelerWorkflowId := "canceler-workflow-id"
		run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			TaskQueue: s.TaskQueue(),
			ID:        cancelerWorkflowId,
		}, cancelWorkflowsInRange, 0, numTargetWorkflows)
		s.NoError(err)
		s.HistoryContainsFailureCausedBy(ctx, cancelerWorkflowId, enumspb.WORKFLOW_TASK_FAILED_CAUSE_PENDING_REQUEST_CANCEL_LIMIT_EXCEEDED)
		{
			ctx, cancel := context.WithTimeout(ctx, time.Second*3)
			defer cancel()
			s.Error(run.Get(ctx, nil))
		}
		namespaceID := s.GetNamespaceID(s.Namespace())
		shardID := common.WorkflowIDToHistoryShard(namespaceID, cancelerWorkflowId, s.TestClusterConfig().HistoryConfig.NumHistoryShards)
		workflowExecution, err := s.TestCluster().ExecutionManager().GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
			ShardID:     shardID,
			NamespaceID: namespaceID,
			WorkflowID:  cancelerWorkflowId,
			RunID:       run.GetRunID(),
		})
		s.NoError(err)
		numCancelRequests := len(workflowExecution.State.RequestCancelInfos)
		s.Assert().Zero(numCancelRequests)
		err = s.SdkClient().CancelWorkflow(ctx, cancelerWorkflowId, "")
		s.NoError(err)
	})

	// try to cancel all the workflows in separate batches of cancel workflows and verify that it works
	s.Run("CancelWorkflowsInSeparateBatches", func() {
		var runs []sdkclient.WorkflowRun
		var stop int
		for start := 0; start < numTargetWorkflows; start = stop {
			stop = start + s.maxPendingCancelRequests
			if stop > numTargetWorkflows {
				stop = numTargetWorkflows
			}
			run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
				TaskQueue: s.TaskQueue(),
			}, cancelWorkflowsInRange, start, stop)
			s.NoError(err)
			runs = append(runs, run)
		}

		for _, run := range runs {
			s.NoError(run.Get(ctx, nil))
		}
	})
}

func (s *ClientMiscTestSuite) TestTooManyPendingSignals() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	receiverId := "receiver-id"
	signalName := "my-signal"
	sender := func(ctx workflow.Context, n int) error {
		var futures []workflow.Future
		for i := 0; i < n; i++ {
			future := workflow.SignalExternalWorkflow(ctx, receiverId, "", signalName, nil)
			futures = append(futures, future)
		}
		var errs error
		for _, future := range futures {
			err := future.Get(ctx, nil)
			errs = multierr.Combine(errs, err)
		}
		return errs
	}
	s.Worker().RegisterWorkflow(sender)

	receiver := func(ctx workflow.Context) error {
		channel := workflow.GetSignalChannel(ctx, signalName)
		for {
			channel.Receive(ctx, nil)
		}
	}
	s.Worker().RegisterWorkflow(receiver)
	_, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: s.TaskQueue(),
		ID:        receiverId,
	}, receiver)
	s.NoError(err)

	successTimeout := time.Second * 5
	s.Run("TooManySignals", func() {
		senderId := "sender-1"
		senderRun, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			TaskQueue: s.TaskQueue(),
			ID:        senderId,
		}, sender, s.maxPendingSignals+1)
		s.NoError(err)
		{
			ctx, cancel := context.WithTimeout(ctx, successTimeout)
			defer cancel()
			err := senderRun.Get(ctx, nil)
			s.Error(err)
		}
		s.HistoryContainsFailureCausedBy(
			ctx,
			senderId,
			enumspb.WORKFLOW_TASK_FAILED_CAUSE_PENDING_SIGNALS_LIMIT_EXCEEDED,
		)
		s.NoError(s.SdkClient().CancelWorkflow(ctx, senderId, ""))
	})

	s.Run("NotTooManySignals", func() {
		senderID := "sender-2"
		senderRun, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			TaskQueue: s.TaskQueue(),
			ID:        senderID,
		}, sender, s.maxPendingSignals)
		s.NoError(err)
		ctx, cancel := context.WithTimeout(ctx, successTimeout)
		defer cancel()
		err = senderRun.Get(ctx, nil)
		s.NoError(err)
	})
}

func continueAsNewTightLoop(ctx workflow.Context, currCount, maxCount int) (int, error) {
	if currCount == maxCount {
		return currCount, nil
	}
	return currCount, workflow.NewContinueAsNewError(ctx, continueAsNewTightLoop, currCount+1, maxCount)
}

func (s *ClientMiscTestSuite) TestContinueAsNewTightLoop() {
	// Simulate continue as new tight loop, and verify server throttle the rate.
	workflowId := "continue_as_new_tight_loop"
	s.Worker().RegisterWorkflow(continueAsNewTightLoop)

	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(time.Minute)
	defer cancel()
	options := sdkclient.StartWorkflowOptions{
		ID:                 workflowId,
		TaskQueue:          s.TaskQueue(),
		WorkflowRunTimeout: time.Second * 10,
	}
	startTime := time.Now()
	future, err := s.SdkClient().ExecuteWorkflow(ctx, options, continueAsNewTightLoop, 1, 5)
	s.NoError(err)

	var runCount int
	err = future.Get(ctx, &runCount)
	s.NoError(err)
	s.Equal(5, runCount)
	duration := time.Since(startTime)
	s.GreaterOrEqual(duration, time.Second*4)
}

func (s *ClientMiscTestSuite) TestStickyAutoReset() {
	// This test starts a workflow, wait and verify that the workflow is on sticky task queue.
	// Then it stops the worker for 10s, this will make matching aware that sticky worker is dead.
	// Then test sends a signal to the workflow to trigger a new workflow task.
	// Test verify that workflow is still on sticky task queue.
	// Then test poll the original workflow task queue directly (not via SDK),
	// and verify that the polled WorkflowTask contains full history.
	workflowId := "sticky_auto_reset"
	wfFn := func(ctx workflow.Context) (string, error) {
		sigCh := workflow.GetSignalChannel(ctx, "sig-name")
		var msg string
		sigCh.Receive(ctx, &msg)
		return msg, nil
	}

	s.Worker().RegisterWorkflow(wfFn)

	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(time.Minute)
	defer cancel()
	options := sdkclient.StartWorkflowOptions{
		ID:                 workflowId,
		TaskQueue:          s.TaskQueue(),
		WorkflowRunTimeout: time.Minute,
	}
	// start the test workflow
	future, err := s.SdkClient().ExecuteWorkflow(ctx, options, wfFn)
	s.NoError(err)

	// wait until wf started and sticky is set
	var stickyQueue string
	s.Eventually(func() bool {
		ms, err := s.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
			Namespace: s.Namespace(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: future.GetID(),
			},
		})
		s.NoError(err)
		stickyQueue = ms.DatabaseMutableState.ExecutionInfo.StickyTaskQueue
		// verify workflow has sticky task queue
		return stickyQueue != "" && stickyQueue != s.TaskQueue()
	}, 5*time.Second, 200*time.Millisecond)

	// stop worker
	s.Worker().Stop()
	time.Sleep(time.Second * 11) // wait 11s (longer than 10s timeout), after this time, matching will detect StickyWorkerUnavailable
	resp, err := s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
		Namespace:     s.Namespace(),
		TaskQueue:     &taskqueuepb.TaskQueue{Name: stickyQueue, Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: s.TaskQueue()},
		TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
	})
	s.NoError(err)
	s.NotNil(resp)
	for _, p := range resp.Pollers {
		s.NotNil(p.LastAccessTime)
		s.Greater(time.Now().Sub(p.LastAccessTime.AsTime()), time.Second*10)
	}

	startTime := time.Now()
	// send a signal which will trigger a new wft, and it will be pushed to original task queue
	err = s.SdkClient().SignalWorkflow(ctx, future.GetID(), "", "sig-name", "sig1")
	s.NoError(err)

	// check that mutable state still has sticky enabled
	ms, err := s.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: s.Namespace(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: future.GetID(),
		},
	})
	s.NoError(err)
	s.NotEmpty(ms.DatabaseMutableState.ExecutionInfo.StickyTaskQueue)
	s.Equal(stickyQueue, ms.DatabaseMutableState.ExecutionInfo.StickyTaskQueue)

	// now poll from normal queue, and it should see the full history.
	task, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: s.TaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	})

	// should be able to get the task without having to wait until sticky timeout (5s)
	pollLatency := time.Now().Sub(startTime)
	s.Less(pollLatency, time.Second*4)

	s.NoError(err)
	s.NotNil(task)
	s.NotNil(task.History)
	s.True(len(task.History.Events) > 0)
	s.Equal(int64(1), task.History.Events[0].EventId)
}
