package tests

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/workflow/update"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/multierr"
)

type ClientMiscTestSuite struct {
	testcore.FunctionalTestBase
}

func TestClientMiscTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(ClientMiscTestSuite))
}

func (s *ClientMiscTestSuite) TestTooManyChildWorkflows() {
	// To ensure that there is one pending child workflow before we try to create the next one,
	// we create a child workflow here that signals the parent when it has started and then blocks forever.
	parentWorkflowID := "client-func-too-many-child-workflows"
	blockingChildWorkflow := func(ctx workflow.Context) error {
		workflow.SignalExternalWorkflow(ctx, parentWorkflowID, "", "blocking-child-started", nil)
		workflow.GetSignalChannel(ctx, "unblock-child").Receive(ctx, nil)
		return nil
	}
	childWorkflow := func(ctx workflow.Context) error {
		return nil
	}

	// define a workflow which creates N blocked children, and then tries to create another, which should fail because
	// it's now past the limit
	maxPendingChildWorkflows := testcore.ClientSuiteLimit
	parentWorkflow := func(ctx workflow.Context) error {
		childStarted := workflow.GetSignalChannel(ctx, "blocking-child-started")
		for i := range maxPendingChildWorkflows {
			childOptions := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
				WorkflowID: fmt.Sprintf("child-%d", i+1),
			})
			workflow.ExecuteChildWorkflow(childOptions, blockingChildWorkflow)
		}
		for range maxPendingChildWorkflows {
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
		ID:                 parentWorkflowID,
		TaskQueue:          s.TaskQueue(),
		WorkflowRunTimeout: timeout,
	}
	future, err := s.SdkClient().ExecuteWorkflow(ctx, options, parentWorkflow)
	s.NoError(err)

	s.WaitForHistoryEventsSuffix(`
 WorkflowTaskFailed {"Cause":26,"Failure":{"Message":"PendingChildWorkflowsLimitExceeded: the number of pending child workflow executions, 10, has reached the per-workflow limit of 10"}}
 WorkflowTaskScheduled
 WorkflowTaskStarted
`, func() []*historypb.HistoryEvent {
		return s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: parentWorkflowID})
	}, 10*time.Second, 500*time.Millisecond)

	// unblock the last child, allowing it to complete, which lowers the number of pending child workflows
	s.NoError(s.SdkClient().SignalWorkflow(
		ctx,
		fmt.Sprintf("child-%d", maxPendingChildWorkflows),
		"",
		"unblock-child",
		nil,
	))

	// verify that the parent workflow completes soon after the number of pending child workflows drops
	s.Eventually(func() bool {
		return future.Get(ctx, nil) == nil
	}, 20*time.Second, 500*time.Millisecond)
}

// TestTooManyPendingActivities verifies that we don't allow users to schedule new activities when they've already
// reached the limit for pending activities.
func (s *ClientMiscTestSuite) TestTooManyPendingActivities() {
	timeout := time.Minute * 5
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	pendingActivities := make(chan activity.Info, testcore.ClientSuiteLimit)
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
		for i := range testcore.ClientSuiteLimit {
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

	workflowID := uuid.NewString()
	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                  workflowID,
		TaskQueue:           s.TaskQueue(),
		WorkflowTaskTimeout: time.Second, // Use shorter timeout so test completes faster.
	}, myWorkflow)
	s.NoError(err)

	// wait until all of the activities are started (but not finished) before trying to schedule the last one
	var activityInfo activity.Info
	for range testcore.ClientSuiteLimit {
		activityInfo = <-pendingActivities
	}
	s.NoError(s.SdkClient().SignalWorkflow(ctx, workflowID, "", readyToScheduleLastActivity, nil))

	// verify that we can't finish the workflow yet
	{
		ctx, cancel := context.WithTimeout(ctx, time.Millisecond*100)
		defer cancel()
		err = workflowRun.Get(ctx, nil)
		s.Error(err, "the workflow should not be done while there are too many pending activities")
	}

	// verify that the workflow's history contains a task that failed because it would otherwise exceed the pending
	// child workflow limit
	s.WaitForHistoryEventsSuffix(`
 21 WorkflowTaskFailed {"Cause":27,"Failure":{"Message":"PendingActivitiesLimitExceeded: the number of pending activities, 10, has reached the per-workflow limit of 10"}}
 22 WorkflowTaskScheduled
 23 WorkflowTaskStarted
`, func() []*historypb.HistoryEvent {
		return s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: workflowRun.GetID(), RunId: workflowRun.GetRunID()})
	}, 3*time.Second, 500*time.Millisecond)

	// mark one of the pending activities as complete and verify that the workflow can now complete
	s.NoError(s.SdkClient().CompleteActivity(ctx, activityInfo.TaskToken, nil, nil))
	s.NoError(workflowRun.Get(ctx, nil))
}

func (s *ClientMiscTestSuite) TestTooManyCancelRequests() {
	// set a timeout for this whole test
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	// create a large number of blocked workflows
	numTargetWorkflows := testcore.ClientSuiteLimit + 1
	targetWorkflow := func(ctx workflow.Context) error {
		return workflow.Await(ctx, func() bool {
			return false
		})
	}
	s.Worker().RegisterWorkflow(targetWorkflow)
	for i := range numTargetWorkflows {
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

		s.WaitForHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted // 29 below is enumspb.WORKFLOW_TASK_FAILED_CAUSE_PENDING_REQUEST_CANCEL_LIMIT_EXCEEDED
  4 WorkflowTaskFailed {"Cause":29,"Failure":{"Message":"PendingRequestCancelLimitExceeded: the number of pending requests to cancel external workflows, 10, has reached the per-workflow limit of 10"}}
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
`, func() []*historypb.HistoryEvent {
			return s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()})
		}, 5*time.Second, 500*time.Millisecond)

		shardID := common.WorkflowIDToHistoryShard(s.NamespaceID().String(), cancelerWorkflowId, s.GetTestClusterConfig().HistoryConfig.NumHistoryShards)
		workflowExecution, err := s.GetTestCluster().ExecutionManager().GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
			ShardID:     shardID,
			NamespaceID: s.NamespaceID().String(),
			WorkflowID:  cancelerWorkflowId,
			RunID:       run.GetRunID(),
			ArchetypeID: chasm.WorkflowArchetypeID,
		})
		s.NoError(err)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, workflowExecution.State.ExecutionState.Status)
		s.Empty(workflowExecution.State.RequestCancelInfos)
		s.NoError(s.SdkClient().CancelWorkflow(ctx, cancelerWorkflowId, ""))
	})

	// try to cancel all the workflows in separate batches of cancel workflows and verify that it works
	s.Run("CancelWorkflowsInSeparateBatches", func() {
		batch1, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			TaskQueue: s.TaskQueue(),
		}, cancelWorkflowsInRange, 0, numTargetWorkflows/2)
		s.NoError(err)

		batch2, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			TaskQueue: s.TaskQueue(),
		}, cancelWorkflowsInRange, numTargetWorkflows/2, numTargetWorkflows)
		s.NoError(err)

		s.NoError(batch1.Get(ctx, nil))
		s.NoError(batch2.Get(ctx, nil))
	})
}

func (s *ClientMiscTestSuite) TestTooManyPendingSignals() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	receiverId := "receiver-id"
	signalName := "my-signal"
	sender := func(ctx workflow.Context, n int) error {
		var futures []workflow.Future
		for range n {
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
		}, sender, testcore.ClientSuiteLimit+1)
		s.NoError(err)
		{
			ctx, cancel := context.WithTimeout(ctx, successTimeout)
			defer cancel()
			err := senderRun.Get(ctx, nil)
			s.Error(err)
		}

		s.WaitForHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted // 28 below is enumspb.WORKFLOW_TASK_FAILED_CAUSE_PENDING_SIGNALS_LIMIT_EXCEEDED
  4 WorkflowTaskFailed {"Cause":28,"Failure":{"Message":"PendingSignalsLimitExceeded: the number of pending signals to external workflows, 10, has reached the per-workflow limit of 10"}}
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
`, func() []*historypb.HistoryEvent {
			return s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: senderRun.GetID(), RunId: senderRun.GetRunID()})
		}, 3*time.Second, 500*time.Millisecond)

		s.NoError(s.SdkClient().CancelWorkflow(ctx, senderId, ""))
	})

	s.Run("NotTooManySignals", func() {
		senderID := "sender-2"
		senderRun, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			TaskQueue: s.TaskQueue(),
			ID:        senderID,
		}, sender, testcore.ClientSuiteLimit)
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
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: future.GetID(),
			},
			Archetype: chasm.WorkflowArchetype,
		})
		s.NoError(err)
		stickyQueue = ms.DatabaseMutableState.ExecutionInfo.StickyTaskQueue
		// verify workflow has sticky task queue
		return stickyQueue != "" && stickyQueue != s.TaskQueue()
	}, 5*time.Second, 200*time.Millisecond)

	// stop worker
	s.Worker().Stop()
	//nolint:forbidigo
	time.Sleep(time.Second * 11) // wait 11s (longer than 10s timeout), after this time, matching will detect StickyWorkerUnavailable
	resp, err := s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
		Namespace:     s.Namespace().String(),
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
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: future.GetID(),
		},
		Archetype: chasm.WorkflowArchetype,
	})
	s.NoError(err)
	s.NotEmpty(ms.DatabaseMutableState.ExecutionInfo.StickyTaskQueue)
	s.Equal(stickyQueue, ms.DatabaseMutableState.ExecutionInfo.StickyTaskQueue)

	// now poll from normal queue, and it should see the full history.
	task, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
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

// Analogous to TestBufferedSignalCausesUnhandledCommandAndSchedulesNewTask
// TODO: rename to previous name (Test_AdmittedUpdateCausesUnhandledCommandAndSchedulesNewTask) when/if admitted updates start to block workflow from completing.
//
//  1. The worker starts executing the first WFT, before any update is sent.
//  2. While the first WFT is being executed, an update is sent.
//  3. Once the server has received the update, the workflow tries to complete itself.
//  4. The server fails update request with error and completes WF.
func (s *ClientMiscTestSuite) TestWorkflowCanBeCompletedDespiteAdmittedUpdate() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tv := testvars.New(s.T()).WithTaskQueue(s.TaskQueue())

	readyToSendUpdate := make(chan bool, 1)
	updateHasBeenAdmitted := make(chan bool)

	localActivityFn := func(ctx context.Context) error {
		readyToSendUpdate <- true // Ensure update is sent after first WFT has started.
		<-updateHasBeenAdmitted   // Ensure WF completion is not attempted until after update has been admitted.
		return nil
	}

	workflowFn := func(ctx workflow.Context) error {
		err := workflow.SetUpdateHandler(ctx, tv.HandlerName(), func(ctx workflow.Context, arg string) (string, error) {
			return "my-update-result", nil
		})
		if err != nil {
			return err
		}
		laCtx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		})
		return workflow.ExecuteLocalActivity(laCtx, localActivityFn).Get(laCtx, nil)
	}

	s.Worker().RegisterWorkflow(workflowFn)

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                  tv.WorkflowID(),
		TaskQueue:           tv.TaskQueue().Name,
		WorkflowTaskTimeout: 10 * time.Second,
		WorkflowRunTimeout:  10 * time.Second,
	}, workflowFn)
	s.NoError(err)

	// Block until first workflow task started.
	<-readyToSendUpdate

	tv = tv.WithRunID(workflowRun.GetRunID())

	// Send update and wait until it is admitted. This isn't convenient: since Admitted is non-durable, we do not expose
	// an API for doing it directly. Instead we send the update and poll until it's reported to be in admitted state.
	updateHandleCh := make(chan sdkclient.WorkflowUpdateHandle)
	updateErrCh := make(chan error)
	go func() {
		handle, err := s.SdkClient().UpdateWorkflow(ctx, sdkclient.UpdateWorkflowOptions{
			UpdateID:     tv.UpdateID(),
			UpdateName:   tv.HandlerName(),
			WorkflowID:   tv.WorkflowID(),
			RunID:        tv.RunID(),
			Args:         []any{"update-value"},
			WaitForStage: sdkclient.WorkflowUpdateStageCompleted,
		})
		updateErrCh <- err
		updateHandleCh <- handle
	}()
	for {
		time.Sleep(10 * time.Millisecond) //nolint:forbidigo
		_, err = s.SdkClient().WorkflowService().PollWorkflowExecutionUpdate(ctx, &workflowservice.PollWorkflowExecutionUpdateRequest{
			Namespace: s.Namespace().String(),
			UpdateRef: tv.UpdateRef(),
			Identity:  "my-identity",
			WaitPolicy: &updatepb.WaitPolicy{
				LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED,
			},
		})
		if err == nil {
			// Update is admitted but doesn't block WF from completion.
			close(updateHasBeenAdmitted)
			break
		}
	}

	err = workflowRun.Get(ctx, nil)
	s.NoError(err)
	updateErr := <-updateErrCh
	s.Error(updateErr)
	var notFound *serviceerror.NotFound
	s.ErrorAs(updateErr, &notFound)
	s.ErrorContains(updateErr, update.AbortedByWorkflowClosingErr.Error())
	updateHandle := <-updateHandleCh
	s.Nil(updateHandle)
	// Uncomment the following when durable admitted is implemented.
	// var updateResult string
	// err = updateHandle.Get(ctx, &updateResult)
	// s.NoError(err)
	// s.Equal("my-update-result", updateResult)

	s.HistoryRequire.EqualHistoryEvents(`
	1 WorkflowExecutionStarted
	2 WorkflowTaskScheduled
	3 WorkflowTaskStarted
	4 WorkflowTaskCompleted
	5 MarkerRecorded
	6 WorkflowExecutionCompleted`,
		s.GetHistory(s.Namespace().String(), tv.WorkflowExecution()))
}

func (s *ClientMiscTestSuite) Test_CancelActivityAndTimerBeforeComplete() {
	workflowFn := func(ctx workflow.Context) error {
		ctx, cancelFunc := workflow.WithCancel(ctx)

		activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ScheduleToStartTimeout: 10 * time.Second,
			ScheduleToCloseTimeout: 10 * time.Second,
			StartToCloseTimeout:    1 * time.Second,
			TaskQueue:              "bad_tq",
		})
		_ = workflow.ExecuteActivity(activityCtx, "Prefix_ToUpper", "hello")

		_ = workflow.NewTimer(ctx, 15*time.Second)

		err := workflow.NewTimer(ctx, time.Second).Get(ctx, nil)
		if err != nil {
			return err
		}
		cancelFunc()
		return nil
	}

	s.Worker().RegisterWorkflow(workflowFn)

	id := s.T().Name()
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 id,
		TaskQueue:          s.TaskQueue(),
		WorkflowRunTimeout: 5 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)
	err = workflowRun.Get(ctx, nil)
	s.NoError(err)
}

func (s *ClientMiscTestSuite) Test_FinishWorkflowWithDeferredCommands() {
	activityFn := func(ctx context.Context) error {
		return nil
	}

	childWorkflowFn := func(ctx workflow.Context) error {
		return nil
	}

	workflowFn := func(ctx workflow.Context) error {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
		}
		ctx = workflow.WithActivityOptions(ctx, ao)
		defer workflow.ExecuteActivity(ctx, activityFn)

		childID := "child_workflow"
		cwo := workflow.ChildWorkflowOptions{
			WorkflowID:         childID,
			WorkflowRunTimeout: 10 * time.Second,
			TaskQueue:          s.TaskQueue(),
		}
		ctx = workflow.WithChildOptions(ctx, cwo)
		defer workflow.ExecuteChildWorkflow(ctx, childWorkflowFn)
		workflow.NewTimer(ctx, time.Second)
		return nil
	}

	s.Worker().RegisterWorkflow(workflowFn)
	s.Worker().RegisterWorkflow(childWorkflowFn)
	s.Worker().RegisterActivity(activityFn)

	id := "functional-test-finish-workflow-with-deffered-commands"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 id,
		TaskQueue:          s.TaskQueue(),
		WorkflowRunTimeout: 10 * time.Second,
	}

	ctx := context.Background()
	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	s.NotNil(workflowRun)
	s.True(workflowRun.GetRunID() != "")

	err = workflowRun.Get(ctx, nil)
	s.NoError(err)

	// verify event sequence
	expectedHistory := []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_TIMER_STARTED,
		enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
	}
	s.assertHistory(id, workflowRun.GetRunID(), expectedHistory)
}

// This test simulates workflow generate command with invalid attributes.
// Server is expected to fail the workflow task and schedule a retry immediately for first attempt,
// but if workflow task keeps failing, server will drop the task and wait for timeout to schedule additional retries.
// This is the same behavior as the SDK used to do, but now we would do on server.
func (s *ClientMiscTestSuite) TestInvalidCommandAttribute() {
	activityFn := func(ctx context.Context) error {
		return nil
	}

	var startedTime []time.Time
	workflowFn := func(ctx workflow.Context) error {
		info := workflow.GetInfo(ctx)

		// Simply record time.Now() and check if the difference between the recorded time
		// is higher than the workflow task timeout will not work, because there is a delay
		// between server starts the workflow task and this code is executed.

		var currentAttemptStartedTime time.Time
		err := workflow.SideEffect(ctx, func(_ workflow.Context) any {
			rpcCtx := context.Background()
			if deadline, ok := ctx.Deadline(); ok {
				var cancel context.CancelFunc
				rpcCtx, cancel = context.WithDeadline(rpcCtx, deadline)
				defer cancel()
			}

			resp, err := s.SdkClient().DescribeWorkflowExecution(
				rpcCtx,
				info.WorkflowExecution.ID,
				info.WorkflowExecution.RunID,
			)
			if err != nil {
				panic(err)
			}
			return resp.PendingWorkflowTask.StartedTime.AsTime()
		}).Get(&currentAttemptStartedTime)
		if err != nil {
			return err
		}

		startedTime = append(startedTime, currentAttemptStartedTime)
		ao := workflow.ActivityOptions{} // invalid activity option without StartToClose timeout
		ctx = workflow.WithActivityOptions(ctx, ao)

		return workflow.ExecuteActivity(ctx, activityFn).Get(ctx, nil)
	}

	s.Worker().RegisterWorkflow(workflowFn)
	s.Worker().RegisterActivity(activityFn)

	id := "functional-test-invalid-command-attributes"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        id,
		TaskQueue: s.TaskQueue(),
		// With 3s TaskTimeout and 5s RunTimeout, we expect to see total of 3 attempts.
		// First attempt follow by immediate retry follow by timeout and 3rd attempt after WorkflowTaskTimeout.
		WorkflowTaskTimeout: 3 * time.Second,
		WorkflowRunTimeout:  5 * time.Second,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	s.NotNil(workflowRun)
	s.True(workflowRun.GetRunID() != "")

	// wait until workflow close (it will be timeout)
	err = workflowRun.Get(ctx, nil)
	s.Error(err)
	s.Contains(err.Error(), "timeout")

	// verify event sequence
	expectedHistory := []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
	}
	s.assertHistory(id, workflowRun.GetRunID(), expectedHistory)

	// assert workflow task retried 3 times
	s.Equal(3, len(startedTime))

	s.True(startedTime[1].Sub(startedTime[0]) < time.Second)   // retry immediately
	s.True(startedTime[2].Sub(startedTime[1]) > time.Second*3) // retry after WorkflowTaskTimeout
}

func (s *ClientMiscTestSuite) Test_BufferedQuery() {
	localActivityFn := func(ctx context.Context) error {
		//nolint:forbidigo
		time.Sleep(5 * time.Second) // use local activity sleep to block workflow task to force query to be buffered
		return nil
	}

	wfStarted := sync.WaitGroup{}
	wfStarted.Add(1)
	workflowFn := func(ctx workflow.Context) error {
		wfStarted.Done()
		status := "init"
		if err := workflow.SetQueryHandler(ctx, "foo", func() (string, error) {
			return status, nil
		}); err != nil {
			return err
		}
		ctx1 := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
			ScheduleToCloseTimeout: 10 * time.Second,
		})
		status = "calling"
		f1 := workflow.ExecuteLocalActivity(ctx1, localActivityFn)
		status = "waiting"
		err1 := f1.Get(ctx1, nil)
		status = "done"

		return multierr.Combine(err1, workflow.Sleep(ctx, 5*time.Second))
	}

	s.Worker().RegisterWorkflow(workflowFn)

	id := "functional-test-buffered-query"
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
	s.True(workflowRun.GetRunID() != "")

	// wait until first wf task started
	wfStarted.Wait()

	go func() {
		// sleep 2s to make sure DescribeMutableState is called after QueryWorkflow
		time.Sleep(2 * time.Second) //nolint:forbidigo
		// make DescribeMutableState call, which force mutable state to reload from db
		_, err := s.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      workflowRun.GetRunID(),
			},
			Archetype: chasm.WorkflowArchetype,
		})
		s.Assert().NoError(err)
	}()

	// this query will be buffered in mutable state because workflow task is in-flight.
	encodedQueryResult, err := s.SdkClient().QueryWorkflow(ctx, id, workflowRun.GetRunID(), "foo")

	s.NoError(err)
	var queryResult string
	err = encodedQueryResult.Get(&queryResult)
	s.NoError(err)
	s.Equal("done", queryResult)

	err = workflowRun.Get(ctx, nil)
	s.NoError(err)
}

func (s *ClientMiscTestSuite) assertHistory(wid, rid string, expected []enumspb.EventType) {
	iter := s.SdkClient().GetWorkflowHistory(context.Background(), wid, rid, false, 0)
	var events []enumspb.EventType
	for iter.HasNext() {
		event, err := iter.Next()
		s.NoError(err)
		events = append(events, event.GetEventType())
	}

	s.Equal(expected, events)
}

// This test simulates workflow try to complete itself while there is buffered event.
// Event sequence:
//
//	1st WorkflowTask runs a local activity.
//	While local activity is running, a signal is received by server.
//	After signal is received, local activity completed, and workflow drains signal chan (no signal yet) and complete workflow.
//	Server failed the complete request because there is unhandled signal.
//	Server rescheduled a new workflow task.
//	Workflow runs the local activity again and drain the signal chan (with one signal) and complete workflow.
//	Server complete workflow as requested.
func (s *ClientMiscTestSuite) TestBufferedSignalCausesUnhandledCommandAndSchedulesNewTask() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tv := testvars.New(s.T()).WithTaskQueue(s.TaskQueue())

	sigReadyToSendChan := make(chan struct{}, 1)
	sigSendDoneChan := make(chan struct{})
	localActivityFn := func(ctx context.Context) error {
		// Unblock signal sending, so it is sent after first workflow task started.
		sigReadyToSendChan <- struct{}{}
		// Block workflow task and cause the signal to become buffered event.
		select {
		case <-sigSendDoneChan:
		case <-ctx.Done():
		}
		return nil
	}

	var receivedSig string
	workflowFn := func(ctx workflow.Context) error {
		ctx1 := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		})
		if err := workflow.ExecuteLocalActivity(ctx1, localActivityFn).Get(ctx1, nil); err != nil {
			return err
		}
		sigCh := workflow.GetSignalChannel(ctx, tv.HandlerName())
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

	s.Worker().RegisterWorkflow(workflowFn)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        tv.WorkflowID(),
		TaskQueue: tv.TaskQueue().Name,
		// Intentionally use same timeout for WorkflowTaskTimeout and WorkflowRunTimeout so if workflow task is not
		// correctly dispatched, it would time out which would fail the workflow and cause test to fail.
		WorkflowTaskTimeout: 10 * time.Second,
		WorkflowRunTimeout:  10 * time.Second,
	}
	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	s.NotNil(workflowRun)
	s.True(workflowRun.GetRunID() != "")
	tv = tv.WithRunID(workflowRun.GetRunID())

	// block until first workflow task started
	<-sigReadyToSendChan

	err = s.SdkClient().SignalWorkflow(ctx, tv.WorkflowID(), tv.RunID(), tv.HandlerName(), "signal-value")
	s.NoError(err)

	close(sigSendDoneChan)

	err = workflowRun.Get(ctx, nil)
	s.NoError(err) // if new workflow task is not correctly dispatched, it would cause timeout error here
	s.Equal("signal-value", receivedSig)

	s.HistoryRequire.EqualHistoryEvents(`
	1 WorkflowExecutionStarted
	2 WorkflowTaskScheduled
	3 WorkflowTaskStarted
	4 WorkflowTaskFailed        // Unhandled signal prevented workflow completion
	5 WorkflowExecutionSignaled // This is the buffered signal
	6 WorkflowTaskScheduled
	7 WorkflowTaskStarted
	8 WorkflowTaskCompleted
	9 MarkerRecorded
	10 WorkflowExecutionCompleted`,
		s.GetHistory(s.Namespace().String(), tv.WorkflowExecution()))
}

func (s *ClientMiscTestSuite) Test_StickyWorkerRestartWorkflowTask() {
	testCases := []struct {
		name     string
		waitTime time.Duration
		doQuery  bool
		doSignal bool
	}{
		{
			name:     "new workflow task after 10s, no delay",
			waitTime: 10 * time.Second,
			doSignal: true,
		},
		{
			name:     "new workflow task immediately, no delay",
			waitTime: 0,
			doSignal: true,
		},
		{
			name:     "new query after 10s, no delay",
			waitTime: 10 * time.Second,
			doQuery:  true,
		},
		{
			name:     "new query immediately, no delay",
			waitTime: 0,
			doQuery:  true,
		},
	}
	for _, tt := range testCases {
		s.Run(tt.name, func() {
			workflowFn := func(ctx workflow.Context) (string, error) {
				if err := workflow.SetQueryHandler(ctx, "test", func() (string, error) {
					return "query works", nil
				}); err != nil {
					return "", err
				}

				signalCh := workflow.GetSignalChannel(ctx, "test")
				var msg string
				signalCh.Receive(ctx, &msg)
				return msg, nil
			}

			taskQueue := "task-queue-" + tt.name

			oldWorker := worker.New(s.SdkClient(), taskQueue, worker.Options{})
			oldWorker.RegisterWorkflow(workflowFn)
			err := oldWorker.Start()
			s.NoError(err)

			id := "test-sticky-delay" + tt.name
			workflowOptions := sdkclient.StartWorkflowOptions{
				ID:                 id,
				TaskQueue:          taskQueue,
				WorkflowRunTimeout: 20 * time.Second,
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
			s.NoError(err)

			s.NotNil(workflowRun)
			s.NotEmpty(workflowRun.GetRunID())

			s.Eventually(func() bool {
				// wait until first workflow task completed (so we know sticky is set on workflow)
				iter := s.SdkClient().GetWorkflowHistory(ctx, id, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
				for iter.HasNext() {
					evt, err := iter.Next()
					s.NoError(err)
					if evt.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
						return true
					}
				}
				return false
			}, 10*time.Second, 200*time.Millisecond)

			// stop old worker
			oldWorker.Stop()

			// maybe wait for 10s, which will make matching aware the old sticky worker is unavailable
			time.Sleep(tt.waitTime) //nolint:forbidigo

			// start a new worker
			newWorker := worker.New(s.SdkClient(), taskQueue, worker.Options{})
			newWorker.RegisterWorkflow(workflowFn)
			err = newWorker.Start()
			s.NoError(err)
			defer newWorker.Stop()

			startTime := time.Now()
			// send a signal, and workflow should complete immediately, there should not be 5s delay
			if tt.doSignal {
				err = s.SdkClient().SignalWorkflow(ctx, id, "", "test", "test")
				s.NoError(err)

				err = workflowRun.Get(ctx, nil)
				s.NoError(err)
			} else if tt.doQuery {
				// send a signal, and workflow should complete immediately, there should not be 5s delay
				queryResult, err := s.SdkClient().QueryWorkflow(ctx, id, "", "test", "test")
				s.NoError(err)

				var queryResultStr string
				err = queryResult.Get(&queryResultStr)
				s.NoError(err)
				s.Equal("query works", queryResultStr)
			}
			endTime := time.Now()
			duration := endTime.Sub(startTime)
			s.Less(duration, 5*time.Second)
		})
	}
}

func (s *ClientMiscTestSuite) TestBatchSignal() {

	type myData struct {
		Stuff  string
		Things []int
	}

	workflowFn := func(ctx workflow.Context) (myData, error) {
		var receivedData myData
		workflow.GetSignalChannel(ctx, "my-signal").Receive(ctx, &receivedData)
		return receivedData, nil
	}
	s.Worker().RegisterWorkflow(workflowFn)

	workflowRun, err := s.SdkClient().ExecuteWorkflow(context.Background(), sdkclient.StartWorkflowOptions{
		ID:                       uuid.NewString(),
		TaskQueue:                s.TaskQueue(),
		WorkflowExecutionTimeout: 10 * time.Second,
	}, workflowFn)
	s.NoError(err)

	input1 := myData{
		Stuff:  "here's some data",
		Things: []int{7, 8, 9},
	}
	inputPayloads, err := converter.GetDefaultDataConverter().ToPayloads(input1)
	s.NoError(err)

	_, err = s.SdkClient().WorkflowService().StartBatchOperation(context.Background(), &workflowservice.StartBatchOperationRequest{
		Namespace: s.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_SignalOperation{
			SignalOperation: &batchpb.BatchOperationSignal{
				Signal: "my-signal",
				Input:  inputPayloads,
			},
		},
		Executions: []*commonpb.WorkflowExecution{
			{
				WorkflowId: workflowRun.GetID(),
				RunId:      workflowRun.GetRunID(),
			},
		},
		JobId:  uuid.NewString(),
		Reason: "test",
	})
	s.NoError(err)

	var returnedData myData
	err = workflowRun.Get(context.Background(), &returnedData)
	s.NoError(err)

	s.Equal(input1, returnedData)
}

func (s *ClientMiscTestSuite) TestBatchReset() {
	var count atomic.Int32

	activityFn := func(ctx context.Context) (int32, error) {
		if val := count.Load(); val != 0 {
			return val, nil
		}
		return 0, temporal.NewApplicationError("some random error", "", false, nil)
	}
	workflowFn := func(ctx workflow.Context) (int, error) {
		ao := workflow.ActivityOptions{
			ScheduleToStartTimeout: 20 * time.Second,
			StartToCloseTimeout:    40 * time.Second,
		}
		ctx = workflow.WithActivityOptions(ctx, ao)

		var result int
		err := workflow.ExecuteActivity(ctx, activityFn).Get(ctx, &result)
		return result, err
	}
	s.Worker().RegisterWorkflow(workflowFn)
	s.Worker().RegisterActivity(activityFn)

	workflowRun, err := s.SdkClient().ExecuteWorkflow(context.Background(), sdkclient.StartWorkflowOptions{
		ID:                       uuid.NewString(),
		TaskQueue:                s.TaskQueue(),
		WorkflowExecutionTimeout: 10 * time.Second,
	}, workflowFn)
	s.NoError(err)

	// make sure it failed the first time
	var result int
	err = workflowRun.Get(context.Background(), &result)
	s.Error(err)

	count.Add(1)

	_, err = s.SdkClient().WorkflowService().StartBatchOperation(context.Background(), &workflowservice.StartBatchOperationRequest{
		Namespace: s.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_ResetOperation{
			ResetOperation: &batchpb.BatchOperationReset{
				ResetType: enumspb.RESET_TYPE_FIRST_WORKFLOW_TASK,
			},
		},
		Executions: []*commonpb.WorkflowExecution{
			{
				WorkflowId: workflowRun.GetID(),
				RunId:      workflowRun.GetRunID(),
			},
		},
		JobId:  uuid.NewString(),
		Reason: "test",
	})
	s.NoError(err)

	// latest run should complete successfully
	s.Eventually(func() bool {
		workflowRun = s.SdkClient().GetWorkflow(context.Background(), workflowRun.GetID(), "")
		err = workflowRun.Get(context.Background(), &result)
		return err == nil && result == 1
	}, 5*time.Second, 200*time.Millisecond)
}

func (s *ClientMiscTestSuite) TestBatchResetByBuildId() {
	tq := testcore.RandomizeStr(s.T().Name())
	buildPrefix := uuid.NewString()[:6] + "-"
	buildIdv1 := buildPrefix + "v1"
	buildIdv2 := buildPrefix + "v2"
	buildIdv3 := buildPrefix + "v3"

	var act1count, act2count, act3count, badcount atomic.Int32
	act1 := func() error { act1count.Add(1); return nil }
	act2 := func() error { act2count.Add(1); return nil }
	act3 := func() error { act3count.Add(1); return nil }
	badact := func() error { badcount.Add(1); return nil }

	wf1 := func(ctx workflow.Context) (string, error) {
		ao := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{ScheduleToCloseTimeout: 5 * time.Second})

		s.NoError(workflow.ExecuteActivity(ao, "act1").Get(ctx, nil))

		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)

		return "done 1!", nil
	}

	wf2 := func(ctx workflow.Context) (string, error) {
		ao := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{ScheduleToCloseTimeout: 5 * time.Second})

		s.NoError(workflow.ExecuteActivity(ao, "act1").Get(ctx, nil))

		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)

		// same as wf1 up to here

		// run act2
		s.NoError(workflow.ExecuteActivity(ao, "act2").Get(ctx, nil))

		// now do something bad in a loop.
		// (we want something that's visible in history, not just failing workflow tasks,
		// otherwise we wouldn't need a reset to "fix" it, just a new build would be enough.)
		for range 1000 {
			s.NoError(workflow.ExecuteActivity(ao, "badact").Get(ctx, nil))
			_ = workflow.Sleep(ctx, time.Second)
		}

		return "done 2!", nil
	}

	wf3 := func(ctx workflow.Context) (string, error) {
		ao := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{ScheduleToCloseTimeout: 5 * time.Second})

		s.NoError(workflow.ExecuteActivity(ao, "act1").Get(ctx, nil))

		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)

		s.NoError(workflow.ExecuteActivity(ao, "act2").Get(ctx, nil))

		// same as wf2 up to here

		// instead of calling badact, do something different to force a non-determinism error
		// (the change of activity type below isn't enough)
		_ = workflow.Sleep(ctx, time.Second)

		// call act3 once
		s.NoError(workflow.ExecuteActivity(ao, "act3").Get(ctx, nil))

		return "done 3!", nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	w1 := worker.New(s.SdkClient(), tq, worker.Options{BuildID: buildIdv1})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	w1.RegisterActivityWithOptions(act1, activity.RegisterOptions{Name: "act1"})
	s.NoError(w1.Start())

	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)
	ex := &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()}
	// wait for first wft and first activity to complete
	s.Eventually(func() bool { return len(s.GetHistory(s.Namespace().String(), ex)) >= 10 }, 5*time.Second, 100*time.Millisecond)

	w1.Stop()

	// should see one run of act1
	s.Equal(int32(1), act1count.Load())

	w2 := worker.New(s.SdkClient(), tq, worker.Options{BuildID: buildIdv2})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	w2.RegisterActivityWithOptions(act1, activity.RegisterOptions{Name: "act1"})
	w2.RegisterActivityWithOptions(act2, activity.RegisterOptions{Name: "act2"})
	w2.RegisterActivityWithOptions(badact, activity.RegisterOptions{Name: "badact"})
	s.NoError(w2.Start())
	defer w2.Stop()

	// unblock the workflow
	s.NoError(s.SdkClient().SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "wait", nil))

	// wait until we see three calls to badact
	s.Eventually(func() bool { return badcount.Load() >= 3 }, 10*time.Second, 200*time.Millisecond)

	// at this point act2 should have been invokved once also
	s.Equal(int32(1), act2count.Load())

	w2.Stop()

	w3 := worker.New(s.SdkClient(), tq, worker.Options{BuildID: buildIdv3})
	w3.RegisterWorkflowWithOptions(wf3, workflow.RegisterOptions{Name: "wf"})
	w3.RegisterActivityWithOptions(act1, activity.RegisterOptions{Name: "act1"})
	w3.RegisterActivityWithOptions(act2, activity.RegisterOptions{Name: "act2"})
	w3.RegisterActivityWithOptions(act3, activity.RegisterOptions{Name: "act3"})
	w3.RegisterActivityWithOptions(badact, activity.RegisterOptions{Name: "badact"})
	s.NoError(w3.Start())
	defer w3.Stop()

	// but v3 is not quite compatible, the workflow should be blocked on non-determinism errors for now.
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	s.Error(run.Get(waitCtx, nil))

	// wait for it to appear in visibility
	query := fmt.Sprintf(`%s = "%s" and %s = "%s"`,
		sadefs.ExecutionStatus, "Running",
		sadefs.BuildIds, worker_versioning.UnversionedBuildIdSearchAttribute(buildIdv2))
	s.Eventually(func() bool {
		resp, err := s.FrontendClient().ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: s.Namespace().String(),
			Query:     query,
		})
		return err == nil && len(resp.Executions) == 1
	}, 10*time.Second, 500*time.Millisecond)

	// reset it using v2 as the bad build ID
	_, err = s.FrontendClient().StartBatchOperation(context.Background(), &workflowservice.StartBatchOperationRequest{
		Namespace:       s.Namespace().String(),
		VisibilityQuery: query,
		JobId:           uuid.NewString(),
		Reason:          "test",
		Operation: &workflowservice.StartBatchOperationRequest_ResetOperation{
			ResetOperation: &batchpb.BatchOperationReset{
				Options: &commonpb.ResetOptions{
					Target: &commonpb.ResetOptions_BuildId{
						BuildId: buildIdv2,
					},
				},
			},
		},
	})
	s.NoError(err)

	// now it can complete on v3. (need to loop since runid will be resolved early and we need
	// to re-resolve to pick up the new run instead of the terminated one)
	s.Eventually(func() bool {
		var out string
		return s.SdkClient().GetWorkflow(ctx, run.GetID(), "").Get(ctx, &out) == nil && out == "done 3!"
	}, 10*time.Second, 200*time.Millisecond)

	s.Equal(int32(1), act1count.Load()) // we should not see an addition run of act1
	s.Equal(int32(2), act2count.Load()) // we should see an addition run of act2 (reset point was before it)
	s.Equal(int32(1), act3count.Load()) // we should see one run of act3
}
