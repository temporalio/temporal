package tests

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
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
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/workflow/update"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/multierr"
)

type ClientMiscTestSuite struct {
	parallelsuite.Suite[*ClientMiscTestSuite]
}

func TestClientMiscTestSuite(t *testing.T) {
	parallelsuite.Run(t, &ClientMiscTestSuite{})
}

func (s *ClientMiscTestSuite) TestTooManyChildWorkflows() {
	env := testcore.NewEnv(s.T())
	// To ensure that there is one pending child workflow before we try to create the next one,
	// we create a child workflow here that signals the parent when it has started and then blocks forever.
	parentWorkflowID := "parent"
	maxPendingChildWorkflows := testcore.ClientSuiteLimit
	childWorkflowIDs := make([]string, maxPendingChildWorkflows+1)
	for i := range childWorkflowIDs {
		childWorkflowIDs[i] = fmt.Sprintf("child-%d", i+1)
	}
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
	parentWorkflow := func(ctx workflow.Context) error {
		childStarted := workflow.GetSignalChannel(ctx, "blocking-child-started")
		for i := range maxPendingChildWorkflows {
			childOptions := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
				WorkflowID: childWorkflowIDs[i],
			})
			workflow.ExecuteChildWorkflow(childOptions, blockingChildWorkflow)
		}
		for range maxPendingChildWorkflows {
			childStarted.Receive(ctx, nil)
		}
		return workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID: childWorkflowIDs[maxPendingChildWorkflows],
		}), childWorkflow).Get(ctx, nil)
	}

	// register all the workflows
	env.SdkWorker().RegisterWorkflow(blockingChildWorkflow)
	env.SdkWorker().RegisterWorkflow(childWorkflow)
	env.SdkWorker().RegisterWorkflow(parentWorkflow)

	// start the parent workflow
	timeout := time.Minute * 5
	options := sdkclient.StartWorkflowOptions{
		ID:                 parentWorkflowID,
		TaskQueue:          env.WorkerTaskQueue(),
		WorkflowRunTimeout: timeout,
	}
	future, err := env.SdkClient().ExecuteWorkflow(s.Context(), options, parentWorkflow)
	s.NoError(err)

	s.WaitForHistoryEventsSuffix(`
 WorkflowTaskFailed {"Cause":26,"Failure":{"Message":"PendingChildWorkflowsLimitExceeded: the number of pending child workflow executions, 10, has reached the per-workflow limit of 10"}}
 WorkflowTaskScheduled
 WorkflowTaskStarted
`, func() []*historypb.HistoryEvent {
		return env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: parentWorkflowID})
	}, 10*time.Second, 500*time.Millisecond)

	// unblock the last child, allowing it to complete, which lowers the number of pending child workflows
	s.NoError(env.SdkClient().SignalWorkflow(
		s.Context(),
		childWorkflowIDs[maxPendingChildWorkflows-1],
		"",
		"unblock-child",
		nil,
	))

	// verify that the parent workflow completes soon after the number of pending child workflows drops
	s.Eventually(func() bool {
		return future.Get(s.Context(), nil) == nil
	}, 20*time.Second, 500*time.Millisecond)
}

// TestTooManyPendingActivities verifies that we don't allow users to schedule new activities when they've already
// reached the limit for pending activities.
func (s *ClientMiscTestSuite) TestTooManyPendingActivities() {
	env := testcore.NewEnv(s.T())

	pendingActivities := make(chan activity.Info, testcore.ClientSuiteLimit)
	pendingActivity := func(ctx context.Context) error {
		pendingActivities <- activity.GetInfo(ctx)
		return activity.ErrResultPending
	}
	env.SdkWorker().RegisterActivity(pendingActivity)
	lastActivity := func(ctx context.Context) error {
		return nil
	}
	env.SdkWorker().RegisterActivity(lastActivity)

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
	env.SdkWorker().RegisterWorkflow(myWorkflow)

	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
		ID:                  env.Tv().WorkflowID(),
		TaskQueue:           env.WorkerTaskQueue(),
		WorkflowTaskTimeout: time.Second, // Use shorter timeout so test completes faster.
	}, myWorkflow)
	s.NoError(err)

	// wait until all of the activities are started (but not finished) before trying to schedule the last one
	var activityInfo activity.Info
	for range testcore.ClientSuiteLimit {
		activityInfo = <-pendingActivities
	}
	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), workflowRun.GetID(), "", readyToScheduleLastActivity, nil))

	// verify that we can't finish the workflow yet
	{
		ctx, cancel := context.WithTimeout(s.Context(), time.Millisecond*100)
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
		return env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: workflowRun.GetID(), RunId: workflowRun.GetRunID()})
	}, 3*time.Second, 500*time.Millisecond)

	// mark one of the pending activities as complete and verify that the workflow can now complete
	s.NoError(env.SdkClient().CompleteActivity(s.Context(), activityInfo.TaskToken, nil, nil))
	s.NoError(workflowRun.Get(s.Context(), nil))
}

func (s *ClientMiscTestSuite) TestTooManyCancelRequests() {
	env := testcore.NewEnv(s.T())

	// create a large number of blocked workflows
	numTargetWorkflows := testcore.ClientSuiteLimit + 1
	targetWorkflow := func(ctx workflow.Context) error {
		return workflow.Await(ctx, func() bool {
			return false
		})
	}
	env.SdkWorker().RegisterWorkflow(targetWorkflow)
	targetWorkflowIDs := make([]string, numTargetWorkflows)
	for i := range numTargetWorkflows {
		targetWorkflowIDs[i] = fmt.Sprintf("workflow-%d", i)
		_, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
			ID:        targetWorkflowIDs[i],
			TaskQueue: env.WorkerTaskQueue(),
		}, targetWorkflow)
		s.NoError(err)
	}

	// define a workflow that attempts to cancel a given subsequence of the blocked workflows
	cancelWorkflowsInRange := func(ctx workflow.Context, start, stop int) error {
		var futures []workflow.Future
		for i := start; i < stop; i++ {
			future := workflow.RequestCancelExternalWorkflow(ctx, targetWorkflowIDs[i], "")
			futures = append(futures, future)
		}
		for _, future := range futures {
			if err := future.Get(ctx, nil); err != nil {
				return err
			}
		}
		return nil
	}
	env.SdkWorker().RegisterWorkflow(cancelWorkflowsInRange)

	// try to cancel all the workflows at once and verify that we can't because of the limit violation
	s.Run("CancelAllWorkflowsAtOnce", func(s *ClientMiscTestSuite) {
		run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
			TaskQueue: env.WorkerTaskQueue(),
			ID:        "canceler",
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
			return env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()})
		}, 5*time.Second, 500*time.Millisecond)

		shardID := common.WorkflowIDToHistoryShard(env.NamespaceID().String(), run.GetID(), env.GetTestClusterConfig().HistoryConfig.NumHistoryShards)
		workflowExecution, err := env.GetTestCluster().ExecutionManager().GetWorkflowExecution(s.Context(), &persistence.GetWorkflowExecutionRequest{
			ShardID:     shardID,
			NamespaceID: env.NamespaceID().String(),
			WorkflowID:  run.GetID(),
			RunID:       run.GetRunID(),
			ArchetypeID: chasm.WorkflowArchetypeID,
		})
		s.NoError(err)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, workflowExecution.State.ExecutionState.Status)
		s.Empty(workflowExecution.State.RequestCancelInfos)
		s.NoError(env.SdkClient().CancelWorkflow(s.Context(), run.GetID(), ""))
	})

	// try to cancel all the workflows in separate batches of cancel workflows and verify that it works
	s.Run("CancelWorkflowsInSeparateBatches", func(s *ClientMiscTestSuite) {
		batch1, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
			TaskQueue: env.WorkerTaskQueue(),
		}, cancelWorkflowsInRange, 0, numTargetWorkflows/2)
		s.NoError(err)

		batch2, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
			TaskQueue: env.WorkerTaskQueue(),
		}, cancelWorkflowsInRange, numTargetWorkflows/2, numTargetWorkflows)
		s.NoError(err)

		s.NoError(batch1.Get(s.Context(), nil))
		s.NoError(batch2.Get(s.Context(), nil))
	})
}

func (s *ClientMiscTestSuite) TestTooManyPendingSignals() {
	env := testcore.NewEnv(s.T())
	receiverID := "receiver"
	signalName := "my-signal"
	sender := func(ctx workflow.Context, n int) error {
		var futures []workflow.Future
		for range n {
			future := workflow.SignalExternalWorkflow(ctx, receiverID, "", signalName, nil)
			futures = append(futures, future)
		}
		var errs error
		for _, future := range futures {
			err := future.Get(ctx, nil)
			errs = multierr.Combine(errs, err)
		}
		return errs
	}
	env.SdkWorker().RegisterWorkflow(sender)

	receiver := func(ctx workflow.Context) error {
		channel := workflow.GetSignalChannel(ctx, signalName)
		for {
			channel.Receive(ctx, nil)
		}
	}
	env.SdkWorker().RegisterWorkflow(receiver)
	_, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
		TaskQueue: env.WorkerTaskQueue(),
		ID:        receiverID,
	}, receiver)
	s.NoError(err)

	successTimeout := time.Second * 5
	s.Run("TooManySignals", func(s *ClientMiscTestSuite) {
		senderRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
			TaskQueue: env.WorkerTaskQueue(),
			ID:        "sender-1",
		}, sender, testcore.ClientSuiteLimit+1)
		s.NoError(err)
		{
			ctx, cancel := context.WithTimeout(s.Context(), successTimeout)
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
			return env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: senderRun.GetID(), RunId: senderRun.GetRunID()})
		}, 3*time.Second, 500*time.Millisecond)

		s.NoError(env.SdkClient().CancelWorkflow(s.Context(), senderRun.GetID(), ""))
	})

	s.Run("NotTooManySignals", func(s *ClientMiscTestSuite) {
		senderRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
			TaskQueue: env.WorkerTaskQueue(),
			ID:        "sender-2",
		}, sender, testcore.ClientSuiteLimit)
		s.NoError(err)
		ctx, cancel := context.WithTimeout(s.Context(), successTimeout)
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
	env := testcore.NewEnv(s.T())
	// Simulate continue as new tight loop, and verify server throttle the rate.
	env.SdkWorker().RegisterWorkflow(continueAsNewTightLoop)

	options := sdkclient.StartWorkflowOptions{
		ID:                 env.Tv().WorkflowID(),
		TaskQueue:          env.WorkerTaskQueue(),
		WorkflowRunTimeout: time.Second * 10,
	}
	startTime := time.Now()
	future, err := env.SdkClient().ExecuteWorkflow(s.Context(), options, continueAsNewTightLoop, 1, 5)
	s.NoError(err)

	var runCount int
	err = future.Get(s.Context(), &runCount)
	s.NoError(err)
	s.Equal(5, runCount)
	duration := time.Since(startTime)
	s.GreaterOrEqual(duration, time.Second*4)
}

func (s *ClientMiscTestSuite) TestStickyAutoReset() {
	env := testcore.NewEnv(s.T())
	// This test starts a workflow, wait and verify that the workflow is on sticky task queue.
	// Then it stops the worker for 10s, this will make matching aware that sticky worker is dead.
	// Then test sends a signal to the workflow to trigger a new workflow task.
	// Test verify that workflow is still on sticky task queue.
	// Then test poll the original workflow task queue directly (not via SDK),
	// and verify that the polled WorkflowTask contains full history.
	wfFn := func(ctx workflow.Context) (string, error) {
		sigCh := workflow.GetSignalChannel(ctx, "sig-name")
		var msg string
		sigCh.Receive(ctx, &msg)
		return msg, nil
	}

	env.SdkWorker().RegisterWorkflow(wfFn)

	options := sdkclient.StartWorkflowOptions{
		ID:                 env.Tv().WorkflowID(),
		TaskQueue:          env.WorkerTaskQueue(),
		WorkflowRunTimeout: time.Minute,
	}
	// start the test workflow
	future, err := env.SdkClient().ExecuteWorkflow(s.Context(), options, wfFn)
	s.NoError(err)

	// wait until wf started and sticky is set
	var stickyQueue string
	s.Eventually(func() bool {
		ms, err := env.AdminClient().DescribeMutableState(s.Context(), &adminservice.DescribeMutableStateRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: future.GetID(),
			},
			Archetype: chasm.WorkflowArchetype,
		})
		s.NoError(err)
		stickyQueue = ms.DatabaseMutableState.ExecutionInfo.StickyTaskQueue
		// verify workflow has sticky task queue
		return stickyQueue != "" && stickyQueue != env.WorkerTaskQueue()
	}, 5*time.Second, 200*time.Millisecond)

	// stop worker
	env.SdkWorker().Stop()
	//nolint:forbidigo
	time.Sleep(time.Second * 11) // wait 11s (longer than 10s timeout), after this time, matching will detect StickyWorkerUnavailable
	resp, err := env.FrontendClient().DescribeTaskQueue(s.Context(), &workflowservice.DescribeTaskQueueRequest{
		Namespace:     env.Namespace().String(),
		TaskQueue:     &taskqueuepb.TaskQueue{Name: stickyQueue, Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: env.WorkerTaskQueue()},
		TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
	})
	s.NoError(err)
	s.NotNil(resp)
	for _, p := range resp.Pollers {
		s.NotNil(p.LastAccessTime)
		s.Greater(time.Since(p.LastAccessTime.AsTime()), time.Second*10)
	}

	startTime := time.Now()
	// send a signal which will trigger a new wft, and it will be pushed to original task queue
	err = env.SdkClient().SignalWorkflow(s.Context(), future.GetID(), "", "sig-name", "sig1")
	s.NoError(err)

	// check that mutable state still has sticky enabled
	ms, err := env.AdminClient().DescribeMutableState(s.Context(), &adminservice.DescribeMutableStateRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: future.GetID(),
		},
		Archetype: chasm.WorkflowArchetype,
	})
	s.NoError(err)
	s.NotEmpty(ms.DatabaseMutableState.ExecutionInfo.StickyTaskQueue)
	s.Equal(stickyQueue, ms.DatabaseMutableState.ExecutionInfo.StickyTaskQueue)

	// now poll from normal queue, and it should see the full history.
	task, err := env.FrontendClient().PollWorkflowTaskQueue(s.Context(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	})

	// should be able to get the task without having to wait until sticky timeout (5s)
	pollLatency := time.Since(startTime)
	s.Less(pollLatency, time.Second*4)

	s.NoError(err)
	s.NotNil(task)
	s.NotNil(task.History)
	s.NotEmpty(task.History.Events)
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
	env := testcore.NewEnv(s.T())

	readyToSendUpdate := make(chan bool, 1)
	updateHasBeenAdmitted := make(chan bool)

	localActivityFn := func(ctx context.Context) error {
		readyToSendUpdate <- true // Ensure update is sent after first WFT has started.
		<-updateHasBeenAdmitted   // Ensure WF completion is not attempted until after update has been admitted.
		return nil
	}

	workflowFn := func(ctx workflow.Context) error {
		err := workflow.SetUpdateHandler(ctx, env.Tv().HandlerName(), func(ctx workflow.Context, arg string) (string, error) {
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

	env.SdkWorker().RegisterWorkflow(workflowFn)

	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
		ID:                  env.Tv().WorkflowID(),
		TaskQueue:           env.WorkerTaskQueue(),
		WorkflowTaskTimeout: 10 * time.Second,
		WorkflowRunTimeout:  10 * time.Second,
	}, workflowFn)
	s.NoError(err)

	// Block until first workflow task started.
	<-readyToSendUpdate

	// Send update and wait until it is admitted. This isn't convenient: since Admitted is non-durable, we do not expose
	// an API for doing it directly. Instead we send the update and poll until it's reported to be in admitted state.
	updateHandleCh := make(chan sdkclient.WorkflowUpdateHandle)
	updateErrCh := make(chan error)
	go func() {
		handle, err := env.SdkClient().UpdateWorkflow(s.Context(), sdkclient.UpdateWorkflowOptions{
			UpdateID:     env.Tv().UpdateID(),
			UpdateName:   env.Tv().HandlerName(),
			WorkflowID:   workflowRun.GetID(),
			RunID:        workflowRun.GetRunID(),
			Args:         []any{"update-value"},
			WaitForStage: sdkclient.WorkflowUpdateStageCompleted,
		})
		updateErrCh <- err
		updateHandleCh <- handle
	}()
	for {
		time.Sleep(10 * time.Millisecond) //nolint:forbidigo
		_, err = env.SdkClient().WorkflowService().PollWorkflowExecutionUpdate(s.Context(), &workflowservice.PollWorkflowExecutionUpdateRequest{
			Namespace: env.Namespace().String(),
			UpdateRef: env.Tv().WithRunID(workflowRun.GetRunID()).UpdateRef(),
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

	err = workflowRun.Get(s.Context(), nil)
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

	s.EqualHistoryEvents(`
	1 WorkflowExecutionStarted
	2 WorkflowTaskScheduled
	3 WorkflowTaskStarted
	4 WorkflowTaskCompleted
	5 MarkerRecorded
	6 WorkflowExecutionCompleted`,
		env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
			RunId:      workflowRun.GetRunID(),
		}))
}

func (s *ClientMiscTestSuite) Test_CancelActivityAndTimerBeforeComplete() {
	env := testcore.NewEnv(s.T())
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

	env.SdkWorker().RegisterWorkflow(workflowFn)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 env.Tv().WorkflowID(),
		TaskQueue:          env.WorkerTaskQueue(),
		WorkflowRunTimeout: 5 * time.Second,
	}
	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, workflowFn)
	s.NoError(err)
	err = workflowRun.Get(s.Context(), nil)
	s.NoError(err)
}

func (s *ClientMiscTestSuite) Test_FinishWorkflowWithDeferredCommands() {
	env := testcore.NewEnv(s.T())
	activityFn := func(ctx context.Context) error {
		return nil
	}

	childWorkflowFn := func(ctx workflow.Context) error {
		return nil
	}

	childID := "child"
	workflowFn := func(ctx workflow.Context) error {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
		}
		ctx = workflow.WithActivityOptions(ctx, ao)
		defer workflow.ExecuteActivity(ctx, activityFn)

		cwo := workflow.ChildWorkflowOptions{
			WorkflowID:         childID,
			WorkflowRunTimeout: 10 * time.Second,
			TaskQueue:          env.WorkerTaskQueue(),
		}
		ctx = workflow.WithChildOptions(ctx, cwo)
		defer workflow.ExecuteChildWorkflow(ctx, childWorkflowFn)
		workflow.NewTimer(ctx, time.Second)
		return nil
	}

	env.SdkWorker().RegisterWorkflow(workflowFn)
	env.SdkWorker().RegisterWorkflow(childWorkflowFn)
	env.SdkWorker().RegisterActivity(activityFn)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 env.Tv().WorkflowID(),
		TaskQueue:          env.WorkerTaskQueue(),
		WorkflowRunTimeout: 10 * time.Second,
	}

	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, workflowFn)
	s.NoError(err)

	s.NotNil(workflowRun)
	s.NotEmpty(workflowRun.GetRunID())

	err = workflowRun.Get(s.Context(), nil)
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
	s.assertHistory(env, workflowRun.GetID(), workflowRun.GetRunID(), expectedHistory)
}

// This test simulates workflow generate command with invalid attributes.
// Server is expected to fail the workflow task and schedule a retry immediately for first attempt,
// but if workflow task keeps failing, server will drop the task and wait for timeout to schedule additional retries.
// This is the same behavior as the SDK used to do, but now we would do on server.
func (s *ClientMiscTestSuite) TestInvalidCommandAttribute() {
	env := testcore.NewEnv(s.T())
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

			resp, err := env.SdkClient().DescribeWorkflowExecution(
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

	env.SdkWorker().RegisterWorkflow(workflowFn)
	env.SdkWorker().RegisterActivity(activityFn)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        env.Tv().WorkflowID(),
		TaskQueue: env.WorkerTaskQueue(),
		// With 3s TaskTimeout and 5s RunTimeout, we expect to see total of 3 attempts.
		// First attempt follow by immediate retry follow by timeout and 3rd attempt after WorkflowTaskTimeout.
		WorkflowTaskTimeout: 3 * time.Second,
		WorkflowRunTimeout:  5 * time.Second,
	}

	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, workflowFn)
	s.NoError(err)

	s.NotNil(workflowRun)
	s.NotEmpty(workflowRun.GetRunID())

	// wait until workflow close (it will be timeout)
	err = workflowRun.Get(s.Context(), nil)
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
	s.assertHistory(env, workflowRun.GetID(), workflowRun.GetRunID(), expectedHistory)

	// assert workflow task retried 3 times
	s.Len(startedTime, 3)

	s.Less(startedTime[1].Sub(startedTime[0]), time.Second)      // retry immediately
	s.Greater(startedTime[2].Sub(startedTime[1]), time.Second*3) // retry after WorkflowTaskTimeout
}

func (s *ClientMiscTestSuite) Test_BufferedQuery() {
	env := testcore.NewEnv(s.T())
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

	env.SdkWorker().RegisterWorkflow(workflowFn)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 env.Tv().WorkflowID(),
		TaskQueue:          env.WorkerTaskQueue(),
		WorkflowRunTimeout: 20 * time.Second,
	}
	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, workflowFn)
	s.NoError(err)

	s.NotNil(workflowRun)
	s.NotEmpty(workflowRun.GetRunID())

	// wait until first wf task started
	wfStarted.Wait()

	describeErrCh := make(chan error, 1)
	go func() {
		// sleep 2s to make sure DescribeMutableState is called after QueryWorkflow
		time.Sleep(2 * time.Second) //nolint:forbidigo
		// make DescribeMutableState call, which force mutable state to reload from db
		_, err := env.AdminClient().DescribeMutableState(s.Context(), &adminservice.DescribeMutableStateRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowRun.GetID(),
				RunId:      workflowRun.GetRunID(),
			},
			Archetype: chasm.WorkflowArchetype,
		})
		describeErrCh <- err
	}()

	// this query will be buffered in mutable state because workflow task is in-flight.
	encodedQueryResult, err := env.SdkClient().QueryWorkflow(s.Context(), workflowRun.GetID(), workflowRun.GetRunID(), "foo")

	s.NoError(err)
	var queryResult string
	err = encodedQueryResult.Get(&queryResult)
	s.NoError(err)
	s.Equal("done", queryResult)

	err = workflowRun.Get(s.Context(), nil)
	s.NoError(err)
	s.NoError(<-describeErrCh) // assert on test goroutine after workflow completes
}

func (s *ClientMiscTestSuite) assertHistory(env *testcore.TestEnv, wid, rid string, expected []enumspb.EventType) {
	iter := env.SdkClient().GetWorkflowHistory(s.Context(), wid, rid, false, 0)
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
	env := testcore.NewEnv(s.T())

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
		sigCh := workflow.GetSignalChannel(ctx, env.Tv().HandlerName())
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

	env.SdkWorker().RegisterWorkflow(workflowFn)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        env.Tv().WorkflowID(),
		TaskQueue: env.WorkerTaskQueue(),
		// Intentionally use same timeout for WorkflowTaskTimeout and WorkflowRunTimeout so if workflow task is not
		// correctly dispatched, it would time out which would fail the workflow and cause test to fail.
		WorkflowTaskTimeout: 10 * time.Second,
		WorkflowRunTimeout:  10 * time.Second,
	}
	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, workflowFn)
	s.NoError(err)

	s.NotNil(workflowRun)
	s.NotEmpty(workflowRun.GetRunID())

	// block until first workflow task started
	<-sigReadyToSendChan

	err = env.SdkClient().SignalWorkflow(
		s.Context(),
		workflowRun.GetID(),
		workflowRun.GetRunID(),
		env.Tv().HandlerName(),
		"signal-value",
	)
	s.NoError(err)

	close(sigSendDoneChan)

	err = workflowRun.Get(s.Context(), nil)
	s.NoError(err) // if new workflow task is not correctly dispatched, it would cause timeout error here
	s.Equal("signal-value", receivedSig)

	s.EqualHistoryEvents(`
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
		env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
			RunId:      workflowRun.GetRunID(),
		}))
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
		s.Run(tt.name, func(s *ClientMiscTestSuite) {
			env := testcore.NewEnv(s.T())
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

			oldWorker := worker.New(env.SdkClient(), taskQueue, worker.Options{})
			oldWorker.RegisterWorkflow(workflowFn)
			err := oldWorker.Start()
			s.NoError(err)

			workflowOptions := sdkclient.StartWorkflowOptions{
				ID:                 "workflow",
				TaskQueue:          taskQueue,
				WorkflowRunTimeout: 20 * time.Second,
			}
			workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, workflowFn)
			s.NoError(err)

			s.NotNil(workflowRun)
			s.NotEmpty(workflowRun.GetRunID())

			s.Eventually(func() bool {
				// wait until first workflow task completed (so we know sticky is set on workflow)
				iter := env.SdkClient().GetWorkflowHistory(s.Context(), workflowRun.GetID(), "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
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
			newWorker := worker.New(env.SdkClient(), taskQueue, worker.Options{})
			newWorker.RegisterWorkflow(workflowFn)
			err = newWorker.Start()
			s.NoError(err)
			defer newWorker.Stop()

			startTime := time.Now()
			// send a signal, and workflow should complete immediately, there should not be 5s delay
			if tt.doSignal {
				err = env.SdkClient().SignalWorkflow(s.Context(), workflowRun.GetID(), "", "test", "test")
				s.NoError(err)

				err = workflowRun.Get(s.Context(), nil)
				s.NoError(err)
			} else if tt.doQuery {
				// send a signal, and workflow should complete immediately, there should not be 5s delay
				queryResult, err := env.SdkClient().QueryWorkflow(s.Context(), workflowRun.GetID(), "", "test", "test")
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
	env := testcore.NewEnv(s.T(), testcore.WithWorkerService("batch operations"))

	type myData struct {
		Stuff  string
		Things []int
	}

	workflowFn := func(ctx workflow.Context) (myData, error) {
		var receivedData myData
		workflow.GetSignalChannel(ctx, "my-signal").Receive(ctx, &receivedData)
		return receivedData, nil
	}
	env.SdkWorker().RegisterWorkflow(workflowFn)

	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
		ID:                       env.Tv().WorkflowID(),
		TaskQueue:                env.WorkerTaskQueue(),
		WorkflowExecutionTimeout: 10 * time.Second,
	}, workflowFn)
	s.NoError(err)

	input1 := myData{
		Stuff:  "here's some data",
		Things: []int{7, 8, 9},
	}
	inputPayloads, err := converter.GetDefaultDataConverter().ToPayloads(input1)
	s.NoError(err)

	_, err = env.SdkClient().WorkflowService().StartBatchOperation(s.Context(), &workflowservice.StartBatchOperationRequest{
		Namespace: env.Namespace().String(),
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
	err = workflowRun.Get(s.Context(), &returnedData)
	s.NoError(err)

	s.Equal(input1, returnedData)
}

// TestListBatchOperations starts several batch operations of different types and
// verifies that both ListBatchOperations and DescribeBatchOperation report the
// expected fields for each (job id, operation type, state, start time, and — for
// Describe — the visibility query and reason).
func (s *ClientMiscTestSuite) TestListBatchOperations() {
	env := testcore.NewEnv(s.T(),
		testcore.WithWorkerService("batch operations"),
		// This test starts multiple batch operations in the same namespace; the
		// default per-namespace limit is 1, so raise it to the functional-test limit.
		testcore.WithDynamicConfig(dynamicconfig.FrontendMaxConcurrentBatchOperationPerNamespace, testcore.ClientSuiteLimit),
	)
	ctx := s.Context()

	const reason = "list-describe-batch-ops-test"

	type startedBatch struct {
		jobID  string
		query  string
		opType enumspb.BatchOperationType
	}

	// Each operation uses a visibility query that matches no workflows. The batch
	// operation is still created, recorded, and listable/describable with its
	// fields — so we don't need real target workflows to assert metadata.
	operations := []struct {
		opType  enumspb.BatchOperationType
		request *workflowservice.StartBatchOperationRequest
	}{
		{
			enumspb.BATCH_OPERATION_TYPE_TERMINATE,
			&workflowservice.StartBatchOperationRequest{
				Operation: &workflowservice.StartBatchOperationRequest_TerminationOperation{
					TerminationOperation: &batchpb.BatchOperationTermination{},
				},
			},
		},
		{
			enumspb.BATCH_OPERATION_TYPE_SIGNAL,
			&workflowservice.StartBatchOperationRequest{
				Operation: &workflowservice.StartBatchOperationRequest_SignalOperation{
					SignalOperation: &batchpb.BatchOperationSignal{Signal: "my-signal"},
				},
			},
		},
		{
			enumspb.BATCH_OPERATION_TYPE_CANCEL,
			&workflowservice.StartBatchOperationRequest{
				Operation: &workflowservice.StartBatchOperationRequest_CancellationOperation{
					CancellationOperation: &batchpb.BatchOperationCancellation{},
				},
			},
		},
	}

	started := make([]startedBatch, 0, len(operations))
	for _, op := range operations {
		jobID := uuid.NewString()
		query := fmt.Sprintf("WorkflowType = '%s'", testcore.RandomizeStr(s.T().Name()))
		req := op.request
		req.Namespace = env.Namespace().String()
		req.VisibilityQuery = query
		req.JobId = jobID
		req.Reason = reason
		_, err := env.SdkClient().WorkflowService().StartBatchOperation(ctx, req)
		s.NoError(err)
		started = append(started, startedBatch{jobID: jobID, query: query, opType: op.opType})
	}

	// ListBatchOperations should eventually surface every started job. The batch
	// operations are themselves workflows indexed in visibility, so allow a moment.
	listed := make(map[string]*batchpb.BatchOperationInfo)
	//nolint:forbidigo // for tests with waits
	s.Eventually(func() bool {
		resp, err := env.SdkClient().WorkflowService().ListBatchOperations(ctx, &workflowservice.ListBatchOperationsRequest{
			Namespace: env.Namespace().String(),
		})
		if err != nil {
			return false
		}
		clear(listed)
		for _, op := range resp.GetOperationInfo() {
			listed[op.GetJobId()] = op
		}
		for _, b := range started {
			if _, ok := listed[b.jobID]; !ok {
				return false
			}
		}
		return true
	}, 15*time.Second, 200*time.Millisecond)

	for _, b := range started {
		info := listed[b.jobID]
		s.NotNil(info, "batch operation %s was not returned by ListBatchOperations", b.jobID)
		s.Equal(b.jobID, info.GetJobId())
		s.Equal(b.opType, info.GetOperationType(), "ListBatchOperations operation type for %s", b.jobID)
		s.NotEqual(enumspb.BATCH_OPERATION_STATE_UNSPECIFIED, info.GetState(), "ListBatchOperations state for %s", b.jobID)
		s.NotNil(info.GetStartTime(), "ListBatchOperations start time for %s", b.jobID)

		desc, err := env.SdkClient().WorkflowService().DescribeBatchOperation(ctx, &workflowservice.DescribeBatchOperationRequest{
			Namespace: env.Namespace().String(),
			JobId:     b.jobID,
		})
		s.NoError(err)
		s.Equal(b.jobID, desc.GetJobId())
		s.Equal(b.opType, desc.GetOperationType(), "DescribeBatchOperation operation type for %s", b.jobID)
		s.Equal(b.query, desc.GetQuery(), "DescribeBatchOperation visibility query for %s", b.jobID)
		s.Equal(reason, desc.GetReason(), "DescribeBatchOperation reason for %s", b.jobID)
		s.NotEqual(enumspb.BATCH_OPERATION_STATE_UNSPECIFIED, desc.GetState(), "DescribeBatchOperation state for %s", b.jobID)
	}
}

func (s *ClientMiscTestSuite) TestBatchReset() {
	env := testcore.NewEnv(s.T(), testcore.WithWorkerService("batch operations"))
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
	env.SdkWorker().RegisterWorkflow(workflowFn)
	env.SdkWorker().RegisterActivity(activityFn)

	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
		ID:                       env.Tv().WorkflowID(),
		TaskQueue:                env.WorkerTaskQueue(),
		WorkflowExecutionTimeout: 10 * time.Second,
	}, workflowFn)
	s.NoError(err)

	// make sure it failed the first time
	var result int
	err = workflowRun.Get(s.Context(), &result)
	s.Error(err)

	count.Add(1)

	_, err = env.SdkClient().WorkflowService().StartBatchOperation(s.Context(), &workflowservice.StartBatchOperationRequest{
		Namespace: env.Namespace().String(),
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
		workflowRun = env.SdkClient().GetWorkflow(s.Context(), workflowRun.GetID(), "")
		err = workflowRun.Get(s.Context(), &result)
		return err == nil && result == 1
	}, 5*time.Second, 200*time.Millisecond)
}

func (s *ClientMiscTestSuite) TestBatchResetByBuildId() {
	env := testcore.NewEnv(s.T(), testcore.WithWorkerService("batch operations"))
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

	w1 := worker.New(env.SdkClient(), tq, worker.Options{BuildID: buildIdv1})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	w1.RegisterActivityWithOptions(act1, activity.RegisterOptions{Name: "act1"})
	s.NoError(w1.Start())

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)
	ex := &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()}
	// wait for first wft and first activity to complete
	s.Eventually(func() bool { return len(env.GetHistory(env.Namespace().String(), ex)) >= 10 }, 5*time.Second, 100*time.Millisecond) //nolint:forbidigo

	w1.Stop()

	// should see one run of act1
	s.Equal(int32(1), act1count.Load())

	w2 := worker.New(env.SdkClient(), tq, worker.Options{BuildID: buildIdv2})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	w2.RegisterActivityWithOptions(act1, activity.RegisterOptions{Name: "act1"})
	w2.RegisterActivityWithOptions(act2, activity.RegisterOptions{Name: "act2"})
	w2.RegisterActivityWithOptions(badact, activity.RegisterOptions{Name: "badact"})
	s.NoError(w2.Start())
	defer w2.Stop()

	// unblock the workflow
	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), run.GetRunID(), "wait", nil))

	// wait until we see three calls to badact
	s.Eventually(func() bool { return badcount.Load() >= 3 }, 10*time.Second, 200*time.Millisecond)

	// at this point act2 should have been invokved once also
	s.Equal(int32(1), act2count.Load())

	w2.Stop()

	w3 := worker.New(env.SdkClient(), tq, worker.Options{BuildID: buildIdv3})
	w3.RegisterWorkflowWithOptions(wf3, workflow.RegisterOptions{Name: "wf"})
	w3.RegisterActivityWithOptions(act1, activity.RegisterOptions{Name: "act1"})
	w3.RegisterActivityWithOptions(act2, activity.RegisterOptions{Name: "act2"})
	w3.RegisterActivityWithOptions(act3, activity.RegisterOptions{Name: "act3"})
	w3.RegisterActivityWithOptions(badact, activity.RegisterOptions{Name: "badact"})
	s.NoError(w3.Start())
	defer w3.Stop()

	// but v3 is not quite compatible, the workflow should be blocked on non-determinism errors for now.
	waitCtx, cancel := context.WithTimeout(s.Context(), 2*time.Second)
	defer cancel()
	s.Error(run.Get(waitCtx, nil))

	// wait for it to appear in visibility
	query := fmt.Sprintf(`%s = "%s" and %s = "%s"`,
		sadefs.ExecutionStatus, "Running",
		sadefs.BuildIds, worker_versioning.UnversionedBuildIdSearchAttribute(buildIdv2))
	s.Eventually(func() bool {
		resp, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: env.Namespace().String(),
			Query:     query,
		})
		return err == nil && len(resp.Executions) == 1
	}, 10*time.Second, 500*time.Millisecond)

	// reset it using v2 as the bad build ID
	_, err = env.FrontendClient().StartBatchOperation(s.Context(), &workflowservice.StartBatchOperationRequest{
		Namespace:       env.Namespace().String(),
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
		return env.SdkClient().GetWorkflow(s.Context(), run.GetID(), "").Get(s.Context(), &out) == nil && out == "done 3!"
	}, 10*time.Second, 200*time.Millisecond)

	s.Equal(int32(1), act1count.Load()) // we should not see an addition run of act1
	s.Equal(int32(2), act2count.Load()) // we should see an addition run of act2 (reset point was before it)
	s.Equal(int32(1), act3count.Load()) // we should see one run of act3
}
