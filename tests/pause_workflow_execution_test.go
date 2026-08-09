package tests

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type PauseWorkflowExecutionSuite struct {
	parallelsuite.Suite[*PauseWorkflowExecutionSuite]
}

type pauseWorkflowExecutionEnv struct {
	*testcore.TestEnv

	testEndSignal string
	pauseIdentity string
	pauseReason   string

	workflowFn      func(ctx workflow.Context) (string, error)
	childWorkflowFn func(ctx workflow.Context) (string, error)
	activityFn      func(ctx context.Context) (string, error)

	activityCompletedCh   chan struct{}
	activityCompletedOnce sync.Once

	activityShouldSucceed atomic.Bool
}

func TestPauseWorkflowExecutionSuite(t *testing.T) {
	parallelsuite.Run(t, &PauseWorkflowExecutionSuite{})
}

// newTestEnv creates a TestEnv with the dynamic config this suite needs and
// registers the workflows/activities the tests use.
func (s *PauseWorkflowExecutionSuite) newTestEnv(opts ...testcore.TestOption) *pauseWorkflowExecutionEnv {
	baseOpts := []testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.WorkflowPauseEnabled, true),
	}
	testEnv := testcore.NewEnv(s.T(), append(baseOpts, opts...)...)

	env := &pauseWorkflowExecutionEnv{
		TestEnv:             testEnv,
		testEndSignal:       "test-end",
		pauseIdentity:       "functional-test",
		pauseReason:         "pausing workflow for acceptance test",
		activityCompletedCh: make(chan struct{}, 1),
	}

	env.workflowFn = func(ctx workflow.Context) (string, error) {
		env.Logger.Debug("workflow started")
		ao := workflow.ActivityOptions{
			StartToCloseTimeout:    5 * time.Second,
			ScheduleToCloseTimeout: 10 * time.Second,
		}
		ctx = workflow.WithActivityOptions(ctx, ao)

		var activityResult string
		env.Logger.Debug("executing activity")
		if err := workflow.ExecuteActivity(ctx, env.activityFn).Get(ctx, &activityResult); err != nil {
			return "", err
		}

		var childResult string
		env.Logger.Debug("executing child workflow")
		if err := workflow.ExecuteChildWorkflow(ctx, env.childWorkflowFn).Get(ctx, &childResult); err != nil {
			return "", err
		}

		env.Logger.Debug("waiting to receive signal to complete the workflow")
		signalCh := workflow.GetSignalChannel(ctx, env.testEndSignal)
		var signalPayload string
		signalCh.Receive(ctx, &signalPayload)
		env.Logger.Debug("signal received to complete the workflow")
		return signalPayload + activityResult + childResult, nil
	}

	env.childWorkflowFn = func(ctx workflow.Context) (string, error) {
		return "child-workflow", nil
	}

	env.activityFn = func(ctx context.Context) (string, error) {
		env.Logger.Debug("activity started")
		env.activityCompletedOnce.Do(func() {
			// blocks until the test case unblocks the activity.
			<-env.activityCompletedCh
		})
		env.Logger.Debug("activity completed")
		return "activity", nil
	}

	env.SdkWorker().RegisterWorkflow(env.workflowFn)
	env.SdkWorker().RegisterWorkflow(env.childWorkflowFn)
	env.SdkWorker().RegisterActivity(env.activityFn)

	// Setup for TestPauseWorkflowAndActivity
	env.activityShouldSucceed.Store(false)
	env.SdkWorker().RegisterWorkflow(env.workflowWithFailingActivity)
	env.SdkWorker().RegisterActivity(env.failingActivity)

	return env
}

// failingActivity is an activity that fails until activityShouldSucceed is set to true.
func (env *pauseWorkflowExecutionEnv) failingActivity(ctx context.Context) (string, error) {
	if env.activityShouldSucceed.Load() {
		return "activity-completed", nil
	}
	return "", errors.New("activity-failure")
}

// workflowWithFailingActivity is a workflow that executes the failing activity.
func (env *pauseWorkflowExecutionEnv) workflowWithFailingActivity(ctx workflow.Context) (string, error) {
	ao := workflow.ActivityOptions{
		ActivityID:             "failing-activity",
		StartToCloseTimeout:    5 * time.Second,
		ScheduleToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    1 * time.Second,
			BackoffCoefficient: 1,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var activityResult string
	if err := workflow.ExecuteActivity(ctx, env.failingActivity).Get(ctx, &activityResult); err != nil {
		return "", err
	}

	return activityResult, nil
}

// TestPauseUnpauseWorkflowExecution tests that the pause and unpause workflow execution APIs work as expected.
// Test sequence:
// 1. Start the workflow.
// 2. Pause the workflow. Assert that the workflow is paused.
// 3. Send signal to the workflow. Assert that the workflow is still paused.
// 4. Unpause the workflow. Assert that the workflow is now running.
// 5. Unblock the activity to complete the workflow.
// 6. Assert that the workflow is completed.
func (s *PauseWorkflowExecutionSuite) TestPauseUnpauseWorkflowExecution() {
	env := s.newTestEnv()

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("pause-wf-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}

	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, env.workflowFn)
	s.NoError(err)
	workflowID := workflowRun.GetID()
	runID := workflowRun.GetRunID()

	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.GetStatus())
		// Wait for the workflow task to be processed and the activity to be scheduled,
		// so that a subsequent pause request is applied to a fully initialized workflow.
		require.NotEmpty(t, desc.PendingActivities)
	}, 5*time.Second, 100*time.Millisecond)

	pauseRequest := &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	}

	pauseResp, err := env.FrontendClient().PauseWorkflowExecution(s.Context(), pauseRequest)
	s.NoError(err)
	s.NotNil(pauseResp)

	// ensure that the workflow is paused
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		s.assertWorkflowIsPaused(env, workflowID, runID)
	}, 5*time.Second, 200*time.Millisecond)

	// Send signal to the workflow to complete the workflow. Since the workflow is paused, it should stay paused.
	err = env.SdkClient().SignalWorkflow(s.Context(), workflowID, runID, env.testEndSignal, "signal to complete the workflow")
	s.NoError(err)
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		s.assertWorkflowIsPaused(env, workflowID, runID)
	}, 5*time.Second, 200*time.Millisecond)

	// Unpause the workflow.
	unpauseRequest := &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	}
	unpauseResp, err := env.FrontendClient().UnpauseWorkflowExecution(s.Context(), unpauseRequest)
	s.NoError(err)
	s.NotNil(unpauseResp)

	// Workflow status should be running after unpausing. The workflow won't complete until the activity is unblocked.
	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.GetStatus(), "workflow is not running. Status: %s", info.GetStatus())

		// Verify TemporalPauseInfo search attribute is removed after unpause
		searchAttrs := info.GetSearchAttributes()
		if searchAttrs != nil {
			pauseInfoPayload, hasPauseInfo := searchAttrs.GetIndexedFields()["TemporalPauseInfo"]
			if hasPauseInfo && pauseInfoPayload != nil {
				var pauseInfoEntries []string
				err = payload.Decode(pauseInfoPayload, &pauseInfoEntries)
				require.NoError(t, err)
				require.Empty(t, pauseInfoEntries, "TemporalPauseInfo should be empty after unpause")
			}
		}
	}, 5*time.Second, 200*time.Millisecond)

	// Unblock the activity to complete the workflow.
	env.SendToChannel(env.activityCompletedCh)

	// assert that the workflow completes now.
	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, info.GetStatus(), "workflow is not completed. Status: %s", info.GetStatus())
	}, 10*time.Second, 200*time.Millisecond)
}

// TestPauseUnpauseWorkflowExecution_PendingWorkflowTaskAtPauseTime is a regression
// test for a bug where pausing a workflow that already had a pending (undelivered)
// workflow task left that stale task in place across unpause. Since the unpause API
// only schedules a fresh workflow task when none is already pending, the stale task
// silently blocked any new workflow task from ever being created - the workflow was
// stuck forever, with no dispatchable task.
//
// The fix fails the pending-but-not-started workflow task outright (in the same
// transaction as the paused event) instead of just invalidating it, so it resolves
// cleanly in history and no longer blocks the unpause API from scheduling a fresh one.
//
// Test sequence:
//  1. Start a workflow on a task queue with no active pollers, so its first workflow
//     task remains pending and undelivered.
//  2. Pause the workflow while that task is still pending.
//  3. Unpause the workflow.
//  4. Only now start a worker for the task queue, and assert the workflow actually
//     receives a workflow task and completes.
func (s *PauseWorkflowExecutionSuite) TestPauseUnpauseWorkflowExecution_PendingWorkflowTaskAtPauseTime() {
	env := s.newTestEnv()

	// A dedicated, unpolled task queue keeps the first workflow task pending until
	// we deliberately start a worker for it below.
	taskQueue := testcore.RandomizeStr("pause-pending-wft-tq")
	workflowFn := func(ctx workflow.Context) (string, error) {
		return "done", nil
	}

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("pause-pending-wft-wf-" + s.T().Name()),
		TaskQueue: taskQueue,
	}
	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, workflowFn)
	s.NoError(err)
	workflowID := workflowRun.GetID()
	runID := workflowRun.GetRunID()

	// Confirm the first workflow task is pending and undelivered before pausing.
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		s.NoError(err)
		s.NotNil(desc.GetPendingWorkflowTask(), "expected an undelivered workflow task before pausing")
	}, 5*time.Second, 100*time.Millisecond)

	pauseRequest := &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	}
	pauseResp, err := env.FrontendClient().PauseWorkflowExecution(s.Context(), pauseRequest)
	s.NoError(err)
	s.NotNil(pauseResp)

	s.Await(func(s *PauseWorkflowExecutionSuite) {
		s.assertWorkflowIsPaused(env, workflowID, runID)
	}, 5*time.Second, 200*time.Millisecond)

	unpauseRequest := &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	}
	unpauseResp, err := env.FrontendClient().UnpauseWorkflowExecution(s.Context(), unpauseRequest)
	s.NoError(err)
	s.NotNil(unpauseResp)

	// The first workflow task was never delivered, so pausing must fail it
	// outright (in the same transaction as the paused event), followed by
	// unpause and a freshly scheduled workflow task. If the stale pre-pause
	// task were left dangling instead of explicitly failed, no second
	// WorkflowTaskScheduled event would appear here. The failure itself must
	// point back at the first task (ScheduledEventId 2) and be attributed to
	// the history service, not a worker.
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		events := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		})
		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskFailed {"Cause":39,"ScheduledEventId":2,"Identity":"history-service"}
  4 WorkflowExecutionPaused
  5 WorkflowExecutionUnpaused
  6 WorkflowTaskScheduled`, events)
	}, 5*time.Second, 200*time.Millisecond)

	// Only now start a worker for the task queue. Without the fix, the stale
	// pre-pause task blocks a fresh one from ever being scheduled, and the
	// workflow never completes.
	worker := sdkworker.New(env.SdkClient(), taskQueue, sdkworker.Options{})
	worker.RegisterWorkflow(workflowFn)
	s.NoError(worker.Start())
	defer worker.Stop()

	ctx, cancel := context.WithTimeout(s.Context(), 15*time.Second)
	defer cancel()
	var result string
	s.NoError(workflowRun.Get(ctx, &result), "workflow did not complete after unpause; a fresh workflow task was likely never scheduled")
	s.Equal("done", result)
}

// TestPauseWorkflowExecution_StartedWorkflowTaskSurvivesPause is the counterpart
// to TestPauseUnpauseWorkflowExecution_PendingWorkflowTaskAtPauseTime: it verifies
// that pausing while a workflow task is already started (polled by a worker, but
// not yet completed) does not interfere with it. The worker's eventual completion
// must still be accepted normally, in contrast to a merely-scheduled (never polled)
// task, which pausing fails outright.
//
// Test sequence:
//  1. Start a workflow and manually poll its first workflow task (marking it
//     started), without responding yet.
//  2. Pause the workflow while that task is still outstanding. Because a
//     workflow task is in flight, the paused event is buffered rather than
//     landing in history immediately - this is ordinary Temporal event-batching
//     behavior, unrelated to pause specifically.
//  3. Complete the task with no commands and assert the completion succeeds and
//     the task resolves as completed, not failed. (A command that closes the
//     workflow, like CompleteWorkflowExecution, would be rejected here for an
//     unrelated reason: Temporal never allows a terminal command in the same
//     batch as buffered events, regardless of pause.)
func (s *PauseWorkflowExecutionSuite) TestPauseWorkflowExecution_StartedWorkflowTaskSurvivesPause() {
	env := s.newTestEnv()
	tv := env.Tv()

	we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           tv.RequestID(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(60 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            tv.WorkerIdentity(),
	})
	s.NoError(err)
	workflowID := tv.WorkflowID()
	runID := we.GetRunId()

	// Poll (and thus start) the first workflow task, but don't respond yet.
	polledTask, err := env.FrontendClient().PollWorkflowTaskQueue(s.Context(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: tv.TaskQueue(),
		Identity:  tv.WorkerIdentity(),
	})
	s.NoError(err)
	s.NotEmpty(polledTask.GetTaskToken())

	// Pause while that task is started (in flight with the "worker" above).
	pauseRequest := &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	}
	pauseResp, err := env.FrontendClient().PauseWorkflowExecution(s.Context(), pauseRequest)
	s.NoError(err)
	s.NotNil(pauseResp)

	s.Await(func(s *PauseWorkflowExecutionSuite) {
		s.assertWorkflowIsPaused(env, workflowID, runID)
	}, 5*time.Second, 200*time.Millisecond)

	// The task that was already started before pause must still be completable,
	// even with no commands to process.
	_, err = env.TaskPoller().HandleWorkflowTask(tv, polledTask, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	})
	s.NoError(err, "a workflow task that was already started before pause must still be completable")

	// The task resolved as completed (not failed), and the paused event -
	// buffered while the task was in flight - is flushed right after it.
	events := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	})
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionPaused`, events)

	// The workflow remains paused (we never unpaused it); no new workflow task
	// was scheduled as a side effect of completing the in-flight one.
	s.assertWorkflowIsPaused(env, workflowID, runID)
}

// TestPauseWorkflowExecution_StartedWorkflowTaskTimesOutWhilePaused covers if
// a workflow task was started before pause and its worker crashes, the task
// must still resolve via its own normal start-to-close timeout. The
// workflow must remain correctly paused (no new task scheduled) until unpaused,
// at which point a fresh workflow task becomes available.
func (s *PauseWorkflowExecutionSuite) TestPauseWorkflowExecution_StartedWorkflowTaskTimesOutWhilePaused() {
	env := s.newTestEnv()
	tv := env.Tv()

	we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           tv.RequestID(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(60 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            tv.WorkerIdentity(),
	})
	s.NoError(err)
	workflowID := tv.WorkflowID()
	runID := we.GetRunId()

	// Poll (and thus start) the first workflow task, but never respond - simulating
	// a worker that crashed or otherwise never comes back.
	polledTask, err := env.FrontendClient().PollWorkflowTaskQueue(s.Context(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: tv.TaskQueue(),
		Identity:  tv.WorkerIdentity(),
	})
	s.NoError(err)
	s.NotEmpty(polledTask.GetTaskToken())

	pauseRequest := &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	}
	pauseResp, err := env.FrontendClient().PauseWorkflowExecution(s.Context(), pauseRequest)
	s.NoError(err)
	s.NotNil(pauseResp)

	s.Await(func(s *PauseWorkflowExecutionSuite) {
		s.assertWorkflowIsPaused(env, workflowID, runID)
	}, 5*time.Second, 200*time.Millisecond)

	// The task's own start-to-close timeout must still fire normally - it must
	// not be silently dropped as stale - and the buffered paused event flushes
	// right after it, same as a normal completion would.
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		events := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		})
		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskTimedOut
  5 WorkflowExecutionPaused`, events)
	}, 15*time.Second, 500*time.Millisecond)

	// No new workflow task was scheduled as a result of the timeout, since the
	// workflow is still paused.
	desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
	s.NoError(err)
	s.Nil(desc.GetPendingWorkflowTask(), "no new workflow task should be scheduled while paused")

	// Unpause; a fresh workflow task must become available and completable.
	unpauseRequest := &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	}
	unpauseResp, err := env.FrontendClient().UnpauseWorkflowExecution(s.Context(), unpauseRequest)
	s.NoError(err)
	s.NotNil(unpauseResp)

	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	})
	s.NoError(err, "a fresh workflow task must become available and completable after unpause")
}

// TestPauseWorkflowExecution_StartedWorkflowTaskFailsWhilePaused is the third
// variant alongside TestPauseWorkflowExecution_StartedWorkflowTaskSurvivesPause
// (completed) and TestPauseWorkflowExecution_StartedWorkflowTaskTimesOutWhilePaused
// (timed out): if a worker explicitly fails a workflow task that was already
// started before pause (e.g. reporting a workflow panic), that failure must
// still be accepted normally. Unlike a plain in-flight completion, a failure's
// own request to create a retry task is suppressed while paused (the same
// paused gate that skips scheduling a follow-up after any in-flight
// resolution), so no retry task is scheduled until the workflow is unpaused.
func (s *PauseWorkflowExecutionSuite) TestPauseWorkflowExecution_StartedWorkflowTaskFailsWhilePaused() {
	env := s.newTestEnv()
	tv := env.Tv()

	we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           tv.RequestID(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(60 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            tv.WorkerIdentity(),
	})
	s.NoError(err)
	workflowID := tv.WorkflowID()
	runID := we.GetRunId()

	// Poll (and thus start) the first workflow task, but don't respond yet.
	polledTask, err := env.FrontendClient().PollWorkflowTaskQueue(s.Context(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: tv.TaskQueue(),
		Identity:  tv.WorkerIdentity(),
	})
	s.NoError(err)
	s.NotEmpty(polledTask.GetTaskToken())

	// Pause while that task is started (in flight with the "worker" above).
	pauseRequest := &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	}
	pauseResp, err := env.FrontendClient().PauseWorkflowExecution(s.Context(), pauseRequest)
	s.NoError(err)
	s.NotNil(pauseResp)

	s.Await(func(s *PauseWorkflowExecutionSuite) {
		s.assertWorkflowIsPaused(env, workflowID, runID)
	}, 5*time.Second, 200*time.Millisecond)

	// The worker now reports a failure (e.g. a workflow panic) for the task
	// that was already started before pause. This must still be accepted.
	_, err = env.FrontendClient().RespondWorkflowTaskFailed(s.Context(), &workflowservice.RespondWorkflowTaskFailedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: polledTask.GetTaskToken(),
		Cause:     enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
		Identity:  tv.WorkerIdentity(),
	})
	s.NoError(err, "a workflow task that was already started before pause must still be failable")

	// The task resolved as failed, and the paused event - buffered while the
	// task was in flight - is flushed right after it. No retry task is
	// scheduled: RespondWorkflowTaskFailed's own request to create one is
	// suppressed by the same paused gate that applies to any in-flight
	// resolution (completion, failure, or timeout).
	events := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	})
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionPaused`, events)

	desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
	s.NoError(err)
	s.Nil(desc.GetPendingWorkflowTask(), "no retry task should be scheduled while paused")

	// Unpause; a fresh workflow task must become available and completable.
	unpauseRequest := &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	}
	unpauseResp, err := env.FrontendClient().UnpauseWorkflowExecution(s.Context(), unpauseRequest)
	s.NoError(err)
	s.NotNil(unpauseResp)

	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	})
	s.NoError(err, "a fresh workflow task must become available and completable after unpause")
}

// TestListWorkflowExecutionsPausedHasNoCloseTime verifies that a paused workflow,
// which is still an open execution, is returned by ListWorkflowExecutions without a
// CloseTime (i.e. nil "End" time), just like a running workflow. A paused workflow
// must not be reported with the Go zero time as its CloseTime.
func (s *PauseWorkflowExecutionSuite) TestListWorkflowExecutionsPausedHasNoCloseTime() {
	env := s.newTestEnv()

	workflowID := testcore.RandomizeStr("pause-wf-list-" + s.T().Name())
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: env.WorkerTaskQueue(),
	}

	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, env.workflowFn)
	s.NoError(err)
	runID := workflowRun.GetRunID()

	// Wait for the workflow to be running with a scheduled activity, so that the
	// subsequent pause request is applied to a fully initialized workflow.
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		s.NoError(err)
		info := desc.GetWorkflowExecutionInfo()
		s.NotNil(info)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.GetStatus())
		s.NotEmpty(desc.PendingActivities)
	}, 5*time.Second, 100*time.Millisecond)

	pauseResp, err := env.FrontendClient().PauseWorkflowExecution(s.Context(), &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)
	s.NotNil(pauseResp)

	s.Await(func(s *PauseWorkflowExecutionSuite) {
		s.assertWorkflowIsPaused(env, workflowID, runID)
	}, 5*time.Second, 200*time.Millisecond)

	// List the workflow via visibility and assert that the paused execution is
	// reported as open: paused status but no CloseTime / ExecutionDuration.
	query := fmt.Sprintf("WorkflowId = '%s'", workflowID)
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		listResp, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: env.Namespace().String(),
			PageSize:  10,
			Query:     query,
		})
		s.NoError(err)
		s.NotNil(listResp)
		s.Len(listResp.GetExecutions(), 1)

		execution := listResp.GetExecutions()[0]
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED, execution.GetStatus())
		s.Nil(execution.GetCloseTime())
		s.Nil(execution.GetExecutionDuration())
	}, 10*time.Second, 200*time.Millisecond)
}

// TestPauseWorkflowAndActivity tests the coexistence of workflow and activity pause entries in TemporalPauseInfo.
// 1. Start a workflow with a failing activity
// 2. Pause the activity
// 3. Pause the workflow
// 4. Verify TemporalPauseInfo contains both workflow and activity pause entries
// 5. Unblock the failing activity and unpause it
// 6. Verify activity completes but workflow remains paused (only workflow pause entries in TemporalPauseInfo)
// 7. Unpause the workflow
// 8. Verify workflow completes successfully
func (s *PauseWorkflowExecutionSuite) TestPauseWorkflowAndActivity() {
	env := s.newTestEnv()

	// Reset the activity success flag for this test
	env.activityShouldSucceed.Store(false)

	// This matches the activity ID defined in workflowWithFailingActivity
	activityID := "failing-activity"

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("pause-wf-and-activity-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}

	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, env.workflowWithFailingActivity)
	s.NoError(err)
	workflowID := workflowRun.GetID()
	runID := workflowRun.GetRunID()

	// Wait for activity to start and fail at least once
	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		require.Len(t, desc.PendingActivities, 1)
		require.NotNil(t, desc.PendingActivities[0].LastFailure)
	}, 5*time.Second, 200*time.Millisecond)

	// Pause the activity
	pauseActivityRequest := &workflowservice.PauseActivityRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		Activity: &workflowservice.PauseActivityRequest_Id{Id: activityID},
		Identity: env.pauseIdentity,
		Reason:   "pausing activity for test",
	}
	pauseActivityResp, err := env.FrontendClient().PauseActivity(s.Context(), pauseActivityRequest)
	s.NoError(err)
	s.NotNil(pauseActivityResp)

	// Wait for activity to be paused
	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		require.Len(t, desc.PendingActivities, 1)
		require.True(t, desc.PendingActivities[0].Paused)
	}, 5*time.Second, 200*time.Millisecond)

	// Pause the workflow
	pauseWorkflowRequest := &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	}
	pauseWorkflowResp, err := env.FrontendClient().PauseWorkflowExecution(s.Context(), pauseWorkflowRequest)
	s.NoError(err)
	s.NotNil(pauseWorkflowResp)

	// Verify both workflow and activity are paused, and TemporalPauseInfo contains entries for both
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		// Use existing helper to verify workflow is paused (includes workflow pause search attribute checks)
		s.assertWorkflowIsPaused(env, workflowID, runID)

		// Additionally verify activity is paused
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		s.NoError(err)
		s.Len(desc.PendingActivities, 1)
		s.True(desc.PendingActivities[0].Paused)

		// Additionally verify TemporalPauseInfo contains activity pause entry
		s.True(env.hasActivityPauseEntries(desc), "Should contain at least one activity pause entry")
	}, 5*time.Second, 200*time.Millisecond)

	// Unblock the failing activity so it can succeed
	env.activityShouldSucceed.Store(true)

	// Unpause the activity
	unpauseActivityRequest := &workflowservice.UnpauseActivityRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		Activity: &workflowservice.UnpauseActivityRequest_Id{Id: activityID},
		Identity: env.pauseIdentity,
	}
	unpauseActivityResp, err := env.FrontendClient().UnpauseActivity(s.Context(), unpauseActivityRequest)
	s.NoError(err)
	s.NotNil(unpauseActivityResp)

	// Verify activity completes but workflow remains paused
	// TemporalPauseInfo should only contain workflow pause entries (activity entries removed)
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		// Use existing helper to verify workflow is still paused (includes workflow pause search attribute checks)
		s.assertWorkflowIsPaused(env, workflowID, runID)

		// Additionally verify activity is no longer pending (completed)
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		s.NoError(err)
		s.Empty(desc.PendingActivities, "Activity should have completed")

		// Verify the activity completed successfully by checking workflow history
		// Since this workflow only has one activity, the presence of ActivityTaskCompleted confirms success
		hist := env.SdkClient().GetWorkflowHistory(s.Context(), workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		activityCompleted := false
		for hist.HasNext() {
			event, err := hist.Next()
			s.NoError(err)
			if event.EventType == enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED {
				activityCompleted = true
				break
			}
		}
		s.True(activityCompleted, "Activity should have completed successfully")

		// Additionally verify TemporalPauseInfo no longer contains activity pause entries
		s.False(env.hasActivityPauseEntries(desc), "Should not contain activity pause entries")
	}, 5*time.Second, 200*time.Millisecond)

	// Unpause the workflow
	unpauseWorkflowRequest := &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	}
	unpauseWorkflowResp, err := env.FrontendClient().UnpauseWorkflowExecution(s.Context(), unpauseWorkflowRequest)
	s.NoError(err)
	s.NotNil(unpauseWorkflowResp)

	// Verify workflow completes successfully
	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, info.GetStatus(), "workflow is not completed. Status: %s", info.GetStatus())

		// Verify TemporalPauseInfo is empty after unpause
		searchAttrs := info.GetSearchAttributes()
		if searchAttrs != nil {
			pauseInfoPayload, hasPauseInfo := searchAttrs.GetIndexedFields()["TemporalPauseInfo"]
			if hasPauseInfo && pauseInfoPayload != nil {
				var pauseInfoEntries []string
				err = payload.Decode(pauseInfoPayload, &pauseInfoEntries)
				require.NoError(t, err)
				require.Empty(t, pauseInfoEntries, "TemporalPauseInfo should be empty after workflow completes")
			}
		}
	}, 10*time.Second, 200*time.Millisecond)
}

// TestUnpauseWorkflowKeepsActivityPaused tests that unpausing a workflow does not unpause its paused activities.
// 1. Start a workflow with a failing activity
// 2. Pause the activity
// 3. Pause the workflow
// 4. Unpause the workflow (while the activity is still paused)
// 5. Verify the activity remains paused and the workflow is running
func (s *PauseWorkflowExecutionSuite) TestUnpauseWorkflowKeepsActivityPaused() {
	env := s.newTestEnv()

	env.activityShouldSucceed.Store(false)

	activityID := "failing-activity"

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("unpause-wf-keeps-activity-paused-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}

	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, env.workflowWithFailingActivity)
	s.NoError(err)
	workflowID := workflowRun.GetID()
	runID := workflowRun.GetRunID()

	// Wait for activity to fail at least once
	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		require.Len(t, desc.PendingActivities, 1)
		require.NotNil(t, desc.PendingActivities[0].LastFailure)
	}, 5*time.Second, 200*time.Millisecond)

	// Pause the activity
	_, err = env.FrontendClient().PauseActivity(s.Context(), &workflowservice.PauseActivityRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		Activity:  &workflowservice.PauseActivityRequest_Id{Id: activityID},
		Identity:  env.pauseIdentity,
		Reason:    "pausing activity for test",
	})
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		require.Len(t, desc.PendingActivities, 1)
		require.True(t, desc.PendingActivities[0].Paused)
	}, 5*time.Second, 200*time.Millisecond)

	// Pause the workflow
	_, err = env.FrontendClient().PauseWorkflowExecution(s.Context(), &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)

	s.Await(func(s *PauseWorkflowExecutionSuite) {
		s.assertWorkflowIsPaused(env, workflowID, runID)
	}, 5*time.Second, 200*time.Millisecond)

	// Unpause the workflow only
	_, err = env.FrontendClient().UnpauseWorkflowExecution(s.Context(), &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)

	// Verify the workflow is running but the activity remains paused
	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.GetStatus(),
			"workflow should be running after unpause, got: %s", info.GetStatus())
		require.Len(t, desc.PendingActivities, 1)
		require.True(t, desc.PendingActivities[0].Paused, "activity should still be paused after workflow unpause")
	}, 5*time.Second, 200*time.Millisecond)

	// Cleanup: unblock and unpause the activity so the workflow can complete
	env.activityShouldSucceed.Store(true)
	_, err = env.FrontendClient().UnpauseActivity(s.Context(), &workflowservice.UnpauseActivityRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		Activity:  &workflowservice.UnpauseActivityRequest_Id{Id: activityID},
		Identity:  env.pauseIdentity,
	})
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, desc.GetWorkflowExecutionInfo().GetStatus())
	}, 10*time.Second, 200*time.Millisecond)
}

func (s *PauseWorkflowExecutionSuite) TestQueryWorkflowWhenPaused() {
	env := s.newTestEnv()

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("pause-wf-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}

	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, env.workflowFn)
	s.NoError(err)
	workflowID := workflowRun.GetID()
	runID := workflowRun.GetRunID()

	// Wait for the workflow task to be processed and the activity to be scheduled,
	// so that a subsequent pause request is applied to a fully initialized workflow.
	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.GetStatus())
		require.NotEmpty(t, desc.PendingActivities)
	}, 5*time.Second, 100*time.Millisecond)

	// Pause the workflow.
	pauseRequest := &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	}
	pauseResp, err := env.FrontendClient().PauseWorkflowExecution(s.Context(), pauseRequest)
	s.NoError(err)
	s.NotNil(pauseResp)

	// Wait until paused.
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		s.assertWorkflowIsPaused(env, workflowID, runID)
	}, 5*time.Second, 200*time.Millisecond)

	// Issue a query to the paused workflow. It should return QueryRejected with WORKFLOW_EXECUTION_STATUS_PAUSED status.
	queryReq := &workflowservice.QueryWorkflowRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		Query: &querypb.WorkflowQuery{
			QueryType: "__stack_trace",
		},
	}
	queryResp, err := env.FrontendClient().QueryWorkflow(s.Context(), queryReq)
	s.NoError(err)
	s.NotNil(queryResp)
	s.NotNil(queryResp.GetQueryRejected())
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED, queryResp.GetQueryRejected().GetStatus())

	// Unpause the workflow.
	unpauseRequest := &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	}
	unpauseResp, err := env.FrontendClient().UnpauseWorkflowExecution(s.Context(), unpauseRequest)
	s.NoError(err)
	s.NotNil(unpauseResp)

	// Unblock the activity and send the signal to complete the workflow.
	env.SendToChannel(env.activityCompletedCh)
	err = env.SdkClient().SignalWorkflow(s.Context(), workflowID, runID, env.testEndSignal, "test end signal")
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, info.GetStatus())
	}, 5*time.Second, 200*time.Millisecond)
}

// TestPauseWorkflowExecutionRequestValidation tests that pause workflow execution request validation. We don't really need a valid workflow to test this.
// - fails when the identity is too long.
// - fails when the reason is too long.
// - fails when the request id is too long.
// - fails when the dynamic config is disabled.
func (s *PauseWorkflowExecutionSuite) TestPauseWorkflowExecutionRequestValidation() {
	env := s.newTestEnv()

	namespaceName := env.Namespace().String()

	// fails when the identity is too long.
	pauseRequest := &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  namespaceName,
		WorkflowId: "test-workflow-id",
		RunId:      uuid.NewString(),
		Identity:   strings.Repeat("x", 2000),
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	}
	resp, err := env.FrontendClient().PauseWorkflowExecution(s.Context(), pauseRequest)
	s.Error(err)
	s.Nil(resp)
	var invalidArgumentErr *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgumentErr)
	s.NotNil(invalidArgumentErr)
	s.Contains(invalidArgumentErr.Error(), "identity is too long.")

	// fails when the reason is too long.
	pauseRequest = &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  namespaceName,
		WorkflowId: "test-workflow-id",
		RunId:      uuid.NewString(),
		Identity:   env.pauseIdentity,
		Reason:     strings.Repeat("x", 2000),
		RequestId:  uuid.NewString(),
	}
	resp, err = env.FrontendClient().PauseWorkflowExecution(s.Context(), pauseRequest)
	s.Error(err)
	s.Nil(resp)
	s.ErrorAs(err, &invalidArgumentErr)
	s.NotNil(invalidArgumentErr)
	s.Contains(invalidArgumentErr.Error(), "reason is too long.")

	// fails when the request id is too long.
	pauseRequest = &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  namespaceName,
		WorkflowId: "test-workflow-id",
		RunId:      uuid.NewString(),
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  strings.Repeat("x", 2000),
	}
	resp, err = env.FrontendClient().PauseWorkflowExecution(s.Context(), pauseRequest)
	s.Error(err)
	s.Nil(resp)
	s.ErrorAs(err, &invalidArgumentErr)
	s.NotNil(invalidArgumentErr)
	s.Contains(invalidArgumentErr.Error(), "request id is too long.")

	// fails when the dynamic config is disabled.
	env.OverrideDynamicConfig(dynamicconfig.WorkflowPauseEnabled, false)
	pauseRequest = &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  namespaceName,
		WorkflowId: "test-workflow-id",
		RunId:      uuid.NewString(),
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	}
	resp, err = env.FrontendClient().PauseWorkflowExecution(s.Context(), pauseRequest)
	s.Error(err)
	s.Nil(resp)
	var unimplementedErr *serviceerror.Unimplemented
	s.ErrorAs(err, &unimplementedErr)
	s.NotNil(unimplementedErr)
	s.Contains(unimplementedErr.Error(), namespaceName)
}

// TestUnpauseWorkflowExecutionRequestValidation tests unpause workflow execution request validation. We don't need a valid workflow.
// - fails when the identity is too long.
// - fails when the reason is too long.
// - fails when the request id is too long.
func (s *PauseWorkflowExecutionSuite) TestUnpauseWorkflowExecutionRequestValidation() {
	env := s.newTestEnv()

	namespaceName := env.Namespace().String()

	// fails when the identity is too long.
	unpauseRequest := &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  namespaceName,
		WorkflowId: "test-workflow-id",
		RunId:      uuid.NewString(),
		Identity:   strings.Repeat("x", 2000),
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	}
	resp, err := env.FrontendClient().UnpauseWorkflowExecution(s.Context(), unpauseRequest)
	s.Error(err)
	s.Nil(resp)
	var invalidArgumentErr *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgumentErr)
	s.NotNil(invalidArgumentErr)
	s.Contains(invalidArgumentErr.Error(), "identity is too long.")

	// fails when the reason is too long.
	unpauseRequest = &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  namespaceName,
		WorkflowId: "test-workflow-id",
		RunId:      uuid.NewString(),
		Identity:   env.pauseIdentity,
		Reason:     strings.Repeat("x", 2000),
		RequestId:  uuid.NewString(),
	}
	resp, err = env.FrontendClient().UnpauseWorkflowExecution(s.Context(), unpauseRequest)
	s.Error(err)
	s.Nil(resp)
	s.ErrorAs(err, &invalidArgumentErr)
	s.NotNil(invalidArgumentErr)
	s.Contains(invalidArgumentErr.Error(), "reason is too long.")

	// fails when the request id is too long.
	unpauseRequest = &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  namespaceName,
		WorkflowId: "test-workflow-id",
		RunId:      uuid.NewString(),
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  strings.Repeat("x", 2000),
	}
	resp, err = env.FrontendClient().UnpauseWorkflowExecution(s.Context(), unpauseRequest)
	s.Error(err)
	s.Nil(resp)
	s.ErrorAs(err, &invalidArgumentErr)
	s.NotNil(invalidArgumentErr)
	s.Contains(invalidArgumentErr.Error(), "request id is too long.")
}

func (s *PauseWorkflowExecutionSuite) TestPauseWorkflowExecutionAlreadyPaused() {
	env := s.newTestEnv()

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("pause-wf-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}

	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, env.workflowFn)
	s.NoError(err)
	workflowID := workflowRun.GetID()
	runID := workflowRun.GetRunID()

	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.GetStatus())
		// Wait for the workflow task to be processed and the activity to be scheduled,
		// so that a subsequent pause request is applied to a fully initialized workflow.
		require.NotEmpty(t, desc.PendingActivities)
	}, 5*time.Second, 100*time.Millisecond)

	// 1st pause request should succeed.
	pauseRequest := &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	}
	pauseResp, err := env.FrontendClient().PauseWorkflowExecution(s.Context(), pauseRequest)
	s.NoError(err)
	s.NotNil(pauseResp)

	// Wait until paused.
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		s.assertWorkflowIsPaused(env, workflowID, runID)
	}, 5*time.Second, 200*time.Millisecond)

	// 2nd pause request should fail with failed precondition error.
	pauseRequest.RequestId = uuid.NewString()
	pauseResp, err = env.FrontendClient().PauseWorkflowExecution(s.Context(), pauseRequest)
	s.Error(err)
	s.Nil(pauseResp)
	var failedPreconditionErr *serviceerror.FailedPrecondition
	s.ErrorAs(err, &failedPreconditionErr)
	s.NotNil(failedPreconditionErr)
	s.Contains(failedPreconditionErr.Error(), "workflow is already paused.")

	unpauseRequest := &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     "cleanup after paused workflow test",
		RequestId:  uuid.NewString(),
	}
	unpauseResp, err := env.FrontendClient().UnpauseWorkflowExecution(s.Context(), unpauseRequest)
	s.NoError(err)
	s.NotNil(unpauseResp)

	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.GetStatus())
		require.Nil(t, desc.GetWorkflowExtendedInfo().GetPauseInfo())
	}, 5*time.Second, 200*time.Millisecond)

	// Unblock the activity and send the signal to complete the workflow.
	env.SendToChannel(env.activityCompletedCh)
	err = env.SdkClient().SignalWorkflow(s.Context(), workflowID, runID, env.testEndSignal, "test end signal")
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, info.GetStatus())
	}, 5*time.Second, 200*time.Millisecond)
}

// TestPauseDuringInFlightWorkflowTask reproduces the race described in
// https://github.com/temporalio/temporal/issues/10239: if
// PauseWorkflowExecution arrives while a worker has a workflow task in flight,
// the worker's RespondWorkflowTaskCompleted can still be accepted after the
// WORKFLOW_EXECUTION_PAUSED event is appended. A follow-up WORKFLOW_TASK_SCHEDULED
// is then written and the next workflow task completion resets Status to RUNNING
// without clearing executionInfo.PauseInfo. The workflow ends up stuck:
// Status=RUNNING with pauseInfo set, and UnpauseWorkflowExecution rejects with
// FailedPrecondition because it only inspects Status.
//
// The bug is timing-sensitive. It often does not reproduce on a single run.
// Run with -count=N (e.g. -count=50) to reliably observe failures.
func (s *PauseWorkflowExecutionSuite) TestPauseDuringInFlightWorkflowTask() {
	env := s.newTestEnv()

	const (
		tickActivityName = "pause-race-tick-activity"
		busyWorkflowName = "pause-race-busy-workflow"
		iterations       = 200
	)

	// tickActivity completes immediately so the workflow keeps cycling
	// through workflow tasks with minimal wall time between them.
	tickActivity := func(ctx context.Context) error {
		return nil
	}

	// busyWorkflow runs a long tight loop of short activities so that workflow
	// tasks are being scheduled/started/completed continuously. This widens
	// the chance of a Pause RPC arriving while a WT is in flight on the worker.
	busyWorkflow := func(ctx workflow.Context) error {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout:    5 * time.Second,
			ScheduleToCloseTimeout: 30 * time.Second,
		}
		ctx = workflow.WithActivityOptions(ctx, ao)
		for range iterations {
			if err := workflow.ExecuteActivity(ctx, tickActivityName).Get(ctx, nil); err != nil {
				return err
			}
		}
		return nil
	}

	env.SdkWorker().RegisterWorkflowWithOptions(busyWorkflow, workflow.RegisterOptions{Name: busyWorkflowName})
	env.SdkWorker().RegisterActivityWithOptions(tickActivity, activity.RegisterOptions{Name: tickActivityName})

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("pause-race-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}

	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, busyWorkflowName)
	s.NoError(err)
	workflowID := workflowRun.GetID()
	runID := workflowRun.GetRunID()

	// Wait until the workflow has progressed a few iterations so workflow
	// tasks are actively flowing through the worker.
	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.GetStatus())
		require.GreaterOrEqual(t, info.GetHistoryLength(), int64(15),
			"workflow has not started cycling through tasks yet")
	}, 10*time.Second, 50*time.Millisecond)

	// Issue the Pause while the worker is still busy. Repeat until either we
	// observe Status=PAUSED or we hit the desync state (Status=RUNNING with
	// pauseInfo set). The race window is small, so we don't always hit it on
	// the first pause/unpause cycle.
	pauseResp, err := env.FrontendClient().PauseWorkflowExecution(s.Context(), &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)
	s.NotNil(pauseResp)

	// Eventually the workflow should reach a stable PAUSED state. The bug
	// manifests as Status=RUNNING with pauseInfo still populated; this assertion
	// is what fails when the race fires.
	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED, info.GetStatus(),
			"workflow ended up desynced: Status=%s, pauseInfo=%v (issue #10239 race)",
			info.GetStatus(), desc.GetWorkflowExtendedInfo().GetPauseInfo())
	}, 10*time.Second, 100*time.Millisecond)

	// Verify history contains no WORKFLOW_TASK_SCHEDULED event after
	// WORKFLOW_EXECUTION_PAUSED — that event (eventId #1963 in the issue
	// reproduction) is the smoking gun for the race.
	hist := env.SdkClient().GetWorkflowHistory(s.Context(), workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	inPaused := false
	for hist.HasNext() {
		event, herr := hist.Next()
		s.NoError(herr)
		switch event.EventType {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_PAUSED:
			inPaused = true
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UNPAUSED:
			inPaused = false
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED:
			s.False(inPaused,
				"WORKFLOW_TASK_SCHEDULED at eventId=%d appended after WORKFLOW_EXECUTION_PAUSED (issue #10239 race)",
				event.EventId)
		default:
		}
	}

	// Unpause should succeed. When the race fires, Status is RUNNING and the
	// unpause API rejects with FailedPrecondition: "workflow is not paused".
	unpauseResp, err := env.FrontendClient().UnpauseWorkflowExecution(s.Context(), &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	})
	s.NoError(err, "UnpauseWorkflowExecution failed; workflow is stuck with Status=RUNNING and pauseInfo set (issue #10239 race)")
	s.NotNil(unpauseResp)

	// Cleanup: terminate so the busy loop doesn't run to completion.
	_, _ = env.FrontendClient().TerminateWorkflowExecution(s.Context(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		Reason: "cleanup after pause race test",
	})
}

// TestUpdateWorkflowWhilePaused asserts that an Update sent to a paused workflow
// is rejected at the client with a FailedPrecondition error, and that the same
// update succeeds once the workflow is unpaused.
func (s *PauseWorkflowExecutionSuite) TestUpdateWorkflowWhilePaused() {
	env := s.newTestEnv()

	const (
		updateName         = "pause-update-handler"
		updateWorkflowName = "pause-update-workflow"
	)
	// A workflow that registers an update handler and then blocks on the end
	// signal. It deliberately has no activity so the update can be processed as
	// soon as the workflow is unpaused.
	updateWorkflow := func(ctx workflow.Context) (string, error) {
		if err := workflow.SetUpdateHandler(ctx, updateName, func(ctx workflow.Context, arg string) (string, error) {
			return "updated:" + arg, nil
		}); err != nil {
			return "", err
		}
		signalCh := workflow.GetSignalChannel(ctx, env.testEndSignal)
		var signalPayload string
		signalCh.Receive(ctx, &signalPayload)
		return signalPayload, nil
	}
	env.SdkWorker().RegisterWorkflowWithOptions(updateWorkflow, workflow.RegisterOptions{Name: updateWorkflowName})

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("pause-update-wf-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}
	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, updateWorkflowName)
	s.NoError(err)
	workflowID := workflowRun.GetID()
	runID := workflowRun.GetRunID()

	// Wait for the first workflow task to complete so the update handler is
	// registered and the workflow is fully initialized before pausing.
	s.waitUntilFirstWorkflowTaskCompleted(env, workflowID, runID)

	// Pause the workflow.
	_, err = env.FrontendClient().PauseWorkflowExecution(s.Context(), &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		s.assertWorkflowIsPaused(env, workflowID, runID)
	}, 5*time.Second, 200*time.Millisecond)

	// An update sent while paused is rejected with FailedPrecondition.
	_, err = env.SdkClient().UpdateWorkflow(s.Context(), sdkclient.UpdateWorkflowOptions{
		WorkflowID:   workflowID,
		RunID:        runID,
		UpdateName:   updateName,
		Args:         []any{"while-paused"},
		WaitForStage: sdkclient.WorkflowUpdateStageCompleted,
	})
	s.Error(err)
	var failedPreconditionErr *serviceerror.FailedPrecondition
	s.ErrorAs(err, &failedPreconditionErr)
	s.NotNil(failedPreconditionErr)
	s.Contains(failedPreconditionErr.Error(), "Workflow is paused")

	// Unpause the workflow.
	_, err = env.FrontendClient().UnpauseWorkflowExecution(s.Context(), &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)

	// The same update now succeeds.
	handle, err := env.SdkClient().UpdateWorkflow(s.Context(), sdkclient.UpdateWorkflowOptions{
		WorkflowID:   workflowID,
		RunID:        runID,
		UpdateName:   updateName,
		Args:         []any{"after-unpause"},
		WaitForStage: sdkclient.WorkflowUpdateStageCompleted,
	})
	s.NoError(err)
	var updateResult string
	s.NoError(handle.Get(s.Context(), &updateResult))
	s.Equal("updated:after-unpause", updateResult)

	// Complete the workflow.
	err = env.SdkClient().SignalWorkflow(s.Context(), workflowID, runID, env.testEndSignal, "done")
	s.NoError(err)
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		s.NoError(err)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, desc.GetWorkflowExecutionInfo().GetStatus())
	}, 5*time.Second, 200*time.Millisecond)
}

// TestSignalWithStartWhilePaused asserts that a SignalWithStart targeting an
// already-running, paused workflow succeeds at the client, the signal is
// buffered (recorded in history without scheduling a workflow task), and the
// buffered signal is delivered once the workflow is unpaused.
func (s *PauseWorkflowExecutionSuite) TestSignalWithStartWhilePaused() {
	env := s.newTestEnv()

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("pause-sws-wf-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}
	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, env.workflowFn)
	s.NoError(err)
	workflowID := workflowRun.GetID()
	runID := workflowRun.GetRunID()

	s.waitUntilRunningWithPendingActivity(env, workflowID, runID)

	// Pause the workflow.
	_, err = env.FrontendClient().PauseWorkflowExecution(s.Context(), &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		s.assertWorkflowIsPaused(env, workflowID, runID)
	}, 5*time.Second, 200*time.Millisecond)

	// SignalWithStart against the existing (paused) run. The workflow is already
	// running, so this only delivers the signal; it must not start a new run.
	const signalPayload = "buffered-via-signal-with-start"
	signalWithStartRun, err := env.SdkClient().SignalWithStartWorkflow(
		s.Context(),
		workflowID,
		env.testEndSignal,
		signalPayload,
		workflowOptions,
		env.workflowFn,
	)
	s.NoError(err)
	s.Equal(runID, signalWithStartRun.GetRunID(), "SignalWithStart must target the existing run, not start a new one")

	// SignalWithStart is synchronous: once it returns, the signal is buffered and
	// the workflow is still paused with no workflow task scheduled.
	s.assertWorkflowIsPaused(env, workflowID, runID)
	s.True(s.historyHasEventType(env, workflowID, runID, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED),
		"signal should be recorded in history while paused")

	// Unpause and unblock the activity so the workflow can drain the buffered
	// signal and complete.
	_, err = env.FrontendClient().UnpauseWorkflowExecution(s.Context(), &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)
	env.SendToChannel(env.activityCompletedCh)

	// The buffered signal is delivered: the workflow completes and its result
	// includes the signal payload.
	var result string
	s.NoError(workflowRun.Get(s.Context(), &result))
	s.Equal(signalPayload+"activity"+"child-workflow", result)
}

// TestSignalBufferingOrderWhilePaused asserts that multiple signals sent while a
// workflow is paused are buffered in order and delivered in a single workflow
// task once the workflow is unpaused.
func (s *PauseWorkflowExecutionSuite) TestSignalBufferingOrderWhilePaused() {
	env := s.newTestEnv()

	const (
		signalName            = "ordered-signal"
		signalCount           = 5
		collectorWorkflowName = "pause-signal-collector-workflow"
	)
	// A workflow that receives signalCount signals and returns them joined in the
	// order received.
	collectorWorkflow := func(ctx workflow.Context) (string, error) {
		ch := workflow.GetSignalChannel(ctx, signalName)
		received := make([]string, 0, signalCount)
		for range signalCount {
			var v string
			ch.Receive(ctx, &v)
			received = append(received, v)
		}
		return strings.Join(received, ","), nil
	}
	env.SdkWorker().RegisterWorkflowWithOptions(collectorWorkflow, workflow.RegisterOptions{Name: collectorWorkflowName})

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("pause-signal-order-wf-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}
	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, collectorWorkflowName)
	s.NoError(err)
	workflowID := workflowRun.GetID()
	runID := workflowRun.GetRunID()

	s.waitUntilFirstWorkflowTaskCompleted(env, workflowID, runID)

	// Pause the workflow.
	_, err = env.FrontendClient().PauseWorkflowExecution(s.Context(), &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		s.assertWorkflowIsPaused(env, workflowID, runID)
	}, 5*time.Second, 200*time.Millisecond)

	// Send all signals while paused; they should be buffered.
	expected := make([]string, 0, signalCount)
	for i := 1; i <= signalCount; i++ {
		v := fmt.Sprintf("sig-%d", i)
		expected = append(expected, v)
		err = env.SdkClient().SignalWorkflow(s.Context(), workflowID, runID, signalName, v)
		s.NoError(err)
	}

	// SignalWorkflow is synchronous: once the calls return, all signals are
	// buffered and the workflow is still paused with no workflow task scheduled.
	s.assertWorkflowIsPaused(env, workflowID, runID)

	// Unpause; the buffered signals are now delivered.
	_, err = env.FrontendClient().UnpauseWorkflowExecution(s.Context(), &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)

	// The workflow completes and the signals were delivered in order.
	var result string
	s.NoError(workflowRun.Get(s.Context(), &result))
	s.Equal(strings.Join(expected, ","), result)

	// All signals are recorded in history while paused (none lost), and exactly
	// one workflow task drains them after unpause. No signals are sent before the
	// pause, so a single "unpaused" boundary is enough: signals before it are the
	// buffered ones, and the workflow's initial (pre-pause) workflow task is
	// naturally excluded from the post-unpause count.
	hist := env.SdkClient().GetWorkflowHistory(s.Context(), workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	unpaused := false
	totalSignals := 0
	signalsBeforeUnpause := 0
	workflowTasksAfterUnpause := 0
	for hist.HasNext() {
		event, herr := hist.Next()
		s.NoError(herr)
		switch event.EventType {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UNPAUSED:
			unpaused = true
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
			totalSignals++
			if !unpaused {
				signalsBeforeUnpause++
			}
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
			if unpaused {
				workflowTasksAfterUnpause++
			}
		default:
			continue
		}
	}
	s.Equal(signalCount, totalSignals, "all signals should be recorded in history (none lost)")
	s.Equal(signalCount, signalsBeforeUnpause, "all signals should be buffered while the workflow is paused")
	s.Equal(1, workflowTasksAfterUnpause, "buffered signals should be drained in a single workflow task")

	// The workflow has run to completion (its result was retrieved above).
	desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, desc.GetWorkflowExecutionInfo().GetStatus())
}

// TestPauseIdempotentSameRequestId asserts that re-issuing a pause with the same
// request id is a no-op (succeeds without error and adds no second pause event),
// unlike a pause with a new request id which fails with "already paused".
func (s *PauseWorkflowExecutionSuite) TestPauseIdempotentSameRequestId() {
	env := s.newTestEnv()

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("pause-idempotent-wf-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}
	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, env.workflowFn)
	s.NoError(err)
	workflowID := workflowRun.GetID()
	runID := workflowRun.GetRunID()

	s.waitUntilRunningWithPendingActivity(env, workflowID, runID)

	requestID := uuid.NewString()
	pauseRequest := &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  requestID,
	}
	_, err = env.FrontendClient().PauseWorkflowExecution(s.Context(), pauseRequest)
	s.NoError(err)
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		s.assertWorkflowIsPaused(env, workflowID, runID)
	}, 5*time.Second, 200*time.Millisecond)

	// Re-issuing the pause with the SAME request id is a no-op.
	pauseResp, err := env.FrontendClient().PauseWorkflowExecution(s.Context(), pauseRequest)
	s.NoError(err)
	s.NotNil(pauseResp)

	// PauseWorkflowExecution is synchronous: the workflow is still paused and only
	// a single pause event was ever written.
	s.assertWorkflowIsPaused(env, workflowID, runID)
	hist := env.SdkClient().GetWorkflowHistory(s.Context(), workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	pauseEvents := 0
	for hist.HasNext() {
		event, herr := hist.Next()
		s.NoError(herr)
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_PAUSED {
			pauseEvents++
		}
	}
	s.Equal(1, pauseEvents, "idempotent pause must not add a second pause event")

	// Cleanup: unpause and complete the workflow.
	_, err = env.FrontendClient().UnpauseWorkflowExecution(s.Context(), &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)
	env.SendToChannel(env.activityCompletedCh)
	err = env.SdkClient().SignalWorkflow(s.Context(), workflowID, runID, env.testEndSignal, "done")
	s.NoError(err)
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		s.NoError(err)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, desc.GetWorkflowExecutionInfo().GetStatus())
	}, 10*time.Second, 200*time.Millisecond)
}

// TestTerminateWhilePaused asserts that a paused workflow can still be terminated
// (terminate is terminal and bypasses workflow-task scheduling).
func (s *PauseWorkflowExecutionSuite) TestTerminateWhilePaused() {
	env := s.newTestEnv()

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("pause-terminate-wf-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}
	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, env.workflowFn)
	s.NoError(err)
	workflowID := workflowRun.GetID()
	runID := workflowRun.GetRunID()

	s.waitUntilRunningWithPendingActivity(env, workflowID, runID)

	// Pause the workflow.
	_, err = env.FrontendClient().PauseWorkflowExecution(s.Context(), &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		s.assertWorkflowIsPaused(env, workflowID, runID)
	}, 5*time.Second, 200*time.Millisecond)

	// Terminate succeeds on a paused workflow.
	_, err = env.FrontendClient().TerminateWorkflowExecution(s.Context(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		Reason: "terminate while paused",
	})
	s.NoError(err)

	s.Await(func(s *PauseWorkflowExecutionSuite) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		s.NoError(err)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, desc.GetWorkflowExecutionInfo().GetStatus())
	}, 5*time.Second, 200*time.Millisecond)
}

// TestPauseLatestRunEmptyRunId asserts that a pause request with an empty RunId
// targets the latest run of the workflow: with a continued-as-new chain, only
// the latest run is paused while the closed original run is unaffected.
func (s *PauseWorkflowExecutionSuite) TestPauseLatestRunEmptyRunId() {
	env := s.newTestEnv()

	const latestRunWorkflowName = "pause-latest-run-workflow"
	// A workflow whose first run immediately continues-as-new, leaving the
	// WorkflowID with a closed original run and a latest run that blocks on the
	// end signal.
	latestRunWorkflow := func(ctx workflow.Context, generation int) error {
		if generation == 0 {
			return workflow.NewContinueAsNewError(ctx, latestRunWorkflowName, generation+1)
		}
		signalCh := workflow.GetSignalChannel(ctx, env.testEndSignal)
		var v string
		signalCh.Receive(ctx, &v)
		return nil
	}
	env.SdkWorker().RegisterWorkflowWithOptions(latestRunWorkflow, workflow.RegisterOptions{Name: latestRunWorkflowName})

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("pause-empty-runid-wf-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}
	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, latestRunWorkflowName, 0)
	s.NoError(err)
	workflowID := workflowRun.GetID()
	originalRunID := workflowRun.GetRunID()

	// Wait for the original run to continue-as-new into the latest run, which is
	// RUNNING and has completed its first workflow task.
	var latestRunID string
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, "")
		s.NoError(err)
		info := desc.GetWorkflowExecutionInfo()
		s.NotNil(info)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.GetStatus())
		s.NotEqual(originalRunID, info.GetExecution().GetRunId(), "expected continue-as-new to a new run")
		s.GreaterOrEqual(info.GetHistoryLength(), int64(4), "latest run has not completed its first workflow task")
		latestRunID = info.GetExecution().GetRunId()
	}, 10*time.Second, 100*time.Millisecond)

	// Pause with an empty RunId; it should target the latest run.
	_, err = env.FrontendClient().PauseWorkflowExecution(s.Context(), &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      "",
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		s.assertWorkflowIsPaused(env, workflowID, latestRunID)
	}, 5*time.Second, 200*time.Millisecond)

	// Only the latest run is paused: the original run remains closed as
	// CONTINUED_AS_NEW.
	originalDesc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, originalRunID)
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, originalDesc.GetWorkflowExecutionInfo().GetStatus())

	// Cleanup: unpause and complete the latest run.
	_, err = env.FrontendClient().UnpauseWorkflowExecution(s.Context(), &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      latestRunID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)
	err = env.SdkClient().SignalWorkflow(s.Context(), workflowID, latestRunID, env.testEndSignal, "done")
	s.NoError(err)
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, latestRunID)
		s.NoError(err)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, desc.GetWorkflowExecutionInfo().GetStatus())
	}, 10*time.Second, 200*time.Millisecond)
}

// TestRunTimeoutWhilePaused asserts that a workflow's run timeout is not
// suppressed by pause: a paused workflow still TIMED_OUT on wall-clock.
func (s *PauseWorkflowExecutionSuite) TestRunTimeoutWhilePaused() {
	env := s.newTestEnv()
	s.assertPausedWorkflowTimesOut(env, sdkclient.StartWorkflowOptions{
		ID:                 testcore.RandomizeStr("pause-run-timeout-wf-" + s.T().Name()),
		TaskQueue:          env.WorkerTaskQueue(),
		WorkflowRunTimeout: 10 * time.Second,
	})
}

// TestExecutionTimeoutWhilePaused asserts that a workflow's execution timeout is
// not suppressed by pause: a paused workflow still TIMED_OUT on wall-clock.
func (s *PauseWorkflowExecutionSuite) TestExecutionTimeoutWhilePaused() {
	env := s.newTestEnv()
	s.assertPausedWorkflowTimesOut(env, sdkclient.StartWorkflowOptions{
		ID:                       testcore.RandomizeStr("pause-execution-timeout-wf-" + s.T().Name()),
		TaskQueue:                env.WorkerTaskQueue(),
		WorkflowExecutionTimeout: 10 * time.Second,
	})
}

// TestCancelWhilePaused is a regression test documenting how RequestCancel
// interacts with pause. The cancel is accepted and recorded while paused, but it
// is not processed until the workflow is unpaused (workflow-task scheduling is
// gated by pause). After unpause the workflow observes the cancel and ends
// CANCELED. This pins the current behavior; the ordering of cancel vs. buffered
// signals at unpause is an open design question and is intentionally not
// asserted here.
func (s *PauseWorkflowExecutionSuite) TestCancelWhilePaused() {
	env := s.newTestEnv()

	const cancelWorkflowName = "pause-cancel-workflow"
	// A workflow that blocks until its context is canceled, then returns the
	// cancellation error so it ends in the CANCELED state.
	cancelWorkflow := func(ctx workflow.Context) error {
		ctx.Done().Receive(ctx, nil)
		return ctx.Err()
	}
	env.SdkWorker().RegisterWorkflowWithOptions(cancelWorkflow, workflow.RegisterOptions{Name: cancelWorkflowName})

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("pause-cancel-wf-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}
	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, cancelWorkflowName)
	s.NoError(err)
	workflowID := workflowRun.GetID()
	runID := workflowRun.GetRunID()

	s.waitUntilFirstWorkflowTaskCompleted(env, workflowID, runID)

	// Pause the workflow.
	_, err = env.FrontendClient().PauseWorkflowExecution(s.Context(), &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		s.assertWorkflowIsPaused(env, workflowID, runID)
	}, 5*time.Second, 200*time.Millisecond)

	// RequestCancel while paused succeeds.
	_, err = env.FrontendClient().RequestCancelWorkflowExecution(s.Context(), &workflowservice.RequestCancelWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		Identity:  env.pauseIdentity,
		RequestId: uuid.NewString(),
	})
	s.NoError(err)

	// RequestCancel is synchronous: once it returns, the cancel is recorded but
	// deferred. The workflow stays paused with a cancel-requested event written
	// and no workflow task scheduled.
	s.assertWorkflowIsPaused(env, workflowID, runID)
	s.True(s.historyHasEventType(env, workflowID, runID, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED),
		"cancel should be recorded in history while paused")

	// Unpause; the deferred cancel is now processed.
	_, err = env.FrontendClient().UnpauseWorkflowExecution(s.Context(), &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)

	// After unpause the workflow observes the cancel and ends CANCELED.
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		s.NoError(err)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED, desc.GetWorkflowExecutionInfo().GetStatus())
	}, 10*time.Second, 200*time.Millisecond)
}

// TestResetWhilePaused is a regression test documenting that resetting a paused
// workflow succeeds and produces a fresh run that is RUNNING — pause is per-run
// and is not carried onto the reset run — while the original run is terminated
// by the reset.
func (s *PauseWorkflowExecutionSuite) TestResetWhilePaused() {
	env := s.newTestEnv()

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("pause-reset-wf-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}
	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, env.workflowFn)
	s.NoError(err)
	workflowID := workflowRun.GetID()
	runID := workflowRun.GetRunID()

	s.waitUntilRunningWithPendingActivity(env, workflowID, runID)

	// Pause the workflow.
	_, err = env.FrontendClient().PauseWorkflowExecution(s.Context(), &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		s.assertWorkflowIsPaused(env, workflowID, runID)
	}, 5*time.Second, 200*time.Millisecond)

	// Find the last completed workflow task to reset to (before the pause event).
	var resetEventID int64
	hist := env.SdkClient().GetWorkflowHistory(s.Context(), workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for hist.HasNext() {
		event, herr := hist.Next()
		s.NoError(herr)
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			resetEventID = event.GetEventId()
		}
	}
	s.NotZero(resetEventID, "expected a completed workflow task to reset to")

	// Reset succeeds on a paused workflow and returns a fresh run.
	resetResp, err := env.FrontendClient().ResetWorkflowExecution(s.Context(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		Reason:                    "reset while paused",
		WorkflowTaskFinishEventId: resetEventID,
		RequestId:                 uuid.NewString(),
	})
	s.NoError(err)
	newRunID := resetResp.GetRunId()
	s.NotEmpty(newRunID)
	s.NotEqual(runID, newRunID)

	// The reset run is RUNNING and not paused: pause is per-run and is not
	// carried onto the new run.
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, newRunID)
		s.NoError(err)
		info := desc.GetWorkflowExecutionInfo()
		s.NotNil(info)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.GetStatus(),
			"reset run should be RUNNING, not paused")
		s.Nil(desc.GetWorkflowExtendedInfo().GetPauseInfo(), "reset run should not inherit pause info")
	}, 10*time.Second, 200*time.Millisecond)

	// The original run is terminated by the reset (no longer paused).
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		s.NoError(err)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, desc.GetWorkflowExecutionInfo().GetStatus(),
			"original run should be terminated by the reset")
	}, 10*time.Second, 200*time.Millisecond)

	// Cleanup: terminate the reset run so it doesn't linger.
	_, _ = env.FrontendClient().TerminateWorkflowExecution(s.Context(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      newRunID,
		},
		Reason: "cleanup after reset-while-paused test",
	})
}

// TestActivityRetryDeferredWhilePaused validates that pausing a workflow while
// one of its activities is retrying defers the activity's retries: the pending
// activity's attempt count stops increasing for the duration of the pause and
// resumes once the workflow is unpaused. Pausing the workflow invalidates its
// pending activities by bumping their stamp; it does not mark the activity
// itself as paused.
func (s *PauseWorkflowExecutionSuite) TestActivityRetryDeferredWhilePaused() {
	env := s.newTestEnv()
	env.activityShouldSucceed.Store(false)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("pause-activity-retry-wf-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}
	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, env.workflowWithFailingActivity)
	s.NoError(err)
	workflowID := workflowRun.GetID()
	runID := workflowRun.GetRunID()

	// Wait until the activity has failed and retried at least twice, confirming
	// retries are actively happening before the pause.
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		s.NoError(err)
		s.Len(desc.PendingActivities, 1)
		s.NotNil(desc.PendingActivities[0].LastFailure)
		s.GreaterOrEqual(desc.PendingActivities[0].Attempt, int32(2))
	}, 15*time.Second, 200*time.Millisecond)

	// Pause the workflow.
	_, err = env.FrontendClient().PauseWorkflowExecution(s.Context(), &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		s.assertWorkflowIsPaused(env, workflowID, runID)
	}, 5*time.Second, 200*time.Millisecond)

	// Let any in-flight retry settle after the pause, then sample the attempt count.
	s.NoError(util.InterruptibleSleep(s.Context(), 3*time.Second))
	descAfterPause, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
	s.NoError(err)
	s.Len(descAfterPause.PendingActivities, 1)
	attemptWhilePaused := descAfterPause.PendingActivities[0].Attempt
	// Workflow pause invalidates the activity via its stamp but does not mark the
	// activity itself as paused.
	s.False(descAfterPause.PendingActivities[0].Paused, "workflow pause should not set the activity's Paused flag")

	// Wait across multiple retry intervals; the attempt count must not grow while
	// the workflow is paused (retries are deferred).
	s.NoError(util.InterruptibleSleep(s.Context(), 3*time.Second))
	descStill, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
	s.NoError(err)
	s.Len(descStill.PendingActivities, 1, "activity should still be pending while paused")
	s.Equal(attemptWhilePaused, descStill.PendingActivities[0].Attempt, "activity must not retry while the workflow is paused")
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED, descStill.GetWorkflowExecutionInfo().GetStatus())

	// Unpause; retries resume and the attempt count climbs again.
	_, err = env.FrontendClient().UnpauseWorkflowExecution(s.Context(), &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)

	// Let the activity succeed so the workflow can complete.
	env.activityShouldSucceed.Store(true)
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		s.NoError(err)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, desc.GetWorkflowExecutionInfo().GetStatus())
	}, 15*time.Second, 200*time.Millisecond)
}

// assertPausedWorkflowTimesOut starts env.workflowFn (which blocks on its
// activity), pauses it, and asserts that the configured wall-clock timeout still
// fires and transitions the paused workflow to TIMED_OUT.
func (s *PauseWorkflowExecutionSuite) assertPausedWorkflowTimesOut(env *pauseWorkflowExecutionEnv, workflowOptions sdkclient.StartWorkflowOptions) {
	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, env.workflowFn)
	s.NoError(err)
	workflowID := workflowRun.GetID()
	runID := workflowRun.GetRunID()

	s.waitUntilRunningWithPendingActivity(env, workflowID, runID)

	// Pause the workflow well before its timeout fires.
	_, err = env.FrontendClient().PauseWorkflowExecution(s.Context(), &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		s.assertWorkflowIsPaused(env, workflowID, runID)
	}, 5*time.Second, 200*time.Millisecond)

	// Without unpausing, the wall-clock timeout still fires and times out the
	// workflow.
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		s.NoError(err)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, desc.GetWorkflowExecutionInfo().GetStatus(),
			"paused workflow should still time out on wall-clock")
	}, 25*time.Second, 500*time.Millisecond)
}

// waitUntilRunningWithPendingActivity waits until the workflow is RUNNING and has
// scheduled its activity, so a subsequent pause is applied to a fully
// initialized workflow.
func (s *PauseWorkflowExecutionSuite) waitUntilRunningWithPendingActivity(env *pauseWorkflowExecutionEnv, workflowID, runID string) {
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		s.NoError(err)
		info := desc.GetWorkflowExecutionInfo()
		s.NotNil(info)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.GetStatus())
		s.NotEmpty(desc.PendingActivities)
	}, 5*time.Second, 100*time.Millisecond)
}

// waitUntilFirstWorkflowTaskCompleted waits until the workflow is RUNNING and has
// completed its first workflow task (so a workflow without an activity is fully
// initialized before pausing).
func (s *PauseWorkflowExecutionSuite) waitUntilFirstWorkflowTaskCompleted(env *pauseWorkflowExecutionEnv, workflowID, runID string) {
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		s.NoError(err)
		info := desc.GetWorkflowExecutionInfo()
		s.NotNil(info)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.GetStatus())
		s.GreaterOrEqual(info.GetHistoryLength(), int64(4), "first workflow task has not completed yet")
	}, 10*time.Second, 100*time.Millisecond)
}

// historyHasEventType reports whether the workflow's history contains at least
// one event of the given type.
func (s *PauseWorkflowExecutionSuite) historyHasEventType(env *pauseWorkflowExecutionEnv, workflowID, runID string, eventType enumspb.EventType) bool {
	hist := env.SdkClient().GetWorkflowHistory(s.Context(), workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for hist.HasNext() {
		event, err := hist.Next()
		s.NoError(err)
		if event.EventType == eventType {
			return true
		}
	}
	return false
}

// assertWorkflowIsPaused is a helper method which asserts that,
// - the workflow status is paused.
// - the workflow has the correct pause info.
// - the TemporalPauseInfo search attribute contains workflow pause entries.
// - there is no workflow task scheduled event inbetween pause and unpause events.
func (s *PauseWorkflowExecutionSuite) assertWorkflowIsPaused(env *pauseWorkflowExecutionEnv, workflowID string, runID string) {
	desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
	s.NoError(err)
	info := desc.GetWorkflowExecutionInfo()
	s.NotNil(info)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED, info.GetStatus(), "workflow is not paused. Status: %s", info.GetStatus())
	if pauseInfo := desc.GetWorkflowExtendedInfo().GetPauseInfo(); pauseInfo != nil {
		s.Equal(env.pauseIdentity, pauseInfo.GetIdentity(), "pause identity is not correct")
		s.Equal(env.pauseReason, pauseInfo.GetReason(), "pause reason is not correct")
	}

	// Verify TemporalPauseInfo search attribute is set with workflow pause entries
	searchAttrs := info.GetSearchAttributes()
	s.NotNil(searchAttrs, "Search attributes should not be nil")
	pauseInfoPayload, hasPauseInfo := searchAttrs.GetIndexedFields()["TemporalPauseInfo"]
	s.True(hasPauseInfo, "TemporalPauseInfo search attribute should exist")
	s.NotNil(pauseInfoPayload)
	var pauseInfoEntries []string
	err = payload.Decode(pauseInfoPayload, &pauseInfoEntries)
	s.NoError(err)
	s.Contains(pauseInfoEntries, fmt.Sprintf("Workflow:%s", workflowID), "Should contain workflow ID")
	s.Contains(pauseInfoEntries, "Reason:"+env.pauseReason, "Should contain pause reason")

	// Also assert that there is no workflow task scheduled event after the pause event.
	hist := env.SdkClient().GetWorkflowHistory(s.Context(), workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	isPaused := false
	for hist.HasNext() {
		event, err := hist.Next()
		s.NoError(err)
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_PAUSED {
			isPaused = true
			continue
		}
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UNPAUSED {
			isPaused = false
			continue
		}
		if isPaused {
			// These are all events after the pause event. None of these should be workflow task scheduled events.
			s.NotEqual(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, event.EventType)
		}
	}
}

// hasActivityPauseEntries checks if the TemporalPauseInfo search attribute contains any activity pause entries.
func (env *pauseWorkflowExecutionEnv) hasActivityPauseEntries(desc *workflowservice.DescribeWorkflowExecutionResponse) bool {
	searchAttrs := desc.GetWorkflowExecutionInfo().GetSearchAttributes()
	if searchAttrs == nil {
		return false
	}

	pauseInfoPayload, hasPauseInfo := searchAttrs.GetIndexedFields()["TemporalPauseInfo"]
	if !hasPauseInfo || pauseInfoPayload == nil {
		return false
	}

	var pauseInfoEntries []string
	if err := payload.Decode(pauseInfoPayload, &pauseInfoEntries); err != nil {
		return false
	}

	for _, entry := range pauseInfoEntries {
		if strings.HasPrefix(entry, "property:activityType=") {
			return true
		}
	}
	return false
}
