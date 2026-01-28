package tests

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

var (
	unreachableErr = errors.New("unreachable code")
)

func TestUpdateWorkflowSdk(t *testing.T) {
	t.Run("TerminateWorkflowAfterUpdateAdmitted", func(t *testing.T) {
		s := testcore.NewEnv(t)

		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		sdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
			Logger:    log.NewSdkLogger(s.Logger),
		})
		require.NoError(t, err)
		defer sdkClient.Close()

		taskQueue := s.Tv().TaskQueue().Name
		worker := sdkworker.New(sdkClient, taskQueue, sdkworker.Options{})

		tv := testvars.New(t).
			WithTaskQueue(taskQueue).
			WithNamespaceName(s.Namespace())

		workflowFn := func(ctx workflow.Context) error {
			if err := workflow.SetUpdateHandler(ctx, tv.HandlerName(), func(ctx workflow.Context, arg string) error {
				if err := workflow.Await(ctx, func() bool { return false }); err != nil {
					panic(err)
				}
				return unreachableErr
			}); err != nil {
				panic(err)
			}
			if err := workflow.Await(ctx, func() bool { return false }); err != nil {
				panic(err)
			}
			return unreachableErr
		}

		// Start workflow and wait until update is admitted, without starting the worker
		run := startWorkflowForUpdate(t, ctx, sdkClient, tv, workflowFn)
		updateWorkflowWaitAdmittedHelper(t, ctx, sdkClient, tv, "update-arg")

		worker.RegisterWorkflow(workflowFn)
		require.NoError(t, worker.Start())
		defer worker.Stop()

		require.NoError(t, sdkClient.TerminateWorkflow(ctx, tv.WorkflowID(), run.GetRunID(), "reason"))

		_, err = pollUpdateHelper(ctx, sdkClient, tv, &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED})
		var notFound *serviceerror.NotFound
		require.ErrorAs(t, err, &notFound)

		hist := s.GetHistory(s.Namespace().String(), tv.WorkflowExecution())
		s.EqualHistoryEventsPrefix(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled`, hist)

		s.EqualHistoryEventsSuffix(`
WorkflowExecutionTerminated // This can be EventID=3 if WF is terminated before 1st WFT is started or 5 if after.`, hist)
	})

	// TestUpdateWorkflow_TimeoutWorkflowAfterUpdateAccepted executes an update, and while WF awaits
	// server times out the WF after the update has been accepted but before it has been completed. It checks
	// that the client gets a NotFound error when attempting to fetch the update result (rather than a timeout).
	t.Run("TimeoutWorkflowAfterUpdateAccepted", func(t *testing.T) {
		s := testcore.NewEnv(t)

		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		sdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
			Logger:    log.NewSdkLogger(s.Logger),
		})
		require.NoError(t, err)
		defer sdkClient.Close()

		taskQueue := s.Tv().TaskQueue().Name
		worker := sdkworker.New(sdkClient, taskQueue, sdkworker.Options{})

		tv := testvars.New(t).
			WithTaskQueue(taskQueue).
			WithNamespaceName(s.Namespace())

		workflowFn := func(ctx workflow.Context) error {
			if err := workflow.SetUpdateHandler(ctx, tv.HandlerName(), func(ctx workflow.Context, arg string) error {
				if err := workflow.Await(ctx, func() bool { return false }); err != nil {
					panic(err)
				}
				return unreachableErr
			}); err != nil {
				panic(err)
			}
			if err := workflow.Await(ctx, func() bool { return false }); err != nil {
				panic(err)
			}
			return unreachableErr
		}

		worker.RegisterWorkflow(workflowFn)
		require.NoError(t, worker.Start())
		defer worker.Stop()

		wfRun, err := sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			ID:                       tv.WorkflowID(),
			TaskQueue:                tv.TaskQueue().Name,
			WorkflowExecutionTimeout: time.Second,
		}, workflowFn)
		require.NoError(t, err)

		// Wait for the first WFT to complete.
		s.WaitForHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted`,
			s.GetHistoryFunc(tv.NamespaceName().String(), tv.WorkflowExecution()),
			1*time.Second, 200*time.Millisecond)

		updateHandle, err := updateWorkflowWaitAcceptedHelper(ctx, sdkClient, tv, "my-update-arg")
		require.NoError(t, err)

		err = updateHandle.Get(ctx, nil)
		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.Contains(t, appErr.Message(), "Workflow Update failed because the Workflow completed before the Update completed.")

		pollFailure, pollErr := pollUpdateHelper(ctx, sdkClient, tv, &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
		require.NoError(t, pollErr)
		require.Equal(t, "Workflow Update failed because the Workflow completed before the Update completed.", pollFailure.GetOutcome().GetFailure().GetMessage())

		var wee *temporal.WorkflowExecutionError
		require.ErrorAs(t, wfRun.Get(ctx, nil), &wee)

		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted
  8 WorkflowExecutionUpdateAccepted
  9 WorkflowExecutionTimedOut`, s.GetHistory(s.Namespace().String(), tv.WorkflowExecution()))
	})

	// TestUpdateWorkflow_TerminateWorkflowAfterUpdateAccepted executes an update, and while WF awaits
	// server terminates the WF after the update has been accepted but before it has been completed. It checks
	// that the client gets a NotFound error when attempting to fetch the update result (rather than a timeout).
	t.Run("TerminateWorkflowAfterUpdateAccepted", func(t *testing.T) {
		s := testcore.NewEnv(t)

		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		sdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
			Logger:    log.NewSdkLogger(s.Logger),
		})
		require.NoError(t, err)
		defer sdkClient.Close()

		taskQueue := s.Tv().TaskQueue().Name
		worker := sdkworker.New(sdkClient, taskQueue, sdkworker.Options{})

		tv := testvars.New(t).
			WithTaskQueue(taskQueue).
			WithNamespaceName(s.Namespace())

		workflowFn := func(ctx workflow.Context) error {
			if err := workflow.SetUpdateHandler(ctx, tv.HandlerName(), func(ctx workflow.Context, arg string) error {
				if err := workflow.Await(ctx, func() bool { return false }); err != nil {
					panic(err)
				}
				return unreachableErr
			}); err != nil {
				panic(err)
			}
			if err := workflow.Await(ctx, func() bool { return false }); err != nil {
				panic(err)
			}
			return unreachableErr
		}

		worker.RegisterWorkflow(workflowFn)
		require.NoError(t, worker.Start())
		defer worker.Stop()

		wfRun := startWorkflowForUpdate(t, ctx, sdkClient, tv, workflowFn)

		// Wait for the first WFT to complete.
		s.WaitForHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted`,
			s.GetHistoryFunc(tv.NamespaceName().String(), tv.WorkflowExecution()),
			1*time.Second, 200*time.Millisecond)

		updateHandle, err := updateWorkflowWaitAcceptedHelper(ctx, sdkClient, tv, "my-update-arg")
		require.NoError(t, err)

		require.NoError(t, sdkClient.TerminateWorkflow(ctx, tv.WorkflowID(), wfRun.GetRunID(), "reason"))

		err = updateHandle.Get(ctx, nil)
		var appErr *temporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		require.Contains(t, appErr.Message(), "Workflow Update failed because the Workflow completed before the Update completed.")

		pollFailure, pollErr := pollUpdateHelper(ctx, sdkClient, tv, &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
		require.NoError(t, pollErr)
		require.Equal(t, "Workflow Update failed because the Workflow completed before the Update completed.", pollFailure.GetOutcome().GetFailure().GetMessage())

		var wee *temporal.WorkflowExecutionError
		require.ErrorAs(t, wfRun.Get(ctx, nil), &wee)

		s.EqualHistoryEvents(`
	1 WorkflowExecutionStarted
	2 WorkflowTaskScheduled
	3 WorkflowTaskStarted
	4 WorkflowTaskCompleted
	5 WorkflowTaskScheduled
	6 WorkflowTaskStarted
	7 WorkflowTaskCompleted
	8 WorkflowExecutionUpdateAccepted
	9 WorkflowExecutionTerminated`, s.GetHistory(s.Namespace().String(), tv.WorkflowExecution()))
	})

	t.Run("ContinueAsNewAfterUpdateAdmitted", func(t *testing.T) {
		/*
			Start Workflow and send Update to itself from LA to make sure it is admitted
			by server while WFT is running. This WFT does CAN. For test simplicity,
			it used another WF function for 2nd run. This 2nd function has Update handler
			registered. When server receives CAN it abort all Updates with retryable
			"workflow is closing" error and server internally retries. In the meantime, server process CAN,
			starts 2nd run, Update is delivered to it, and processed by registered handler.
		*/
		s := testcore.NewEnv(t)

		rootCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		sdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
			Logger:    log.NewSdkLogger(s.Logger),
		})
		require.NoError(t, err)
		defer sdkClient.Close()

		taskQueue := s.Tv().TaskQueue().Name
		worker := sdkworker.New(sdkClient, taskQueue, sdkworker.Options{})

		tv := testvars.New(t).
			WithTaskQueue(taskQueue).
			WithNamespaceName(s.Namespace())

		sendUpdateActivityFn := func(ctx context.Context) error {
			updateWorkflowWaitAdmittedHelper(t, rootCtx, sdkClient, tv, "update-arg")
			return nil
		}

		workflowFn2 := func(ctx workflow.Context) error {
			if err := workflow.SetUpdateHandler(ctx, tv.HandlerName(), func(ctx workflow.Context, arg string) (string, error) {
				return workflow.GetInfo(ctx).WorkflowExecution.RunID, nil
			}); err != nil {
				panic(err)
			}

			if err := workflow.Await(ctx, func() bool { return false }); err != nil {
				panic(err)
			}
			return unreachableErr
		}

		workflowFn1 := func(ctx workflow.Context) error {
			ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
				StartToCloseTimeout: 5 * time.Second,
			})
			if err := workflow.ExecuteLocalActivity(ctx, sendUpdateActivityFn).Get(ctx, nil); err != nil {
				panic(err)
			}

			return workflow.NewContinueAsNewError(ctx, workflowFn2)
		}

		worker.RegisterWorkflow(workflowFn1)
		worker.RegisterWorkflow(workflowFn2)
		worker.RegisterActivity(sendUpdateActivityFn)
		require.NoError(t, worker.Start())
		defer worker.Stop()

		var firstRun sdkclient.WorkflowRun
		firstRun = startWorkflowForUpdate(t, rootCtx, sdkClient, tv, workflowFn1)
		var secondRunID string
		s.Eventually(func() bool {
			resp, err := pollUpdateHelper(rootCtx, sdkClient, tv, &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
			if err != nil {
				var notFoundErr *serviceerror.NotFound
				var resourceExhaustedErr *serviceerror.ResourceExhausted
				// If poll lands on 1st run, it will get ResourceExhausted.
				// If poll lands on 2nd run, it will get NotFound error for few attempts.
				// All other errors are unexpected.
				require.True(t, errors.As(err, &notFoundErr) || errors.As(err, &resourceExhaustedErr), "error must be NotFound or ResourceExhausted")
				return false
			}
			secondRunID = testcore.DecodeString(t, resp.GetOutcome().GetSuccess())
			return true
		}, 5*time.Second, 100*time.Millisecond, "update did not reach Completed stage")

		require.NotEqual(t, firstRun.GetRunID(), secondRunID, "RunId of started WF and WF that received Update should be different")

		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 MarkerRecorded
  6 WorkflowExecutionContinuedAsNew`, s.GetHistory(s.Namespace().String(), tv.WithRunID(firstRun.GetRunID()).WorkflowExecution()))

		hist2 := s.GetHistory(s.Namespace().String(), tv.WithRunID(secondRunID).WorkflowExecution())
		s.EqualHistoryEventsPrefix(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted`, hist2)
		s.EqualHistoryEventsSuffix(`
WorkflowTaskScheduled // This can be EventID=2 if Update is retried before 1st WFT is completed or 5 if 1st WFT completes first.
WorkflowTaskStarted
WorkflowTaskCompleted
WorkflowExecutionUpdateAccepted
WorkflowExecutionUpdateCompleted`, hist2)
	})

	t.Run("TimeoutWithRetryAfterUpdateAdmitted", func(t *testing.T) {
		/*
			Test ensures that admitted Updates are aborted with retriable error
			when WF times out with retries and carried over to the new run.

			Send update to WF with short timeout (1s) w/o running worker for this WF. Update gets admitted
			by server but not processed by WF. WF times out, Update is aborted with retriable error,
			server starts new run, and Update is retried on that new run. In the meantime, worker is started
			and catch up the second run.
		*/
		s := testcore.NewEnv(t)

		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		sdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
			Logger:    log.NewSdkLogger(s.Logger),
		})
		require.NoError(t, err)
		defer sdkClient.Close()

		taskQueue := s.Tv().TaskQueue().Name
		worker := sdkworker.New(sdkClient, taskQueue, sdkworker.Options{})

		tv := testvars.New(t).
			WithTaskQueue(taskQueue).
			WithNamespaceName(s.Namespace())

		workflowFn := func(ctx workflow.Context) error {
			if err := workflow.SetUpdateHandler(ctx, tv.HandlerName(), func(ctx workflow.Context, arg string) (string, error) {
				return workflow.GetInfo(ctx).WorkflowExecution.RunID, nil
			}); err != nil {
				panic(err)
			}
			if err := workflow.Await(ctx, func() bool { return false }); err != nil {
				panic(err)
			}
			return unreachableErr
		}

		// Start the worker first (without registering the workflow) so it can pick up
		// the WFT and fail it (because workflow is not registered yet).
		require.NoError(t, worker.Start())
		defer worker.Stop()

		firstRun, err := sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			ID:                 tv.WorkflowID(),
			TaskQueue:          tv.TaskQueue().Name,
			WorkflowRunTimeout: 1 * time.Second,
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts: 2,
			},
		}, workflowFn)
		require.NoError(t, err)
		updateWorkflowWaitAdmittedHelper(t, ctx, sdkClient, tv, tv.Any().String())

		err = firstRun.GetWithOptions(ctx, nil, sdkclient.WorkflowRunGetOptions{DisableFollowingRuns: true})
		var canErr *workflow.ContinueAsNewError
		require.ErrorAs(t, err, &canErr)

		// "start" worker for workflowFn - register the workflow now so the second run can succeed.
		worker.RegisterWorkflow(workflowFn)

		var secondRunID string
		s.Eventually(func() bool {
			resp, err := pollUpdateHelper(ctx, sdkClient, tv, &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
			if err != nil {
				var notFoundErr *serviceerror.NotFound
				// If a poll beats internal update retries, it will get NotFound error for a few attempts.
				// All other errors are unexpected.
				require.ErrorAs(t, err, &notFoundErr, "error must be NotFound")
				return false
			}
			secondRunID = testcore.DecodeString(t, resp.GetOutcome().GetSuccess())
			require.NotEmpty(t, secondRunID)
			return true
		}, 5*time.Second, 100*time.Millisecond, "update did not reach Completed stage")

		require.NotEqual(t, firstRun.GetRunID(), secondRunID, "RunId of started WF and WF that received Update should be different")

		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionTimedOut`, s.GetHistory(s.Namespace().String(), tv.WithRunID(firstRun.GetRunID()).WorkflowExecution()))
		s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 WorkflowExecutionUpdateCompleted`, s.GetHistory(s.Namespace().String(), tv.WithRunID(secondRunID).WorkflowExecution()))
	})
}

func startWorkflowForUpdate(t *testing.T, ctx context.Context, sdkClient sdkclient.Client, tv *testvars.TestVars, workflowFn any) sdkclient.WorkflowRun {
	run, err := sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        tv.WorkflowID(),
		TaskQueue: tv.TaskQueue().Name,
	}, workflowFn)
	require.NoError(t, err)
	return run
}

func updateWorkflowWaitAdmittedHelper(t *testing.T, ctx context.Context, sdkClient sdkclient.Client, tv *testvars.TestVars, arg string) {
	go func() { _, _ = updateWorkflowWaitAcceptedHelper(ctx, sdkClient, tv, arg) }()
	require.Eventually(t, func() bool {
		resp, err := pollUpdateHelper(ctx, sdkClient, tv, &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED})
		if err == nil {
			require.Equal(t, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED, resp.Stage)
			return true
		}
		var notFoundErr *serviceerror.NotFound
		require.ErrorAs(t, err, &notFoundErr) // poll beat send in race
		return false
	}, 5*time.Second, 100*time.Millisecond, fmt.Sprintf("update %s did not reach Admitted stage", tv.UpdateID()))
}

func updateWorkflowWaitAcceptedHelper(ctx context.Context, sdkClient sdkclient.Client, tv *testvars.TestVars, arg string) (sdkclient.WorkflowUpdateHandle, error) {
	return sdkClient.UpdateWorkflow(ctx, sdkclient.UpdateWorkflowOptions{
		UpdateID:     tv.UpdateID(),
		WorkflowID:   tv.WorkflowID(),
		RunID:        tv.RunID(),
		UpdateName:   tv.HandlerName(),
		Args:         []interface{}{arg},
		WaitForStage: sdkclient.WorkflowUpdateStageAccepted,
	})
}

func pollUpdateHelper(ctx context.Context, sdkClient sdkclient.Client, tv *testvars.TestVars, waitPolicy *updatepb.WaitPolicy) (*workflowservice.PollWorkflowExecutionUpdateResponse, error) {
	return sdkClient.WorkflowService().PollWorkflowExecutionUpdate(ctx, &workflowservice.PollWorkflowExecutionUpdateRequest{
		Namespace:  tv.NamespaceName().String(),
		UpdateRef:  tv.UpdateRef(),
		Identity:   tv.ClientIdentity(),
		WaitPolicy: waitPolicy,
	})
}
