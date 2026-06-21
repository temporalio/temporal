package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

var (
	unreachableErr = errors.New("unreachable code")
)

type UpdateWorkflowSdkSuite struct {
	parallelsuite.Suite[*UpdateWorkflowSdkSuite]
}

func TestUpdateWorkflowSdkSuite(t *testing.T) {
	parallelsuite.Run(t, &UpdateWorkflowSdkSuite{})
}

func (s *UpdateWorkflowSdkSuite) TestTerminateWorkflowAfterUpdateAdmitted() {
	env := testcore.NewEnv(s.T())

	workflowFn := func(ctx workflow.Context) error {
		s.NoError(workflow.SetUpdateHandler(ctx, env.Tv().HandlerName(), func(ctx workflow.Context, arg string) error {
			s.NoError(workflow.Await(ctx, func() bool { return false }))
			return unreachableErr
		}))
		s.NoError(workflow.Await(ctx, func() bool { return false }))
		return unreachableErr
	}

	// Start workflow and wait until update is admitted, without starting the worker
	run := s.startWorkflow(env, workflowFn)
	s.updateWorkflowWaitAdmitted(env, "update-arg")

	env.SdkWorker().RegisterWorkflow(workflowFn)

	s.NoError(env.SdkClient().TerminateWorkflow(s.Context(), env.Tv().WorkflowID(), run.GetRunID(), "reason"))

	_, err := s.pollUpdate(env, &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED})
	var notFound *serviceerror.NotFound
	s.ErrorAs(err, &notFound)

	hist := env.GetHistory(env.Namespace().String(), env.Tv().WorkflowExecution())
	s.EqualHistoryEventsPrefix(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled`, hist)

	s.EqualHistoryEventsSuffix(`
WorkflowExecutionTerminated // This can be EventID=3 if WF is terminated before 1st WFT is started or 5 if after.`, hist)
}

// TestUpdateWorkflow_TimeoutWorkflowAfterUpdateAccepted executes an update, and while WF awaits
// server times out the WF after the update has been accepted but before it has been completed. It checks
// that the client gets a NotFound error when attempting to fetch the update result (rather than a timeout).
func (s *UpdateWorkflowSdkSuite) TestTimeoutWorkflowAfterUpdateAccepted() {
	env := testcore.NewEnv(s.T())

	workflowFn := func(ctx workflow.Context) error {
		s.NoError(workflow.SetUpdateHandler(ctx, env.Tv().HandlerName(), func(ctx workflow.Context, arg string) error {
			s.NoError(workflow.Await(ctx, func() bool { return false }))
			return unreachableErr
		}))
		s.NoError(workflow.Await(ctx, func() bool { return false }))
		return unreachableErr
	}

	env.SdkWorker().RegisterWorkflow(workflowFn)

	wfRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
		ID:                       env.Tv().WorkflowID(),
		TaskQueue:                env.WorkerTaskQueue(),
		WorkflowExecutionTimeout: time.Second,
	}, workflowFn)
	s.NoError(err)

	// Wait for the first WFT to complete.
	s.WaitForHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted`,
		env.GetHistoryFunc(env.Namespace().String(), env.Tv().WorkflowExecution()),
		1*time.Second, 200*time.Millisecond)

	updateHandle, err := s.updateWorkflowWaitAccepted(env, "my-update-arg")
	s.NoError(err)

	err = updateHandle.Get(s.Context(), nil)
	var appErr *temporal.ApplicationError
	s.ErrorAs(err, &appErr)
	s.Contains("Workflow Update failed because the Workflow completed before the Update completed.", appErr.Message())

	pollFailure, pollErr := s.pollUpdate(env, &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
	s.NoError(pollErr)
	s.Equal("Workflow Update failed because the Workflow completed before the Update completed.", pollFailure.GetOutcome().GetFailure().GetMessage())

	var wee *temporal.WorkflowExecutionError
	s.ErrorAs(wfRun.Get(s.Context(), nil), &wee)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted
  8 WorkflowExecutionUpdateAccepted
  9 WorkflowExecutionTimedOut`, env.GetHistory(env.Namespace().String(), env.Tv().WorkflowExecution()))
}

// TestUpdateWorkflow_TerminateWorkflowAfterUpdateAccepted executes an update, and while WF awaits
// server terminates the WF after the update has been accepted but before it has been completed. It checks
// that the client gets a NotFound error when attempting to fetch the update result (rather than a timeout).
func (s *UpdateWorkflowSdkSuite) TestTerminateWorkflowAfterUpdateAccepted() {
	env := testcore.NewEnv(s.T())

	workflowFn := func(ctx workflow.Context) error {
		s.NoError(workflow.SetUpdateHandler(ctx, env.Tv().HandlerName(), func(ctx workflow.Context, arg string) error {
			s.NoError(workflow.Await(ctx, func() bool { return false }))
			return unreachableErr
		}))
		s.NoError(workflow.Await(ctx, func() bool { return false }))
		return unreachableErr
	}

	env.SdkWorker().RegisterWorkflow(workflowFn)
	wfRun := s.startWorkflow(env, workflowFn)

	// Wait for the first WFT to complete.
	s.WaitForHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted`,
		env.GetHistoryFunc(env.Namespace().String(), env.Tv().WorkflowExecution()),
		1*time.Second, 200*time.Millisecond)

	updateHandle, err := s.updateWorkflowWaitAccepted(env, "my-update-arg")
	s.NoError(err)

	s.NoError(env.SdkClient().TerminateWorkflow(s.Context(), env.Tv().WorkflowID(), wfRun.GetRunID(), "reason"))

	err = updateHandle.Get(s.Context(), nil)
	var appErr *temporal.ApplicationError
	s.ErrorAs(err, &appErr)
	s.Contains("Workflow Update failed because the Workflow completed before the Update completed.", appErr.Message())

	pollFailure, pollErr := s.pollUpdate(env, &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
	s.NoError(pollErr)
	s.Equal("Workflow Update failed because the Workflow completed before the Update completed.", pollFailure.GetOutcome().GetFailure().GetMessage())

	var wee *temporal.WorkflowExecutionError
	s.ErrorAs(wfRun.Get(s.Context(), nil), &wee)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowTaskScheduled
  6 WorkflowTaskStarted
  7 WorkflowTaskCompleted
  8 WorkflowExecutionUpdateAccepted
  9 WorkflowExecutionTerminated`, env.GetHistory(env.Namespace().String(), env.Tv().WorkflowExecution()))
}

func (s *UpdateWorkflowSdkSuite) TestContinueAsNewAfterUpdateAdmitted() {
	/*
		Start Workflow and send Update to itself from LA to make sure it is admitted
		by server while WFT is running. This WFT does CAN. For test simplicity,
		it used another WF function for 2nd run. This 2nd function has Update handler
		registered. When server receives CAN it abort all Updates with retryable
		"workflow is closing" error and server internally retries. In the meantime, server process CAN,
		starts 2nd run, Update is delivered to it, and processed by registered handler.
	*/

	env := testcore.NewEnv(s.T())

	sendUpdateActivityFn := func(ctx context.Context) error {
		s.updateWorkflowWaitAdmitted(env, "update-arg")
		return nil
	}

	workflowFn2 := func(ctx workflow.Context) error {
		s.NoError(workflow.SetUpdateHandler(ctx, env.Tv().HandlerName(), func(ctx workflow.Context, arg string) (string, error) {
			return workflow.GetInfo(ctx).WorkflowExecution.RunID, nil
		}))

		s.NoError(workflow.Await(ctx, func() bool { return false }))
		return unreachableErr
	}

	workflowFn1 := func(ctx workflow.Context) error {
		ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
			StartToCloseTimeout: 5 * time.Second,
		})
		s.NoError(workflow.ExecuteLocalActivity(ctx, sendUpdateActivityFn).Get(ctx, nil))

		return workflow.NewContinueAsNewError(ctx, workflowFn2)
	}

	env.SdkWorker().RegisterWorkflow(workflowFn1)
	env.SdkWorker().RegisterWorkflow(workflowFn2)
	env.SdkWorker().RegisterActivity(sendUpdateActivityFn)

	var firstRun sdkclient.WorkflowRun = s.startWorkflow(env, workflowFn1)
	var secondRunID string
	s.Eventually(func() bool {
		resp, err := s.pollUpdate(env, &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
		if err != nil {
			var notFoundErr *serviceerror.NotFound
			var resourceExhaustedErr *serviceerror.ResourceExhausted
			// If poll lands on 1st run, it will get ResourceExhausted.
			// If poll lands on 2nd run, it will get NotFound error for few attempts.
			// All other errors are unexpected.
			s.True(errors.As(err, &notFoundErr) || errors.As(err, &resourceExhaustedErr), "error must be NotFound or ResourceExhausted")
			return false
		}
		secondRunID = testcore.DecodeString(s.T(), resp.GetOutcome().GetSuccess())
		return true
	}, 5*time.Second, 100*time.Millisecond, "update did not reach Completed stage")

	s.NotEqual(firstRun.GetRunID(), secondRunID, "RunId of started WF and WF that received Update should be different")

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 MarkerRecorded
  6 WorkflowExecutionContinuedAsNew`, env.GetHistory(env.Namespace().String(), env.Tv().WithRunID(firstRun.GetRunID()).WorkflowExecution()))

	hist2 := env.GetHistory(env.Namespace().String(), env.Tv().WithRunID(secondRunID).WorkflowExecution())
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
}

func (s *UpdateWorkflowSdkSuite) TestTimeoutWithRetryAfterUpdateAdmitted() {
	/*
		Test ensures that admitted Updates are aborted with retriable error
		when WF times out with retries and carried over to the new run.

		Send update to WF with short timeout (1s) w/o running worker for this WF. Update gets admitted
		by server but not processed by WF. WF times out, Update is aborted with retriable error,
		server starts new run, and Update is retried on that new run. In the meantime, worker is started
		and catch up the second run.
	*/

	env := testcore.NewEnv(s.T())

	workflowFn := func(ctx workflow.Context) error {
		s.NoError(workflow.SetUpdateHandler(ctx, env.Tv().HandlerName(), func(ctx workflow.Context, arg string) (string, error) {
			return workflow.GetInfo(ctx).WorkflowExecution.RunID, nil
		}))
		s.NoError(workflow.Await(ctx, func() bool { return false }))
		return unreachableErr
	}

	// Start the worker polling the task queue, but without workflowFn registered yet, so
	// the first run's WFTs fail until the run times out (workflowFn is registered below).
	env.SdkWorker()

	firstRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
		ID:                 env.Tv().WorkflowID(),
		TaskQueue:          env.WorkerTaskQueue(),
		WorkflowRunTimeout: 1 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 2,
		},
	}, workflowFn)
	s.NoError(err)
	s.updateWorkflowWaitAdmitted(env, "update-arg")

	// With DisableFollowingRuns the modern SDK surfaces the run timeout (the run is retried
	// into a new run, which the assertions below verify).
	err = firstRun.GetWithOptions(s.Context(), nil, sdkclient.WorkflowRunGetOptions{DisableFollowingRuns: true})
	var timeoutErr *temporal.TimeoutError
	s.ErrorAs(err, &timeoutErr)

	// "start" worker for workflowFn.
	env.SdkWorker().RegisterWorkflow(workflowFn)

	var secondRunID string
	s.Eventually(func() bool {
		resp, err := s.pollUpdate(env, &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
		if err != nil {
			var notFoundErr *serviceerror.NotFound
			// If a poll beats internal update retries, it will get NotFound error for a few attempts.
			// All other errors are unexpected.
			s.ErrorAs(err, &notFoundErr, "error must be NotFound")
			return false
		}
		secondRunID = testcore.DecodeString(s.T(), resp.GetOutcome().GetSuccess())
		s.NotEmpty(secondRunID)
		return true
	}, 5*time.Second, 100*time.Millisecond, "update did not reach Completed stage")

	s.NotEqual(firstRun.GetRunID(), secondRunID, "RunId of started WF and WF that received Update should be different")

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionTimedOut`, env.GetHistory(env.Namespace().String(), env.Tv().WithRunID(firstRun.GetRunID()).WorkflowExecution()))
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 WorkflowExecutionUpdateCompleted`, env.GetHistory(env.Namespace().String(), env.Tv().WithRunID(secondRunID).WorkflowExecution()))
}

func (s *UpdateWorkflowSdkSuite) startWorkflow(env *testcore.TestEnv, workflowFn any) sdkclient.WorkflowRun {
	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
		ID:        env.Tv().WorkflowID(),
		TaskQueue: env.WorkerTaskQueue(),
	}, workflowFn)
	s.NoError(err)
	return run
}

func (s *UpdateWorkflowSdkSuite) updateWorkflowWaitAdmitted(env *testcore.TestEnv, arg string) {
	go func() { _, _ = s.updateWorkflowWaitAccepted(env, arg) }()
	s.Eventually(func() bool {
		resp, err := s.pollUpdate(env, &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED})
		if err == nil {
			s.Equal(enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED, resp.Stage)
			return true
		}
		var notFoundErr *serviceerror.NotFound
		s.ErrorAs(err, &notFoundErr) // poll beat send in race
		return false
	}, 5*time.Second, 100*time.Millisecond, "update %s did not reach Admitted stage", env.Tv().UpdateID())
}

func (s *UpdateWorkflowSdkSuite) updateWorkflowWaitAccepted(env *testcore.TestEnv, arg string) (sdkclient.WorkflowUpdateHandle, error) {
	return env.SdkClient().UpdateWorkflow(s.Context(), sdkclient.UpdateWorkflowOptions{
		UpdateID:     env.Tv().UpdateID(),
		WorkflowID:   env.Tv().WorkflowID(),
		UpdateName:   env.Tv().HandlerName(),
		Args:         []any{arg},
		WaitForStage: sdkclient.WorkflowUpdateStageAccepted,
	})
}

func (s *UpdateWorkflowSdkSuite) pollUpdate(env *testcore.TestEnv, waitPolicy *updatepb.WaitPolicy) (*workflowservice.PollWorkflowExecutionUpdateResponse, error) {
	return env.SdkClient().WorkflowService().PollWorkflowExecutionUpdate(s.Context(), &workflowservice.PollWorkflowExecutionUpdateRequest{
		Namespace:  env.Namespace().String(),
		UpdateRef:  env.Tv().UpdateRef(),
		Identity:   env.Tv().ClientIdentity(),
		WaitPolicy: waitPolicy,
	})
}

// TestUpdateSameRequestIDDeduplicatesCallbacks verifies requestID-based
// deduplication in AttachCallbacks. The update blocks (stays in stateAccepted), then:
//   - A second request with the same requestID is deduped (no new callback).
//   - A third request with a different requestID creates an additional callback.
//
// The workflow should end up with exactly 2 update callbacks (from requestID1 and requestID2).
func (s *UpdateWorkflowSdkSuite) TestUpdateSameRequestIDDeduplicatesCallbacks() {
	env := testcore.NewEnv(s.T(),
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableWorkflowUpdateCallbacks, true),
	)

	requestID1 := env.Tv().RequestID()
	requestID2 := env.Tv().Sub("request-2").RequestID()

	// Workflow where the update handler blocks until signaled.
	wf := func(ctx workflow.Context, input string) (string, error) {
		if err := workflow.SetUpdateHandler(ctx, env.Tv().HandlerName(), func(ctx workflow.Context, input string) (string, error) {
			signalCh := workflow.GetSignalChannel(ctx, "complete-update")
			signalCh.Receive(ctx, nil)
			return "updated: " + input, nil
		}); err != nil {
			return "", err
		}
		signalCh := workflow.GetSignalChannel(ctx, "stop")
		signalCh.Receive(ctx, nil)
		return "done: " + input, nil
	}

	w := worker.New(env.SdkClient(), env.WorkerTaskQueue(), worker.Options{})
	w.RegisterWorkflow(wf)
	s.NoError(w.Start())
	s.T().Cleanup(w.Stop)

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
		ID:        env.Tv().WorkflowID(),
		TaskQueue: env.WorkerTaskQueue(),
	}, wf, "input")
	s.NoError(err)

	makeRequest := func(reqID string) *workflowservice.UpdateWorkflowExecutionRequest {
		return &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace:         env.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: run.GetID()},
			WaitPolicy:        &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED},
			Request: &updatepb.Request{
				Meta:      &updatepb.Meta{UpdateId: env.Tv().UpdateID()},
				Input:     &updatepb.Input{Name: env.Tv().HandlerName(), Args: &commonpb.Payloads{Payloads: []*commonpb.Payload{testcore.MustToPayload(s.T(), "test")}}},
				RequestId: reqID,
				CompletionCallbacks: []*commonpb.Callback{{
					Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: "http://localhost:9999/callback"}},
				}},
			},
		}
	}

	// First request: triggers the update, waits for acceptance (update blocks in handler).
	_, err = env.FrontendClient().UpdateWorkflowExecution(s.Context(), makeRequest(requestID1))
	s.NoError(err)

	// Second request: same requestID → should be deduped by AttachCallbacks (no new callback).
	_, err = env.FrontendClient().UpdateWorkflowExecution(s.Context(), makeRequest(requestID1))
	s.NoError(err)

	// Third request: different requestID → should create a new callback via AttachCallbacks.
	_, err = env.FrontendClient().UpdateWorkflowExecution(s.Context(), makeRequest(requestID2))
	s.NoError(err)

	// Verify exactly 2 update callbacks: one from requestID1 (first request),
	// one from requestID2 (third request). The second request was deduped.
	descResp, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: run.GetID()},
	})
	s.NoError(err)
	updateCallbackCount := 0
	for _, cb := range descResp.GetCallbacks() {
		if cb.GetTrigger().GetUpdateWorkflowExecutionCompleted() != nil {
			updateCallbackCount++
		}
	}
	s.Equal(2, updateCallbackCount, "expected 2 callbacks: requestID1 (original) + requestID2 (new), with duplicate requestID1 deduped")

	// Clean up.
	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), run.GetRunID(), "complete-update", nil))
	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), run.GetRunID(), "stop", nil))
}
