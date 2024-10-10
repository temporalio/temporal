// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

var (
	unreachableErr = errors.New("unreachable code")
)

type UpdateWorkflowSdkSuite struct {
	testcore.ClientFunctionalSuite
}

func TestUpdateWorkflowSdkSuite(t *testing.T) {
	t.Parallel()
	s := new(UpdateWorkflowSdkSuite)
	suite.Run(t, s)
}

func (s *UpdateWorkflowSdkSuite) TestUpdateWorkflow_TerminateWorkflowAfterUpdateAdmitted() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tv := testvars.New(s.T()).WithTaskQueue(s.TaskQueue()).WithNamespaceName(namespace.Name(s.Namespace()))

	workflowFn := func(ctx workflow.Context) error {
		s.NoError(workflow.SetUpdateHandler(ctx, tv.HandlerName(), func(ctx workflow.Context, arg string) error {
			s.NoError(workflow.Await(ctx, func() bool { return false }))
			return unreachableErr
		}))
		s.NoError(workflow.Await(ctx, func() bool { return false }))
		return unreachableErr
	}

	// Start workflow and wait until update is admitted, without starting the worker
	run := s.startWorkflow(ctx, tv, workflowFn)
	s.updateWorkflowWaitAdmitted(ctx, tv, "update-arg")

	s.Worker().RegisterWorkflow(workflowFn)

	s.NoError(s.SdkClient().TerminateWorkflow(ctx, tv.WorkflowID(), run.GetRunID(), "reason"))

	_, err := s.pollUpdate(ctx, tv, &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED})
	var notFound *serviceerror.NotFound
	s.ErrorAs(err, &notFound)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskFailed
  5 WorkflowExecutionTerminated`, s.GetHistory(s.Namespace(), tv.WorkflowExecution()))
}

// TestUpdateWorkflow_TimeoutWorkflowAfterUpdateAccepted executes an update, and while WF awaits
// server times out the WF after the update has been accepted but before it has been completed. It checks
// that the client gets a NotFound error when attempting to fetch the update result (rather than a timeout).
func (s *UpdateWorkflowSdkSuite) TestUpdateWorkflow_TimeoutWorkflowAfterUpdateAccepted() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tv := testvars.New(s.T()).WithTaskQueue(s.TaskQueue()).WithNamespaceName(namespace.Name(s.Namespace()))

	workflowFn := func(ctx workflow.Context) error {
		s.NoError(workflow.SetUpdateHandler(ctx, tv.HandlerName(), func(ctx workflow.Context, arg string) error {
			s.NoError(workflow.Await(ctx, func() bool { return false }))
			return unreachableErr
		}))
		s.NoError(workflow.Await(ctx, func() bool { return false }))
		return unreachableErr
	}

	s.Worker().RegisterWorkflow(workflowFn)

	wfRun, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                       tv.WorkflowID(),
		TaskQueue:                tv.TaskQueue().Name,
		WorkflowExecutionTimeout: time.Second,
	}, workflowFn)
	s.NoError(err)

	updateHandle, err := s.updateWorkflowWaitAccepted(ctx, tv, "my-update-arg")
	s.NoError(err)

	var notFound *serviceerror.NotFound
	s.ErrorAs(updateHandle.Get(ctx, nil), &notFound)

	_, pollErr := s.pollUpdate(ctx, tv, &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
	s.ErrorAs(pollErr, &notFound)

	var wee *temporal.WorkflowExecutionError
	s.ErrorAs(wfRun.Get(ctx, nil), &wee)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 WorkflowExecutionTimedOut`, s.GetHistory(s.Namespace(), tv.WorkflowExecution()))
}

// TestUpdateWorkflow_TerminateWorkflowAfterUpdateAccepted executes an update, and while WF awaits
// server terminates the WF after the update has been accepted but before it has been completed. It checks
// that the client gets a NotFound error when attempting to fetch the update result (rather than a timeout).
func (s *UpdateWorkflowSdkSuite) TestUpdateWorkflow_TerminateWorkflowAfterUpdateAccepted() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tv := testvars.New(s.T()).WithTaskQueue(s.TaskQueue()).WithNamespaceName(namespace.Name(s.Namespace()))

	workflowFn := func(ctx workflow.Context) error {
		s.NoError(workflow.SetUpdateHandler(ctx, tv.HandlerName(), func(ctx workflow.Context, arg string) error {
			s.NoError(workflow.Await(ctx, func() bool { return false }))
			return unreachableErr
		}))
		s.NoError(workflow.Await(ctx, func() bool { return false }))
		return unreachableErr
	}

	s.Worker().RegisterWorkflow(workflowFn)
	wfRun := s.startWorkflow(ctx, tv, workflowFn)

	updateHandle, err := s.updateWorkflowWaitAccepted(ctx, tv, "my-update-arg")
	s.NoError(err)

	s.NoError(s.SdkClient().TerminateWorkflow(ctx, tv.WorkflowID(), wfRun.GetRunID(), "reason"))

	var notFound *serviceerror.NotFound
	s.ErrorAs(updateHandle.Get(ctx, nil), &notFound)

	_, pollErr := s.pollUpdate(ctx, tv, &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
	s.ErrorAs(pollErr, &notFound)

	var wee *temporal.WorkflowExecutionError
	s.ErrorAs(wfRun.Get(ctx, nil), &wee)

	hist := s.GetHistory(s.Namespace(), tv.WorkflowExecution())
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, hist[len(hist)-1].GetEventType())
	// Not EqualHistoryEvents because there is a race and Update might be on first WFT and might be on second.
	// TODO: Use s.EqualHistorySuffix when it is implemented.
	// s.EqualHistoryEvents(`
	// 1 WorkflowExecutionStarted
	// 2 WorkflowTaskScheduled
	// 3 WorkflowTaskStarted
	// 4 WorkflowTaskCompleted
	// 5 WorkflowExecutionUpdateAccepted
	// 6 WorkflowExecutionTerminated`, s.GetHistory(s.Namespace(), tv.WorkflowExecution()))
}

func (s *UpdateWorkflowSdkSuite) TestUpdateWorkflow_ContinueAsNewAfterUpdateAdmitted() {
	s.T().Skip("flaky test")
	/*
		Start Workflow and send Update to itself from LA to make sure it is admitted
		by server while WFT is running. This WFT does CAN. For test simplicity,
		it used another WF function for 2nd run. This 2nd function has Update handler
		registered. When server receives CAN it abort all Updates with retryable
		"workflow is closing" error and SDK retries. In mean time, server process CAN,
		starts 2nd run, Update is delivered to it, and processed by registered handler.
	*/

	tv := testvars.New(s.T()).WithTaskQueue(s.TaskQueue()).WithNamespaceName(namespace.Name(s.Namespace()))

	rootCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	sendUpdateActivityFn := func(ctx context.Context) error {
		s.updateWorkflowWaitAdmitted(rootCtx, tv, "update-arg")
		return nil
	}

	workflowFn2 := func(ctx workflow.Context) error {
		s.NoError(workflow.SetUpdateHandler(ctx, tv.HandlerName(), func(ctx workflow.Context, arg string) (string, error) {
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

	s.Worker().RegisterWorkflow(workflowFn1)
	s.Worker().RegisterWorkflow(workflowFn2)
	s.Worker().RegisterActivity(sendUpdateActivityFn)

	var firstRun sdkclient.WorkflowRun
	firstRun = s.startWorkflow(rootCtx, tv, workflowFn1)
	var secondRunID string
	s.Eventually(func() bool {
		resp, err := s.pollUpdate(rootCtx, tv, &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
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
  6 WorkflowExecutionContinuedAsNew`, s.GetHistory(s.Namespace(), tv.WithRunID(firstRun.GetRunID()).WorkflowExecution()))
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 WorkflowExecutionUpdateCompleted`, s.GetHistory(s.Namespace(), tv.WithRunID(secondRunID).WorkflowExecution()))
}

func (s *UpdateWorkflowSdkSuite) TestUpdateWorkflow_TimeoutWithRetryAfterUpdateAdmitted() {
	/*
		Test ensures that admitted Updates are aborted with retriable error
		when WF times out with retries and carried over to the new run.

		Send update to WF with short timeout (1s) w/o running worker for this WF. Update gets admitted
		by server but not processed by WF. WF times out, Update is aborted with retriable error,
		server starts new run, and Update is retried on that new run. In the meantime, worker is started
		and catch up the second run.
	*/

	tv := testvars.New(s.T()).WithTaskQueue(s.TaskQueue()).WithNamespaceName(namespace.Name(s.Namespace()))

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	workflowFn := func(ctx workflow.Context) error {
		s.NoError(workflow.SetUpdateHandler(ctx, tv.HandlerName(), func(ctx workflow.Context, arg string) (string, error) {
			return workflow.GetInfo(ctx).WorkflowExecution.RunID, nil
		}))
		s.NoError(workflow.Await(ctx, func() bool { return false }))
		return unreachableErr
	}

	firstRun, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                 tv.WorkflowID(),
		TaskQueue:          tv.TaskQueue().Name,
		WorkflowRunTimeout: 1 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Nanosecond,
			MaximumAttempts: 2,
		},
	}, workflowFn)
	s.NoError(err)
	s.updateWorkflowWaitAdmitted(ctx, tv, tv.Any().String())

	err = firstRun.GetWithOptions(ctx, nil, sdkclient.WorkflowRunGetOptions{DisableFollowingRuns: true})
	var canErr *workflow.ContinueAsNewError
	s.ErrorAs(err, &canErr)

	// "start" worker for workflowFn.
	s.Worker().RegisterWorkflow(workflowFn)

	var secondRunID string
	s.Eventually(func() bool {
		resp, err := s.pollUpdate(ctx, tv, &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED})
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
  5 WorkflowExecutionTimedOut`, s.GetHistory(s.Namespace(), tv.WithRunID(firstRun.GetRunID()).WorkflowExecution()))
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionUpdateAccepted
  6 WorkflowExecutionUpdateCompleted`, s.GetHistory(s.Namespace(), tv.WithRunID(secondRunID).WorkflowExecution()))
}

func (s *UpdateWorkflowSdkSuite) startWorkflow(ctx context.Context, tv *testvars.TestVars, workflowFn any) sdkclient.WorkflowRun {
	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        tv.WorkflowID(),
		TaskQueue: tv.TaskQueue().Name,
	}, workflowFn)
	s.NoError(err)
	return run
}

func (s *UpdateWorkflowSdkSuite) updateWorkflowWaitAdmitted(ctx context.Context, tv *testvars.TestVars, arg string) {
	go func() { _, _ = s.updateWorkflowWaitAccepted(ctx, tv, arg) }()
	s.Eventually(func() bool {
		resp, err := s.pollUpdate(ctx, tv, &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED})
		if err == nil {
			s.Equal(enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED, resp.Stage)
			return true
		}
		var notFoundErr *serviceerror.NotFound
		s.ErrorAs(err, &notFoundErr) // poll beat send in race
		return false
	}, 5*time.Second, 100*time.Millisecond, fmt.Sprintf("update %s did not reach Admitted stage", tv.UpdateID()))
}

func (s *UpdateWorkflowSdkSuite) updateWorkflowWaitAccepted(ctx context.Context, tv *testvars.TestVars, arg string) (sdkclient.WorkflowUpdateHandle, error) {
	return s.SdkClient().UpdateWorkflow(ctx, sdkclient.UpdateWorkflowOptions{
		UpdateID:     tv.UpdateID(),
		WorkflowID:   tv.WorkflowID(),
		RunID:        tv.RunID(),
		UpdateName:   tv.HandlerName(),
		Args:         []interface{}{arg},
		WaitForStage: sdkclient.WorkflowUpdateStageAccepted,
	})
}

func (s *UpdateWorkflowSdkSuite) pollUpdate(ctx context.Context, tv *testvars.TestVars, waitPolicy *updatepb.WaitPolicy) (*workflowservice.PollWorkflowExecutionUpdateResponse, error) {
	return s.SdkClient().WorkflowService().PollWorkflowExecutionUpdate(ctx, &workflowservice.PollWorkflowExecutionUpdateRequest{
		Namespace:  tv.NamespaceName().String(),
		UpdateRef:  tv.UpdateRef(),
		Identity:   tv.ClientIdentity(),
		WaitPolicy: waitPolicy,
	})
}
