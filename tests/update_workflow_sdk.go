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
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testvars"
)

func (s *ClientFunctionalSuite) TestUpdateWorkflow_TerminateWorkflowAfterUpdateAdmitted() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tv := testvars.New(s.T()).WithTaskQueue(s.taskQueue).WithNamespaceName(namespace.Name(s.namespace))

	activityDone := make(chan struct{})
	activityFn := func(ctx context.Context) error {
		activityDone <- struct{}{}
		return nil
	}

	workflowFn := func(ctx workflow.Context) error {
		s.NoError(workflow.SetUpdateHandler(ctx, tv.HandlerName(), func(ctx workflow.Context, arg string) error {
			ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 10 * time.Second,
			})
			for {
				s.NoError(workflow.ExecuteActivity(ctx, activityFn).Get(ctx, nil))
				if false {
					// appease compiler
					break
				}
			}
			return nil
		}))
		s.NoError(workflow.Await(ctx, func() bool { return false }))
		return errors.New("unreachable")
	}

	// Start workflow and wait until update is admitted, without starting the worker
	tv, _ = s.startWorkflow(ctx, tv, workflowFn)
	s.updateWorkflowWaitAdmitted(ctx, tv, "update-arg")

	s.worker.RegisterWorkflow(workflowFn)
	s.worker.RegisterActivity(activityFn)

	s.NoError(s.sdkClient.TerminateWorkflow(ctx, tv.WorkflowID(), tv.RunID(), "reason"))

	_, err := s.pollUpdate(ctx, tv, &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED})
	var notFound *serviceerror.NotFound
	s.ErrorAs(err, &notFound)
}

// TestUpdateWorkflow_TerminateWorkflowDuringUpdate executes a long-running update (schedules a sequence of activity
// calls) and terminates the workflow after the update has been accepted but before it has been completed. It checks
// that the client gets a NotFound error when attempting to fetch the update result (rather than a timeout).
func (s *ClientFunctionalSuite) TestUpdateWorkflow_TerminateWorkflowAfterUpdateAccepted() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tv := testvars.New(s.T()).WithTaskQueue(s.taskQueue).WithNamespaceName(namespace.Name(s.namespace))

	activityDone := make(chan struct{})
	activityFn := func(ctx context.Context) error {
		activityDone <- struct{}{}
		return nil
	}

	workflowFn := func(ctx workflow.Context) error {
		s.NoError(workflow.SetUpdateHandler(ctx, tv.HandlerName(), func(ctx workflow.Context, arg string) error {
			ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 10 * time.Second,
			})
			for {
				s.NoError(workflow.ExecuteActivity(ctx, activityFn).Get(ctx, nil))
				if false {
					// appease compiler
					break
				}
			}
			return nil
		}))
		s.NoError(workflow.Await(ctx, func() bool { return false }))
		return errors.New("unreachable")
	}

	s.worker.RegisterWorkflow(workflowFn)
	s.worker.RegisterActivity(activityFn)
	tv, wfRun := s.startWorkflow(ctx, tv, workflowFn)

	updateHandle, err := s.updateWorkflowWaitAccepted(ctx, tv, "my-update-arg")
	s.NoError(err)

	select {
	case <-activityDone:
	case <-ctx.Done():
		s.FailNow("timed out waiting for activity to be called by update handler")
	}
	s.NoError(s.sdkClient.TerminateWorkflow(ctx, tv.WorkflowID(), tv.RunID(), "reason"))

	var notFound *serviceerror.NotFound
	s.ErrorAs(updateHandle.Get(ctx, nil), &notFound)

	var wee *temporal.WorkflowExecutionError
	s.ErrorAs(wfRun.Get(ctx, nil), &wee)
}

func (s *ClientFunctionalSuite) startWorkflow(ctx context.Context, tv *testvars.TestVars, workflowFn interface{}) (*testvars.TestVars, sdkclient.WorkflowRun) {
	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        tv.WorkflowID(),
		TaskQueue: tv.TaskQueue().Name,
	}, workflowFn)
	s.NoError(err)
	return tv.WithRunID(run.GetRunID()), run
}

func (s *ClientFunctionalSuite) updateWorkflowWaitAdmitted(ctx context.Context, tv *testvars.TestVars, arg string) {
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
	}, time.Second, 10*time.Millisecond, fmt.Sprintf("update %s did not reach Admitted stage", tv.UpdateID()))
}

func (s *ClientFunctionalSuite) updateWorkflowWaitAccepted(ctx context.Context, tv *testvars.TestVars, arg string) (sdkclient.WorkflowUpdateHandle, error) {
	return s.sdkClient.UpdateWorkflow(ctx, sdkclient.UpdateWorkflowOptions{
		UpdateID:     tv.UpdateID(),
		WorkflowID:   tv.WorkflowID(),
		RunID:        tv.RunID(),
		UpdateName:   tv.HandlerName(),
		Args:         []interface{}{arg},
		WaitForStage: sdkclient.WorkflowUpdateStageAccepted,
	})
}

func (s *ClientFunctionalSuite) pollUpdate(ctx context.Context, tv *testvars.TestVars, waitPolicy *updatepb.WaitPolicy) (*workflowservice.PollWorkflowExecutionUpdateResponse, error) {
	return s.sdkClient.WorkflowService().PollWorkflowExecutionUpdate(ctx, &workflowservice.PollWorkflowExecutionUpdateRequest{
		Namespace:  tv.NamespaceName().String(),
		UpdateRef:  tv.UpdateRef(),
		Identity:   tv.ClientIdentity(),
		WaitPolicy: waitPolicy,
	})
}
