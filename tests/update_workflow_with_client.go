package tests

import (
	"context"
	"errors"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testvars"
)

// TestUpdateWorkflow_TerminateWorkflowDuringUpdate executes a long-running update (schedules a sequence of activity
// calls) and terminates the workflow after the update has been accepted but before it has been completed. It checks
// that the client gets a NotFound error when attempting to fetch the update result (rather than a timeout).
func (s *ClientFunctionalSuite) TestUpdateWorkflow_TerminateWorkflowDuringUpdate() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tv := testvars.New(s.T().Name()).WithTaskQueue(s.taskQueue).WithNamespaceName(namespace.Name(s.namespace))

	activityDone := make(chan struct{})
	activityFn := func(ctx context.Context) error {
		activityDone <- struct{}{}
		return nil
	}

	workflowFn := func(ctx workflow.Context) error {
		workflow.SetUpdateHandler(ctx, tv.HandlerName(), func(ctx workflow.Context, arg string) error {
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
		})
		workflow.Await(ctx, func() bool { return false })
		return errors.New("unreachable")
	}

	s.worker.RegisterWorkflow(workflowFn)
	s.worker.RegisterActivity(activityFn)
	tv, wfRun := s.startWorkflow(ctx, tv, workflowFn)

	updateHandle := s.updateWorkflowWaitAccepted(ctx, tv, "my-update-id", "my-update-arg")

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

func (s *ClientFunctionalSuite) updateWorkflowWaitAccepted(ctx context.Context, tv *testvars.TestVars, updateID string, arg string) sdkclient.WorkflowUpdateHandle {
	handle, err := s.sdkClient.UpdateWorkflowWithOptions(ctx, &sdkclient.UpdateWorkflowWithOptionsRequest{
		UpdateID:   updateID,
		WorkflowID: tv.WorkflowID(),
		RunID:      tv.RunID(),
		UpdateName: tv.HandlerName(),
		Args:       []interface{}{arg},
		WaitPolicy: &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED},
	})
	s.NoError(err)
	return handle
}
