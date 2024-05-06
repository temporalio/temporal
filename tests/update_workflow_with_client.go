package tests

import (
	"context"
	"errors"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	updatepb "go.temporal.io/api/update/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testvars"
)

// TestUpdateWorkflow_TerminateWorkflowDuringUpdate executes a long-running update (schedules a sequence of timers) and
// terminates the workflow after the update has been accepted but before it has been completed.
func (s *ClientFunctionalSuite) TestUpdateWorkflow_TerminateWorkflowDuringUpdate() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tv := testvars.New(s.T().Name()).WithTaskQueue(s.taskQueue).WithNamespaceName(namespace.Name(s.namespace))

	workflowFn := func(ctx workflow.Context) error {
		l := workflow.GetLogger(ctx)
		workflow.SetUpdateHandler(ctx, tv.HandlerName(), func(ctx workflow.Context, arg string) error {
			for {
				l.Info("in update handler; about to sleep\n")
				workflow.Sleep(ctx, time.Duration(time.Second))
				l.Info("in update handler; after sleep\n")
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
	tv, wfRun := s.startWorkflow(ctx, tv, workflowFn)

	updateHandle := s.updateWorkflowWaitAccepted(ctx, tv, "my-update-id", "my-update-arg")

	time.Sleep(5 * time.Second)
	s.NoError(s.sdkClient.TerminateWorkflow(ctx, tv.WorkflowID(), tv.RunID(), "reason"))

	var updateResult string
	s.NoError(updateHandle.Get(ctx, &updateResult))
	s.Equal("my-update-arg-result", updateResult)

	var wfResult string
	s.NoError(wfRun.Get(ctx, &wfResult))
	s.Equal("wf-result", wfResult)
}

func (s *ClientFunctionalSuite) startWorkflow(ctx context.Context, tv *testvars.TestVars, workflowFn interface{}) (*testvars.TestVars, sdkclient.WorkflowRun) {
	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        tv.WorkflowID(),
		TaskQueue: tv.TaskQueue().Name,
	}, workflowFn)
	s.NoError(err)
	return tv.WithRunID(run.GetRunID()), run
}

func (s *ClientFunctionalSuite) updateWorkflow(ctx context.Context, tv *testvars.TestVars, arg string) sdkclient.WorkflowUpdateHandle {
	handle, err := s.sdkClient.UpdateWorkflow(ctx, tv.WorkflowID(), tv.RunID(), tv.HandlerName(), arg)
	s.NoError(err)
	return handle
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
