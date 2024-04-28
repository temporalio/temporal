package tests

import (
	"context"
	"time"

	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testvars"
)

func (s *ClientFunctionalSuite) TestUpdateWorkflow_ExampleTest() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tv := testvars.New(s.T().Name()).WithTaskQueue(s.taskQueue).WithNamespaceName(namespace.Name(s.namespace))

	workflowFn := func(ctx workflow.Context) (string, error) {
		var updateArgs []string
		workflow.SetUpdateHandlerWithOptions(ctx, tv.HandlerName(),
			func(arg string) (string, error) {
				updateArgs = append(updateArgs, arg)
				return arg + "-result", nil
			},
			workflow.UpdateHandlerOptions{
				Validator: func(arg string) error {
					return nil
				},
			})
		workflow.Await(ctx, func() bool { return len(updateArgs) > 0 })
		return "wf-result", nil
	}

	s.worker.RegisterWorkflow(workflowFn)
	tv, wfRun := s.startWorkflow(ctx, tv, workflowFn)
	updateHandle := s.updateWorkflow(ctx, tv, "my-update-arg")

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
