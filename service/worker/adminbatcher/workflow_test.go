package adminbatcher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	batchspb "go.temporal.io/server/api/batch/v1"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/worker/batcher"
)

func TestWorkflow(t *testing.T) {
	t.Run("rejects a batch without an admin request", func(t *testing.T) {
		env := newTestEnv(t)
		env.ExecuteWorkflow(WorkflowTypeName, &batchspb.BatchOperationInput{
			NamespaceId: "ns-id",
			Request:     &workflowservice.StartBatchOperationRequest{Namespace: "ns"},
		})

		require.True(t, env.IsWorkflowCompleted())
		require.ErrorContains(t, env.GetWorkflowError(), "admin batch workflow requires an admin request")
	})

	t.Run("dispatches the batch activity to the admin batch task queue", func(t *testing.T) {
		env := newTestEnv(t)

		var taskQueue string
		env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
			taskQueue = info.TaskQueue
		})
		env.OnActivity("BatchActivityWithProtobuf", mock.Anything, mock.Anything).
			Return(batcher.HeartBeatDetails{SuccessCount: 1}, nil)

		env.ExecuteWorkflow(WorkflowTypeName, &batchspb.BatchOperationInput{
			NamespaceId: "ns-id",
			AdminRequest: &adminservice.StartAdminBatchOperationRequest{
				Namespace:  "ns",
				Executions: []*commonpb.WorkflowExecution{{WorkflowId: "w"}},
			},
		})

		require.True(t, env.IsWorkflowCompleted())
		require.NoError(t, env.GetWorkflowError())
		require.Equal(t, primitives.AdminBatchActivityTQ, taskQueue)
	})
}

func newTestEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	t.Cleanup(func() { env.AssertExpectations(t) })
	env.RegisterWorkflowWithOptions(Workflow, workflow.RegisterOptions{Name: WorkflowTypeName})
	env.RegisterActivity(&batcher.Activities{})
	return env
}
