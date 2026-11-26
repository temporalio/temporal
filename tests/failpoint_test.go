package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/service/matching"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/testx/failpoint"
)

// completeWorkflows runs a few workflows with timers and an activity on the SDK worker of env,
// which polls sticky task queues, and waits for them to complete.
func completeWorkflows(t *testing.T, env *testcore.TestEnv) {
	t.Helper()
	const workflowName = "failpoint-test-workflow"
	env.SdkWorker().RegisterActivityWithOptions(
		func(context.Context) (int, error) { return 7, nil },
		activity.RegisterOptions{Name: "failpoint-test-activity"},
	)
	env.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) (int, error) {
			for range 3 {
				if err := workflow.Sleep(ctx, 10*time.Millisecond); err != nil {
					return 0, err
				}
			}
			var result int
			ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second})
			err := workflow.ExecuteActivity(ctx, "failpoint-test-activity").Get(ctx, &result)
			return result, err
		},
		workflow.RegisterOptions{Name: workflowName},
	)

	const workflows = 5
	runs := make([]sdkclient.WorkflowRun, 0, workflows)
	for range workflows {
		run, err := env.SdkClient().ExecuteWorkflow(testcontext.For(t), sdkclient.StartWorkflowOptions{
			ID:        testcore.RandomizeStr("failpoint-wf"),
			TaskQueue: env.WorkerTaskQueue(),
		}, workflowName)
		require.NoError(t, err)
		runs = append(runs, run)
	}
	for _, run := range runs {
		ctx, cancel := context.WithTimeout(testcontext.For(t), 60*time.Second)
		var result int
		require.NoError(t, run.Get(ctx, &result))
		cancel()
		require.Equal(t, 7, result)
	}
}

// TestSkipSyncMatch checks that the workflow and activity tasks that matching skips the sync match
// for are delivered from the backlog, to a worker that polls a sticky task queue too.
func TestSkipSyncMatch(t *testing.T) {
	t.Parallel()
	env := testcore.NewEnv(t)
	registry := testcore.FailpointRegistry(env.GetTestCluster())
	key := matching.SkipSyncMatchKey.ForScope(env.NamespaceID().String())
	fp := failpoint.NewFailpoint(key)
	fp.On()
	registry.Set(fp)
	t.Cleanup(func() {
		registry.Delete(key)
		fp.Disable()
	})

	completeWorkflows(t, env)
}
