package passivepath

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/log"
)

func controlActivity(_ context.Context, value string) (string, error) {
	return value, nil
}

func controlWorkflow(ctx workflow.Context) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToCloseTimeout: 20 * time.Second,
	})
	var result string
	err := workflow.ExecuteActivity(ctx, controlActivity, "ok").Get(ctx, &result)
	return result, err
}

// TestControl_NoHook verifies that the cluster setup works without the hook installed.
func TestControl_NoHook(t *testing.T) {
	sdkworker.SetStickyWorkflowCacheSize(0)
	logger := log.NewTestLogger()

	tc := newSingleClusterWithGlobalNamespace(t, logger)
	ns := "control-ns"
	registerGlobalNamespace(t, tc, ns)

	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  tc.Host().FrontendGRPCAddress(),
		Namespace: ns,
	})
	require.NoError(t, err)
	t.Cleanup(sdkClient.Close)

	taskQueue := "control-tq"
	w := sdkworker.New(sdkClient, taskQueue, sdkworker.Options{})
	w.RegisterWorkflow(controlWorkflow)
	w.RegisterActivity(controlActivity)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	run, err := sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID: "control-basic", TaskQueue: taskQueue, WorkflowRunTimeout: 45 * time.Second,
	}, controlWorkflow)
	require.NoError(t, err)
	var s string
	require.NoError(t, run.Get(ctx, &s))
	require.Equal(t, "ok", s)
}
