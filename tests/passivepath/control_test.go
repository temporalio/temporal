package passivepath

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/server/common/log"
)

// TestControl_NoHook is the baseline: the same cluster config and the same workflows,
// with the passive-path hook NOT installed. Any log noise that appears here is
// pre-existing and not attributable to the diversion.
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
	w.RegisterWorkflow(passivePathWorkflow)
	w.RegisterWorkflow(parallelActivityWorkflow)
	w.RegisterActivity(echoActivity)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	run, err := sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID: "control-basic", TaskQueue: taskQueue, WorkflowRunTimeout: 45 * time.Second,
	}, passivePathWorkflow, false)
	require.NoError(t, err)
	var s string
	require.NoError(t, run.Get(ctx, &s))

	run2, err := sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID: "control-parallel", TaskQueue: taskQueue, WorkflowRunTimeout: 45 * time.Second,
	}, parallelActivityWorkflow)
	require.NoError(t, err)
	var n int
	require.NoError(t, run2.Get(ctx, &n))
	require.Equal(t, parallelActivityCount, n)
}
