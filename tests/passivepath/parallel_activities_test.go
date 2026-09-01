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
	"go.temporal.io/server/common/testing/testhooks"
)

const parallelActivityCount = 5

// parallelActivityWorkflow schedules several activities in a single workflow task,
// matching the concurrency pattern used by load generators.
func parallelActivityWorkflow(ctx workflow.Context) (int, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToCloseTimeout: 30 * time.Second,
	})

	futures := make([]workflow.Future, 0, parallelActivityCount)
	for i := 0; i < parallelActivityCount; i++ {
		futures = append(futures, workflow.ExecuteActivity(ctx, echoActivity, "x"))
	}

	done := 0
	for _, f := range futures {
		var out string
		if err := f.Get(ctx, &out); err != nil {
			return done, err
		}
		done++
	}
	return done, nil
}

// TestPassivePath_ParallelActivities is a minimal reproduction of the stall observed
// when running bench-go against the passive-path server: activities sat in state
// Scheduled and were never dispatched. The relevant difference from the primary
// workflow is that several activities are scheduled in one workflow task.
func TestPassivePath_ParallelActivities(t *testing.T) {
	sdkworker.SetStickyWorkflowCacheSize(0)

	logger := log.NewTestLogger()
	harness := NewHarness(logger)

	tc := newSingleClusterWithGlobalNamespace(t, logger)

	t.Cleanup(func() {
		t.Logf("PASSIVEPATH intercepted=%d diverted=%d applied=%d standbyExecutions=%d bailouts=%v allBailouts=%v applyErrs=%v",
			harness.Intercepted(), harness.Diverted(), harness.Applied(),
			harness.StandbyExecutions(),
			harness.Bailouts(), harness.AllBailouts(), harness.ApplyErrors())
	})

	ns := "passive-path-parallel-ns"
	namespaceID := registerGlobalNamespace(t, tc, ns)
	t.Cleanup(tc.InjectHook(t, testhooks.NewHook[testhooks.HistoryPassiveReplicationTestHook](
		testhooks.HistoryPassiveReplicationTest,
		harness,
	), namespaceID))

	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  tc.Host().FrontendGRPCAddress(),
		Namespace: ns,
	})
	require.NoError(t, err)
	t.Cleanup(sdkClient.Close)

	taskQueue := "passive-path-parallel-tq"
	w := sdkworker.New(sdkClient, taskQueue, sdkworker.Options{})
	w.RegisterWorkflow(parallelActivityWorkflow)
	w.RegisterActivity(echoActivity)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	run, err := sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                 "passive-path-parallel",
		TaskQueue:          taskQueue,
		WorkflowRunTimeout: 45 * time.Second,
	}, parallelActivityWorkflow)
	require.NoError(t, err)

	var completed int
	if err := run.Get(ctx, &completed); err != nil {
		dumpStalledWorkflow(t, sdkClient, "passive-path-parallel", run.GetRunID())
		t.Fatalf("workflow stalled: %v", err)
	}
	require.Equal(t, parallelActivityCount, completed)

	bailouts := harness.Bailouts()
	for reason := range bailouts {
		require.Contains(t, []BailReason{BailBufferedEvents, BailClearBufferedEvents}, reason,
			"parallel activity completions only permit buffered-event staging fallbacks and cleanup")
	}
	require.Empty(t, harness.ApplyErrors())
	require.Positive(t, harness.Diverted())
	require.Equal(t, harness.Diverted(), harness.Applied())
	require.Positive(t, harness.StandbyExecutions())
}
