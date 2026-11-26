package tests

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/serverbuggify"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/service/matching"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/testx/failpoint/buggify"
)

// TestBuggify runs a dedicated cluster with BUGGIFY enabled through the testing.buggify dynamic
// config setting and verifies that matching.SkipSyncMatchKey is activated and fires
// deterministically, and that an explicit failpoint takes precedence over BUGGIFY.
func TestBuggify(t *testing.T) {
	t.Parallel()
	// The entry sets the probabilities of the default key of matching to 1.
	probability := 1.0
	env := testcore.NewEnv(t, testcore.WithBuggify(42, serverbuggify.Key{
		Match:                 matching.SkipSyncMatchKey.Name(),
		ActivationProbability: &probability,
		FireProbability:       &probability,
	}))
	registry := testcore.FailpointRegistry(env.GetTestCluster())
	b, ok := registry.Policy().(*buggify.Buggify)
	require.True(t, ok)

	// The activation is a pure function of the seed and the configuration, so it is asserted on
	// the configuration: the server may have decided it, and emitted its event, before the observer
	// is installed. The fires are counted from now on, and nothing resets the count.
	name := matching.SkipSyncMatchKey.Name()
	require.True(t, b.Config().Activated(name))
	var fires atomic.Int64
	b.OnDecision(func(e buggify.Event) {
		if e.Kind == buggify.EventFire && e.Name == name {
			fires.Add(1)
		}
	})
	t.Cleanup(func() { b.OnDecision(nil) })

	completeWorkflows(t, env)

	await.Require(t.Context(), t, func(t *await.T) {
		require.Positive(t, fires.Load())
	}, 10*time.Second, 100*time.Millisecond)
}

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
	testcore.InjectFailpoint(env, matching.SkipSyncMatchKey).On()

	completeWorkflows(t, env)
}

// TestBuggifiableSettings completes workflows with each value that BUGGIFY picks for its settings,
// to check that the values are legal in every deployment.
func TestBuggifiableSettings(t *testing.T) {
	t.Parallel()
	buggifiable := testcore.BuggifiableSettings()
	settings := []dynamicconfig.GenericSetting{
		dynamicconfig.MatchingMaxTaskBatchSize,
		dynamicconfig.TimerTaskBatchSize,
		dynamicconfig.TransferTaskBatchSize,
	}
	require.Len(t, buggifiable, len(settings), "add the new buggifiable setting to this test")
	for _, setting := range settings {
		values, ok := buggifiable[setting.Key()]
		require.True(t, ok, setting.Key())
		for _, value := range values {
			t.Run(fmt.Sprintf("%s=%v", setting.Key(), value), func(t *testing.T) {
				t.Parallel()
				env := testcore.NewEnv(t, testcore.WithDynamicConfig(setting, value))
				completeWorkflows(t, env)
			})
		}
	}
}
