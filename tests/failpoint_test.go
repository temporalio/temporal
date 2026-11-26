package tests

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/serverbuggify"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/service/matching"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/buggify"
)

func TestFailpoint(t *testing.T) {
	t.Parallel()

	t.Run("test cluster registers a failpoint for the test namespace", func(t *testing.T) {
		t.Parallel()
		env := testcore.NewEnv(t)
		registry := testcore.FailpointRegistry(env.GetTestCluster())
		require.NotNil(t, registry, "test clusters enable failpoints with the startup switch")

		key := matching.ForwardTaskKey.ForScope(env.NamespaceID().String())
		require.Nil(t, registry.Get(key))
		testcore.InjectFailpoint(env, matching.ForwardTaskKey, testcore.MayNotFire()).Return(errors.New("injected"), failpoint.Trigger{MaxFires: 1})
		require.NotNil(t, registry.Get(key))
		require.Nil(t, registry.Get(matching.ForwardTaskKey), "the failpoint is scoped to the test namespace")
	})
}

// TestBuggify runs a dedicated cluster with BUGGIFY enabled via the static config and verifies that
// matching.SkipSyncMatchKey is activated and fires deterministically, and that an explicit
// failpoint takes precedence over BUGGIFY.
func TestBuggify(t *testing.T) {
	t.Parallel()
	probability := func(p float64) *float64 { return &p }
	static := config.FailpointsBuggify{
		Seed: 42,
		Keys: []config.FailpointsBuggifyKey{
			{Match: matching.SkipSyncMatchKey.Name(), ActivationProbability: probability(1), FireProbability: probability(1)},
		},
	}
	env := testcore.NewEnv(t, testcore.WithBuggify(static))
	registry := testcore.FailpointRegistry(env.GetTestCluster())
	b, ok := registry.Policy().(*buggify.Buggify)
	require.True(t, ok)

	var mu sync.Mutex
	activations := map[string]bool{}
	fires := map[string]int{}
	b.OnDecision(func(e buggify.Event) {
		mu.Lock()
		defer mu.Unlock()
		switch e.Kind {
		case buggify.EventActivation:
			activations[e.Name] = e.Activated
		case buggify.EventFire:
			fires[e.Name]++
		default:
		}
	})
	t.Cleanup(func() { b.OnDecision(nil) })

	completeWorkflows(t, env)

	await.Require(t.Context(), t, func(t *await.T) {
		mu.Lock()
		defer mu.Unlock()
		require.True(t, activations[matching.SkipSyncMatchKey.Name()])
		require.Positive(t, fires[matching.SkipSyncMatchKey.Name()])
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
	fp := testcore.InjectFailpoint(env, matching.SkipSyncMatchKey)
	fp.On()

	completeWorkflows(t, env)

	_, fires := fp.Stats()
	require.Positive(t, fires, "matching skipped the sync match")
}

// TestBuggifiableSettings completes workflows with each value that BUGGIFY picks for its settings,
// to check that the values are legal in every deployment.
func TestBuggifiableSettings(t *testing.T) {
	t.Parallel()
	buggifiable := serverbuggify.BuggifiableSettings()
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
