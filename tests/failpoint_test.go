package tests

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/serverbuggify"
	"go.temporal.io/server/common/testing/serverfailpoints"
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

	t.Run("gRPC failpoint fails a history call for the test namespace", func(t *testing.T) {
		t.Parallel()
		env := testcore.NewEnv(t)
		testcore.InjectFailpoint(env, serverfailpoints.GRPCKey(historyservice.HistoryService_StartWorkflowExecution_FullMethodName)).
			Return(serviceerror.NewInvalidArgument("injected"), failpoint.Trigger{MaxFires: 1})

		_, err := env.FrontendClient().StartWorkflowExecution(testcore.NewContext(), startWorkflowRequest(env, env.Tv()))
		require.ErrorContains(t, err, "injected")

		mustStartWorkflow(env, env.Tv())
	})
}

// TestBuggify runs a dedicated cluster with BUGGIFY, since BUGGIFY applies to the whole cluster. The
// seed and the keys come from the failpoints.buggify block of the static config.
// The keys only activate gRPC keys and matching.skipSyncMatch, and only let the history
// StartWorkflowExecution call fail, so the rest of the cluster keeps working.
func TestBuggify(t *testing.T) {
	t.Parallel()
	startKey := serverfailpoints.GRPCKey(historyservice.HistoryService_StartWorkflowExecution_FullMethodName)
	probability := func(p float64) *float64 { return &p }
	static := config.FailpointsBuggify{
		Seed: 42,
		Keys: []config.FailpointsBuggifyKey{
			{Match: startKey.Name(), ActivationProbability: probability(1), FireProbability: probability(1), Error: "InvalidArgument"},
			// Half of the other gRPC keys are activated, but never fire.
			{Match: "grpc:*", ActivationProbability: probability(0.5), FireProbability: probability(0), Error: "Unavailable"},
			{Match: matching.SkipSyncMatchKey.Name(), ActivationProbability: probability(0.25), FireProbability: probability(0.25)},
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
			// The test only checks activations and fires.
		}
	})
	t.Cleanup(func() { b.OnDecision(nil) })

	scopedStartKey := startKey.ForScope(env.NamespaceID().String())

	t.Run("a BUGGIFY error surfaces from a gRPC key", func(t *testing.T) {
		_, err := env.FrontendClient().StartWorkflowExecution(testcontext.For(t), startWorkflowRequest(env, env.Tv()))
		_, ok := errors.AsType[*serviceerror.InvalidArgument](err)
		require.True(t, ok)
		require.ErrorContains(t, err, "BUGGIFY: injected by failpoint "+startKey.Name())

		require.True(t, b.Config().Activated(startKey.Name()))
		await.Require(t.Context(), t, func(t *await.T) {
			mu.Lock()
			defer mu.Unlock()
			require.Positive(t, fires[startKey.Name()])
		}, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("an explicit failpoint takes precedence", func(t *testing.T) {
		fp := testcore.InjectFailpointIn(t, env.GetTestCluster(), scopedStartKey)
		fp.Return(serviceerror.NewInvalidArgument("explicit"))
		_, err := env.FrontendClient().StartWorkflowExecution(testcontext.For(t), startWorkflowRequest(env, env.Tv()))
		require.ErrorContains(t, err, "explicit")

		// A registered failpoint keeps BUGGIFY away from its key even while it is disabled.
		fp.Disable()
		mustStartWorkflow(env, env.Tv())
	})

	t.Run("the seed determines the activated keys", func(t *testing.T) {
		resolved, err := serverbuggify.ReadStatic(static)
		require.NoError(t, err)
		want := resolved.Config
		require.Equal(t, want, b.Config())

		require.NoError(t, b.Set(buggify.Config{}))
		require.NoError(t, b.Set(want))
		mu.Lock()
		clear(activations)
		mu.Unlock()

		testcore.InjectFailpointIn(t, env.GetTestCluster(), scopedStartKey, testcore.MayNotFire())
		mustStartWorkflow(env, env.Tv().WithWorkflowIDNumber(3))

		await.Require(t.Context(), t, func(t *await.T) {
			mu.Lock()
			defer mu.Unlock()
			require.Contains(t, activations, matching.SkipSyncMatchKey.Name())
		}, 10*time.Second, 100*time.Millisecond)

		mu.Lock()
		defer mu.Unlock()
		for name, active := range activations {
			require.Equal(t, want.Activated(name), active, name)
			// BUGGIFY doesn't inject into the requests that clients send to the public frontend.
			require.NotContains(t, name, "grpc:/temporal.api.workflowservice.v1.WorkflowService/")
		}
	})
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
