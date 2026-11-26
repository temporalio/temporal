package tests

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/failpointrules"
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

	t.Run("persistence failpoint fails a store method for the test namespace", func(t *testing.T) {
		t.Parallel()
		env := testcore.NewEnv(t)
		otherNamespace := namespace.Name(testcore.RandomizeStr("other-namespace"))
		_, err := env.RegisterNamespace(testcontext.For(t), otherNamespace, 1, enumspb.ARCHIVAL_STATE_DISABLED, "", "")
		require.NoError(t, err)
		testcore.InjectFailpoint(env, serverfailpoints.PersistenceKey(string(config.ExecutionStoreName), "CreateWorkflowExecution")).
			Return(errors.New("injected"))

		_, err = env.FrontendClient().StartWorkflowExecution(testcontext.For(t), startWorkflowRequest(env, env.Tv()))
		require.ErrorContains(t, err, "injected")

		otherRequest := startWorkflowRequest(env, env.Tv())
		otherRequest.Namespace = otherNamespace.String()
		_, err = env.FrontendClient().StartWorkflowExecution(testcontext.For(t), otherRequest)
		require.NoError(t, err)
	})

	t.Run("global persistence failpoint fails a store method", func(t *testing.T) {
		t.Parallel()
		env := testcore.NewEnv(t, testcore.WithDedicatedCluster())
		fp := testcore.InjectUnscopedFailpoint(env, serverfailpoints.PersistenceKey(string(config.ExecutionStoreName), "CreateWorkflowExecution"))
		fp.Return(errors.New("injected"))

		_, err := env.FrontendClient().StartWorkflowExecution(testcore.NewContext(), startWorkflowRequest(env, env.Tv()))
		require.ErrorContains(t, err, "injected")

		fp.Disable()
		mustStartWorkflow(env, env.Tv())
	})

	t.Run("dynamic config rule fails a history call until it is removed", func(t *testing.T) {
		t.Parallel()
		env := testcore.NewEnv(t, testcore.WithDedicatedCluster())
		cleanup := env.OverrideDynamicConfig(failpointrules.Setting, []any{map[string]any{
			"key":     serverfailpoints.GRPCKey(historyservice.HistoryService_StartWorkflowExecution_FullMethodName).Name(),
			"scope":   env.NamespaceID().String(),
			"action":  "Return",
			"error":   "InvalidArgument",
			"message": "injected by rule",
		}})

		await.Require(t.Context(), t, func(t *await.T) {
			_, err := env.FrontendClient().StartWorkflowExecution(t.Context(), startWorkflowRequest(env, env.Tv()))
			_, ok := errors.AsType[*serviceerror.InvalidArgument](err)
			require.True(t, ok)
			require.ErrorContains(t, err, "injected by rule")
		}, 10*time.Second, 100*time.Millisecond)

		cleanup()
		await.Require(t.Context(), t, func(t *await.T) {
			_, err := env.FrontendClient().StartWorkflowExecution(t.Context(), startWorkflowRequest(env, env.Tv()))
			require.NoError(t, err)
		}, 10*time.Second, 100*time.Millisecond)
	})
}

// TestFailpointControls runs the controls of a failpoint and of a rule against a cluster: a block
// holds a call, a response key replaces the result of a handler, an HTTP key fails an outgoing
// call, a Pause rule delays a call, and a failpoint and a gRPC fault on one method.
func TestFailpointControls(t *testing.T) {
	t.Parallel()
	startMethod := historyservice.HistoryService_StartWorkflowExecution_FullMethodName

	t.Run("block holds a history call until the failpoint is disabled", func(t *testing.T) {
		t.Parallel()
		env := testcore.NewEnv(t)
		fp := testcore.InjectFailpoint(env, serverfailpoints.GRPCKey(startMethod))
		fp.Block(failpoint.Trigger{MaxFires: 1})

		done := make(chan error, 1)
		go func() {
			_, err := env.FrontendClient().StartWorkflowExecution(testcontext.For(t), startWorkflowRequest(env, env.Tv()))
			done <- err
		}()

		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		require.NoError(t, fp.Wait(ctx), "the call reaches the block")
		select {
		case err := <-done:
			t.Fatalf("the call returned while it was blocked: %v", err)
		case <-time.After(200 * time.Millisecond):
		}

		fp.Disable()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatal("the call is still blocked after the failpoint was disabled")
		}
	})

	t.Run("response key fails a call after the handler ran", func(t *testing.T) {
		t.Parallel()
		env := testcore.NewEnv(t)
		testcore.InjectFailpoint(env, serverfailpoints.GRPCResponseKey(startMethod)).
			Return(serviceerror.NewInvalidArgument("injected response"), failpoint.Trigger{MaxFires: 1})

		request := startWorkflowRequest(env, env.Tv())
		_, err := env.FrontendClient().StartWorkflowExecution(testcontext.For(t), request)
		require.ErrorContains(t, err, "injected response")

		// The handler ran, so the workflow exists although the caller saw an error.
		_, err = env.FrontendClient().DescribeWorkflowExecution(testcontext.For(t), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: request.WorkflowId},
		})
		require.NoError(t, err)
	})

	t.Run("pause rule delays one call", func(t *testing.T) {
		t.Parallel()
		env := testcore.NewEnv(t, testcore.WithDedicatedCluster())
		key := serverfailpoints.GRPCKey(startMethod)
		const delay = time.Second
		env.OverrideDynamicConfig(failpointrules.Setting, []any{map[string]any{
			"key":     key.Name(),
			"scope":   env.NamespaceID().String(),
			"action":  "Pause",
			"delay":   delay.String(),
			"trigger": map[string]any{"maxFires": 1},
		}})
		registry := testcore.FailpointRegistry(env.GetTestCluster())
		await.Require(t.Context(), t, func(t *await.T) {
			require.NotNil(t, registry.Get(key.ForScope(env.NamespaceID().String())))
		}, 10*time.Second, 50*time.Millisecond)

		start := time.Now()
		mustStartWorkflow(env, env.Tv())
		require.GreaterOrEqual(t, time.Since(start), delay, "the first call pauses")

		mustStartWorkflow(env, env.Tv().WithWorkflowIDNumber(2))
		rule, ok := registry.Get(key.ForScope(env.NamespaceID().String())).(*failpoint.Failpoint[error])
		require.True(t, ok)
		_, fires := rule.Stats()
		require.EqualValues(t, 1, fires, "the rule fires once")
	})

	t.Run("a failpoint on the method wins over a gRPC fault on every method", func(t *testing.T) {
		t.Parallel()
		env := testcore.NewEnv(t)
		testcore.InjectFailpoint(env, serverfailpoints.GRPCKey(startMethod)).
			Return(serviceerror.NewInvalidArgument("from the failpoint"), failpoint.Trigger{MaxFires: 1})
		env.InjectRequestFault(func(req any) error {
			if _, ok := req.(*historyservice.StartWorkflowExecutionRequest); ok {
				return serviceerror.NewInvalidArgument("from the fault")
			}
			return nil
		})

		// The failpoint of the method fires first, and the fault of every method doesn't run.
		_, err := env.FrontendClient().StartWorkflowExecution(testcontext.For(t), startWorkflowRequest(env, env.Tv()))
		require.ErrorContains(t, err, "from the failpoint")

		// The failpoint is spent, so the fault of every method runs. The test doesn't fail for a
		// fault that never fired, since this call fires it.
		_, err = env.FrontendClient().StartWorkflowExecution(testcontext.For(t), startWorkflowRequest(env, env.Tv().WithWorkflowIDNumber(2)))
		require.ErrorContains(t, err, "from the fault")
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

	// The observer replaces the one that logs the decisions of the server. BUGGIFY delivers events
	// from the goroutine of any execution and possibly after the execution returned, so the
	// subtests wait for the events they check.
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

	// The cluster is dedicated, but the history StartWorkflowExecution failpoint fires for every
	// namespace, including system workflows. So the subtests assert at least one fire, and each of
	// them registers its own failpoint for the namespace of the test, with the t of the subtest,
	// where it needs a call to succeed.
	scopedStartKey := startKey.ForScope(env.NamespaceID().String())

	t.Run("a BUGGIFY error surfaces from a gRPC key", func(t *testing.T) {
		_, err := env.FrontendClient().StartWorkflowExecution(testcontext.For(t), startWorkflowRequest(env, env.Tv()))
		_, ok := errors.AsType[*serviceerror.InvalidArgument](err)
		require.True(t, ok)
		require.ErrorContains(t, err, "BUGGIFY: injected by failpoint "+startKey.Name())

		// A fire needs an activated key. The activation event itself can precede the observer, so
		// the configuration says what the activation was.
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

		// A new run activates the names again, so the observer receives the activations of the
		// traffic below, not only those of traffic before it was installed or of other subtests.
		require.NoError(t, b.Set(buggify.Config{}))
		require.NoError(t, b.Set(want))
		mu.Lock()
		clear(activations)
		mu.Unlock()

		// A registered failpoint that never fires lets the workflow start, and its task reaches
		// matching's sync match, a Buggify call site that no failpoint is registered for.
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
