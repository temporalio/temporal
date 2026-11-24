package tests

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/buggify"
	"go.temporal.io/server/common/testing/failpointrules"
	"go.temporal.io/server/common/testing/failpoints"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/testx/failpoint"
	fpbuggify "go.temporal.io/testx/failpoint/buggify"
	"go.temporal.io/testx/failpoint/failpointpb"
)

func TestFailpoint(t *testing.T) {
	t.Parallel()

	t.Run("gRPC failpoint fails a history call for the test namespace", func(t *testing.T) {
		t.Parallel()
		env := testcore.NewEnv(t)
		testcore.InjectFailpoint[error](env, failpoint.GRPCKey(historyservice.HistoryService_StartWorkflowExecution_FullMethodName)).
			Return(serviceerror.NewInvalidArgument("injected"), failpoint.Once)

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
		testcore.InjectFailpoint[error](env, failpoints.PersistenceKey(string(config.ExecutionStoreName), "CreateWorkflowExecution")).
			Return(errors.New("injected"), failpoint.Always)

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
		fp := testcore.InjectGlobalFailpoint[error](env, failpoints.PersistenceKey(string(config.ExecutionStoreName), "CreateWorkflowExecution"))
		fp.Return(errors.New("injected"), failpoint.Always)

		_, err := env.FrontendClient().StartWorkflowExecution(testcore.NewContext(), startWorkflowRequest(env, env.Tv()))
		require.ErrorContains(t, err, "injected")

		fp.Disable()
		mustStartWorkflow(env, env.Tv())
	})

	t.Run("dynamic config rule fails a history call until it is removed", func(t *testing.T) {
		t.Parallel()
		env := testcore.NewEnv(t, testcore.WithDedicatedCluster())
		cleanup := env.OverrideDynamicConfig(failpointrules.Setting, []any{map[string]any{
			"key":     failpoint.GRPCKey(historyservice.HistoryService_StartWorkflowExecution_FullMethodName).Name(),
			"scope":   env.NamespaceID().String(),
			"action":  "Return",
			"error":   "InvalidArgument",
			"message": "injected by rule",
		}})

		await.Require(t.Context(), t, func(t *await.T) {
			_, err := env.FrontendClient().StartWorkflowExecution(t.Context(), startWorkflowRequest(env, env.Tv()))
			var invalidArgument *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgument)
			require.ErrorContains(t, err, "injected by rule")
		}, 10*time.Second, 100*time.Millisecond)

		cleanup()
		await.Require(t.Context(), t, func(t *await.T) {
			_, err := env.FrontendClient().StartWorkflowExecution(t.Context(), startWorkflowRequest(env, env.Tv()))
			require.NoError(t, err)
		}, 10*time.Second, 100*time.Millisecond)
	})

	// The services of the test cluster share one failpoint registry, so the frontend applies each
	// request to it once, and ListFailpoints reports which services serve it. That shows the
	// frontend reached every service; that each registry is only changed once is covered by the
	// fan-out's unit tests.
	t.Run("frontend FailpointService toggles a failpoint for the test namespace", func(t *testing.T) {
		t.Parallel()
		env := testcore.NewEnv(t)
		testFailpointService(t, env, env.NamespaceID().String(), []string{"frontend", "history", "matching"})
	})

	t.Run("frontend FailpointService toggles a failpoint in the whole cluster", func(t *testing.T) {
		t.Parallel()
		// mTLS covers dialing the frontend and internode peers with their TLS configs.
		env := testcore.NewEnv(t,
			testcore.WithWorkerService("frontend fans failpoint requests out to the worker"),
			testcore.WithMTLS(),
		)
		testFailpointService(t, env, "", []string{"frontend", "history", "matching", "worker"})
	})
}

// testFailpointService fails StartWorkflowExecution in history with a failpoint configured through
// the frontend's FailpointService, then disables it.
func testFailpointService(t *testing.T, env *testcore.TestEnv, namespaceID string, wantServices []string) {
	t.Helper()
	client := testcore.FailpointClient(env)
	key := failpoint.GRPCKey(historyservice.HistoryService_StartWorkflowExecution_FullMethodName)

	_, err := client.InjectReturn(testcore.NewContext(), &failpointpb.InjectReturnRequest{
		Key:          key.Name(),
		Scope:        namespaceID,
		ErrorName:    "InvalidArgument",
		ErrorMessage: "injected",
	})
	require.NoError(t, err)

	list, err := client.ListFailpoints(testcore.NewContext(), &failpointpb.ListFailpointsRequest{})
	require.NoError(t, err)
	var services []string
	for _, info := range list.Failpoints {
		if info.Key != key.Name() || info.Scope != namespaceID {
			continue // A failpoint of another test on a shared cluster.
		}
		for _, host := range info.Hosts {
			service, _, _ := strings.Cut(host, "/")
			services = append(services, service)
		}
	}
	require.ElementsMatch(t, wantServices, services)

	_, err = env.FrontendClient().StartWorkflowExecution(testcore.NewContext(), startWorkflowRequest(env, env.Tv()))
	var invalidArgument *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArgument)
	require.ErrorContains(t, err, "injected")

	_, err = client.DisableFailpoint(testcore.NewContext(), &failpointpb.DisableFailpointRequest{Key: key.Name(), Scope: namespaceID})
	require.NoError(t, err)
	mustStartWorkflow(env, env.Tv())
}

// TestBuggify runs a dedicated cluster with BUGGIFY, since BUGGIFY applies to the whole cluster.
// The configuration only activates gRPC keys and matching.skipSyncMatch, and only lets the history
// StartWorkflowExecution call fail, so the rest of the cluster keeps working.
func TestBuggify(t *testing.T) {
	t.Parallel()
	startKey := failpoint.GRPCKey(historyservice.HistoryService_StartWorkflowExecution_FullMethodName)
	value := map[string]any{
		"seed":    42,
		"include": []any{"grpc:*", failpoints.MatchingSkipSyncMatch.Name()},
		"prefixes": []any{
			// Half of the gRPC keys are activated, but never fire.
			map[string]any{"prefix": "grpc:", "activation": 0.5, "fire": 0, "error": "Unavailable"},
			map[string]any{"prefix": startKey.Name(), "activation": 1, "fire": 1, "error": "InvalidArgument"},
		},
	}
	env := testcore.NewEnv(t, testcore.WithDynamicConfig(buggify.Setting, value))
	registry := testcore.FailpointRegistry(env)

	var mu sync.Mutex
	activations := map[string]bool{}
	unsubscribe := registry.Subscribe(func(e failpoint.Event) {
		if e.Kind == failpoint.FallbackActivationEvent {
			mu.Lock()
			defer mu.Unlock()
			activations[e.Key.Name()] = e.Activated
		}
	})
	t.Cleanup(unsubscribe)

	t.Run("a BUGGIFY error surfaces from a gRPC key", func(t *testing.T) {
		_, err := env.FrontendClient().StartWorkflowExecution(testcontext.For(t), startWorkflowRequest(env, env.Tv()))
		var invalidArgument *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgument)
		require.ErrorContains(t, err, "BUGGIFY: injected by failpoint "+startKey.Name())
	})

	t.Run("an explicit failpoint takes precedence", func(t *testing.T) {
		fp := testcore.InjectFailpoint[error](env, startKey)
		fp.Return(serviceerror.NewInvalidArgument("explicit"), failpoint.Always)
		_, err := env.FrontendClient().StartWorkflowExecution(testcontext.For(t), startWorkflowRequest(env, env.Tv()))
		require.ErrorContains(t, err, "explicit")

		// A registered failpoint keeps BUGGIFY away from its key even while it is disabled.
		fp.Disable()
		mustStartWorkflow(env, env.Tv())
	})

	t.Run("the seed determines the activated keys", func(t *testing.T) {
		parsed, err := buggify.ConvertValue(value)
		require.NoError(t, err)
		want, _, err := buggify.Resolve(parsed, buggify.Env{})
		require.NoError(t, err)
		require.Equal(t, want, fpbuggify.For(registry).Config())

		// The started workflow's task reaches matching's sync match, a Buggify call site.
		await.Require(t.Context(), t, func(t *await.T) {
			mu.Lock()
			defer mu.Unlock()
			require.Contains(t, activations, failpoints.MatchingSkipSyncMatch.Name())
		}, 10*time.Second, 100*time.Millisecond)

		mu.Lock()
		defer mu.Unlock()
		activated := 0
		for name, active := range activations {
			require.Equal(t, want.Activated(name), active, name)
			if active {
				activated++
			}
			// BUGGIFY doesn't inject into the requests that clients send to the public frontend.
			require.NotContains(t, name, "grpc:/temporal.api.workflowservice.v1.WorkflowService/")
		}
		require.True(t, activations[startKey.Name()])
		require.Less(t, activated, len(activations))
	})

	t.Run("ListFailpoints reports the BUGGIFY counters of reached keys", func(t *testing.T) {
		list, err := testcore.FailpointClient(env).ListFailpoints(testcontext.For(t), &failpointpb.ListFailpointsRequest{})
		require.NoError(t, err)
		counters := map[string]*failpointpb.FallbackKeyInfo{}
		for _, info := range list.FallbackKeys {
			counters[info.Key] = info
		}
		// The first subtest's call fired; the second subtest's calls had a failpoint registered.
		require.True(t, counters[startKey.Name()].GetActivated())
		require.Equal(t, int64(1), counters[startKey.Name()].GetHits())
		require.Equal(t, int64(1), counters[startKey.Name()].GetFires())
		require.Positive(t, counters[failpoints.MatchingSkipSyncMatch.Name()].GetHits())
	})
}
