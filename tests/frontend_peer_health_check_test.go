package tests

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/service/frontend"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func newPeerHealthCheckEnv(t *testing.T) *testcore.TestEnv {
	return testcore.NewEnv(t,
		testcore.WithDedicatedCluster(),
		testcore.WithDynamicConfig(dynamicconfig.FrontendPeerHealthCheckEnabled, true),
		testcore.WithDynamicConfig(dynamicconfig.FrontendPeerHealthCheckInterval, 200*time.Millisecond),
	)
}

func newFrontendHealthClient(t *testing.T, env *testcore.TestEnv) healthpb.HealthClient {
	conn, err := grpc.NewClient(
		env.FrontendGRPCAddress(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return healthpb.NewHealthClient(conn)
}

func frontendWorkflowServiceStatus(t *testing.T, client healthpb.HealthClient) (healthpb.HealthCheckResponse_ServingStatus, error) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{Service: frontend.WorkflowServiceName})
	return resp.GetStatus(), err
}

// awaitFrontendWorkflowServiceStatus waits until the frontend reports want for WorkflowService.
func awaitFrontendWorkflowServiceStatus(t *testing.T, client healthpb.HealthClient, want healthpb.HealthCheckResponse_ServingStatus, timeout time.Duration) {
	await.RequireTruef(t, func() bool {
		got, err := frontendWorkflowServiceStatus(t, client)
		return err == nil && got == want
	}, timeout, 100*time.Millisecond, "frontend WorkflowService health should become %s", want)
}

// injectTimeoutProbes makes every probe of the role time out and counts the probes.
// testcore runs one host per role, so this is a whole-role outage.
func injectTimeoutProbes(env *testcore.TestEnv, failing primitives.ServiceName, calls *atomic.Int32) func() {
	return env.InjectHook(testhooks.NewHook(testhooks.FrontendPeerHealthProbe,
		func(role primitives.ServiceName, _ string) error {
			if role != failing {
				return nil
			}
			calls.Add(1)
			return status.Error(codes.DeadlineExceeded, "injected")
		},
	))
}

// When every sampled host of a role times out, the peers are down rather than the frontend, so it stays SERVING.
func TestPeerHealthCheck_WholeRoleOutageStaysServing(t *testing.T) {
	env := newPeerHealthCheckEnv(t)
	client := newFrontendHealthClient(t, env)
	// Wait for the real probes to pass first; History starts NOT_SERVING until its shards are acquired.
	awaitFrontendWorkflowServiceStatus(t, client, healthpb.HealthCheckResponse_SERVING, 30*time.Second)

	var calls atomic.Int32
	t.Cleanup(injectTimeoutProbes(env, primitives.MatchingService, &calls))
	for _, want := range []int32{1, 3} {
		await.RequireTrue(t, func() bool { return calls.Load() >= want }, 10*time.Second, 50*time.Millisecond)
		got, err := frontendWorkflowServiceStatus(t, client)
		require.NoError(t, err)
		require.Equal(t, healthpb.HealthCheckResponse_SERVING, got)
	}
}

// With the flag off the checker neither probes nor changes the status.
func TestPeerHealthCheck_FlagOffIgnoresFailures(t *testing.T) {
	env := newPeerHealthCheckEnv(t)
	client := newFrontendHealthClient(t, env)
	awaitFrontendWorkflowServiceStatus(t, client, healthpb.HealthCheckResponse_SERVING, 30*time.Second)

	var calls atomic.Int32
	t.Cleanup(injectTimeoutProbes(env, primitives.HistoryService, &calls))
	await.RequireTrue(t, func() bool { return calls.Load() >= 2 }, 10*time.Second, 50*time.Millisecond)

	t.Cleanup(env.OverrideDynamicConfig(dynamicconfig.FrontendPeerHealthCheckEnabled, false))
	// Wait out a round that was already in flight when the flag changed.
	var last atomic.Int32
	last.Store(-1)
	await.RequireTrue(t, func() bool {
		cur := calls.Load()
		return last.Swap(cur) == cur
	}, 10*time.Second, 500*time.Millisecond)

	probed := calls.Load()
	require.Never(t, func() bool { return calls.Load() != probed }, time.Second, 100*time.Millisecond)
	awaitFrontendWorkflowServiceStatus(t, client, healthpb.HealthCheckResponse_SERVING, 10*time.Second)
}
