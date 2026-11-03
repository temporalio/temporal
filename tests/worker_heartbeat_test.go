package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/all"
	chasmworker "go.temporal.io/server/chasm/lib/worker"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
)

// TestWorkerHeartbeatLifecycle verifies that workers with the same grouping key
// share a lease and that later heartbeats renew it.
func TestWorkerHeartbeatLifecycle(t *testing.T) {
	t.Parallel()
	env := newWorkerHeartbeatEnv(t, true, time.Minute)
	groupKey := env.Tv().Any().String()
	otherGroupKey := env.Tv().Any().String()
	first := workerHeartbeatForGroup(env.Tv().Any().String(), groupKey)
	second := workerHeartbeatForGroup(env.Tv().Any().String(), groupKey)
	other := workerHeartbeatForGroup(env.Tv().Any().String(), otherGroupKey)

	// Workers sharing a grouping key renew one lease; mixed groups get separate runs.
	sendWorkerHeartbeats(t, env, first, second, other)
	group, ctx, err := fetchWorkerGroup(t.Context(), env, groupKey, "")
	require.NoError(t, err)
	require.Len(t, group.Workers, 2)
	require.Contains(t, group.Workers, first.WorkerInstanceKey)
	require.Contains(t, group.Workers, second.WorkerInstanceKey)
	require.Equal(t, groupKey, group.WorkerGroupingKey)
	otherGroup, _, err := fetchWorkerGroup(t.Context(), env, otherGroupKey, "")
	require.NoError(t, err)
	require.Equal(t, otherGroupKey, otherGroup.WorkerGroupingKey)
	require.Contains(t, otherGroup.Workers, other.WorkerInstanceKey)
	firstRunID := ctx.ExecutionKey().RunID
	firstDeadline := group.LeaseExpirationTime.AsTime()

	// A heartbeat from one member renews the same shared lease.
	first.WorkerIdentity = "updated-worker"
	sendWorkerHeartbeats(t, env, first)
	group, ctx, err = fetchWorkerGroup(t.Context(), env, groupKey, "")
	require.NoError(t, err)
	require.Equal(t, firstRunID, ctx.ExecutionKey().RunID)

	require.True(t, group.LeaseExpirationTime.AsTime().After(firstDeadline))

	// Workers are still tracked in the in-memory registry.
	response, err := env.FrontendClient().DescribeWorker(t.Context(), &workflowservice.DescribeWorkerRequest{
		Namespace: env.Namespace().String(), WorkerInstanceKey: first.WorkerInstanceKey,
	})
	require.NoError(t, err)
	require.Equal(t, first.WorkerIdentity, response.GetWorkerInfo().GetWorkerHeartbeat().GetWorkerIdentity())
}

// TestWorkerHeartbeatExpiryAndReconnect verifies that the current run closes when
// heartbeats stop and that a later heartbeat starts a new run.
func TestWorkerHeartbeatExpiryAndReconnect(t *testing.T) {
	t.Parallel()
	env := newWorkerHeartbeatEnv(t, true, 2*time.Second)
	groupKey := env.Tv().Any().String()
	heartbeat := workerHeartbeatForGroup(env.Tv().Any().String(), groupKey)
	sendWorkerHeartbeats(t, env, heartbeat)
	_, ctx, err := fetchWorkerGroup(t.Context(), env, groupKey, "")
	require.NoError(t, err)
	oldRunID := ctx.ExecutionKey().RunID

	// With no further heartbeats, the current run closes when the lease expires.
	await.Require(t.Context(), t, func(t *await.T) {
		group, _, err := fetchWorkerGroup(t.Context(), env, groupKey, oldRunID)
		require.NoError(t, err)
		require.Equal(t, workerstatepb.WORKER_GROUP_STATUS_INACTIVE, group.Status)
	}, 15*time.Second, 100*time.Millisecond)

	// Send another heartbeat with the same grouping key and worker instance key.
	sendWorkerHeartbeats(t, env, heartbeat)
	group, ctx, err := fetchWorkerGroup(t.Context(), env, groupKey, "")
	require.NoError(t, err)
	require.NotEqual(t, oldRunID, ctx.ExecutionKey().RunID)
	require.Equal(t, workerstatepb.WORKER_GROUP_STATUS_ACTIVE, group.Status)
	oldGroup, _, err := fetchWorkerGroup(t.Context(), env, groupKey, oldRunID)
	require.NoError(t, err)
	require.Equal(t, workerstatepb.WORKER_GROUP_STATUS_INACTIVE, oldGroup.Status)
}

func TestWorkerHeartbeatWithoutDurableTracking(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		enabled    bool
		groupKey   string
		businessID string
	}{
		{name: "durable tracking disabled", enabled: false, groupKey: "group", businessID: "group"},
		{name: "missing grouping key", enabled: true, businessID: "worker"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newWorkerHeartbeatEnv(t, tc.enabled, time.Minute)
			heartbeat := workerHeartbeatForGroup("worker", tc.groupKey)
			sendWorkerHeartbeats(t, env, heartbeat)

			// Workers are still tracked in the in-memory registry in both cases.
			_, err := env.FrontendClient().DescribeWorker(t.Context(), &workflowservice.DescribeWorkerRequest{
				Namespace: env.Namespace().String(), WorkerInstanceKey: heartbeat.WorkerInstanceKey,
			})
			require.NoError(t, err)
			_, err = env.AdminClient().DescribeMutableState(t.Context(), &adminservice.DescribeMutableStateRequest{
				Namespace: env.Namespace().String(), Execution: &commonpb.WorkflowExecution{WorkflowId: tc.businessID}, Archetype: chasmworker.Archetype,
			})
			var notFound *serviceerror.NotFound
			require.ErrorAs(t, err, &notFound)
		})
	}
}

func newWorkerHeartbeatEnv(t *testing.T, enabled bool, heartbeatTimeout time.Duration) *testcore.TestEnv {
	t.Helper()
	return testcore.NewEnv(t,
		testcore.WithDedicatedCluster(),
		testcore.WithWorkerService("delete namespace workflow"),
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.WorkerHeartbeatsEnabled, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableDurableWorkerHeartbeats, enabled),
		testcore.WithDynamicConfig(dynamicconfig.MinWorkerHeartbeatTimeout, heartbeatTimeout),
		testcore.WithDynamicConfig(dynamicconfig.TimerProcessorMaxPollInterval, 100*time.Millisecond),
		testcore.WithDynamicConfig(dynamicconfig.TimerProcessorMaxPollIntervalJitterCoefficient, 0.0),
	)
}

func workerHeartbeatForGroup(key, groupKey string) *workerpb.WorkerHeartbeat {
	return &workerpb.WorkerHeartbeat{WorkerInstanceKey: key, TaskQueue: "worker-heartbeats", HostInfo: &workerpb.WorkerHostInfo{WorkerGroupingKey: groupKey}}
}

func sendWorkerHeartbeats(t *testing.T, env *testcore.TestEnv, heartbeats ...*workerpb.WorkerHeartbeat) {
	t.Helper()
	_, err := env.FrontendClient().RecordWorkerHeartbeat(t.Context(), &workflowservice.RecordWorkerHeartbeatRequest{
		Namespace: env.Namespace().String(), WorkerHeartbeat: heartbeats,
	})
	require.NoError(t, err)
}

func fetchWorkerGroup(ctx context.Context, env *testcore.TestEnv, key, runID string) (*chasmworker.WorkerGroup, chasm.Context, error) {
	response, err := env.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: env.Namespace().String(), Execution: &commonpb.WorkflowExecution{WorkflowId: key, RunId: runID}, Archetype: chasmworker.Archetype,
	})
	if err != nil {
		return nil, nil, err
	}
	registry, err := all.NewNilRegistry(log.NewNoopLogger())
	if err != nil {
		return nil, nil, err
	}
	return chasm.NewDetachedExecution[*chasmworker.WorkerGroup](ctx, response.GetDatabaseMutableState(), registry)
}
