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

// TestWorkerHeartbeatLifecycle exercises Frontend → Matching → History and reads
// persisted state through the admin API, rather than accessing the live CHASM engine.
func TestWorkerHeartbeatLifecycle(t *testing.T) {
	t.Parallel()
	env := newWorkerHeartbeatEnv(t, true, time.Minute)
	groupKey := env.Tv().Any().String()
	otherGroupKey := env.Tv().Any().String()
	first := workerHeartbeatForGroup(env.Tv().Any().String(), groupKey)
	second := workerHeartbeatForGroup(env.Tv().Any().String(), groupKey)
	other := workerHeartbeatForGroup(env.Tv().Any().String(), otherGroupKey)

	// Workers in one batch are persisted under their respective grouping keys.
	sendWorkerHeartbeats(t, env, first, second, other)
	group, ctx := readWorkerGroup(t, env, groupKey, "")
	require.Len(t, group.Workers, 2)
	require.Equal(t, first.WorkerInstanceKey, group.Workers[first.WorkerInstanceKey].Get(ctx).WorkerHeartbeat.WorkerInstanceKey)
	otherGroup, _ := readWorkerGroup(t, env, otherGroupKey, "")
	require.Len(t, otherGroup.Workers, 1)
	firstRunID := ctx.ExecutionKey().RunID
	firstDeadline := group.LeaseExpirationTime.AsTime()

	// A later heartbeat updates metadata and renews the same group run. Omitted
	// workers remain registered under the shared lease.
	first.WorkerIdentity = "updated-worker"
	sendWorkerHeartbeats(t, env, first)
	group, ctx = readWorkerGroup(t, env, groupKey, "")
	require.Equal(t, firstRunID, ctx.ExecutionKey().RunID)
	require.Len(t, group.Workers, 2)
	require.Equal(t, first.WorkerIdentity, group.Workers[first.WorkerInstanceKey].Get(ctx).WorkerHeartbeat.WorkerIdentity)
	require.True(t, group.LeaseExpirationTime.AsTime().After(firstDeadline))
}

// TestWorkerHeartbeatExpiryAndReconnect verifies that the real expiry task closes
// the run and a later heartbeat starts a fresh run without changing SDK keys.
func TestWorkerHeartbeatExpiryAndReconnect(t *testing.T) {
	t.Parallel()
	env := newWorkerHeartbeatEnv(t, true, 2*time.Second)
	groupKey := env.Tv().Any().String()
	heartbeat := workerHeartbeatForGroup(env.Tv().Any().String(), groupKey)
	sendWorkerHeartbeats(t, env, heartbeat)
	_, ctx := readWorkerGroup(t, env, groupKey, "")
	oldRunID := ctx.ExecutionKey().RunID

	// Stop heartbeats and wait for the persisted run to close.
	await.Require(t.Context(), t, func(t *await.T) {
		group, _, err := fetchWorkerGroup(t.Context(), env, groupKey, oldRunID)
		require.NoError(t, err)
		require.Equal(t, workerstatepb.WORKER_GROUP_STATUS_INACTIVE, group.Status)
	}, 15*time.Second, 100*time.Millisecond)

	// Reuse both SDK keys; only the internal Run ID changes.
	sendWorkerHeartbeats(t, env, heartbeat)
	group, ctx := readWorkerGroup(t, env, groupKey, "")
	require.NotEqual(t, oldRunID, ctx.ExecutionKey().RunID)
	require.Equal(t, workerstatepb.WORKER_GROUP_STATUS_ACTIVE, group.Status)
	require.Contains(t, group.Workers, heartbeat.WorkerInstanceKey)
	oldGroup, _ := readWorkerGroup(t, env, groupKey, oldRunID)
	require.Equal(t, workerstatepb.WORKER_GROUP_STATUS_INACTIVE, oldGroup.Status)
}

func TestWorkerHeartbeatMetadataOnly(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		enabled  bool
		groupKey string
	}{
		{name: "durable tracking disabled", groupKey: "group"},
		{name: "legacy SDK without grouping key", enabled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newWorkerHeartbeatEnv(t, tc.enabled, time.Minute)
			heartbeat := workerHeartbeatForGroup(env.Tv().Any().String(), tc.groupKey)
			sendWorkerHeartbeats(t, env, heartbeat)

			// The existing metadata registry continues working in both cases.
			_, err := env.FrontendClient().DescribeWorker(t.Context(), &workflowservice.DescribeWorkerRequest{
				Namespace: env.Namespace().String(), WorkerInstanceKey: heartbeat.WorkerInstanceKey,
			})
			require.NoError(t, err)
			businessID := tc.groupKey
			if businessID == "" {
				businessID = heartbeat.WorkerInstanceKey
			}
			_, err = env.AdminClient().DescribeMutableState(t.Context(), &adminservice.DescribeMutableStateRequest{
				Namespace: env.Namespace().String(), Execution: &commonpb.WorkflowExecution{WorkflowId: businessID}, Archetype: chasmworker.Archetype,
			})
			var notFound *serviceerror.NotFound
			require.ErrorAs(t, err, &notFound)
		})
	}
}

func newWorkerHeartbeatEnv(t *testing.T, enabled bool, lease time.Duration) *testcore.TestEnv {
	t.Helper()
	return testcore.NewEnv(t,
		testcore.WithDedicatedCluster(),
		testcore.WithWorkerService("delete namespace workflow"),
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.WorkerHeartbeatsEnabled, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableWorkerStateTracking, enabled),
		testcore.WithDynamicConfig(dynamicconfig.WorkerStateTrackingLeaseDuration, lease),
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

func readWorkerGroup(t testing.TB, env *testcore.TestEnv, key, runID string) (*chasmworker.WorkerGroup, chasm.Context) {
	t.Helper()
	group, ctx, err := fetchWorkerGroup(t.Context(), env, key, runID)
	require.NoError(t, err)
	return group, ctx
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
