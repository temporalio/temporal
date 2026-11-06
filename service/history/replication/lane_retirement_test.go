package replication

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/service/history/tasks"
)

func retirementTask(namespaceID string, id int64) tasks.Task {
	return &tasks.HistoryReplicationTask{
		WorkflowKey: definition.NewWorkflowKey(namespaceID, "workflow", "run"),
		TaskID:      id,
	}
}

func TestSenderLaneRegistryRetirementHasFixedBoundary(t *testing.T) {
	t.Parallel()
	r, err := newSenderLaneRegistry(100, nil)
	require.NoError(t, err)
	lane, _, err := r.Create("a", namespaceLaneFilter("a"), 50, nil)
	require.NoError(t, err)
	oldOwner, acquired := r.AcquireDefault(200)
	require.True(t, acquired)
	require.True(t, r.RequestRetirement("a"))
	bounded, ok := r.SnapshotByKey("a")
	require.True(t, ok)
	require.True(t, bounded.retiring)
	require.Equal(t, int64(200), bounded.scope.Range.ExclusiveMax.TaskID)
	// Already reserved default ranges keep their ownership, while newer ranges take
	// the namespace's suffix. The old default lease deliberately remains in flight.
	require.Equal(t, "a", oldOwner(retirementTask("a", 199)))
	for end := int64(300); end <= 10_000; end += 100 {
		owner, acquired := r.AcquireDefault(end)
		require.True(t, acquired)
		require.Equal(t, "a", owner(retirementTask("a", 199)))
		require.Empty(t, owner(retirementTask("a", 200)))
		require.Empty(t, owner(retirementTask("other", 199)))
		r.ReleaseDefault(end, true)
	}
	_, acquired = r.Acquire(lane.id)
	require.True(t, acquired)
	r.AdvanceLaneCursor(lane.id, 200)
	r.Release(lane.id)
	r.ObserveAcks(map[string]*replicationspb.ReplicationState{lane.id: {InclusiveLowWatermark: 150}})
	require.Empty(t, r.ReadyRetirements())
	state := r.BuildReaderState(syncReplicationState(10_000, 10_000, 10_000))
	require.Equal(t, int64(150), state.Scopes[laneReaderOverallScopeIndex].Range.InclusiveMin.TaskId)
	require.Equal(t, int64(150), state.Scopes[laneReaderHighScopeIndex].Range.InclusiveMin.TaskId)
	require.Equal(t, int64(200), state.Lanes[0].Scope.Range.ExclusiveMax.TaskId)
	// A delayed ACK reaches the fixed end even though the shard keeps advancing.
	r.ObserveAcks(map[string]*replicationspb.ReplicationState{lane.id: {InclusiveLowWatermark: 200}})
	require.Len(t, r.ReadyRetirements(), 1)
	_, completed := r.CompleteRetirement(lane.id)
	require.True(t, completed)
	r.ReleaseDefault(200, true)
	state = r.BuildReaderState(syncReplicationState(10_000, 10_000, 10_000))
	require.Empty(t, state.Lanes)
	require.Equal(t, int64(10_000), state.Scopes[laneReaderOverallScopeIndex].Range.InclusiveMin.TaskId)
}

func TestSenderLaneRegistryRetirementIncludesInFlightSend(t *testing.T) {
	t.Parallel()
	r, err := newSenderLaneRegistry(100, nil)
	require.NoError(t, err)
	lane, _, err := r.Create("a", namespaceLaneFilter("a"), 10, nil)
	require.NoError(t, err)
	_, acquired := r.Acquire(lane.id)
	require.True(t, acquired)
	require.True(t, r.RequestRetirement("a"))
	unbounded, _ := r.SnapshotByKey("a")
	require.Equal(t, int64(math.MaxInt64), unbounded.scope.Range.ExclusiveMax.TaskID)
	_, acquired = r.AcquireDefault(300)
	require.True(t, acquired)
	r.ReleaseDefault(300, true)
	// The lane's in-flight range can end beyond the default reservation.
	r.AdvanceLaneCursor(lane.id, 400)
	r.Release(lane.id)
	bounded, _ := r.SnapshotByKey("a")
	require.Equal(t, int64(400), bounded.scope.Range.ExclusiveMax.TaskID)
	owner, acquired := r.AcquireDefault(500)
	require.True(t, acquired)
	require.Equal(t, "a", owner(retirementTask("a", 399)))
	require.Empty(t, owner(retirementTask("a", 400)))
	r.ReleaseDefault(500, true)
}

func TestSenderLaneRegistryRestoresFiniteRetirement(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		resume int64
	}{{"PrefixPending", 150}, {"PrefixAlreadyAcknowledged", 200}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			resume := tc.resume
			r, err := newSenderLaneRegistry(100, nil)
			require.NoError(t, err)
			lane, _, err := r.Create("a", namespaceLaneFilter("a"), 100, nil)
			require.NoError(t, err)
			_, acquired := r.AcquireDefault(200)
			require.True(t, acquired)
			r.ReleaseDefault(200, true)
			require.True(t, r.RequestRetirement("a"))
			r.ObserveAcks(map[string]*replicationspb.ReplicationState{lane.id: {InclusiveLowWatermark: resume}})
			state := r.BuildReaderState(syncReplicationState(350, 350, 350))
			restored, err := newSenderLaneRegistry(laneDefaultCursor(state), state.Lanes)
			require.NoError(t, err)
			current, ok := restored.SnapshotByKey("a")
			require.True(t, ok)
			require.NotEqual(t, lane.id, current.id)
			require.True(t, current.retiring)
			require.Equal(t, resume, current.cursor)
			require.Equal(t, int64(200), current.scope.Range.ExclusiveMax.TaskID)
			owner, acquired := restored.AcquireDefault(500)
			require.True(t, acquired)
			require.Empty(t, owner(retirementTask("a", 200)))
			// Even an empty restored prefix needs its new receiver-local lane ACK.
			_, acquired = restored.Acquire(current.id)
			require.True(t, acquired)
			restored.AdvanceLaneCursor(current.id, 200)
			restored.Release(current.id)
			restored.ObserveAcks(map[string]*replicationspb.ReplicationState{current.id: {InclusiveLowWatermark: 500}})
			state = restored.BuildReaderState(syncReplicationState(500, 500, 500))
			require.Equal(t, int64(200), state.Lanes[0].Scope.Range.InclusiveMin.TaskId)
			require.Equal(t, int64(200), state.Lanes[0].Scope.Range.ExclusiveMax.TaskId)
			require.Len(t, restored.ReadyRetirements(), 1)
			restored.ReleaseDefault(500, true)
		})
	}
}

func TestSenderLaneRegistryRetirementWaitsForPendingHandoff(t *testing.T) {
	t.Parallel()
	r, err := newSenderLaneRegistry(100, nil)
	require.NoError(t, err)
	_, acquired := r.AcquireDefault(200)
	require.True(t, acquired)
	lane, _, err := r.Create("a", namespaceLaneFilter("a"), 100, nil)
	require.NoError(t, err)
	require.True(t, r.RequestRetirement("a"))
	require.Empty(t, r.ReadyRetirements())
	r.ReleaseDefault(200, true)
	current, acquired := r.Acquire(lane.id)
	require.True(t, acquired)
	require.True(t, current.retiring)
	require.Equal(t, int64(200), current.cursor)
	require.Equal(t, int64(200), current.scope.Range.ExclusiveMax.TaskID)
	r.Release(lane.id)
	r.ObserveAcks(map[string]*replicationspb.ReplicationState{lane.id: {InclusiveLowWatermark: 200}})
	require.Len(t, r.ReadyRetirements(), 1)
}
