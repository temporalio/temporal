package replication

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
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
	// Default must not take the namespace back while its prefix is still unsent.
	unbounded, ok := r.SnapshotByKey("a")
	require.True(t, ok)
	require.Equal(t, int64(math.MaxInt64), unbounded.scope.Range.ExclusiveMax.TaskID)
	_, acquired = r.Acquire(lane.id)
	require.True(t, acquired)
	r.AdvanceLaneCursor(lane.id, 200)
	r.Release(lane.id)
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
			_, acquired = r.Acquire(lane.id)
			require.True(t, acquired)
			r.AdvanceLaneCursor(lane.id, 200)
			r.Release(lane.id)
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

func TestSenderLaneRegistryRetirementWaitsWhileDefaultAdvances(t *testing.T) {
	t.Parallel()
	r, err := newSenderLaneRegistry(100, nil)
	require.NoError(t, err)
	lane, _, err := r.Create("a", namespaceLaneFilter("a"), 100, nil)
	require.NoError(t, err)
	owner, acquired := r.AcquireDefault(200)
	require.True(t, acquired)
	require.Equal(t, "a", owner(retirementTask("a", 199)))
	r.ReleaseDefault(200, true)
	require.True(t, r.RequestRetirement("a"))
	// Each time the lane sends its snapshot, default has already reserved a newer
	// range. Retirement must remain pending rather than exposing its unsent tail.
	for sent := int64(200); sent <= 1000; sent += 100 {
		_, acquired = r.Acquire(lane.id)
		require.True(t, acquired, "an unbounded retiring lane must keep sending")
		owner, acquired = r.AcquireDefault(sent + 100)
		require.True(t, acquired)
		require.Equal(t, "a", owner(retirementTask("a", sent)))
		require.Empty(t, owner(retirementTask("neighbor", sent)))
		r.ReleaseDefault(sent+100, true)
		r.AdvanceLaneCursor(lane.id, sent)
		r.Release(lane.id)
		current, ok := r.SnapshotByID(lane.id)
		require.True(t, ok)
		require.True(t, current.retiring)
		require.Equal(t, int64(math.MaxInt64), current.scope.Range.ExclusiveMax.TaskID)
		require.Empty(t, r.ReadyRetirements())
	}
	// Once the send cursor reaches the reservation, the handoff no longer waits
	// for an ACK round trip. Only final removal waits for ACK >= H.
	_, acquired = r.Acquire(lane.id)
	require.True(t, acquired)
	r.AdvanceLaneCursor(lane.id, 1100)
	r.Release(lane.id)
	current, ok := r.SnapshotByID(lane.id)
	require.True(t, ok)
	require.Equal(t, int64(1100), current.scope.Range.ExclusiveMax.TaskID)
	owner, acquired = r.AcquireDefault(1200)
	require.True(t, acquired)
	require.Empty(t, owner(retirementTask("a", 1100)))
	r.ReleaseDefault(1200, true)
	require.Empty(t, r.ReadyRetirements())
	r.ObserveAcks(map[string]*replicationspb.ReplicationState{lane.id: {InclusiveLowWatermark: 1100}})
	require.Len(t, r.ReadyRetirements(), 1)
}

func TestSenderLaneRegistryRetirementWaitsForResumeBeforeHandoff(t *testing.T) {
	t.Parallel()
	r, err := newSenderLaneRegistry(100, nil)
	require.NoError(t, err)
	lane, _, err := r.Create("a", namespaceLaneFilter("a"), 100, nil)
	require.NoError(t, err)
	_, acquired := r.Acquire(lane.id)
	require.True(t, acquired)
	require.True(t, r.RequestRetirement("a"))
	// A PAUSE arriving before the in-flight send finishes prevents handoff.
	r.ObserveAcks(map[string]*replicationspb.ReplicationState{lane.id: {
		InclusiveLowWatermark: 100,
		FlowControlCommand:    enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE,
	}})
	r.AdvanceLaneCursor(lane.id, 200)
	r.Release(lane.id)
	current, ok := r.SnapshotByID(lane.id)
	require.True(t, ok)
	require.True(t, current.retiring)
	require.Equal(t, int64(math.MaxInt64), current.scope.Range.ExclusiveMax.TaskID)
	owner, acquired := r.AcquireDefault(500)
	require.True(t, acquired)
	require.Equal(t, "a", owner(retirementTask("a", 450)))
	r.ReleaseDefault(500, true)
	require.Empty(t, r.ReadyRetirements())
	_, acquired = r.Acquire(lane.id)
	require.False(t, acquired)
	// RESUME permits catchup, but the unsent tail still belongs to the lane.
	r.ObserveAcks(map[string]*replicationspb.ReplicationState{lane.id: {
		InclusiveLowWatermark: 100,
		FlowControlCommand:    enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME,
	}})
	current, acquired = r.Acquire(lane.id)
	require.True(t, acquired)
	require.Equal(t, int64(math.MaxInt64), current.scope.Range.ExclusiveMax.TaskID)
	r.AdvanceLaneCursor(lane.id, 500)
	r.Release(lane.id)
	current, ok = r.SnapshotByID(lane.id)
	require.True(t, ok)
	require.Equal(t, int64(500), current.scope.Range.ExclusiveMax.TaskID)
	r.ObserveAcks(map[string]*replicationspb.ReplicationState{lane.id: {InclusiveLowWatermark: 500}})
	require.Len(t, r.ReadyRetirements(), 1)
}

func TestSenderLaneRegistryPauseAfterFixedHandoff(t *testing.T) {
	t.Parallel()
	r, err := newSenderLaneRegistry(100, nil)
	require.NoError(t, err)
	lane, _, err := r.Create("a", namespaceLaneFilter("a"), 100, nil)
	require.NoError(t, err)
	require.True(t, r.RequestRetirement("a"))
	r.ObserveAcks(map[string]*replicationspb.ReplicationState{lane.id: {
		InclusiveLowWatermark: 100,
		FlowControlCommand:    enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE,
	}})
	require.Empty(t, r.ReadyRetirements(), "PAUSE prevents starting a retirement marker")
	current, ok := r.SnapshotByID(lane.id)
	require.True(t, ok)
	require.Equal(t, int64(100), current.scope.Range.ExclusiveMax.TaskID)
	owner, acquired := r.AcquireDefault(500)
	require.True(t, acquired)
	require.Empty(t, owner(retirementTask("a", 450)), "an accepted handoff cannot reclaim default ownership")
	r.ReleaseDefault(500, true)
	r.ObserveAcks(map[string]*replicationspb.ReplicationState{lane.id: {
		InclusiveLowWatermark: 100,
		FlowControlCommand:    enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME,
	}})
	require.Len(t, r.ReadyRetirements(), 1)
	// Once the sender has selected the marker, a later PAUSE cannot recall it.
	r.ObserveAcks(map[string]*replicationspb.ReplicationState{lane.id: {
		InclusiveLowWatermark: 100,
		FlowControlCommand:    enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE,
	}})
	_, completed := r.CompleteRetirement(lane.id)
	require.True(t, completed)
}
