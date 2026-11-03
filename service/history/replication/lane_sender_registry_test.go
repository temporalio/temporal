package replication

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSenderLaneRegistryPersistsLogicalKeyWithScope(t *testing.T) {
	registry, err := newSenderLaneRegistry(100, nil)
	require.NoError(t, err)
	laneB, created, err := registry.Create("namespace:b", namespaceLaneFilter("b"), 20, nil)
	require.NoError(t, err)
	require.True(t, created)
	laneA, created, err := registry.Create("namespace:a", namespaceLaneFilter("a"), 10, nil)
	require.NoError(t, err)
	require.True(t, created)
	registry.ObserveAcks(map[string]*replicationspb.ReplicationState{
		laneA.id: {InclusiveLowWatermark: 15},
		laneB.id: {InclusiveLowWatermark: 25},
	})

	state := registry.BuildReaderState(syncReplicationState(100, 120, 200))
	require.Equal(t, int64(15), state.Scopes[laneReaderOverallScopeIndex].Range.InclusiveMin.TaskId)
	require.Equal(t, int64(15), state.Scopes[laneReaderHighScopeIndex].Range.InclusiveMin.TaskId)
	require.Equal(t, int64(120), state.GetReplicationLaneDefaultCursor().GetTaskId())
	require.Len(t, state.Lanes, 2)
	require.Equal(t, "namespace:a", state.Lanes[0].LogicalKey)
	require.Equal(t, int64(15), state.Lanes[0].Scope.Range.InclusiveMin.TaskId)
	require.Equal(t, "namespace:b", state.Lanes[1].LogicalKey)
	require.Equal(t, int64(25), state.Lanes[1].Scope.Range.InclusiveMin.TaskId)

	restored, err := newSenderLaneRegistry(laneDefaultCursor(state), state.Lanes)
	require.NoError(t, err)
	require.Equal(t, int64(120), restored.DefaultReservedCursor())
	restoredA, ok := restored.SnapshotByKey("namespace:a")
	require.True(t, ok)
	require.Equal(t, int64(15), restoredA.cursor)
	require.NotEqual(t, laneA.id, restoredA.id)
}

func TestSenderLaneRegistryPersistsFilterAndPolicyState(t *testing.T) {
	registry, err := newSenderLaneRegistry(100, nil)
	require.NoError(t, err)
	filter := LaneFilter{NamespaceIDs: []namespace.ID{"b", "a"}}
	_, _, err = registry.Create("account:x", filter, 10, []byte("rate=5"))
	require.NoError(t, err)
	registry.SetPolicyState("account:x", []byte("rate=8"))

	state := registry.BuildReaderState(syncReplicationState(100, 120, 200))
	require.Equal(t, []byte("rate=8"), state.Lanes[0].PolicyState)
	restored, err := newSenderLaneRegistry(laneDefaultCursor(state), state.Lanes)
	require.NoError(t, err)
	lane, ok := restored.SnapshotByKey("account:x")
	require.True(t, ok)
	// The restored filter is in namespace ID order.
	require.Equal(t, LaneFilter{NamespaceIDs: []namespace.ID{"a", "b"}}, lane.filter)
	require.Equal(t, []byte("rate=8"), lane.policyState)
}

func TestSenderLaneRegistryResumeFloorCarriesAckTime(t *testing.T) {
	registry, err := newSenderLaneRegistry(100, nil)
	require.NoError(t, err)
	lane, _, err := registry.Create("namespace:a", namespaceLaneFilter("a"), 10, nil)
	require.NoError(t, err)

	// The scope floor binds before any ack: no ack time exists yet.
	floor, floorTime, ok := registry.ResumeFloor()
	require.True(t, ok)
	require.Equal(t, int64(10), floor)
	require.Zero(t, floorTime)

	ackTime := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	registry.ObserveAcks(map[string]*replicationspb.ReplicationState{
		lane.id: {InclusiveLowWatermark: 50, InclusiveLowWatermarkTime: timestamppb.New(ackTime)},
	})
	floor, floorTime, ok = registry.ResumeFloor()
	require.True(t, ok)
	require.Equal(t, int64(50), floor)
	require.Equal(t, ackTime, floorTime)
}

func TestSenderLaneRegistryRejectsInvalidPersistence(t *testing.T) {
	_, err := newSenderLaneRegistry(0, []*persistencespb.QueueReaderLane{{LogicalKey: "namespace:a"}})
	require.Error(t, err)

	// A lane is restored only with a namespace filter.
	universal := queues.NewScope(
		queues.NewRange(tasks.NewImmediateKey(0), tasks.NewImmediateKey(100)),
		predicates.Universal[tasks.Task](),
	)
	_, err = newSenderLaneRegistry(0, []*persistencespb.QueueReaderLane{{
		LogicalKey: "namespace:a",
		Scope:      queues.ToPersistenceScope(universal),
	}})
	require.ErrorContains(t, err, "not a namespace filter")
}

func TestSenderLaneRegistryAvoidsActiveLaneIDCollision(t *testing.T) {
	registry, err := newSenderLaneRegistry(100, nil)
	require.NoError(t, err)
	generated := []string{"duplicate", "duplicate", "unique"}
	registry.generateLaneID = func() string {
		id := generated[0]
		generated = generated[1:]
		return id
	}

	laneA, _, err := registry.Create("namespace:a", namespaceLaneFilter("a"), 10, nil)
	require.NoError(t, err)
	laneB, _, err := registry.Create("namespace:b", namespaceLaneFilter("b"), 10, nil)
	require.NoError(t, err)
	require.Equal(t, "duplicate", laneA.id)
	require.Equal(t, "unique", laneB.id)
}

func TestSenderLaneRegistryPausedLaneDoesNotRetire(t *testing.T) {
	registry, err := newSenderLaneRegistry(100, nil)
	require.NoError(t, err)
	lane, _, err := registry.Create("namespace:a", namespaceLaneFilter("a"), 10, nil)
	require.NoError(t, err)
	changed := registry.Changed()
	registry.ObserveAcks(map[string]*replicationspb.ReplicationState{lane.id: {
		InclusiveLowWatermark: 100,
		FlowControlCommand:    enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE,
	}})
	// A pause wakes the lane's sender.
	<-changed
	require.True(t, registry.Paused("namespace:a"))
	require.False(t, registry.RequestRetirement("namespace:a"))

	// An ack without the lane keeps its pause.
	registry.ObserveAcks(nil)
	require.True(t, registry.Paused("namespace:a"))

	registry.ObserveAcks(map[string]*replicationspb.ReplicationState{lane.id: {
		InclusiveLowWatermark: 100,
		FlowControlCommand:    enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME,
	}})
	require.False(t, registry.Paused("namespace:a"))
	require.True(t, registry.RequestRetirement("namespace:a"))
}

func TestSenderLaneRegistryRetirementWaitsForAckAndLease(t *testing.T) {
	registry, err := newSenderLaneRegistry(100, nil)
	require.NoError(t, err)
	lane, _, err := registry.Create("namespace:a", namespaceLaneFilter("a"), 10, nil)
	require.NoError(t, err)

	leased, ok := registry.Acquire(lane.id)
	require.True(t, ok)
	require.Equal(t, lane.id, leased.id)
	_, ok = registry.Acquire(lane.id)
	require.False(t, ok)
	_, defaultAcquired := registry.AcquireDefault(100)
	require.True(t, defaultAcquired)
	require.False(t, registry.RequestRetirement(lane.logicalKey))
	registry.ObserveAcks(map[string]*replicationspb.ReplicationState{
		lane.id: {InclusiveLowWatermark: 100},
	})
	require.False(t, registry.RequestRetirement(lane.logicalKey))
	registry.Release(lane.id)
	require.True(t, registry.RequestRetirement(lane.logicalKey))
	require.Empty(t, registry.ReadyRetirements())
	_, ok = registry.Acquire(lane.id)
	require.False(t, ok)
	_, defaultAcquired = registry.AcquireDefault(110)
	require.False(t, defaultAcquired)

	registry.ReleaseDefault(100, true)
	require.Equal(t, lane.id, registry.ReadyRetirements()[0].id)
	retired, ok := registry.CompleteRetirement(lane.id)
	require.True(t, ok)
	require.Equal(t, lane.logicalKey, retired.logicalKey)
	_, ok = registry.SnapshotByKey(lane.logicalKey)
	require.False(t, ok)
}

func TestSenderLaneRegistryRetirementWaitsForInFlightLaneAck(t *testing.T) {
	registry, err := newSenderLaneRegistry(100, nil)
	require.NoError(t, err)
	lane, _, err := registry.Create("namespace:a", namespaceLaneFilter("a"), 10, nil)
	require.NoError(t, err)

	_, acquired := registry.Acquire(lane.id)
	require.True(t, acquired)
	registry.ObserveAcks(map[string]*replicationspb.ReplicationState{
		lane.id: {InclusiveLowWatermark: 100},
	})
	require.False(t, registry.RequestRetirement(lane.logicalKey))

	registry.AdvanceLaneCursor(lane.id, 200)
	registry.Release(lane.id)
	require.False(t, registry.RequestRetirement(lane.logicalKey))

	registry.ObserveAcks(map[string]*replicationspb.ReplicationState{
		lane.id: {InclusiveLowWatermark: 200},
	})
	require.True(t, registry.RequestRetirement(lane.logicalKey))
}

func TestSenderLaneRegistryCreationWaitsForDefaultLease(t *testing.T) {
	registry, err := newSenderLaneRegistry(100, nil)
	require.NoError(t, err)

	_, acquired := registry.AcquireDefault(200)
	require.True(t, acquired)
	lane, created, err := registry.Create("namespace:a", namespaceLaneFilter("a"), 100, nil)
	require.NoError(t, err)
	require.True(t, created)
	_, acquired = registry.Acquire(lane.id)
	require.False(t, acquired)

	_, acquired = registry.AcquireDefault(300)
	require.False(t, acquired)
	registry.ReleaseDefault(200, true)

	lanes := registry.Snapshots()
	require.Len(t, lanes, 1)
	require.Equal(t, lane.id, lanes[0].id)
	require.Equal(t, int64(200), lanes[0].cursor)
}

func TestSenderLaneRegistryRecoversFailedDefaultLeaseBeforeHandoff(t *testing.T) {
	registry, err := newSenderLaneRegistry(100, nil)
	require.NoError(t, err)
	_, _, err = registry.Create("namespace:a", namespaceLaneFilter("a"), 100, nil)
	require.NoError(t, err)

	_, acquired := registry.AcquireDefault(200)
	require.True(t, acquired)
	lane, created, err := registry.Create("namespace:b", namespaceLaneFilter("b"), 100, nil)
	require.NoError(t, err)
	require.True(t, created)
	registry.ReleaseDefault(200, false)

	_, acquired = registry.Acquire(lane.id)
	require.False(t, acquired)
	laneOwner, acquired := registry.AcquireDefault(300)
	require.True(t, acquired)
	require.NotNil(t, laneOwner)
	require.Equal(t, "namespace:a", laneOwner(&tasks.HistoryReplicationTask{
		WorkflowKey: definition.NewWorkflowKey("a", "workflow-a", "run-a"),
		TaskID:      100,
	}))
	require.Empty(t, laneOwner(&tasks.HistoryReplicationTask{
		WorkflowKey: definition.NewWorkflowKey("b", "workflow-b", "run-b"),
		TaskID:      100,
	}))
	registry.ReleaseDefault(300, false)

	laneOwner, acquired = registry.AcquireDefault(400)
	require.True(t, acquired)
	require.NotNil(t, laneOwner)
	registry.ReleaseDefault(400, true)
	recovered, ok := registry.SnapshotByKey("namespace:b")
	require.True(t, ok)
	require.Equal(t, lane.id, recovered.id)
	require.Equal(t, int64(400), recovered.cursor)
}

func TestSenderLaneRegistryHandoffSurvivesCrashBeforeRetirementPersistence(t *testing.T) {
	const (
		initialCursor = int64(100)
		handoffCursor = int64(200)
		retireCursor  = int64(300)
	)
	namespaceATask := &tasks.HistoryReplicationTask{
		WorkflowKey: definition.NewWorkflowKey("a", "workflow-a", "run-a"),
		TaskID:      handoffCursor,
	}
	namespaceBTask := &tasks.HistoryReplicationTask{
		WorkflowKey: definition.NewWorkflowKey("b", "workflow-b", "run-b"),
		TaskID:      handoffCursor,
	}

	registry, err := newSenderLaneRegistry(initialCursor, nil)
	require.NoError(t, err)
	sharedFilter, acquired := registry.AcquireDefault(handoffCursor)
	require.True(t, acquired)
	require.Nil(t, sharedFilter)

	lane, created, err := registry.Create("namespace:a", namespaceLaneFilter("a"), initialCursor, nil)
	require.NoError(t, err)
	require.True(t, created)
	_, acquired = registry.Acquire(lane.id)
	require.False(t, acquired)
	registry.ReleaseDefault(handoffCursor, true)

	lane, ok := registry.SnapshotByKey(lane.logicalKey)
	require.True(t, ok)
	require.Equal(t, handoffCursor, lane.cursor)
	leasedLane, acquired := registry.Acquire(lane.id)
	require.True(t, acquired)
	require.True(t, leasedLane.scope.Contains(namespaceATask))
	require.False(t, leasedLane.scope.Contains(namespaceBTask))
	registry.AdvanceLaneCursor(lane.id, retireCursor)
	registry.Release(lane.id)
	registry.ObserveAcks(map[string]*replicationspb.ReplicationState{
		lane.id: {InclusiveLowWatermark: handoffCursor},
	})
	durableState := registry.BuildReaderState(syncReplicationState(handoffCursor, handoffCursor, handoffCursor))
	require.Equal(t, handoffCursor, laneDefaultCursor(durableState))
	require.Len(t, durableState.Lanes, 1)
	require.Equal(t, handoffCursor, durableState.Lanes[0].GetScope().GetRange().GetInclusiveMin().GetTaskId())

	sharedFilter, acquired = registry.AcquireDefault(retireCursor)
	require.True(t, acquired)
	require.NotNil(t, sharedFilter)
	require.Equal(t, "namespace:a", sharedFilter(namespaceATask))
	require.Empty(t, sharedFilter(namespaceBTask))
	registry.ReleaseDefault(retireCursor, true)
	registry.ObserveAcks(map[string]*replicationspb.ReplicationState{
		lane.id: {InclusiveLowWatermark: retireCursor},
	})
	require.True(t, registry.RequestRetirement(lane.logicalKey))
	require.Equal(t, lane.id, registry.ReadyRetirements()[0].id)
	_, completed := registry.CompleteRetirement(lane.id)
	require.True(t, completed)
	sharedFilter, acquired = registry.AcquireDefault(retireCursor + 1)
	require.True(t, acquired)
	require.Nil(t, sharedFilter)
	registry.ReleaseDefault(retireCursor+1, true)

	// The retirement marker is sent before the lane-free reader state is durable.
	// Restarting from the previous state must restore the lane and safely resend its range.
	restored, err := newSenderLaneRegistry(laneDefaultCursor(durableState), durableState.Lanes)
	require.NoError(t, err)
	restoredLane, ok := restored.SnapshotByKey(lane.logicalKey)
	require.True(t, ok)
	require.NotEqual(t, lane.id, restoredLane.id)
	require.Equal(t, handoffCursor, restoredLane.cursor)

	sharedFilter, acquired = restored.AcquireDefault(retireCursor)
	require.True(t, acquired)
	require.NotNil(t, sharedFilter)
	require.Equal(t, "namespace:a", sharedFilter(namespaceATask))
	require.Empty(t, sharedFilter(namespaceBTask))
	leasedLane, acquired = restored.Acquire(restoredLane.id)
	require.True(t, acquired)
	require.Equal(t, handoffCursor, leasedLane.cursor)
	restored.AdvanceLaneCursor(restoredLane.id, retireCursor)
	restored.Release(restoredLane.id)
	restored.ReleaseDefault(retireCursor, true)
	restored.ObserveAcks(map[string]*replicationspb.ReplicationState{
		restoredLane.id: {InclusiveLowWatermark: retireCursor},
	})
	require.True(t, restored.RequestRetirement(restoredLane.logicalKey))
	require.Equal(t, restoredLane.id, restored.ReadyRetirements()[0].id)
	_, completed = restored.CompleteRetirement(restoredLane.id)
	require.True(t, completed)
	sharedFilter, acquired = restored.AcquireDefault(retireCursor + 1)
	require.True(t, acquired)
	require.Nil(t, sharedFilter)
	restored.ReleaseDefault(retireCursor+1, true)
}

func TestSenderLaneRegistryAckNeverRewinds(t *testing.T) {
	registry, err := newSenderLaneRegistry(100, nil)
	require.NoError(t, err)
	lane, _, err := registry.Create("namespace:a", namespaceLaneFilter("a"), 10, nil)
	require.NoError(t, err)
	registry.ObserveAcks(map[string]*replicationspb.ReplicationState{lane.id: {InclusiveLowWatermark: 80}})
	registry.ObserveAcks(map[string]*replicationspb.ReplicationState{lane.id: {InclusiveLowWatermark: 40}})
	got, ok := registry.SnapshotByKey(lane.logicalKey)
	require.True(t, ok)
	require.Equal(t, int64(80), got.acked)
}

func syncReplicationState(overall, high, low int64) *replicationspb.SyncReplicationState {
	return &replicationspb.SyncReplicationState{
		InclusiveLowWatermark: overall,
		HighPriorityState: &replicationspb.ReplicationState{
			InclusiveLowWatermark:     high,
			InclusiveLowWatermarkTime: timestamppb.Now(),
		},
		LowPriorityState: &replicationspb.ReplicationState{
			InclusiveLowWatermark:     low,
			InclusiveLowWatermarkTime: timestamppb.Now(),
		},
	}
}
