package replication

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSenderLaneRegistryPersistsLogicalKeyWithScope(t *testing.T) {
	registry, err := newSenderLaneRegistry(100, nil, 4)
	require.NoError(t, err)
	laneB, created, err := registry.Create("namespace:b", namespaceLaneScope("b", 20), 1)
	require.NoError(t, err)
	require.True(t, created)
	laneA, created, err := registry.Create("namespace:a", namespaceLaneScope("a", 10), 2)
	require.NoError(t, err)
	require.True(t, created)
	registry.ObserveAcks(map[string]*replicationspb.ReplicationState{
		laneA.id: {InclusiveLowWatermark: 15},
		laneB.id: {InclusiveLowWatermark: 25},
	})

	state := registry.BuildReaderState(syncReplicationState(100, 120, 200))
	require.Equal(t, int64(15), state.Scopes[readerOverallScopeIndex].Range.InclusiveMin.TaskId)
	require.Equal(t, int64(15), state.Scopes[readerHighPriorityScopeIndex].Range.InclusiveMin.TaskId)
	require.Equal(t, int64(120), state.GetReplicationLaneDefaultCursor().GetTaskId())
	require.Len(t, state.Lanes, 2)
	require.Equal(t, "namespace:a", state.Lanes[0].LogicalKey)
	require.Equal(t, int64(15), state.Lanes[0].Scope.Range.InclusiveMin.TaskId)
	require.Equal(t, int32(2), state.Lanes[0].ServiceClass)
	require.Equal(t, "namespace:b", state.Lanes[1].LogicalKey)
	require.Equal(t, int64(25), state.Lanes[1].Scope.Range.InclusiveMin.TaskId)
	require.Equal(t, int32(1), state.Lanes[1].ServiceClass)

	restored, err := newSenderLaneRegistry(replicationLaneDefaultCursor(state), state.Lanes, 4)
	require.NoError(t, err)
	require.Equal(t, int64(120), restored.DefaultReservedCursor())
	restoredA, ok := restored.SnapshotByKey("namespace:a")
	require.True(t, ok)
	require.Equal(t, int64(15), restoredA.cursor)
	require.Equal(t, replicationLaneClass(2), restoredA.class)
	require.NotEqual(t, laneA.id, restoredA.id)
}

func TestSenderLaneRegistryRestoresClampedServiceClass(t *testing.T) {
	scope := queues.ToPersistenceScope(namespaceLaneScope("a", 10))
	restored, err := newSenderLaneRegistry(100, []*persistencespb.QueueReaderLane{
		{LogicalKey: "namespace:demoted", Scope: scope, ServiceClass: 9},
		{LogicalKey: "namespace:legacy", Scope: scope, ServiceClass: 0},
	}, 4)
	require.NoError(t, err)
	demoted, ok := restored.SnapshotByKey("namespace:demoted")
	require.True(t, ok)
	require.Equal(t, replicationLaneClass(4), demoted.class)
	legacy, ok := restored.SnapshotByKey("namespace:legacy")
	require.True(t, ok)
	require.Equal(t, replicationLaneClass(1), legacy.class)
}

func TestSenderLaneRegistryResumeFloorCarriesAckTime(t *testing.T) {
	registry, err := newSenderLaneRegistry(100, nil, 4)
	require.NoError(t, err)
	lane, _, err := registry.Create("namespace:a", namespaceLaneScope("a", 10), 1)
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
	_, err := newSenderLaneRegistry(0, []*persistencespb.QueueReaderLane{{LogicalKey: "namespace:a"}}, 4)
	require.Error(t, err)
}

func TestSenderLaneRegistryAvoidsActiveLaneIDCollision(t *testing.T) {
	registry, err := newSenderLaneRegistry(100, nil, 4)
	require.NoError(t, err)
	generated := []string{"duplicate", "duplicate", "unique"}
	registry.generateLaneID = func() string {
		id := generated[0]
		generated = generated[1:]
		return id
	}

	laneA, _, err := registry.Create("namespace:a", namespaceLaneScope("a", 10), 1)
	require.NoError(t, err)
	laneB, _, err := registry.Create("namespace:b", namespaceLaneScope("b", 10), 1)
	require.NoError(t, err)
	require.Equal(t, "duplicate", laneA.id)
	require.Equal(t, "unique", laneB.id)
}

func TestSenderLaneRegistryRetirementWaitsForAckAndLease(t *testing.T) {
	registry, err := newSenderLaneRegistry(100, nil, 4)
	require.NoError(t, err)
	lane, _, err := registry.Create("namespace:a", namespaceLaneScope("a", 10), 1)
	require.NoError(t, err)

	leased, _, ok := registry.Acquire(lane)
	require.True(t, ok)
	require.Equal(t, lane.id, leased.id)
	_, _, ok = registry.Acquire(lane)
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
	_, _, ok = registry.Acquire(lane)
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
	registry, err := newSenderLaneRegistry(100, nil, 4)
	require.NoError(t, err)
	lane, _, err := registry.Create("namespace:a", namespaceLaneScope("a", 10), 1)
	require.NoError(t, err)

	_, _, acquired := registry.Acquire(lane)
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
	registry, err := newSenderLaneRegistry(100, nil, 4)
	require.NoError(t, err)

	_, acquired := registry.AcquireDefault(200)
	require.True(t, acquired)
	lane, created, err := registry.Create("namespace:a", namespaceLaneScope("a", 100), 1)
	require.NoError(t, err)
	require.True(t, created)
	require.Empty(t, registry.ClassSnapshots(1))
	_, _, acquired = registry.Acquire(lane)
	require.False(t, acquired)

	_, acquired = registry.AcquireDefault(300)
	require.False(t, acquired)
	require.Equal(t, []replicationLaneClass{1}, registry.ReleaseDefault(200, true))

	lanes := registry.ClassSnapshots(1)
	require.Len(t, lanes, 1)
	require.Equal(t, lane.id, lanes[0].id)
	require.Equal(t, int64(200), lanes[0].cursor)
}

func TestSenderLaneRegistryRecoversFailedDefaultLeaseBeforeHandoff(t *testing.T) {
	registry, err := newSenderLaneRegistry(100, nil, 4)
	require.NoError(t, err)
	_, _, err = registry.Create("namespace:a", namespaceLaneScope("a", 100), 1)
	require.NoError(t, err)

	_, acquired := registry.AcquireDefault(200)
	require.True(t, acquired)
	lane, created, err := registry.Create("namespace:b", namespaceLaneScope("b", 100), 1)
	require.NoError(t, err)
	require.True(t, created)
	require.Empty(t, registry.ReleaseDefault(200, false))

	_, _, acquired = registry.Acquire(lane)
	require.False(t, acquired)
	belongsToDefaultLane, acquired := registry.AcquireDefault(300)
	require.True(t, acquired)
	require.NotNil(t, belongsToDefaultLane)
	require.False(t, belongsToDefaultLane(&tasks.HistoryReplicationTask{
		WorkflowKey: definition.NewWorkflowKey("a", "workflow-a", "run-a"),
		TaskID:      100,
	}))
	require.True(t, belongsToDefaultLane(&tasks.HistoryReplicationTask{
		WorkflowKey: definition.NewWorkflowKey("b", "workflow-b", "run-b"),
		TaskID:      100,
	}))
	require.Empty(t, registry.ReleaseDefault(300, false))

	belongsToDefaultLane, acquired = registry.AcquireDefault(400)
	require.True(t, acquired)
	require.NotNil(t, belongsToDefaultLane)
	require.Equal(t, []replicationLaneClass{1}, registry.ReleaseDefault(400, true))
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

	registry, err := newSenderLaneRegistry(initialCursor, nil, 4)
	require.NoError(t, err)
	sharedFilter, acquired := registry.AcquireDefault(handoffCursor)
	require.True(t, acquired)
	require.Nil(t, sharedFilter)

	lane, created, err := registry.Create("namespace:a", namespaceLaneScope("a", initialCursor), 1)
	require.NoError(t, err)
	require.True(t, created)
	require.Empty(t, registry.ClassSnapshots(1))
	_, _, acquired = registry.Acquire(lane)
	require.False(t, acquired)
	require.Equal(t, []replicationLaneClass{1}, registry.ReleaseDefault(handoffCursor, true))

	lane, ok := registry.SnapshotByKey(lane.logicalKey)
	require.True(t, ok)
	require.Equal(t, handoffCursor, lane.cursor)
	leasedLane, _, acquired := registry.Acquire(lane)
	require.True(t, acquired)
	require.True(t, leasedLane.scope.Contains(namespaceATask))
	require.False(t, leasedLane.scope.Contains(namespaceBTask))
	registry.AdvanceLaneCursor(lane.id, retireCursor)
	registry.Release(lane.id)
	registry.ObserveAcks(map[string]*replicationspb.ReplicationState{
		lane.id: {InclusiveLowWatermark: handoffCursor},
	})
	durableState := registry.BuildReaderState(syncReplicationState(handoffCursor, handoffCursor, handoffCursor))
	require.Equal(t, handoffCursor, replicationLaneDefaultCursor(durableState))
	require.Len(t, durableState.Lanes, 1)
	require.Equal(t, handoffCursor, durableState.Lanes[0].GetScope().GetRange().GetInclusiveMin().GetTaskId())

	sharedFilter, acquired = registry.AcquireDefault(retireCursor)
	require.True(t, acquired)
	require.NotNil(t, sharedFilter)
	require.False(t, sharedFilter(namespaceATask))
	require.True(t, sharedFilter(namespaceBTask))
	require.Empty(t, registry.ReleaseDefault(retireCursor, true))
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
	restored, err := newSenderLaneRegistry(replicationLaneDefaultCursor(durableState), durableState.Lanes, 4)
	require.NoError(t, err)
	restoredLane, ok := restored.SnapshotByKey(lane.logicalKey)
	require.True(t, ok)
	require.NotEqual(t, lane.id, restoredLane.id)
	require.Equal(t, handoffCursor, restoredLane.cursor)

	sharedFilter, acquired = restored.AcquireDefault(retireCursor)
	require.True(t, acquired)
	require.NotNil(t, sharedFilter)
	require.False(t, sharedFilter(namespaceATask))
	require.True(t, sharedFilter(namespaceBTask))
	leasedLane, _, acquired = restored.Acquire(restoredLane)
	require.True(t, acquired)
	require.Equal(t, handoffCursor, leasedLane.cursor)
	restored.AdvanceLaneCursor(restoredLane.id, retireCursor)
	restored.Release(restoredLane.id)
	require.Empty(t, restored.ReleaseDefault(retireCursor, true))
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
	registry, err := newSenderLaneRegistry(100, nil, 4)
	require.NoError(t, err)
	lane, _, err := registry.Create("namespace:a", namespaceLaneScope("a", 10), 1)
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
