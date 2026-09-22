package replication

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/service/history/queues"
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
	require.Equal(t, int64(15), state.Scopes[0].Range.InclusiveMin.TaskId)
	require.Len(t, state.Lanes, 2)
	require.Equal(t, "namespace:a", state.Lanes[0].LogicalKey)
	require.Equal(t, int64(15), state.Lanes[0].Scope.Range.InclusiveMin.TaskId)
	require.Equal(t, int32(2), state.Lanes[0].ServiceClass)
	require.Equal(t, "namespace:b", state.Lanes[1].LogicalKey)
	require.Equal(t, int64(25), state.Lanes[1].Scope.Range.InclusiveMin.TaskId)
	require.Equal(t, int32(1), state.Lanes[1].ServiceClass)

	restored, err := newSenderLaneRegistry(120, state.Lanes, 4)
	require.NoError(t, err)
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

func TestSenderLaneRegistryRetirementWaitsForAckAndLease(t *testing.T) {
	registry, err := newSenderLaneRegistry(100, nil, 4)
	require.NoError(t, err)
	lane, _, err := registry.Create("namespace:a", namespaceLaneScope("a", 10), 1)
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
	registry, err := newSenderLaneRegistry(100, nil, 4)
	require.NoError(t, err)
	lane, _, err := registry.Create("namespace:a", namespaceLaneScope("a", 10), 1)
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
	registry, err := newSenderLaneRegistry(100, nil, 4)
	require.NoError(t, err)

	_, acquired := registry.AcquireDefault(200)
	require.True(t, acquired)
	lane, created, err := registry.Create("namespace:a", namespaceLaneScope("a", 100), 1)
	require.NoError(t, err)
	require.True(t, created)
	require.Empty(t, registry.ClassSnapshots(1))
	_, acquired = registry.Acquire(lane.id)
	require.False(t, acquired)

	_, acquired = registry.AcquireDefault(300)
	require.False(t, acquired)
	registry.ReleaseDefault(200, true)

	lanes := registry.ClassSnapshots(1)
	require.Len(t, lanes, 1)
	require.Equal(t, lane.id, lanes[0].id)
	require.Equal(t, int64(200), lanes[0].cursor)
}

func TestSenderLaneRegistryCreationDoesNotCrossFailedDefaultLease(t *testing.T) {
	registry, err := newSenderLaneRegistry(100, nil, 4)
	require.NoError(t, err)

	_, acquired := registry.AcquireDefault(200)
	require.True(t, acquired)
	_, created, err := registry.Create("namespace:a", namespaceLaneScope("a", 100), 1)
	require.NoError(t, err)
	require.True(t, created)
	registry.ReleaseDefault(200, false)

	require.Empty(t, registry.ClassSnapshots(1))
	_, acquired = registry.AcquireDefault(300)
	require.False(t, acquired)
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
