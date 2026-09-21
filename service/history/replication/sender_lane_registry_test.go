package replication

import (
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSenderLaneRegistryPersistsLogicalKeyWithScope(t *testing.T) {
	registry, err := newSenderLaneRegistry(100, nil)
	require.NoError(t, err)
	laneB, created, err := registry.Create("namespace:b", namespaceLaneScope("b", 20), 1)
	require.NoError(t, err)
	require.True(t, created)
	laneA, created, err := registry.Create("namespace:a", namespaceLaneScope("a", 10), 1)
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
	require.Equal(t, "namespace:b", state.Lanes[1].LogicalKey)
	require.Equal(t, int64(25), state.Lanes[1].Scope.Range.InclusiveMin.TaskId)

	restored, err := newSenderLaneRegistry(120, state.Lanes)
	require.NoError(t, err)
	restoredA, ok := restored.SnapshotByKey("namespace:a")
	require.True(t, ok)
	require.Equal(t, int64(15), restoredA.cursor)
	require.NotEqual(t, laneA.id, restoredA.id)
}

func TestSenderLaneRegistryRejectsInvalidPersistence(t *testing.T) {
	_, err := newSenderLaneRegistry(0, []*persistencespb.QueueReaderLane{{LogicalKey: "namespace:a"}})
	require.Error(t, err)
}

func TestSenderLaneRegistryRetirementWaitsForAckAndLease(t *testing.T) {
	registry, err := newSenderLaneRegistry(100, nil)
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
	require.True(t, registry.RequestRetirement(lane.logicalKey))
	require.Empty(t, registry.ReadyRetirements())
	_, ok = registry.Acquire(lane.id)
	require.False(t, ok)
	_, defaultAcquired = registry.AcquireDefault(110)
	require.False(t, defaultAcquired)

	registry.Release(lane.id)
	require.Empty(t, registry.ReadyRetirements())
	registry.ReleaseDefault()
	require.Equal(t, lane.id, registry.ReadyRetirements()[0].id)
	retired, ok := registry.CompleteRetirement(lane.id)
	require.True(t, ok)
	require.Equal(t, lane.logicalKey, retired.logicalKey)
	_, ok = registry.SnapshotByKey(lane.logicalKey)
	require.False(t, ok)
}

func TestSenderLaneRegistryAckNeverRewinds(t *testing.T) {
	registry, err := newSenderLaneRegistry(100, nil)
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
