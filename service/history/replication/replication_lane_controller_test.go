package replication

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
)

func TestNamespaceIsolationPolicyReclassifiesAndRetiresThroughRegistry(t *testing.T) {
	registry, err := newSenderLaneRegistry(100, nil, 4)
	require.NoError(t, err)
	controller := newSenderLaneController(
		registry,
		newNamespaceIsolationPolicy(3, 2, 2),
		2,
		log.NewNoopLogger(),
	)

	runnableClasses, err := controller.Reconcile(replicationLanePolicySignals{throttleHighNamespaceIDs: []string{"a", "b", "c"}, sharedHighWatermark: 10}, nil)
	require.NoError(t, err)
	require.Equal(t, []replicationLaneClass{1, 1}, runnableClasses)
	require.Len(t, registry.Snapshots(), 2)
	laneA, ok := registry.SnapshotByKey("namespace:a")
	require.True(t, ok)
	_, ok = registry.SnapshotByKey("namespace:c")
	require.False(t, ok)

	runnableClasses, err = controller.Reconcile(replicationLanePolicySignals{throttleHighNamespaceIDs: []string{"a", "b"}, sharedHighWatermark: 10}, nil)
	require.NoError(t, err)
	require.Equal(t, []replicationLaneClass{2, 2}, runnableClasses)
	laneA, ok = registry.SnapshotByKey("namespace:a")
	require.True(t, ok)
	require.Equal(t, replicationLaneClass(2), laneA.class)

	acks := make(map[string]*replicationspb.ReplicationState)
	for _, lane := range registry.Snapshots() {
		acks[lane.id] = &replicationspb.ReplicationState{InclusiveLowWatermark: 100}
	}
	_, err = controller.Reconcile(replicationLanePolicySignals{sharedHighWatermark: 10}, acks)
	require.NoError(t, err)
	require.Empty(t, registry.ReadyRetirements())
	_, err = controller.Reconcile(replicationLanePolicySignals{sharedHighWatermark: 10}, acks)
	require.NoError(t, err)
	require.Len(t, registry.ReadyRetirements(), 2)

	runnableClasses, err = controller.Reconcile(replicationLanePolicySignals{throttleHighNamespaceIDs: []string{"a"}, sharedHighWatermark: 10}, acks)
	require.NoError(t, err)
	require.Equal(t, []replicationLaneClass{2}, runnableClasses)
	laneA, ok = registry.SnapshotByKey("namespace:a")
	require.True(t, ok)
	require.False(t, laneA.retiring)
}

func TestSenderLaneControllerBoundsTenfoldSignalGrowth(t *testing.T) {
	registry, err := newSenderLaneRegistry(100, nil, 4)
	require.NoError(t, err)
	controller := newSenderLaneController(
		registry,
		newNamespaceIsolationPolicy(4, 3, 3),
		100,
		log.NewNoopLogger(),
	)
	namespaces := make([]string, 1000)
	for i := range namespaces {
		namespaces[i] = fmt.Sprintf("namespace-%d", i)
	}

	_, err = controller.Reconcile(replicationLanePolicySignals{throttleHighNamespaceIDs: namespaces, sharedHighWatermark: 10}, nil)
	require.NoError(t, err)
	require.Len(t, registry.Snapshots(), 100)
	require.Len(t, controller.policy.(*namespaceIsolationPolicy).streaks, 100)
}
