package replication

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/await"
)

func TestSenderLaneTurnCoordinatorReleasesAfterResultDelivery(t *testing.T) {
	registry, err := newSenderLaneRegistry(100, nil, 1)
	require.NoError(t, err)
	lane, _, err := registry.Create("namespace:a", namespaceLaneScope("a", 100), 1)
	require.NoError(t, err)

	runFinished := make(chan struct{})
	wakes := make(chan replicationLaneClass, 1)
	turns := newSenderLaneTurnCoordinator(
		registry,
		func(lane senderLaneSnapshot, _ int64, _ func()) laneSendResult {
			close(runFinished)
			return laneSendResult{laneID: lane.id}
		},
		func(class replicationLaneClass) { wakes <- class },
		nil,
		nil,
	)
	defer turns.Close()

	_, started := turns.Start(lane, 100)
	require.True(t, started)
	await.Rcv(t, runFinished)
	_, acquired := registry.Acquire(lane.id)
	require.False(t, acquired)

	result := await.Rcv(t, turns.Results())
	await.Rcv(t, result.released)
	acquiredLane, acquired := registry.Acquire(lane.id)
	require.True(t, acquired)
	registry.Release(acquiredLane.id)
	require.Empty(t, wakes)
}

func TestSenderLaneTurnCoordinatorAbandonsResultWhenLoopCloses(t *testing.T) {
	registry, err := newSenderLaneRegistry(100, nil, 1)
	require.NoError(t, err)
	lane, _, err := registry.Create("namespace:a", namespaceLaneScope("a", 100), 1)
	require.NoError(t, err)

	runStarted := make(chan struct{})
	releaseRun := make(chan struct{})
	wakes := make(chan replicationLaneClass, 1)
	turns := newSenderLaneTurnCoordinator(
		registry,
		func(lane senderLaneSnapshot, _ int64, _ func()) laneSendResult {
			close(runStarted)
			<-releaseRun
			return laneSendResult{laneID: lane.id}
		},
		func(class replicationLaneClass) { wakes <- class },
		nil,
		nil,
	)
	defer turns.Close()
	runReleased := false
	defer func() {
		if !runReleased {
			close(releaseRun)
		}
	}()

	_, started := turns.Start(lane, 100)
	require.True(t, started)
	await.Rcv(t, runStarted)
	turns.Close()
	close(releaseRun)
	runReleased = true
	require.Equal(t, replicationLaneClass(1), await.Rcv(t, wakes))

	acquiredLane, acquired := registry.Acquire(lane.id)
	require.True(t, acquired)
	registry.Release(acquiredLane.id)
	select {
	case <-turns.Results():
		require.Fail(t, "closed coordinator accepted a result")
	default:
	}
}

func TestConsumeLaneClassWake(t *testing.T) {
	wake := make(chan struct{}, 1)
	wake <- struct{}{}
	require.True(t, consumeLaneClassWake(wake))
	require.False(t, consumeLaneClassWake(wake))
}
