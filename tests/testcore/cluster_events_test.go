package testcore

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClusterEventRecorderTracksOwnedClusterLifetime(t *testing.T) {
	var output bytes.Buffer
	recorder := newClusterEventRecorder(&output)
	recorder.now = func() time.Time { return time.Unix(1, 0) }

	clusterID := recorder.nextClusterID()
	recorder.recordClusterCreated(clusterID, clusterCreationEvent{
		suite:      "Suite",
		test:       "Suite/Test",
		reason:     "per-test",
		duration:   25 * time.Millisecond,
		namespaces: 2,
	})
	recorder.recordClusterDestroyed(clusterID)

	events := decodeClusterEvents(t, &output)
	require.Len(t, events, 2)
	require.Equal(t, clusterEventTypeCreated, events[0].Type)
	require.Equal(t, 1, events[0].LiveClusters)
	require.Equal(t, "per-test", events[0].Reason)
	require.InDelta(t, 25.0, events[0].DurationMS, 0.001)
	require.Equal(t, clusterEventTypeDestroyed, events[1].Type)
	require.Zero(t, events[1].LiveClusters)
}

func TestClusterEventRecorderWritesRuntimeMemory(t *testing.T) {
	var output bytes.Buffer
	recorder := newClusterEventRecorder(&output)
	recorder.recordRuntimeSample(runtimeSample{
		Goroutines:   10,
		HeapInUse:    20,
		Sys:          30,
		RSS:          40,
		LiveClusters: 2,
	})

	events := decodeClusterEvents(t, &output)
	require.Len(t, events, 1)
	require.Equal(t, clusterEventTypeRuntime, events[0].Type)
	require.Equal(t, 10, events[0].Goroutines)
	require.Equal(t, uint64(20), events[0].HeapInUseBytes)
	require.Equal(t, uint64(40), events[0].RSSBytes)
	require.Equal(t, 2, events[0].LiveClusters)
}

func decodeClusterEvents(t *testing.T, output *bytes.Buffer) []clusterEvent {
	t.Helper()
	decoder := json.NewDecoder(output)
	var events []clusterEvent
	for decoder.More() {
		var event clusterEvent
		require.NoError(t, decoder.Decode(&event))
		events = append(events, event)
	}
	return events
}
