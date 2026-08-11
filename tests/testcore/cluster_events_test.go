package testcore

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/await"
)

func TestClusterEventRecorderTracksClusterLifecycle(t *testing.T) {
	var output bytes.Buffer
	now := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)
	recorder := newClusterEventRecorder(&output)
	recorder.now = func() time.Time { return now }

	clusterID := recorder.nextClusterID()
	recorder.recordClusterCreated(clusterID, clusterCreationEvent{
		suite:      "TestSuite",
		test:       "TestSuite/TestCase",
		kind:       clusterKindDedicated,
		reason:     "custom config",
		worker:     true,
		duration:   125 * time.Millisecond,
		namespaces: 2,
		phases: map[string]time.Duration{
			bootPhasePersistence: 25 * time.Millisecond,
			bootPhaseFxGraph:     80 * time.Millisecond,
		},
	})
	now = now.Add(time.Second)
	recorder.recordClusterAcquire(
		clusterID,
		"TestSuite/TestCase",
		4*time.Millisecond,
		clusterAcquireSourceWarmSpare,
	)
	now = now.Add(time.Second)
	recorder.recordNamespaceRegistered(clusterID, "extra-namespace")
	now = now.Add(time.Second)
	recorder.recordClusterDestroyed(clusterID)

	var events []clusterEvent
	decoder := json.NewDecoder(&output)
	for decoder.More() {
		var event clusterEvent
		require.NoError(t, decoder.Decode(&event))
		events = append(events, event)
	}

	require.Len(t, events, 4)
	require.Equal(t, clusterEvent{
		Type:         clusterEventTypeCreated,
		Timestamp:    time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC),
		ClusterID:    clusterID,
		Suite:        "TestSuite",
		Test:         "TestSuite/TestCase",
		Kind:         clusterKindDedicated,
		Reason:       "custom config",
		Worker:       true,
		DurationMS:   125,
		LiveClusters: 1,
		Namespaces:   2,
		PhasesMS: map[string]float64{
			bootPhasePersistence: 25,
			bootPhaseFxGraph:     80,
		},
	}, events[0])
	require.InDelta(t, 4.0, events[1].AcquireMS, 0.001)
	require.Equal(t, clusterAcquireSourceWarmSpare, events[1].AcquireSource)
	require.Equal(t, 1, events[1].LiveClusters)
	require.Equal(t, clusterEventTypeNamespace, events[2].Type)
	require.Equal(t, "extra-namespace", events[2].Namespace)
	require.Equal(t, 0, events[3].LiveClusters)
}

func TestClusterEventRecorderSamplesRuntime(t *testing.T) {
	var output bytes.Buffer
	recorder := newClusterEventRecorder(&output)
	recorder.recordRuntimeSample(runtimeSample{
		Goroutines:   123,
		HeapInUse:    456,
		Sys:          789,
		RSS:          1011,
		LiveClusters: 3,
	})

	var event clusterEvent
	require.NoError(t, json.NewDecoder(&output).Decode(&event))
	require.Equal(t, clusterEventTypeRuntime, event.Type)
	require.Equal(t, 123, event.Goroutines)
	require.Equal(t, uint64(456), event.HeapInUseBytes)
	require.Equal(t, uint64(789), event.SysBytes)
	require.Equal(t, uint64(1011), event.RSSBytes)
	require.Equal(t, 3, event.LiveClusters)
}

func TestClusterEventRecorderFromEnvironmentOwnsFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	t.Setenv("TEMPORAL_TEST_CLUSTER_EVENTS_FILE", path)

	recorder := newClusterEventRecorderFromEnvironment()
	require.NotNil(t, recorder)
	recorder.recordRunStarted()
	recorder.close()

	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(contents), `"type":"run_started"`)
}

func TestBootPhaseDurationsRecordsConcurrentPhases(t *testing.T) {
	phases := newBootPhaseDurations()
	var waitGroup sync.WaitGroup
	for range 10 {
		waitGroup.Go(func() {
			phases.record(bootPhaseFxGraph, time.Millisecond)
		})
	}
	waitGroup.Wait()

	require.Equal(t, map[string]time.Duration{
		bootPhaseFxGraph: 10 * time.Millisecond,
	}, phases.snapshot())
}

func TestRunTestsRecordsBoundaries(t *testing.T) {
	var output bytes.Buffer
	recorder := newClusterEventRecorder(&output)
	previousRecorder := testClusterRouter.events
	previousPerTest := testClusterRouter.perTest
	testClusterRouter.events = recorder
	testClusterRouter.perTest = nil
	t.Cleanup(func() {
		testClusterRouter.events = previousRecorder
		testClusterRouter.perTest = previousPerTest
	})

	exitCode := RunTests(func() int { return 7 })

	require.Equal(t, 7, exitCode)
	decoder := json.NewDecoder(&output)
	var events []clusterEvent
	for decoder.More() {
		var event clusterEvent
		require.NoError(t, decoder.Decode(&event))
		events = append(events, event)
	}
	require.Len(t, events, 4)
	require.Equal(t, clusterEventTypeRunStarted, events[0].Type)
	require.Equal(t, clusterEventTypeRuntime, events[1].Type)
	require.Positive(t, events[1].Goroutines)
	require.Equal(t, clusterEventTypeRuntime, events[2].Type)
	require.Equal(t, clusterEventTypeRunFinished, events[3].Type)
	require.Equal(t, 7, events[3].ExitCode)
}

func TestRunTestsStopsRuntimeSamplerBeforeFinishing(t *testing.T) {
	var output bytes.Buffer
	recorder := newClusterEventRecorder(&output)
	recorder.samplerInterval = time.Millisecond
	previousRecorder := testClusterRouter.events
	previousPerTest := testClusterRouter.perTest
	testClusterRouter.events = recorder
	testClusterRouter.perTest = nil
	t.Cleanup(func() {
		testClusterRouter.events = previousRecorder
		testClusterRouter.perTest = previousPerTest
	})

	RunTests(func() int {
		await.RequireTrue(t, func() bool {
			recorder.mu.Lock()
			defer recorder.mu.Unlock()
			return bytes.Count(output.Bytes(), []byte(`"type":"runtime"`)) >= 2
		}, time.Second, time.Millisecond)
		return 0
	})

	decoder := json.NewDecoder(&output)
	var events []clusterEvent
	for decoder.More() {
		var event clusterEvent
		require.NoError(t, decoder.Decode(&event))
		events = append(events, event)
	}
	require.Greater(t, len(events), 4)
	require.Equal(t, clusterEventTypeRunStarted, events[0].Type)
	require.Equal(t, clusterEventTypeRunFinished, events[len(events)-1].Type)
	for _, event := range events[1 : len(events)-1] {
		require.Equal(t, clusterEventTypeRuntime, event.Type)
	}
}

func TestRunFinishedRecordsLogicalDestructionOfLiveClusters(t *testing.T) {
	var output bytes.Buffer
	recorder := newClusterEventRecorder(&output)
	clusterID := recorder.nextClusterID()
	recorder.recordClusterCreated(clusterID, clusterCreationEvent{})
	recorder.recordRunFinished(0)

	decoder := json.NewDecoder(&output)
	var events []clusterEvent
	for decoder.More() {
		var event clusterEvent
		require.NoError(t, decoder.Decode(&event))
		events = append(events, event)
	}
	require.Len(t, events, 3)
	require.Equal(t, clusterEventTypeDestroyed, events[1].Type)
	require.Equal(t, clusterID, events[1].ClusterID)
	require.Equal(t, 0, events[1].LiveClusters)
	require.Equal(t, clusterEventTypeRunFinished, events[2].Type)
}
