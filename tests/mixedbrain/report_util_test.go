package mixedbrain

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMixedBrainReportMarkdownComplete(t *testing.T) {
	started := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	events := &chaosEvents{}
	events.append(processChaosEvent{
		Target:      "release",
		StartedAt:   started,
		RestartedAt: started.Add(time.Second),
		ReformedAt:  started.Add(2 * time.Second),
	})
	report := mixedBrainReport{
		StartedAt:      started,
		FinishedAt:     started.Add(5 * time.Minute),
		Passed:         true,
		CurrentVersion: "1.32.0",
		ReleaseVersion: "v1.31.2",
		Scenarios:      []string{"throughput_stress", "scheduler_stress"},
		ChaosInterval:  time.Minute,
		ChaosEvents:    events,
		ProxyCounts:    map[string]int64{"current": 12, "release": 10},
	}

	markdown := report.markdown()
	for _, expected := range []string{
		"Result: **PASSED**",
		"Duration: 5m0s",
		"Current version: 1.32.0",
		"Previous release: v1.31.2",
		"throughput_stress, scheduler_stress",
		"Process restarts: 1",
		"current=12, release=10",
		"| release |",
	} {
		require.Contains(t, markdown, expected)
	}
}

func TestMixedBrainReportMarkdownPartial(t *testing.T) {
	started := time.Now()
	markdown := (&mixedBrainReport{
		StartedAt:  started,
		FinishedAt: started,
	}).markdown()

	require.Contains(t, markdown, "Result: **FAILED**")
	require.Equal(t, 5, strings.Count(markdown, "unavailable"))
	require.Contains(t, markdown, "Process restarts: 0")
}

func TestBoundedLogTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "server.log")
	require.NoError(t, os.WriteFile(path, []byte("one\ntwo\nthree\nfour\n"), 0644))

	tail, err := boundedLogTail(path, 2, 64)
	require.NoError(t, err)
	require.Equal(t, "three\nfour", tail)
}
