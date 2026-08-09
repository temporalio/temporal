package main

import (
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
)

func TestWriteHistoryProducesReplayCompatibleGzipJSON(t *testing.T) {
	history := &historypb.History{Events: []*historypb.HistoryEvent{{
		EventId:   1,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{},
		},
	}}}
	path := filepath.Join(t.TempDir(), "history.json.gz")
	require.NoError(t, writeHistory(path, history))

	f, err := os.Open(path)
	require.NoError(t, err)
	gz, err := gzip.NewReader(f)
	require.NoError(t, err)
	decoded, err := client.HistoryFromJSON(gz, client.HistoryJSONOptions{})
	require.NoError(t, err)
	require.NoError(t, gz.Close())
	require.NoError(t, f.Close())
	require.Equal(t, history, decoded)
}

func TestSafePathComponent(t *testing.T) {
	component := safePathComponent("../team/schedule")
	require.NotContains(t, component, "/")
	require.NotContains(t, component, "..")
	require.Equal(t, component, safePathComponent("../team/schedule"))
	require.NotEqual(t, component, safePathComponent("../team_schedule"))
	require.LessOrEqual(t, len(safePathComponent(strings.Repeat("a", 300))), 93)
}

func TestParseFlagsValidatesBatchOptions(t *testing.T) {
	opts, err := parseFlags([]string{"-batch", "-all-namespaces", "-sample-size", "5", "-history-dir", t.TempDir()})
	require.NoError(t, err)
	require.True(t, opts.batch)
	require.True(t, opts.allNamespaces)
	require.Equal(t, 5, opts.sampleSize)

	_, err = parseFlags([]string{"-batch", "-sample-size", "0"})
	require.EqualError(t, err, "-sample-size must be greater than zero")
	_, err = parseFlags([]string{"-migrate"})
	require.ErrorContains(t, err, "flag provided but not defined: -migrate")
}

func TestReplayExistingScheduleHistories(t *testing.T) {
	paths, err := filepath.Glob("../../../service/worker/scheduler/testdata/replay_*.json.gz")
	require.NoError(t, err)
	require.NotEmpty(t, paths)
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			f, err := os.Open(path)
			require.NoError(t, err)
			gz, err := gzip.NewReader(f)
			require.NoError(t, err)
			history, err := client.HistoryFromJSON(gz, client.HistoryJSONOptions{})
			require.NoError(t, err)
			require.NoError(t, gz.Close())
			require.NoError(t, f.Close())
			require.NoError(t, replayHistory(history))
		})
	}
}
