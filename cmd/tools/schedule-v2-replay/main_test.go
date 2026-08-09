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
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

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
	opts, err := parseFlags([]string{"-batch", "-all-namespaces", "-sample-size", "5", "-history-dir", t.TempDir(), "-acknowledge-sensitive-data"})
	require.NoError(t, err)
	require.True(t, opts.batch)
	require.True(t, opts.allNamespaces)
	require.Equal(t, 5, opts.sampleSize)

	_, err = parseFlags([]string{"-batch", "-sample-size", "0", "-acknowledge-sensitive-data"})
	require.EqualError(t, err, "-sample-size must be greater than zero")
	_, err = parseFlags([]string{"-batch"})
	require.EqualError(t, err, "-acknowledge-sensitive-data is required in batch mode")
	_, err = parseFlags([]string{"-migrate"})
	require.ErrorContains(t, err, "flag provided but not defined: -migrate")

	opts, err = parseFlags([]string{"-generate-scenarios", "-history-dir", t.TempDir()})
	require.NoError(t, err)
	require.True(t, opts.generateScenarios)
	_, err = parseFlags([]string{"-generate-scenarios", "-batch"})
	require.EqualError(t, err, "-batch and -generate-scenarios are mutually exclusive")
}

func TestCollectionManifestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	expected := collectionManifest{
		Version: collectionManifestVersion, Namespace: "payments", Seed: "seed",
		SampleSize: 10, CohortSize: 2, MaxRuns: 3,
		Cases: []collectionCase{{ScheduleID: "schedule", RunID: "run", Status: "collected"}},
	}
	require.NoError(t, writeCollectionManifest(path, expected))
	actual, err := loadCollectionManifest(path)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestValidateManifestFilesDetectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.json.gz")
	checksum, _, err := writeHistorySecure(path, &historypb.History{Events: []*historypb.HistoryEvent{{EventId: 1}}})
	require.NoError(t, err)
	manifest := collectionManifest{Cases: []collectionCase{{Status: "collected", History: path, Checksum: checksum}}}
	require.NoError(t, validateManifestFiles(manifest))
	require.NoError(t, os.WriteFile(path, []byte("corrupt"), 0o600))
	require.ErrorContains(t, validateManifestFiles(manifest), "checksum differs")
}

func TestEnsureSecureDirectoryRejectsBroadPermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "histories")
	require.NoError(t, os.Mkdir(path, 0o700))
	require.NoError(t, os.Chmod(path, 0o755))
	require.ErrorContains(t, ensureSecureDirectory(path), "must not be accessible")
	require.NoError(t, os.Chmod(path, 0o700))
	require.NoError(t, ensureSecureDirectory(path))
}

func TestProductionIdentifiersAreDeterministicAndOpaque(t *testing.T) {
	require.Equal(t, sampleKey("seed", "namespace", "schedule"), sampleKey("seed", "namespace", "schedule"))
	require.NotEqual(t, sampleKey("seed", "namespace", "schedule"), sampleKey("other", "namespace", "schedule"))
	require.NotContains(t, opaqueID("customer-namespace"), "customer")
	require.Len(t, opaqueID("customer-namespace"), 24)
}

func TestHistoryCohorts(t *testing.T) {
	testCases := map[string][]string{
		"interval":           {"spec_interval"},
		"update":             {"update"},
		"backfill-allow-all": {"backfill"},
		"pause-unpause":      {"pause_interaction"},
		"skip-running":       {"overlap_skip"},
	}
	for fixture, expected := range testCases {
		fixture := fixture
		t.Run(fixture, func(t *testing.T) {
			path := filepath.Join("../../../chasm/lib/scheduler/testdata/v1-replay/current-v1", fixture+".json.gz")
			f, err := os.Open(path)
			require.NoError(t, err)
			gz, err := gzip.NewReader(f)
			require.NoError(t, err)
			history, err := client.HistoryFromJSON(gz, client.HistoryJSONOptions{})
			require.NoError(t, err)
			require.NoError(t, gz.Close())
			require.NoError(t, f.Close())
			for _, cohort := range expected {
				require.Contains(t, historyCohorts(history), cohort)
			}
		})
	}
}

func TestFixtureScenariosCoverConformanceMatrix(t *testing.T) {
	scenarios := fixtureScenarios()
	names := make([]string, 0, len(scenarios))
	for _, scenario := range scenarios {
		names = append(names, scenario.name)
		require.Positive(t, scenario.targetActions)
		if scenario.name != "backfill-allow-all" {
			require.Positive(t, scenario.remainingActions)
		}
	}
	require.Equal(t, []string{
		"interval",
		"calendar",
		"cron",
		"jitter",
		"update",
		"pause-unpause",
		"backfill-allow-all",
		"buffer-all-running",
		"skip-running",
	}, names)
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
