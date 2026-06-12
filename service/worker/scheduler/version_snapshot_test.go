package scheduler_test

import (
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/server/service/worker/scheduler/versionguard"
)

// The version snapshot corpus freezes each activated SchedulerWorkflowVersion's decision
// procedure: one golden history per (version, scenario), generated against a real server
// clamped to that version. TestReplays replays every snapshot (they match its glob), so an
// ungated behavior change breaks the frozen versions it reaches; the tests below keep the
// corpus complete and prove each snapshot actually exercises every version gate on the
// correct side. Existing snapshots must never be regenerated: gate new behavior behind a
// new version instead, and generate that version's snapshots.

const generateSnapshotsHint = "run: GENERATE_SCHEDULER_VERSION_SNAPSHOTS=1 go test ./tests/ -run TestGenerateSchedulerVersionSnapshots -v -count=1"

// TestVersionSnapshotsComplete fails the moment a release activates a new version without
// freezing it: every version from 1 through the current one must have a snapshot per
// scenario.
func TestVersionSnapshotsComplete(t *testing.T) {
	for version := 1; version <= versionguard.MaxVersion(); version++ {
		for _, scenario := range versionguard.Scenarios() {
			name := versionguard.SnapshotName(version, scenario)
			_, err := os.Stat(filepath.Join("testdata", name))
			require.NoError(t, err, "missing version snapshot %s; %s", name, generateSnapshotsHint)
		}
	}
}

// TestVersionSnapshotTokens re-proves, on every run, that each committed snapshot
// exhibits every version token on the side its clamp demands and that no token went
// unexercised. Combined with TestReplays this is the frozen-gate guard: replay catches
// command drift, tokens catch coverage drift.
func TestVersionSnapshotTokens(t *testing.T) {
	files, err := filepath.Glob("testdata/replay_snapshot_v*.json.gz")
	require.NoError(t, err)

	for _, filename := range files {
		version, scenario, err := parseSnapshotName(filepath.Base(filename))
		require.NoError(t, err)

		t.Run(fmt.Sprintf("v%02d_%s", version, scenario), func(t *testing.T) {
			f, err := os.Open(filename)
			require.NoError(t, err)
			defer func() { _ = f.Close() }()
			r, err := gzip.NewReader(f)
			require.NoError(t, err)
			defer func() { _ = r.Close() }()
			history, err := client.HistoryFromJSON(r, client.HistoryJSONOptions{})
			require.NoError(t, err)

			require.NoError(t, versionguard.Evaluate(history.Events, version, scenario))
		})
	}
}

func parseSnapshotName(base string) (version int, scenario string, err error) {
	trimmed := strings.TrimSuffix(strings.TrimPrefix(base, "replay_snapshot_v"), ".json.gz")
	digits, scenario, found := strings.Cut(trimmed, "_")
	if !found {
		return 0, "", fmt.Errorf("snapshot name %q does not match replay_snapshot_v<NN>_<scenario>.json.gz", base)
	}
	version, err = strconv.Atoi(digits)
	if err != nil {
		return 0, "", fmt.Errorf("snapshot name %q has non-numeric version: %w", base, err)
	}
	return version, scenario, nil
}
