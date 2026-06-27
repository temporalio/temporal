package scheduler_test

import (
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/server/service/worker/scheduler/versionguard"
)

// The version-gate history corpus freezes each SchedulerWorkflowVersion's decision procedure.
// For every gate (see versionguard.Gates) the generator records its minimal scenario twice:
// unclamped (the current version) and clamped just below the gate version. TestVersionGates
// asserts the gate is observed in the first and absent in the second; TestReplays
// (replay_test.go) replays both under the current binary. A broken or misplaced clamp is caught
// here; an ungated behavior change is caught by replay. Existing histories must never be
// regenerated: gate new behavior behind a new version and generate that gate's pair instead.

const generateGatesHint = "run: GENERATE_SCHEDULER_VERSION_SNAPSHOTS=1 go test ./tests/ -run TestGenerateVersionGates -v -count=1"

// TestVersionGates proves, on every run, that each gate's behavior is present exactly when
// the recorded version is at or above it: present in the unclamped history, absent in the
// clamped one. The absent half is load-bearing; a clamp that fails to lower the version
// would leave the gate present in both histories and fail here.
func TestVersionGates(t *testing.T) {
	gates := versionguard.Gates()
	for _, g := range gates {
		t.Run(fmt.Sprintf("v%02d_%s", int(g.Version), g.Name), func(t *testing.T) {
			current := parseGateHistory(t, versionguard.GateFixtureName(int(g.Version), g.Name, true))
			clamped := parseGateHistory(t, versionguard.GateFixtureName(int(g.Version), g.Name, false))
			require.Truef(t, g.IsObserved(current), "%s: unclamped, gate must be PRESENT", g.Why)
			require.Falsef(t, g.IsObserved(clamped), "%s: clamped at v%d, gate must be ABSENT", g.Why, int(g.Version)-1)
		})
	}

	// No orphaned histories: exactly one at/below pair per gate, so a removed gate cannot leave
	// a stale fixture that is silently never asserted.
	files, err := filepath.Glob(filepath.Join("testdata", "replay_gate_*.json.gz"))
	require.NoError(t, err)
	require.Len(t, files, 2*len(gates), "expected one at/below history pair per gate; %s", generateGatesHint)
}

func parseGateHistory(t *testing.T, name string) *versionguard.Observations {
	t.Helper()
	obs, err := versionguard.Observe(loadHistory(t, name))
	require.NoError(t, err)
	return obs
}

func loadHistory(t *testing.T, name string) []*historypb.HistoryEvent {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", name))
	require.NoError(t, err, "missing history %s; %s", name, generateGatesHint)
	defer func() { _ = f.Close() }()
	r, err := gzip.NewReader(f)
	require.NoError(t, err)
	defer func() { _ = r.Close() }()
	history, err := client.HistoryFromJSON(r, client.HistoryJSONOptions{})
	require.NoError(t, err)
	return history.Events
}
