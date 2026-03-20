package tests

// TestTemporalFS_ResearchAgent exercises TemporalFS through a real Temporal
// server with CHASM enabled. It injects the TemporalFS fx module into the
// history service, extracts the FSStoreProvider via fx.Populate, and creates
// a real filesystem backed by PebbleDB through the full server wiring.
//
// This verifies that the TemporalFS fx module correctly wires into the CHASM
// registry, the PebbleStoreProvider functions correctly under the server's
// lifecycle, and the full FS API (Mkdir, WriteFile, ReadFile, CreateSnapshot,
// OpenSnapshot, ReadDir, ListSnapshots) works end-to-end.
//
// Run:
//
//	go test ./tests/ -run TestTemporalFS -v -count 1
//
// Architecture: FunctionalTestBase → HistoryService(TemporalFS HistoryModule) →
// PebbleStoreProvider → store.Store → tfs.FS

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	tfs "github.com/temporalio/temporal-fs/pkg/fs"
	"go.temporal.io/server/chasm/lib/temporalfs"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/fx"
)

type TemporalFSTestSuite struct {
	testcore.FunctionalTestBase
	storeProvider temporalfs.FSStoreProvider
}

func TestTemporalFS(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(TemporalFSTestSuite))
}

func (s *TemporalFSTestSuite) SetupSuite() {
	s.FunctionalTestBase.SetupSuiteWithCluster(
		testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
			dynamicconfig.EnableChasm.Key(): true,
		}),
		// TemporalFS HistoryModule is already registered in service/history/fx.go.
		// We only need fx.Populate to extract the FSStoreProvider from the graph.
		testcore.WithFxOptionsForService(primitives.HistoryService,
			fx.Populate(&s.storeProvider),
		),
	)
}

func (s *TemporalFSTestSuite) TearDownSuite() {
	s.FunctionalTestBase.TearDownSuite()
}

// TestResearchAgent_RealServer runs the 3-iteration research agent scenario
// through a real Temporal server's TemporalFS subsystem.
func (s *TemporalFSTestSuite) TestResearchAgent_RealServer() {
	t := s.T()

	// Content for each iteration.
	sourcesV1 := []byte("# Sources v1\n1. Feynman (1982)\n2. Shor (1994)\n")
	sourcesV2 := []byte("# Sources v2\n1. Feynman (1982)\n2. Shor (1994)\n3. Preskill (2018)\n")
	analysisContent := []byte("# Analysis\nQuantum error correction is the bottleneck.\n")
	reportContent := []byte("# Final Report\nQC has reached an inflection point.\n")

	// Create a real FS through the server's PebbleStoreProvider.
	store, err := s.storeProvider.GetStore(1, s.NamespaceID().String(), "research-agent-fs")
	s.NoError(err)

	f, err := tfs.Create(store, tfs.Options{})
	s.NoError(err)
	defer func() { s.NoError(f.Close()) }()

	// ─── Iteration 1: Gather Sources ─────────────────────────────────────

	s.NoError(f.Mkdir("/research", 0o755))
	s.NoError(f.Mkdir("/research/quantum-computing", 0o755))
	s.NoError(f.WriteFile("/research/quantum-computing/sources.md", sourcesV1, 0o644))

	snap1, err := f.CreateSnapshot("step-1-sources")
	s.NoError(err)
	assert.Equal(t, "step-1-sources", snap1.Name)

	// ─── Iteration 2: Analyze & Synthesize ───────────────────────────────

	s.NoError(f.WriteFile("/research/quantum-computing/sources.md", sourcesV2, 0o644))
	s.NoError(f.WriteFile("/research/quantum-computing/analysis.md", analysisContent, 0o644))

	snap2, err := f.CreateSnapshot("step-2-analysis")
	s.NoError(err)
	assert.Greater(t, snap2.TxnID, snap1.TxnID)

	// ─── Iteration 3: Final Report ───────────────────────────────────────

	s.NoError(f.WriteFile("/research/quantum-computing/report.md", reportContent, 0o644))

	snap3, err := f.CreateSnapshot("step-3-final")
	s.NoError(err)
	assert.Greater(t, snap3.TxnID, snap2.TxnID)

	// ─── Verify current filesystem state ─────────────────────────────────

	gotSources, err := f.ReadFile("/research/quantum-computing/sources.md")
	s.NoError(err)
	assert.Equal(t, sourcesV2, gotSources)

	gotAnalysis, err := f.ReadFile("/research/quantum-computing/analysis.md")
	s.NoError(err)
	assert.Equal(t, analysisContent, gotAnalysis)

	gotReport, err := f.ReadFile("/research/quantum-computing/report.md")
	s.NoError(err)
	assert.Equal(t, reportContent, gotReport)

	entries, err := f.ReadDir("/research/quantum-computing")
	s.NoError(err)
	assert.Len(t, entries, 3)

	// ─── Verify snapshot 1: step-1-sources ───────────────────────────────

	snap1FS, err := f.OpenSnapshot("step-1-sources")
	s.NoError(err)
	defer func() { s.NoError(snap1FS.Close()) }()

	snap1Sources, err := snap1FS.ReadFile("/research/quantum-computing/sources.md")
	s.NoError(err)
	assert.Equal(t, sourcesV1, snap1Sources, "snapshot 1 should have sources.md v1")

	_, err = snap1FS.ReadFile("/research/quantum-computing/analysis.md")
	s.ErrorIs(err, tfs.ErrNotFound, "snapshot 1 should NOT have analysis.md")

	snap1Entries, err := snap1FS.ReadDir("/research/quantum-computing")
	s.NoError(err)
	assert.Len(t, snap1Entries, 1, "snapshot 1 should have 1 file")

	// ─── Verify snapshot 2: step-2-analysis ──────────────────────────────

	snap2FS, err := f.OpenSnapshot("step-2-analysis")
	s.NoError(err)
	defer func() { s.NoError(snap2FS.Close()) }()

	snap2Sources, err := snap2FS.ReadFile("/research/quantum-computing/sources.md")
	s.NoError(err)
	assert.Equal(t, sourcesV2, snap2Sources, "snapshot 2 should have sources.md v2")

	_, err = snap2FS.ReadFile("/research/quantum-computing/report.md")
	s.ErrorIs(err, tfs.ErrNotFound, "snapshot 2 should NOT have report.md")

	snap2Entries, err := snap2FS.ReadDir("/research/quantum-computing")
	s.NoError(err)
	assert.Len(t, snap2Entries, 2, "snapshot 2 should have 2 files")

	// ─── Verify snapshot 3: step-3-final ─────────────────────────────────

	snap3FS, err := f.OpenSnapshot("step-3-final")
	s.NoError(err)
	defer func() { s.NoError(snap3FS.Close()) }()

	snap3Entries, err := snap3FS.ReadDir("/research/quantum-computing")
	s.NoError(err)
	assert.Len(t, snap3Entries, 3, "snapshot 3 should have 3 files")

	// ─── Verify snapshot listing ─────────────────────────────────────────

	snapshots, err := f.ListSnapshots()
	s.NoError(err)
	s.Len(snapshots, 3)
	assert.Equal(t, "step-1-sources", snapshots[0].Name)
	assert.Equal(t, "step-2-analysis", snapshots[1].Name)
	assert.Equal(t, "step-3-final", snapshots[2].Name)

	// ─── Verify metrics ──────────────────────────────────────────────────

	m := f.Metrics()
	assert.Equal(t, int64(3), m.FilesCreated.Load(), "3 files created")
	assert.Equal(t, int64(2), m.DirsCreated.Load(), "2 dirs created")
	assert.Positive(t, m.BytesWritten.Load())
}
