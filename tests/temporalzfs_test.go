package tests

// TestTemporalZFS_ResearchAgent exercises TemporalZFS through a real Temporal
// server with CHASM enabled. It injects the TemporalZFS fx module into the
// history service, extracts the FSStoreProvider via fx.Populate, and creates
// a real filesystem backed by PebbleDB through the full server wiring.
//
// This verifies that the TemporalZFS fx module correctly wires into the CHASM
// registry, the PebbleStoreProvider functions correctly under the server's
// lifecycle, and the full FS API (Mkdir, WriteFile, ReadFile, CreateSnapshot,
// OpenSnapshot, ReadDir, ListSnapshots) works end-to-end.
//
// Run:
//
//	go test ./tests/ -run TestTemporalZFS -v -count 1
//
// Architecture: FunctionalTestBase → HistoryService(TemporalZFS HistoryModule) →
// PebbleStoreProvider → store.Store → tzfs.FS

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	tzfs "github.com/temporalio/temporal-zfs/pkg/fs"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/temporalzfs"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/fx"
)

type TemporalZFSTestSuite struct {
	testcore.FunctionalTestBase //nolint:forbidigo // NewEnv doesn't support WithFxOptionsForService needed for fx.Populate
	storeProvider               temporalzfs.FSStoreProvider
}

func TestTemporalZFS(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(TemporalZFSTestSuite))
}

func (s *TemporalZFSTestSuite) SetupSuite() {
	s.SetupSuiteWithCluster( //nolint:forbidigo // NewEnv doesn't support WithFxOptionsForService
		testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
			dynamicconfig.EnableChasm.Key(): true,
		}),
		// TemporalZFS HistoryModule is already registered in service/history/fx.go.
		// We only need fx.Populate to extract the FSStoreProvider from the graph.
		testcore.WithFxOptionsForService(primitives.HistoryService,
			fx.Populate(&s.storeProvider),
		),
	)
}

func (s *TemporalZFSTestSuite) TearDownSuite() {
	s.FunctionalTestBase.TearDownSuite() //nolint:forbidigo // NewEnv doesn't support WithFxOptionsForService
}

// TestResearchAgent_RealServer runs the 3-iteration research agent scenario
// through a real Temporal server's TemporalZFS subsystem.
func (s *TemporalZFSTestSuite) TestResearchAgent_RealServer() {
	t := s.T()

	// Content for each iteration.
	sourcesV1 := []byte("# Sources v1\n1. Feynman (1982)\n2. Shor (1994)\n")
	sourcesV2 := []byte("# Sources v2\n1. Feynman (1982)\n2. Shor (1994)\n3. Preskill (2018)\n")
	analysisContent := []byte("# Analysis\nQuantum error correction is the bottleneck.\n")
	reportContent := []byte("# Final Report\nQC has reached an inflection point.\n")

	// Create a real FS through the server's PebbleStoreProvider.
	store, err := s.storeProvider.GetStore(1, s.NamespaceID().String(), "research-agent-fs")
	s.NoError(err)

	f, err := tzfs.Create(store, tzfs.Options{})
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
	s.ErrorIs(err, tzfs.ErrNotFound, "snapshot 1 should NOT have analysis.md")

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
	s.ErrorIs(err, tzfs.ErrNotFound, "snapshot 2 should NOT have report.md")

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

// TestResearchAgent_Workflow runs the research agent as a real Temporal workflow
// with activities. Each step of the research agent is an activity that operates
// on TemporalZFS. The workflow orchestrates the 3 steps sequentially. After the
// workflow completes, the test verifies MVCC snapshot isolation.
//
// This demonstrates the real-world pattern: a Temporal workflow orchestrating
// an AI agent whose activities read/write a durable versioned filesystem.
func (s *TemporalZFSTestSuite) TestResearchAgent_Workflow() {
	t := s.T()

	sourcesV1 := []byte("# Sources v1\n1. Feynman (1982)\n2. Shor (1994)\n")
	sourcesV2 := []byte("# Sources v2\n1. Feynman (1982)\n2. Shor (1994)\n3. Preskill (2018)\n")
	analysisContent := []byte("# Analysis\nQuantum error correction is the bottleneck.\n")
	reportContent := []byte("# Final Report\nQC has reached an inflection point.\n")

	// Create FS backed by the real server's PebbleStoreProvider.
	store, err := s.storeProvider.GetStore(1, s.NamespaceID().String(), "research-wf-fs")
	s.NoError(err)

	f, err := tzfs.Create(store, tzfs.Options{})
	s.NoError(err)
	defer func() { s.NoError(f.Close()) }()

	// ─── Define activities ───────────────────────────────────────────────
	// Each activity performs one step of the research agent workflow.
	// Activities share the FS instance via closure (in-process worker).

	gatherSources := func(ctx context.Context) error {
		if err := f.Mkdir("/research", 0o755); err != nil {
			return err
		}
		if err := f.Mkdir("/research/quantum-computing", 0o755); err != nil {
			return err
		}
		if err := f.WriteFile("/research/quantum-computing/sources.md", sourcesV1, 0o644); err != nil {
			return err
		}
		_, err := f.CreateSnapshot("step-1-sources")
		return err
	}

	analyzeSources := func(ctx context.Context) error {
		if err := f.WriteFile("/research/quantum-computing/sources.md", sourcesV2, 0o644); err != nil {
			return err
		}
		if err := f.WriteFile("/research/quantum-computing/analysis.md", analysisContent, 0o644); err != nil {
			return err
		}
		_, err := f.CreateSnapshot("step-2-analysis")
		return err
	}

	writeFinalReport := func(ctx context.Context) error {
		if err := f.WriteFile("/research/quantum-computing/report.md", reportContent, 0o644); err != nil {
			return err
		}
		_, err := f.CreateSnapshot("step-3-final")
		return err
	}

	// ─── Define workflow ─────────────────────────────────────────────────

	researchAgentWorkflow := func(ctx workflow.Context) error {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second * debug.TimeoutMultiplier,
		}
		ctx = workflow.WithActivityOptions(ctx, ao)

		if err := workflow.ExecuteActivity(ctx, gatherSources).Get(ctx, nil); err != nil {
			return err
		}
		if err := workflow.ExecuteActivity(ctx, analyzeSources).Get(ctx, nil); err != nil {
			return err
		}
		return workflow.ExecuteActivity(ctx, writeFinalReport).Get(ctx, nil)
	}

	// ─── Register and execute ────────────────────────────────────────────

	s.Worker().RegisterWorkflow(researchAgentWorkflow)
	s.Worker().RegisterActivity(gatherSources)
	s.Worker().RegisterActivity(analyzeSources)
	s.Worker().RegisterActivity(writeFinalReport)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second*debug.TimeoutMultiplier)
	defer cancel()

	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        "research-agent-workflow",
		TaskQueue: s.TaskQueue(),
	}, researchAgentWorkflow)
	s.NoError(err)
	s.NoError(run.Get(ctx, nil))

	// ─── Verify FS state after workflow completion ───────────────────────

	entries, err := f.ReadDir("/research/quantum-computing")
	s.NoError(err)
	assert.Len(t, entries, 3, "workflow should have created 3 files")

	gotSources, err := f.ReadFile("/research/quantum-computing/sources.md")
	s.NoError(err)
	assert.Equal(t, sourcesV2, gotSources)

	// Verify MVCC snapshot isolation.
	snap1FS, err := f.OpenSnapshot("step-1-sources")
	s.NoError(err)
	snap1Data, err := snap1FS.ReadFile("/research/quantum-computing/sources.md")
	s.NoError(err)
	assert.Equal(t, sourcesV1, snap1Data, "snapshot 1 should have v1")
	s.NoError(snap1FS.Close())

	snap2FS, err := f.OpenSnapshot("step-2-analysis")
	s.NoError(err)
	snap2Entries, err := snap2FS.ReadDir("/research/quantum-computing")
	s.NoError(err)
	assert.Len(t, snap2Entries, 2, "snapshot 2 should have 2 files")
	s.NoError(snap2FS.Close())

	snap3FS, err := f.OpenSnapshot("step-3-final")
	s.NoError(err)
	snap3Entries, err := snap3FS.ReadDir("/research/quantum-computing")
	s.NoError(err)
	assert.Len(t, snap3Entries, 3, "snapshot 3 should have 3 files")
	s.NoError(snap3FS.Close())

	snapshots, err := f.ListSnapshots()
	s.NoError(err)
	s.Len(snapshots, 3)
}
