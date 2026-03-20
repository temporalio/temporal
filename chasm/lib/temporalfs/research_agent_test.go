package temporalfs

// TestResearchAgent_HandlerLevel demonstrates a multi-step AI research agent
// through the TemporalFS gRPC handler API, mirroring how a Temporal activity
// would interact with TemporalFS in an OSS deployment.
//
// Scenario: An AI agent researches "Quantum Computing" in 3 iterations:
//
//  1. Gather Sources — creates workspace dirs, creates sources.md, writes content, snapshots
//  2. Analyze & Synthesize — overwrites sources.md, creates analysis.md, snapshots
//  3. Final Report — creates report.md, snapshots
//
// The handler test exercises the proto request/response API (Mkdir, CreateFile,
// WriteChunks, ReadChunks, ReadDir, Getattr, CreateSnapshot). Snapshot
// time-travel verification uses the library directly since the handler does not
// expose snapshot read operations.
//
// Run:
//
//	go test ./chasm/lib/temporalfs/ -run TestResearchAgent -v
//
// This exercises the OSS handler layer backed by PebbleStoreProvider.

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/temporalio/temporal-fs/pkg/failpoint"
	tfs "github.com/temporalio/temporal-fs/pkg/fs"
	temporalfspb "go.temporal.io/server/chasm/lib/temporalfs/gen/temporalfspb/v1"
)

func TestResearchAgent_HandlerLevel(t *testing.T) {
	h, provider := newTestHandler(t)
	nsID, fsID := "ns-research", "fs-research-agent"
	initHandlerFS(t, h, nsID, fsID)

	ctx := context.Background()
	const rootInode uint64 = 1

	// ─── Content for each iteration ──────────────────────────────────────

	sourcesV1 := []byte(`# Sources — Quantum Computing

1. Feynman, R. "Simulating Physics with Computers" (1982)
2. Shor, P. "Algorithms for Quantum Computation" (1994)
3. Nielsen & Chuang, "Quantum Computation and Quantum Information" (2000)
`)

	sourcesV2 := []byte(`# Sources — Quantum Computing (Updated)

1. Feynman, R. "Simulating Physics with Computers" (1982)
2. Shor, P. "Algorithms for Quantum Computation" (1994)
3. Nielsen & Chuang, "Quantum Computation and Quantum Information" (2000)
4. Preskill, J. "Quantum Computing in the NISQ Era and Beyond" (2018)
5. Arute et al. "Quantum Supremacy using a Programmable Superconducting Processor" (2019)
`)

	analysisContent := []byte(`# Analysis — Quantum Computing

## Key Themes
- Quantum error correction remains the primary bottleneck.
- NISQ-era devices show promise but lack fault tolerance.
- Shor's algorithm threatens RSA; post-quantum cryptography is urgent.
`)

	reportContent := []byte(`# Final Report — Quantum Computing Research

## Executive Summary
Quantum computing has reached an inflection point. Practical fault-tolerant
quantum computers remain years away, but near-term applications are emerging.

## Recommendations
1. Monitor NISQ algorithm developments for near-term applications.
2. Begin migration planning to post-quantum cryptographic standards.
3. Evaluate quantum-classical hybrid approaches for optimization problems.
`)

	// ─── Iteration 1: Gather Sources ─────────────────────────────────────

	// Create /research directory.
	researchDir, err := h.Mkdir(ctx, &temporalfspb.MkdirRequest{
		NamespaceId:   nsID,
		FilesystemId:  fsID,
		ParentInodeId: rootInode,
		Name:          "research",
		Mode:          0o755,
	})
	require.NoError(t, err)
	researchInodeID := researchDir.InodeId

	// Create /research/quantum-computing directory.
	qcDir, err := h.Mkdir(ctx, &temporalfspb.MkdirRequest{
		NamespaceId:   nsID,
		FilesystemId:  fsID,
		ParentInodeId: researchInodeID,
		Name:          "quantum-computing",
		Mode:          0o755,
	})
	require.NoError(t, err)
	qcInodeID := qcDir.InodeId

	// Create sources.md file.
	sourcesFile, err := h.CreateFile(ctx, &temporalfspb.CreateFileRequest{
		NamespaceId:   nsID,
		FilesystemId:  fsID,
		ParentInodeId: qcInodeID,
		Name:          "sources.md",
		Mode:          0o644,
	})
	require.NoError(t, err)
	sourcesInodeID := sourcesFile.InodeId

	// Write content to sources.md.
	writeResp, err := h.WriteChunks(ctx, &temporalfspb.WriteChunksRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		InodeId:      sourcesInodeID,
		Offset:       0,
		Data:         sourcesV1,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(len(sourcesV1)), writeResp.BytesWritten)

	// Verify read back.
	readResp, err := h.ReadChunks(ctx, &temporalfspb.ReadChunksRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		InodeId:      sourcesInodeID,
		Offset:       0,
		ReadSize:     int64(len(sourcesV1)),
	})
	require.NoError(t, err)
	assert.Equal(t, sourcesV1, readResp.Data)

	// Create snapshot.
	snap1Resp, err := h.CreateSnapshot(ctx, &temporalfspb.CreateSnapshotRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		SnapshotName: "step-1-sources",
	})
	require.NoError(t, err)
	assert.Positive(t, snap1Resp.SnapshotTxnId)

	// ─── Iteration 2: Analyze & Synthesize ───────────────────────────────

	// Overwrite sources.md with updated content (truncate + write).
	_, err = h.Truncate(ctx, &temporalfspb.TruncateRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		InodeId:      sourcesInodeID,
		NewSize:      0,
	})
	require.NoError(t, err)
	_, err = h.WriteChunks(ctx, &temporalfspb.WriteChunksRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		InodeId:      sourcesInodeID,
		Offset:       0,
		Data:         sourcesV2,
	})
	require.NoError(t, err)

	// Create analysis.md.
	analysisFile, err := h.CreateFile(ctx, &temporalfspb.CreateFileRequest{
		NamespaceId:   nsID,
		FilesystemId:  fsID,
		ParentInodeId: qcInodeID,
		Name:          "analysis.md",
		Mode:          0o644,
	})
	require.NoError(t, err)
	analysisInodeID := analysisFile.InodeId

	_, err = h.WriteChunks(ctx, &temporalfspb.WriteChunksRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		InodeId:      analysisInodeID,
		Offset:       0,
		Data:         analysisContent,
	})
	require.NoError(t, err)

	// Verify ReadDir shows 2 files.
	dirResp, err := h.ReadDir(ctx, &temporalfspb.ReadDirRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		InodeId:      qcInodeID,
	})
	require.NoError(t, err)
	assert.Len(t, dirResp.Entries, 2, "iteration 2 should show 2 files")

	snap2Resp, err := h.CreateSnapshot(ctx, &temporalfspb.CreateSnapshotRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		SnapshotName: "step-2-analysis",
	})
	require.NoError(t, err)
	assert.Greater(t, snap2Resp.SnapshotTxnId, snap1Resp.SnapshotTxnId)

	// ─── Iteration 3: Final Report ───────────────────────────────────────

	reportFile, err := h.CreateFile(ctx, &temporalfspb.CreateFileRequest{
		NamespaceId:   nsID,
		FilesystemId:  fsID,
		ParentInodeId: qcInodeID,
		Name:          "report.md",
		Mode:          0o644,
	})
	require.NoError(t, err)

	_, err = h.WriteChunks(ctx, &temporalfspb.WriteChunksRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		InodeId:      reportFile.InodeId,
		Offset:       0,
		Data:         reportContent,
	})
	require.NoError(t, err)

	snap3Resp, err := h.CreateSnapshot(ctx, &temporalfspb.CreateSnapshotRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		SnapshotName: "step-3-final",
	})
	require.NoError(t, err)
	assert.Greater(t, snap3Resp.SnapshotTxnId, snap2Resp.SnapshotTxnId)

	// ─── Verify final state via handler ──────────────────────────────────

	// ReadDir should show 3 files.
	finalDir, err := h.ReadDir(ctx, &temporalfspb.ReadDirRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		InodeId:      qcInodeID,
	})
	require.NoError(t, err)
	assert.Len(t, finalDir.Entries, 3, "final state should have 3 files")

	// Getattr on report file.
	reportAttr, err := h.Getattr(ctx, &temporalfspb.GetattrRequest{
		NamespaceId:  nsID,
		FilesystemId: fsID,
		InodeId:      reportFile.InodeId,
	})
	require.NoError(t, err)
	assert.Positive(t, reportAttr.Attr.FileSize)

	// ─── Verify snapshot time-travel via library ─────────────────────────
	// The handler doesn't expose snapshot read operations, so we verify
	// through the library directly. This matches the existing test pattern.

	s, err := provider.GetStore(0, nsID, fsID)
	require.NoError(t, err)
	f, err := tfs.Open(s)
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()

	// Snapshot 1: only sources.md (v1).
	snap1FS, err := f.OpenSnapshot("step-1-sources")
	require.NoError(t, err)
	defer func() { require.NoError(t, snap1FS.Close()) }()

	snap1Sources, err := snap1FS.ReadFile("/research/quantum-computing/sources.md")
	require.NoError(t, err)
	assert.Equal(t, sourcesV1, snap1Sources, "snapshot 1 should have sources v1")

	_, err = snap1FS.ReadFile("/research/quantum-computing/analysis.md")
	require.ErrorIs(t, err, tfs.ErrNotFound, "snapshot 1 should NOT have analysis.md")

	snap1Entries, err := snap1FS.ReadDir("/research/quantum-computing")
	require.NoError(t, err)
	assert.Len(t, snap1Entries, 1, "snapshot 1 should have 1 file")

	// Snapshot 2: sources.md (v2) + analysis.md.
	snap2FS, err := f.OpenSnapshot("step-2-analysis")
	require.NoError(t, err)
	defer func() { require.NoError(t, snap2FS.Close()) }()

	snap2Sources, err := snap2FS.ReadFile("/research/quantum-computing/sources.md")
	require.NoError(t, err)
	assert.Equal(t, sourcesV2, snap2Sources, "snapshot 2 should have sources v2")

	_, err = snap2FS.ReadFile("/research/quantum-computing/report.md")
	require.ErrorIs(t, err, tfs.ErrNotFound, "snapshot 2 should NOT have report.md")

	snap2Entries, err := snap2FS.ReadDir("/research/quantum-computing")
	require.NoError(t, err)
	assert.Len(t, snap2Entries, 2, "snapshot 2 should have 2 files")

	// Snapshot 3: all 3 files.
	snap3FS, err := f.OpenSnapshot("step-3-final")
	require.NoError(t, err)
	defer func() { require.NoError(t, snap3FS.Close()) }()

	snap3Entries, err := snap3FS.ReadDir("/research/quantum-computing")
	require.NoError(t, err)
	assert.Len(t, snap3Entries, 3, "snapshot 3 should have 3 files")

	snap3Report, err := snap3FS.ReadFile("/research/quantum-computing/report.md")
	require.NoError(t, err)
	assert.Equal(t, reportContent, snap3Report)

	// Verify snapshot listing.
	snapshots, err := f.ListSnapshots()
	require.NoError(t, err)
	require.Len(t, snapshots, 3)
	assert.Equal(t, "step-1-sources", snapshots[0].Name)
	assert.Equal(t, "step-2-analysis", snapshots[1].Name)
	assert.Equal(t, "step-3-final", snapshots[2].Name)
}

// TestResearchAgent_HandlerCrashRecovery verifies that handler operations are
// atomic: if a failpoint causes an operation to fail mid-batch, the next handler
// call (which reopens the FS) sees only the previously committed state.
func TestResearchAgent_HandlerCrashRecovery(t *testing.T) {
	injected := func() error { return errors.New("injected crash") }

	h, provider := newTestHandler(t)
	nsID, fsID := "ns-crash", "fs-crash-agent"
	initHandlerFS(t, h, nsID, fsID)

	ctx := context.Background()
	const rootInode uint64 = 1

	// ─── Complete step 1 via handler ─────────────────────────────────────

	researchDir, err := h.Mkdir(ctx, &temporalfspb.MkdirRequest{
		NamespaceId: nsID, FilesystemId: fsID,
		ParentInodeId: rootInode, Name: "research", Mode: 0o755,
	})
	require.NoError(t, err)

	qcDir, err := h.Mkdir(ctx, &temporalfspb.MkdirRequest{
		NamespaceId: nsID, FilesystemId: fsID,
		ParentInodeId: researchDir.InodeId, Name: "quantum-computing", Mode: 0o755,
	})
	require.NoError(t, err)
	qcInodeID := qcDir.InodeId

	sourcesFile, err := h.CreateFile(ctx, &temporalfspb.CreateFileRequest{
		NamespaceId: nsID, FilesystemId: fsID,
		ParentInodeId: qcInodeID, Name: "sources.md", Mode: 0o644,
	})
	require.NoError(t, err)
	sourcesInodeID := sourcesFile.InodeId

	sourcesV1 := []byte("# Sources v1\n")
	_, err = h.WriteChunks(ctx, &temporalfspb.WriteChunksRequest{
		NamespaceId: nsID, FilesystemId: fsID,
		InodeId: sourcesInodeID, Offset: 0, Data: sourcesV1,
	})
	require.NoError(t, err)

	_, err = h.CreateSnapshot(ctx, &temporalfspb.CreateSnapshotRequest{
		NamespaceId: nsID, FilesystemId: fsID,
		SnapshotName: "step-1-sources",
	})
	require.NoError(t, err)

	// ─── Step 2: inject failure during CreateFile (analysis.md) ──────────
	// The first op (creating analysis.md inode) fails via failpoint.
	// The handler returns an error. On the next call, the FS reopens and
	// shows step 1 state — the failed CreateFile left no trace.

	failpoint.Enable("after-create-inode", injected)
	_, err = h.CreateFile(ctx, &temporalfspb.CreateFileRequest{
		NamespaceId: nsID, FilesystemId: fsID,
		ParentInodeId: qcInodeID, Name: "analysis.md", Mode: 0o644,
	})
	require.Error(t, err, "CreateFile should fail with injected error")
	failpoint.Disable("after-create-inode")

	// Verify: handler still works, ReadDir shows only step 1 files.
	dirResp, err := h.ReadDir(ctx, &temporalfspb.ReadDirRequest{
		NamespaceId: nsID, FilesystemId: fsID,
		InodeId: qcInodeID,
	})
	require.NoError(t, err)
	assert.Len(t, dirResp.Entries, 1, "after failed CreateFile, only sources.md should exist")

	// Verify: step 1 snapshot intact via library.
	s, err := provider.GetStore(0, nsID, fsID)
	require.NoError(t, err)
	f, err := tfs.Open(s)
	require.NoError(t, err)

	snap1, err := f.OpenSnapshot("step-1-sources")
	require.NoError(t, err)
	snap1Entries, err := snap1.ReadDir("/research/quantum-computing")
	require.NoError(t, err)
	assert.Len(t, snap1Entries, 1, "step-1 snapshot should have 1 file")
	require.NoError(t, snap1.Close())

	// No step-2 snapshot should exist.
	_, err = f.OpenSnapshot("step-2-analysis")
	require.ErrorIs(t, err, tfs.ErrSnapshotNotFound)
	require.NoError(t, f.Close())

	// ─── Recovery: retry step 2 successfully ─────────────────────────────

	analysisFile, err := h.CreateFile(ctx, &temporalfspb.CreateFileRequest{
		NamespaceId: nsID, FilesystemId: fsID,
		ParentInodeId: qcInodeID, Name: "analysis.md", Mode: 0o644,
	})
	require.NoError(t, err)

	_, err = h.WriteChunks(ctx, &temporalfspb.WriteChunksRequest{
		NamespaceId: nsID, FilesystemId: fsID,
		InodeId: analysisFile.InodeId, Offset: 0, Data: []byte("# Analysis\n"),
	})
	require.NoError(t, err)

	_, err = h.CreateSnapshot(ctx, &temporalfspb.CreateSnapshotRequest{
		NamespaceId: nsID, FilesystemId: fsID,
		SnapshotName: "step-2-analysis",
	})
	require.NoError(t, err)

	// ─── Step 3: inject failure during Mkdir (wrong op type) ─────────────
	// This tests that failures in unexpected operations are also atomic.

	failpoint.Enable("after-create-inode", injected)
	_, err = h.CreateFile(ctx, &temporalfspb.CreateFileRequest{
		NamespaceId: nsID, FilesystemId: fsID,
		ParentInodeId: qcInodeID, Name: "report.md", Mode: 0o644,
	})
	require.Error(t, err)
	failpoint.Disable("after-create-inode")

	// Verify step 2 state intact.
	dirResp, err = h.ReadDir(ctx, &temporalfspb.ReadDirRequest{
		NamespaceId: nsID, FilesystemId: fsID,
		InodeId: qcInodeID,
	})
	require.NoError(t, err)
	assert.Len(t, dirResp.Entries, 2, "after failed step 3, should still have 2 files")

	// ─── Recovery: complete step 3 ───────────────────────────────────────

	reportFile, err := h.CreateFile(ctx, &temporalfspb.CreateFileRequest{
		NamespaceId: nsID, FilesystemId: fsID,
		ParentInodeId: qcInodeID, Name: "report.md", Mode: 0o644,
	})
	require.NoError(t, err)

	_, err = h.WriteChunks(ctx, &temporalfspb.WriteChunksRequest{
		NamespaceId: nsID, FilesystemId: fsID,
		InodeId: reportFile.InodeId, Offset: 0, Data: []byte("# Report\n"),
	})
	require.NoError(t, err)

	dirResp, err = h.ReadDir(ctx, &temporalfspb.ReadDirRequest{
		NamespaceId: nsID, FilesystemId: fsID,
		InodeId: qcInodeID,
	})
	require.NoError(t, err)
	assert.Len(t, dirResp.Entries, 3, "all 3 files after recovery + completion")
}
