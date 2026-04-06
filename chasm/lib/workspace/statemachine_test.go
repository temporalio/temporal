package workspace

import (
	"testing"

	"github.com/stretchr/testify/require"
	apiworkspacepb "go.temporal.io/api/workspace/v1"
	"go.temporal.io/server/chasm"
	workspacepb "go.temporal.io/server/chasm/lib/workspace/gen/workspacepb/v1"
)

func newTestWorkspace(status workspacepb.WorkspaceExecutionStatus) *Workspace {
	return &Workspace{
		WorkspaceState: &workspacepb.WorkspaceState{
			CommittedVersion: 0,
			Status:           status,
		},
	}
}

func TestTransitionAcquireWriter(t *testing.T) {
	ws := newTestWorkspace(workspacepb.WORKSPACE_EXECUTION_STATUS_ACTIVE)
	mctx := &chasm.MockMutableContext{}

	err := TransitionAcquireWriter.Apply(ws, mctx, acquireWriterEvent{
		WorkflowID: "wf-1",
		RunID:      "run-1",
	})
	require.NoError(t, err)
	require.Equal(t, workspacepb.WORKSPACE_EXECUTION_STATUS_WRITER_LOCKED, ws.StateMachineState())
}

func TestTransitionAcquireWriter_AlreadyLocked(t *testing.T) {
	ws := newTestWorkspace(workspacepb.WORKSPACE_EXECUTION_STATUS_WRITER_LOCKED)

	// Cannot apply AcquireWriter from WRITER_LOCKED state
	require.False(t, TransitionAcquireWriter.Possible(ws))
}

func TestTransitionCommit_SingleVersion(t *testing.T) {
	ws := newTestWorkspace(workspacepb.WORKSPACE_EXECUTION_STATUS_WRITER_LOCKED)
	mctx := &chasm.MockMutableContext{}

	// Set writer lock
	ws.WriterLock = chasm.NewDataField(mctx, &workspacepb.WorkspaceWriterLock{
		WorkflowId: "wf-1",
		RunId:      "run-1",
	})

	err := TransitionCommit.Apply(ws, mctx, commitEvent{
		WorkflowID: "wf-1",
		RunID:      "run-1",
		NewVersion: 1,
		DiffRecords: []*apiworkspacepb.DiffRecord{
			{FromVersion: 0, ToVersion: 1, SizeBytes: 100},
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), ws.CommittedVersion)
	require.Len(t, ws.Diffs, 1)
	require.Equal(t, workspacepb.WORKSPACE_EXECUTION_STATUS_WRITER_LOCKED, ws.StateMachineState())
}

func TestTransitionCommit_BatchVersions(t *testing.T) {
	ws := newTestWorkspace(workspacepb.WORKSPACE_EXECUTION_STATUS_WRITER_LOCKED)
	mctx := &chasm.MockMutableContext{}

	ws.WriterLock = chasm.NewDataField(mctx, &workspacepb.WorkspaceWriterLock{
		WorkflowId: "wf-1",
		RunId:      "run-1",
	})

	diffs := make([]*apiworkspacepb.DiffRecord, 5)
	for i := range diffs {
		diffs[i] = &apiworkspacepb.DiffRecord{
			FromVersion: int64(i),
			ToVersion:   int64(i + 1),
			SizeBytes:   100,
		}
	}

	err := TransitionCommit.Apply(ws, mctx, commitEvent{
		WorkflowID:  "wf-1",
		RunID:       "run-1",
		NewVersion:  5,
		DiffRecords: diffs,
	})
	require.NoError(t, err)
	require.Equal(t, int64(5), ws.CommittedVersion)
	require.Len(t, ws.Diffs, 5)
}

func TestTransitionCommit_VersionMismatch(t *testing.T) {
	ws := newTestWorkspace(workspacepb.WORKSPACE_EXECUTION_STATUS_WRITER_LOCKED)
	ws.CommittedVersion = 3
	mctx := &chasm.MockMutableContext{}

	ws.WriterLock = chasm.NewDataField(mctx, &workspacepb.WorkspaceWriterLock{
		WorkflowId: "wf-1",
		RunId:      "run-1",
	})

	// Trying to commit version 5 with only 1 diff (expects 4)
	err := TransitionCommit.Apply(ws, mctx, commitEvent{
		WorkflowID: "wf-1",
		RunID:      "run-1",
		NewVersion: 5,
		DiffRecords: []*apiworkspacepb.DiffRecord{
			{FromVersion: 3, ToVersion: 5},
		},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "version mismatch")
	// Version should not have advanced
	require.Equal(t, int64(3), ws.CommittedVersion)
}

func TestTransitionCommit_WrongWriter(t *testing.T) {
	ws := newTestWorkspace(workspacepb.WORKSPACE_EXECUTION_STATUS_WRITER_LOCKED)
	mctx := &chasm.MockMutableContext{}

	ws.WriterLock = chasm.NewDataField(mctx, &workspacepb.WorkspaceWriterLock{
		WorkflowId: "wf-1",
		RunId:      "run-1",
	})

	err := TransitionCommit.Apply(ws, mctx, commitEvent{
		WorkflowID: "wf-OTHER",
		RunID:      "run-OTHER",
		NewVersion: 1,
		DiffRecords: []*apiworkspacepb.DiffRecord{
			{FromVersion: 0, ToVersion: 1},
		},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "writer lock held by")
}

func TestTransitionCommit_NotLocked(t *testing.T) {
	ws := newTestWorkspace(workspacepb.WORKSPACE_EXECUTION_STATUS_ACTIVE)

	// Cannot commit when not locked
	require.False(t, TransitionCommit.Possible(ws))
}

func TestTransitionReleaseWriter(t *testing.T) {
	ws := newTestWorkspace(workspacepb.WORKSPACE_EXECUTION_STATUS_WRITER_LOCKED)
	mctx := &chasm.MockMutableContext{}

	ws.WriterLock = chasm.NewDataField(mctx, &workspacepb.WorkspaceWriterLock{
		WorkflowId: "wf-1",
		RunId:      "run-1",
	})

	err := TransitionReleaseWriter.Apply(ws, mctx, releaseWriterEvent{
		WorkflowID: "wf-1",
		RunID:      "run-1",
	})
	require.NoError(t, err)
	require.Equal(t, workspacepb.WORKSPACE_EXECUTION_STATUS_ACTIVE, ws.StateMachineState())
}

func TestTransitionReleaseWriter_NotLocked(t *testing.T) {
	ws := newTestWorkspace(workspacepb.WORKSPACE_EXECUTION_STATUS_ACTIVE)

	require.False(t, TransitionReleaseWriter.Possible(ws))
}

func TestTransitionClose(t *testing.T) {
	ws := newTestWorkspace(workspacepb.WORKSPACE_EXECUTION_STATUS_ACTIVE)
	mctx := &chasm.MockMutableContext{}

	err := TransitionClose.Apply(ws, mctx, closeEvent{})
	require.NoError(t, err)
	require.Equal(t, workspacepb.WORKSPACE_EXECUTION_STATUS_CLOSED, ws.StateMachineState())
}

func TestTransitionClose_WhileLocked(t *testing.T) {
	ws := newTestWorkspace(workspacepb.WORKSPACE_EXECUTION_STATUS_WRITER_LOCKED)

	// Cannot close while locked
	require.False(t, TransitionClose.Possible(ws))
}
