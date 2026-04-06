package workspace

import (
	"fmt"

	apiworkspacepb "go.temporal.io/api/workspace/v1"
	"go.temporal.io/server/chasm"
	workspacepb "go.temporal.io/server/chasm/lib/workspace/gen/workspacepb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// -- Event types --

type acquireWriterEvent struct {
	WorkflowID string
	RunID      string
}

type commitEvent struct {
	WorkflowID  string
	RunID       string
	NewVersion  int64
	DiffRecords []*apiworkspacepb.DiffRecord
}

type releaseWriterEvent struct {
	WorkflowID string
	RunID      string
}

type closeEvent struct{}

// -- Transitions --

// TransitionAcquireWriter transitions from ACTIVE to WRITER_LOCKED, or is
// idempotent if the same workflow already holds the lock (WRITER_LOCKED → WRITER_LOCKED).
var TransitionAcquireWriter = chasm.NewTransition(
	[]workspacepb.WorkspaceExecutionStatus{
		workspacepb.WORKSPACE_EXECUTION_STATUS_ACTIVE,
		workspacepb.WORKSPACE_EXECUTION_STATUS_WRITER_LOCKED,
	},
	workspacepb.WORKSPACE_EXECUTION_STATUS_WRITER_LOCKED,
	func(w *Workspace, ctx chasm.MutableContext, event acquireWriterEvent) error {
		// If already locked by the same workflow, this is an idempotent re-acquire.
		if lock, ok := w.WriterLock.TryGet(ctx); ok && lock != nil {
			if lock.WorkflowId == event.WorkflowID {
				return nil // already locked by same workflow
			}
			return fmt.Errorf("workspace writer lock held by workflow %s, not %s",
				lock.WorkflowId, event.WorkflowID)
		}
		w.WriterLock = chasm.NewDataField(ctx, &workspacepb.WorkspaceWriterLock{
			WorkflowId: event.WorkflowID,
			RunId:      event.RunID,
			AcquiredAt: timestamppb.New(ctx.Now(w)),
		})
		return nil
	},
)

// TransitionCommit advances the workspace version while the writer lock is held.
// Validates version continuity and writer ownership. Supports batch commit
// (multiple diffs at once for checkin).
var TransitionCommit = chasm.NewTransition(
	[]workspacepb.WorkspaceExecutionStatus{workspacepb.WORKSPACE_EXECUTION_STATUS_WRITER_LOCKED},
	workspacepb.WORKSPACE_EXECUTION_STATUS_WRITER_LOCKED,
	func(w *Workspace, ctx chasm.MutableContext, event commitEvent) error {
		// Validate writer ownership.
		if err := w.ValidateWriter(ctx, event.WorkflowID, event.RunID); err != nil {
			return err
		}

		// Validate version continuity.
		expectedVersion := w.CommittedVersion + int64(len(event.DiffRecords))
		if event.NewVersion != expectedVersion {
			return fmt.Errorf(
				"workspace version mismatch: current=%d, expected_new=%d, got_new=%d",
				w.CommittedVersion, expectedVersion, event.NewVersion,
			)
		}

		// Idempotent: if version already at or past target, skip.
		if w.CommittedVersion >= event.NewVersion {
			return nil
		}

		// Advance version and append diffs.
		w.CommittedVersion = event.NewVersion
		w.Diffs = append(w.Diffs, event.DiffRecords...)
		return nil
	},
)

// TransitionReleaseWriter transitions from WRITER_LOCKED to ACTIVE.
// Clears the writer lock.
var TransitionReleaseWriter = chasm.NewTransition(
	[]workspacepb.WorkspaceExecutionStatus{workspacepb.WORKSPACE_EXECUTION_STATUS_WRITER_LOCKED},
	workspacepb.WORKSPACE_EXECUTION_STATUS_ACTIVE,
	func(w *Workspace, ctx chasm.MutableContext, event releaseWriterEvent) error {
		if err := w.ValidateWriter(ctx, event.WorkflowID, event.RunID); err != nil {
			return err
		}
		w.WriterLock = chasm.NewDataField[*workspacepb.WorkspaceWriterLock](ctx, nil)
		return nil
	},
)

// TransitionClose transitions from ACTIVE to CLOSED (terminal).
var TransitionClose = chasm.NewTransition(
	[]workspacepb.WorkspaceExecutionStatus{workspacepb.WORKSPACE_EXECUTION_STATUS_ACTIVE},
	workspacepb.WORKSPACE_EXECUTION_STATUS_CLOSED,
	func(w *Workspace, ctx chasm.MutableContext, _ closeEvent) error {
		return nil
	},
)
