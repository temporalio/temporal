package history

import (
	"context"
	"fmt"

	workspacepb "go.temporal.io/api/workspace/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/workspace"
	"go.temporal.io/server/common/log/tag"
	workspaceworkflow "go.temporal.io/server/components/workspace/workflow"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
)

// processWorkspaceCreateAndAcquire creates a standalone workspace CHASM execution
// and acquires the writer lock for the originating workflow.
func (t *transferQueueActiveTaskExecutor) processWorkspaceCreateAndAcquire(
	ctx context.Context,
	task *tasks.WorkspaceCreateAndAcquireTask,
) error {
	namespaceID := task.GetNamespaceID()
	wsHandler := t.getWorkspaceHandler()
	if wsHandler == nil {
		return fmt.Errorf("workspace handler not configured")
	}

	if err := wsHandler.CreateWorkspace(ctx, namespaceID, task.WorkspaceID); err != nil {
		// Ignore AlreadyExists.
	}

	_, err := wsHandler.AcquireWriter(
		ctx, namespaceID, task.WorkspaceID,
		task.GetWorkflowID(), task.GetRunID(),
	)
	return err
}

// processWorkspaceSyncAndRelease pushes accumulated diffs to the standalone
// workspace and releases the writer lock.
func (t *transferQueueActiveTaskExecutor) processWorkspaceSyncAndRelease(
	ctx context.Context,
	task *tasks.WorkspaceSyncAndReleaseTask,
) error {
	namespaceID := task.GetNamespaceID()
	wsHandler := t.getWorkspaceHandler()
	if wsHandler == nil {
		return fmt.Errorf("workspace handler not configured")
	}

	if len(task.DiffRecords) > 0 {
		if err := wsHandler.CommitVersion(
			ctx, namespaceID, task.WorkspaceID,
			task.WorkflowID, task.RunID,
			task.NewVersion, task.DiffRecords,
		); err != nil {
			return err
		}
	}

	return wsHandler.ReleaseWriter(
		ctx, namespaceID, task.WorkspaceID,
		task.WorkflowID, task.RunID,
	)
}

// processWorkspaceFork creates a new standalone workspace from an existing one.
func (t *transferQueueActiveTaskExecutor) processWorkspaceFork(
	ctx context.Context,
	task *tasks.WorkspaceForkTask,
) error {
	namespaceID := task.GetNamespaceID()
	wsHandler := t.getWorkspaceHandler()
	if wsHandler == nil {
		return fmt.Errorf("workspace handler not configured")
	}

	return wsHandler.ForkWorkspace(
		ctx, namespaceID,
		task.SourceWorkspaceID, task.TargetWorkspaceID,
	)
}

// getWorkspaceHandler returns the workspace handler from the executor.
func (t *transferQueueActiveTaskExecutor) getWorkspaceHandler() *workspace.Handler {
	return t.workspaceHandler
}

// syncWorkspacesToStandalone syncs workspace state from a workflow's
// ExecutionInfo.WorkspaceInfos to standalone CHASM workspace executions.
//
// When forChildStart is true, this runs before a child workflow starts:
//   - Workspaces with WriteAccessRevokedSentinel (Handoff): sync + release lock.
//   - All other workspaces (Fork): sync but keep lock (parent continues writing).
//
// When forChildStart is false, this runs on workflow close:
//   - All workspaces: sync + release lock.
func (t *transferQueueActiveTaskExecutor) syncWorkspacesToStandalone(
	ctx context.Context,
	mutableState historyi.MutableState,
	wsHandler *workspace.Handler,
	forChildStart bool,
) {
	ctx = chasm.NewEngineContext(ctx, t.chasmEngine)

	executionInfo := mutableState.GetExecutionInfo()
	wsInfos := executionInfo.GetWorkspaceInfos()
	if len(wsInfos) == 0 {
		return
	}

	namespaceID := executionInfo.GetNamespaceId()
	workflowID := executionInfo.GetWorkflowId()
	runID := mutableState.GetExecutionState().GetRunId()

	for wsID, wsInfo := range wsInfos {
		isRevoked := wsInfo.ActiveWriterScheduledId == workspaceworkflow.WriteAccessRevokedSentinel

		// Determine whether to release the standalone lock after syncing.
		// - Close: always release.
		// - Child start + Handoff (revoked): release so child can acquire.
		// - Child start + Fork (not revoked): keep lock — parent continues writing.
		releaseLock := !forChildStart || isRevoked

		t.syncSingleWorkspace(ctx, namespaceID, wsID, wsInfo, workflowID, runID, wsHandler, releaseLock)

		// For Fork transfers: create a new standalone workspace from the source.
		if forkTarget := wsInfo.GetForkTargetWorkspaceId(); forkTarget != "" {
			if err := wsHandler.ForkWorkspace(ctx, namespaceID, wsID, forkTarget); err != nil {
				t.logger.Warn("Failed to fork workspace",
					tag.WorkflowNamespaceID(namespaceID),
					tag.NewStringTag("source-workspace-id", wsID),
					tag.NewStringTag("target-workspace-id", forkTarget),
					tag.Error(err),
				)
			}
			// Clear the fork target so it's not re-processed on retry.
			wsInfo.ForkTargetWorkspaceId = ""
		}
	}
}

// syncSingleWorkspace syncs one workspace to standalone CHASM.
func (t *transferQueueActiveTaskExecutor) syncSingleWorkspace(
	ctx context.Context,
	namespaceID, wsID string,
	wsInfo *workspacepb.WorkspaceInfo,
	workflowID, runID string,
	wsHandler *workspace.Handler,
	releaseLock bool,
) {
	if err := wsHandler.CreateWorkspace(ctx, namespaceID, wsID); err != nil {
		t.logger.Warn("Failed to create standalone workspace",
			tag.WorkflowNamespaceID(namespaceID),
			tag.NewStringTag("workspace-id", wsID),
			tag.Error(err),
		)
	}

	if _, err := wsHandler.AcquireWriter(ctx, namespaceID, wsID, workflowID, runID); err != nil {
		t.logger.Warn("Failed to acquire workspace writer",
			tag.WorkflowNamespaceID(namespaceID),
			tag.NewStringTag("workspace-id", wsID),
			tag.Error(err),
		)
		return
	}

	if wsInfo.CommittedVersion > 0 && len(wsInfo.Diffs) > 0 {
		standaloneInfo, _ := wsHandler.GetWorkspaceInfo(ctx, namespaceID, wsID)
		standaloneVersion := int64(0)
		if standaloneInfo != nil {
			standaloneVersion = standaloneInfo.CommittedVersion
		}
		if wsInfo.CommittedVersion > standaloneVersion {
			var newDiffs []*workspacepb.DiffRecord
			for _, d := range wsInfo.Diffs {
				if d.ToVersion > standaloneVersion {
					newDiffs = append(newDiffs, d)
				}
			}
			if len(newDiffs) > 0 {
				if err := wsHandler.CommitVersion(
					ctx, namespaceID, wsID, workflowID, runID,
					wsInfo.CommittedVersion, newDiffs,
				); err != nil {
					t.logger.Warn("Failed to sync workspace to standalone",
						tag.WorkflowNamespaceID(namespaceID),
						tag.NewStringTag("workspace-id", wsID),
						tag.Error(err),
					)
				}
			}
		}
	}

	if releaseLock {
		if err := wsHandler.ReleaseWriter(ctx, namespaceID, wsID, workflowID, runID); err != nil {
			t.logger.Warn("Failed to release workspace writer",
				tag.WorkflowNamespaceID(namespaceID),
				tag.NewStringTag("workspace-id", wsID),
				tag.Error(err),
			)
		}
	}
}
