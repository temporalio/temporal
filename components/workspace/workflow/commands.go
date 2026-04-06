package workflow

import (
	"context"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	workspacepb "go.temporal.io/api/workspace/v1"
	"go.temporal.io/server/chasm"
	chasmworkspace "go.temporal.io/server/chasm/lib/workspace"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
)

const (
	// WriteAccessRevokedSentinel is stored in ActiveWriterScheduledId to indicate
	// that write access was transferred to another workflow (child) via Handoff.
	// The local snapshot is still valid for read-only access, but read-write
	// access requires re-acquiring from the standalone CHASM workspace.
	WriteAccessRevokedSentinel int64 = -1
)

// WorkspaceOption configures EnsureWorkspaceForActivity behavior.
type WorkspaceOption func(*workspaceOptions)

type workspaceOptions struct {
	handler     *chasmworkspace.Handler
	chasmEngine chasm.Engine
	namespaceID string
	ctx         context.Context
}

// WithWorkspaceHandler enables V2 standalone CHASM fallback for cross-workflow
// workspace lookup (child workflows, transfers).
func WithWorkspaceHandler(ctx context.Context, h *chasmworkspace.Handler, engine chasm.Engine, namespaceID string) WorkspaceOption {
	return func(o *workspaceOptions) {
		o.handler = h
		o.chasmEngine = engine
		o.namespaceID = namespaceID
		o.ctx = ctx
	}
}

// SetForkTarget records the target workspace ID for a pending Fork transfer.
// The transfer task will use this to call ForkWorkspace on the standalone CHASM.
func SetForkTarget(ms historyi.MutableState, workspaceID, targetWorkspaceID string) {
	executionInfo := ms.GetExecutionInfo()
	if executionInfo.WorkspaceInfos == nil {
		return
	}
	ws, exists := executionInfo.WorkspaceInfos[workspaceID]
	if !exists {
		return
	}
	ws.ForkTargetWorkspaceId = targetWorkspaceID
}

// RevokeWriteAccess marks a workspace as having lost write access (transferred to
// another workflow via Handoff). The local snapshot is preserved for read-only use.
func RevokeWriteAccess(ms historyi.MutableState, workspaceID string) {
	executionInfo := ms.GetExecutionInfo()
	if executionInfo.WorkspaceInfos == nil {
		return
	}
	ws, exists := executionInfo.WorkspaceInfos[workspaceID]
	if !exists {
		return
	}
	ws.ActiveWriterScheduledId = WriteAccessRevokedSentinel
}

// IsForkTransfer returns true if the workspace was received via Fork transfer.
func IsForkTransfer(ws *workspacepb.WorkspaceInfo) bool {
	return ws.GetTransferMode() == enumspb.WORKSPACE_TRANSFER_MODE_FORK
}

// EnsureWorkspaceForActivity ensures the workspace exists in
// ExecutionInfo.WorkspaceInfos, creating it lazily on first use.
//
// When write access was previously revoked (Handoff to a child) and the caller
// requests read-write access, the function re-acquires write access from the
// standalone CHASM workspace, which returns the latest committed version
// (including changes made by the child).
//
// When write access was revoked but the caller requests read-only access,
// the local snapshot (from before the transfer) is returned.
func EnsureWorkspaceForActivity(
	ms historyi.MutableState,
	workspaceID string,
	accessMode enumspb.WorkspaceAccessMode,
	opts ...WorkspaceOption,
) (*workspacepb.WorkspaceInfo, error) {
	var cfg workspaceOptions
	for _, o := range opts {
		o(&cfg)
	}

	executionInfo := ms.GetExecutionInfo()
	if executionInfo.WorkspaceInfos == nil {
		executionInfo.WorkspaceInfos = make(map[string]*workspacepb.WorkspaceInfo)
	}

	mode := normalizeAccessMode(accessMode)

	ws, exists := executionInfo.WorkspaceInfos[workspaceID]
	if exists {
		if ws.ActiveWriterScheduledId == WriteAccessRevokedSentinel && mode == enumspb.WORKSPACE_ACCESS_MODE_READ_WRITE {
			// Write access was revoked (Handoff). Re-acquire from standalone.
			if cfg.handler != nil && cfg.chasmEngine != nil && cfg.namespaceID != "" && cfg.ctx != nil {
				engineCtx := chasm.NewEngineContext(cfg.ctx, cfg.chasmEngine)
				wfID := executionInfo.GetWorkflowId()
				runID := ms.GetExecutionState().GetRunId()

				wsInfo, err := cfg.handler.AcquireWriter(engineCtx, cfg.namespaceID, workspaceID, wfID, runID)
				if err != nil {
					return nil, workflow.FailWorkflowTaskError{
						Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES,
						Message: fmt.Sprintf("workspace %q: failed to re-acquire write access: %v", workspaceID, err),
					}
				}
				wsInfo.ActiveWriterScheduledId = 0
				wsInfo.TransferMode = enumspb.WORKSPACE_TRANSFER_MODE_HANDOFF
				executionInfo.WorkspaceInfos[workspaceID] = wsInfo
				return wsInfo, nil
			}
			return nil, workflow.FailWorkflowTaskError{
				Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES,
				Message: fmt.Sprintf("workspace %q: write access was revoked but no standalone handler available", workspaceID),
			}
		}
		return ws, nil
	}

	// Workspace not found locally. Try standalone CHASM (cross-workflow access).
	if cfg.handler != nil && cfg.chasmEngine != nil && cfg.namespaceID != "" && cfg.ctx != nil {
		engineCtx := chasm.NewEngineContext(cfg.ctx, cfg.chasmEngine)
		wsInfo, err := cfg.handler.GetWorkspaceInfo(engineCtx, cfg.namespaceID, workspaceID)
		if err == nil && wsInfo != nil {
			// Determine transfer mode by probing the standalone lock.
			// If we can acquire → Handoff (parent released, child owns lock).
			// If locked by parent → Fork (child has its own forked standalone).
			wfID := executionInfo.GetWorkflowId()
			runID := ms.GetExecutionState().GetRunId()
			if _, acquireErr := cfg.handler.AcquireWriter(engineCtx, cfg.namespaceID, workspaceID, wfID, runID); acquireErr != nil {
				wsInfo.TransferMode = enumspb.WORKSPACE_TRANSFER_MODE_FORK
			} else {
				wsInfo.TransferMode = enumspb.WORKSPACE_TRANSFER_MODE_HANDOFF
			}
			executionInfo.WorkspaceInfos[workspaceID] = wsInfo
			return wsInfo, nil
		}
	}

	// Lazy creation.
	ws = &workspacepb.WorkspaceInfo{
		WorkspaceId:      workspaceID,
		CommittedVersion: 0,
	}
	executionInfo.WorkspaceInfos[workspaceID] = ws
	return ws, nil
}

// normalizeAccessMode returns READ_WRITE for UNSPECIFIED, otherwise the mode as-is.
func normalizeAccessMode(mode enumspb.WorkspaceAccessMode) enumspb.WorkspaceAccessMode {
	if mode == enumspb.WORKSPACE_ACCESS_MODE_UNSPECIFIED {
		return enumspb.WORKSPACE_ACCESS_MODE_READ_WRITE
	}
	return mode
}

// hasActiveWriter returns true if a real activity (positive event ID) holds write access.
func hasActiveWriter(ws *workspacepb.WorkspaceInfo) bool {
	return ws.ActiveWriterScheduledId > 0
}

// ValidateWorkspaceAccess checks whether the requested access mode is compatible
// with the current workspace lock state.
func ValidateWorkspaceAccess(
	ws *workspacepb.WorkspaceInfo,
	accessMode enumspb.WorkspaceAccessMode,
) error {
	mode := normalizeAccessMode(accessMode)

	switch mode {
	case enumspb.WORKSPACE_ACCESS_MODE_READ_WRITE:
		if hasActiveWriter(ws) {
			return workflow.FailWorkflowTaskError{
				Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES,
				Message: fmt.Sprintf("workspace %q is in use by writer activity (scheduled event ID %d)", ws.WorkspaceId, ws.ActiveWriterScheduledId),
			}
		}
		if len(ws.ActiveReaderScheduledIds) > 0 {
			return workflow.FailWorkflowTaskError{
				Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES,
				Message: fmt.Sprintf("workspace %q is in use by %d reader activity(s)", ws.WorkspaceId, len(ws.ActiveReaderScheduledIds)),
			}
		}
	case enumspb.WORKSPACE_ACCESS_MODE_READ_ONLY:
		if hasActiveWriter(ws) {
			return workflow.FailWorkflowTaskError{
				Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES,
				Message: fmt.Sprintf("workspace %q is in use by writer activity (scheduled event ID %d)", ws.WorkspaceId, ws.ActiveWriterScheduledId),
			}
		}
	}
	return nil
}

// AcquireWorkspaceAccess records the activity's scheduled event ID in the
// workspace lock state.
func AcquireWorkspaceAccess(
	ws *workspacepb.WorkspaceInfo,
	accessMode enumspb.WorkspaceAccessMode,
	scheduledEventID int64,
) {
	mode := normalizeAccessMode(accessMode)
	switch mode {
	case enumspb.WORKSPACE_ACCESS_MODE_READ_WRITE:
		ws.ActiveWriterScheduledId = scheduledEventID
	case enumspb.WORKSPACE_ACCESS_MODE_READ_ONLY:
		ws.ActiveReaderScheduledIds = append(ws.ActiveReaderScheduledIds, scheduledEventID)
	}
}

// ReleaseWorkspaceAccess removes the activity's scheduled event ID from the
// workspace lock state.
func ReleaseWorkspaceAccess(
	ms historyi.MutableState,
	workspaceID string,
	scheduledEventID int64,
) {
	if workspaceID == "" {
		return
	}
	executionInfo := ms.GetExecutionInfo()
	if executionInfo.WorkspaceInfos == nil {
		return
	}
	ws, exists := executionInfo.WorkspaceInfos[workspaceID]
	if !exists {
		return
	}

	if ws.ActiveWriterScheduledId == scheduledEventID {
		ws.ActiveWriterScheduledId = 0
	}
	for i, id := range ws.ActiveReaderScheduledIds {
		if id == scheduledEventID {
			ws.ActiveReaderScheduledIds = append(ws.ActiveReaderScheduledIds[:i], ws.ActiveReaderScheduledIds[i+1:]...)
			break
		}
	}
}
