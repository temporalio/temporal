package workspace

import (
	"fmt"

	apiworkspacepb "go.temporal.io/api/workspace/v1"
	"go.temporal.io/server/chasm"
	workspacepb "go.temporal.io/server/chasm/lib/workspace/gen/workspacepb/v1"
)

const (
	libraryName   = "workspace"
	componentName = "workspace"
)

var (
	Archetype   = chasm.FullyQualifiedName(libraryName, componentName)
	ArchetypeID = chasm.GenerateTypeID(Archetype)
)

// Workspace is the standalone CHASM root component for durable workspace state.
// It tracks the version chain, writer lock, and fork lineage. The actual
// filesystem data (chunks, manifests) lives in external storage — the component
// only holds metadata (storage claims).
type Workspace struct {
	chasm.UnimplementedComponent
	*workspacepb.WorkspaceState

	WriterLock chasm.Field[*workspacepb.WorkspaceWriterLock]
	ForkInfo   chasm.Field[*workspacepb.WorkspaceForkInfo]
	Visibility chasm.Field[*chasm.Visibility]
}

// StateMachineState returns the workspace execution status.
func (w *Workspace) StateMachineState() workspacepb.WorkspaceExecutionStatus {
	return w.Status
}

// SetStateMachineState sets the workspace execution status.
func (w *Workspace) SetStateMachineState(state workspacepb.WorkspaceExecutionStatus) {
	w.Status = state
}

// LifecycleState maps workspace status to CHASM lifecycle state.
func (w *Workspace) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	switch w.Status {
	case workspacepb.WORKSPACE_EXECUTION_STATUS_CLOSED:
		return chasm.LifecycleStateCompleted
	default:
		return chasm.LifecycleStateRunning
	}
}

// Terminate implements the chasm.RootComponent interface.
func (w *Workspace) Terminate(
	_ chasm.MutableContext,
	_ chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, nil
}

// NewWorkspace creates a new standalone workspace from a create request.
func NewWorkspace(ctx chasm.MutableContext, req *workspacepb.CreateWorkspaceRequest) (*Workspace, error) {
	ws := &Workspace{
		WorkspaceState: &workspacepb.WorkspaceState{
			CommittedVersion: 0,
			Status:           workspacepb.WORKSPACE_EXECUTION_STATUS_ACTIVE,
		},
		Visibility: chasm.NewComponentField(ctx, chasm.NewVisibility(ctx)),
	}
	return ws, nil
}

// NewForkedWorkspace creates a new standalone workspace forked from an existing one.
func NewForkedWorkspace(
	ctx chasm.MutableContext,
	sourceState *workspacepb.WorkspaceState,
	sourceWorkspaceID string,
) (*Workspace, error) {
	ws := &Workspace{
		WorkspaceState: &workspacepb.WorkspaceState{
			CommittedVersion: sourceState.CommittedVersion,
			Diffs:            sourceState.Diffs,
			BaseSnapshot:     sourceState.BaseSnapshot,
			Status:           workspacepb.WORKSPACE_EXECUTION_STATUS_ACTIVE,
		},
		ForkInfo: chasm.NewDataField(ctx, &workspacepb.WorkspaceForkInfo{
			SourceWorkspaceId: sourceWorkspaceID,
			ForkedAtVersion:   sourceState.CommittedVersion,
		}),
		Visibility: chasm.NewComponentField(ctx, chasm.NewVisibility(ctx)),
	}
	return ws, nil
}

// ToWorkspaceInfo converts the standalone workspace state to a WorkspaceInfo
// message suitable for sending to workers.
func (w *Workspace) ToWorkspaceInfo(ctx chasm.Context) *apiworkspacepb.WorkspaceInfo {
	return &apiworkspacepb.WorkspaceInfo{
		WorkspaceId:      "", // Filled in by caller from execution key.
		CommittedVersion: w.CommittedVersion,
		BaseSnapshot:     w.BaseSnapshot,
		Diffs:            w.Diffs,
	}
}

// ValidateWriter checks that the given workflow holds the writer lock.
func (w *Workspace) ValidateWriter(ctx chasm.Context, workflowID, runID string) error {
	lock, ok := w.WriterLock.TryGet(ctx)
	if !ok || lock == nil {
		return fmt.Errorf("workspace has no writer lock")
	}
	if lock.WorkflowId != workflowID {
		return fmt.Errorf("workspace writer lock held by workflow %s, not %s",
			lock.WorkflowId, workflowID)
	}
	return nil
}
