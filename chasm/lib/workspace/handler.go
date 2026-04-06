package workspace

import (
	"context"

	apiworkspacepb "go.temporal.io/api/workspace/v1"
	"go.temporal.io/server/chasm"
	workspacepb "go.temporal.io/server/chasm/lib/workspace/gen/workspacepb/v1"
	"go.temporal.io/server/common/log"
)

// Handler implements workspace operations via the CHASM engine.
type Handler struct {
	config *Config
	logger log.Logger
}

func newHandler(config *Config, logger log.Logger) *Handler {
	return &Handler{
		config: config,
		logger: logger,
	}
}

// CreateWorkspace creates a new standalone workspace CHASM execution.
func (h *Handler) CreateWorkspace(
	ctx context.Context,
	namespaceID string,
	workspaceID string,
) error {
	_, err := chasm.StartExecution(
		ctx,
		chasm.ExecutionKey{
			NamespaceID: namespaceID,
			BusinessID:  workspaceID,
		},
		func(mctx chasm.MutableContext, req *workspacepb.CreateWorkspaceRequest) (*Workspace, error) {
			return NewWorkspace(mctx, req)
		},
		&workspacepb.CreateWorkspaceRequest{
			NamespaceId: namespaceID,
			WorkspaceId: workspaceID,
		},
		chasm.WithRequestID(workspaceID),
	)
	return err
}

// AcquireWriter acquires the writer lock and returns current workspace state.
func (h *Handler) AcquireWriter(
	ctx context.Context,
	namespaceID string,
	workspaceID string,
	workflowID string,
	runID string,
) (*apiworkspacepb.WorkspaceInfo, error) {
	ref := chasm.NewComponentRef[*Workspace](chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  workspaceID,
	})

	wsInfo, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		func(w *Workspace, mctx chasm.MutableContext, event acquireWriterEvent) (*apiworkspacepb.WorkspaceInfo, error) {
			if err := TransitionAcquireWriter.Apply(w, mctx, event); err != nil {
				return nil, err
			}
			info := w.ToWorkspaceInfo(mctx)
			info.WorkspaceId = workspaceID
			return info, nil
		},
		acquireWriterEvent{
			WorkflowID: workflowID,
			RunID:      runID,
		},
	)
	return wsInfo, err
}

// CommitVersion advances the workspace version (batch commit for checkin).
func (h *Handler) CommitVersion(
	ctx context.Context,
	namespaceID string,
	workspaceID string,
	workflowID string,
	runID string,
	newVersion int64,
	diffs []*apiworkspacepb.DiffRecord,
) error {
	ref := chasm.NewComponentRef[*Workspace](chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  workspaceID,
	})

	_, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		func(w *Workspace, mctx chasm.MutableContext, event commitEvent) (any, error) {
			return nil, TransitionCommit.Apply(w, mctx, event)
		},
		commitEvent{
			WorkflowID:  workflowID,
			RunID:       runID,
			NewVersion:  newVersion,
			DiffRecords: diffs,
		},
	)
	return err
}

// ReleaseWriter releases the writer lock.
func (h *Handler) ReleaseWriter(
	ctx context.Context,
	namespaceID string,
	workspaceID string,
	workflowID string,
	runID string,
) error {
	ref := chasm.NewComponentRef[*Workspace](chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  workspaceID,
	})

	_, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		func(w *Workspace, mctx chasm.MutableContext, event releaseWriterEvent) (any, error) {
			return nil, TransitionReleaseWriter.Apply(w, mctx, event)
		},
		releaseWriterEvent{
			WorkflowID: workflowID,
			RunID:      runID,
		},
	)
	return err
}

// GetWorkspaceInfo reads the workspace state without modifying it.
func (h *Handler) GetWorkspaceInfo(
	ctx context.Context,
	namespaceID string,
	workspaceID string,
) (*apiworkspacepb.WorkspaceInfo, error) {
	ref := chasm.NewComponentRef[*Workspace](chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  workspaceID,
	})

	wsInfo, err := chasm.ReadComponent(
		ctx,
		ref,
		func(w *Workspace, rctx chasm.Context, _ any) (*apiworkspacepb.WorkspaceInfo, error) {
			info := w.ToWorkspaceInfo(rctx)
			info.WorkspaceId = workspaceID
			return info, nil
		},
		nil,
	)
	return wsInfo, err
}

// ForkWorkspace creates a new standalone workspace from an existing one (zero-copy).
func (h *Handler) ForkWorkspace(
	ctx context.Context,
	namespaceID string,
	sourceWorkspaceID string,
	targetWorkspaceID string,
) error {
	// 1. Read source workspace state.
	sourceRef := chasm.NewComponentRef[*Workspace](chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  sourceWorkspaceID,
	})

	sourceState, err := chasm.ReadComponent(
		ctx,
		sourceRef,
		func(w *Workspace, rctx chasm.Context, _ any) (*workspacepb.WorkspaceState, error) {
			return w.WorkspaceState, nil
		},
		nil,
	)
	if err != nil {
		return err
	}

	// 2. Create new workspace with same diff chain.
	_, err = chasm.StartExecution(
		ctx,
		chasm.ExecutionKey{
			NamespaceID: namespaceID,
			BusinessID:  targetWorkspaceID,
		},
		func(mctx chasm.MutableContext, state *workspacepb.WorkspaceState) (*Workspace, error) {
			return NewForkedWorkspace(mctx, state, sourceWorkspaceID)
		},
		sourceState,
		chasm.WithRequestID(targetWorkspaceID),
	)
	return err
}

// CloseWorkspace closes a workspace (terminal state).
func (h *Handler) CloseWorkspace(
	ctx context.Context,
	namespaceID string,
	workspaceID string,
) error {
	ref := chasm.NewComponentRef[*Workspace](chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  workspaceID,
	})

	_, _, err := chasm.UpdateComponent(
		ctx,
		ref,
		func(w *Workspace, mctx chasm.MutableContext, event closeEvent) (any, error) {
			return nil, TransitionClose.Apply(w, mctx, event)
		},
		closeEvent{},
	)
	return err
}
