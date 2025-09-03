package workflow

import (
	"context"

	"go.temporal.io/server/common/persistence"
)

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination transaction_mock.go
type (
	Transaction interface {
		CreateWorkflowExecution(
			ctx context.Context,
			createMode persistence.CreateWorkflowMode,
			newWorkflowFailoverVersion int64,
			newWorkflowSnapshot *persistence.WorkflowSnapshot,
			newWorkflowEventsSeq []*persistence.WorkflowEvents,
			isWorkflow bool,
		) (int64, error)

		ConflictResolveWorkflowExecution(
			ctx context.Context,
			conflictResolveMode persistence.ConflictResolveWorkflowMode,
			resetWorkflowFailoverVersion int64,
			resetWorkflowSnapshot *persistence.WorkflowSnapshot,
			resetWorkflowEventsSeq []*persistence.WorkflowEvents,
			newWorkflowFailoverVersion *int64,
			newWorkflowSnapshot *persistence.WorkflowSnapshot,
			newWorkflowEventsSeq []*persistence.WorkflowEvents,
			currentWorkflowFailoverVersion *int64,
			currentWorkflowMutation *persistence.WorkflowMutation,
			currentWorkflowEventsSeq []*persistence.WorkflowEvents,
			isWorkflow bool,
		) (int64, int64, int64, error)

		UpdateWorkflowExecution(
			ctx context.Context,
			updateMode persistence.UpdateWorkflowMode,
			currentWorkflowFailoverVersion int64,
			currentWorkflowMutation *persistence.WorkflowMutation,
			currentWorkflowEventsSeq []*persistence.WorkflowEvents,
			newWorkflowFailoverVersion *int64,
			newWorkflowSnapshot *persistence.WorkflowSnapshot,
			newWorkflowEventsSeq []*persistence.WorkflowEvents,
			isWorkflow bool,
		) (int64, int64, error)

		SetWorkflowExecution(
			ctx context.Context,
			workflowSnapshot *persistence.WorkflowSnapshot,
		) error
	}
)
