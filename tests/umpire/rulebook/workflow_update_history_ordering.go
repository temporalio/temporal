package rulebook

import (
	"time"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/entity"
)

// WorkflowUpdateHistoryOrderingRule verifies that when a workflow
// completes on the same task that carries an update, the update reaches a
// terminal state (completed or rejected) before or at the time of workflow
// completion. An update left in accepted state after workflow closure indicates
// inconsistent history ordering.
type WorkflowUpdateHistoryOrderingRule struct{}

func (m *WorkflowUpdateHistoryOrderingRule) Name() string {
	return "WorkflowUpdateHistoryOrderingRule"
}

func (m *WorkflowUpdateHistoryOrderingRule) CheckSafety(c *umpire.SafetyContext) {
	// Build set of completed workflows.
	completedWorkflows := make(map[string]time.Time)
	for r := range umpire.ChangedEntities[entity.Workflow](c) {
		wf := r.Entity
		if wf.WorkflowID == "" {
			continue
		}
		if wf.FSM.Current() == "completed" && !wf.CompletedAt.IsZero() {
			completedWorkflows[wf.WorkflowID] = wf.CompletedAt
		}
	}

	for r := range umpire.ChangedEntities[entity.WorkflowUpdate](c) {
		wu := r.Entity
		if wu.WorkflowID == "" || wu.UpdateID == "" {
			continue
		}
		closedAt, closed := completedWorkflows[wu.WorkflowID]
		if !closed {
			continue
		}
		// An update in "accepted" state after workflow completion means the
		// history did not include the required Completed/Rejected event before
		// the terminal workflow event.
		c.Eval(r.Key, wu.FSM.Current() != "accepted", umpire.Violation{
			Message: "update stuck in accepted state after workflow completed — history should include update completion before workflow terminal event",
			Tags: map[string]string{
				"workflowID":    wu.WorkflowID,
				"updateID":      wu.UpdateID,
				"acceptedAt":    wu.AcceptedAt.Format(time.RFC3339),
				"wfCompletedAt": closedAt.Format(time.RFC3339),
			},
		})
	}
}
