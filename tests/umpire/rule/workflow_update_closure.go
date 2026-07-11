package rule

import (
	"time"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/entity"
)

// WorkflowUpdateClosure checks that no update transitions occur after
// the parent workflow has reached a terminal state.
type WorkflowUpdateClosure struct{}

func (m *WorkflowUpdateClosure) Name() string {
	return "WorkflowUpdateClosureRule"
}

func (m *WorkflowUpdateClosure) CheckSafety(c *umpire.SafetyContext) {
	// Build set of completed workflow IDs with their completion times.
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
		violated := false
		// Check if update accepted after workflow closed.
		if !wu.AcceptedAt.IsZero() && wu.AcceptedAt.After(closedAt) {
			violated = true
			c.Eval(r.Key+":accepted-after-close", false, umpire.Violation{
				Message: "workflow update accepted after workflow closed",
				Tags: map[string]string{
					"workflowID": wu.WorkflowID,
					"updateID":   wu.UpdateID,
					"closedAt":   closedAt.Format(time.RFC3339),
					"acceptedAt": wu.AcceptedAt.Format(time.RFC3339),
				},
			})
		}
		// Check if update completed after workflow closed.
		if !wu.CompletedAt.IsZero() && wu.CompletedAt.After(closedAt) {
			violated = true
			c.Eval(r.Key+":completed-after-close", false, umpire.Violation{
				Message: "workflow update completed after workflow closed",
				Tags: map[string]string{
					"workflowID":  wu.WorkflowID,
					"updateID":    wu.UpdateID,
					"closedAt":    closedAt.Format(time.RFC3339),
					"completedAt": wu.CompletedAt.Format(time.RFC3339),
				},
			})
		}
		if !violated {
			c.Pass(r.Key)
		}
	}
}
