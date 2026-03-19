package rulebook

import (
	"fmt"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/entity"
)

// WorkflowUpdateContextClearRule detects updates stranded in a
// non-terminal state when no pending workflow task exists to drive them forward.
// This covers both stale-token scenarios and context-clear scenarios
// where the update registry is lost and no new task is scheduled.
type WorkflowUpdateContextClearRule struct{}

func (m *WorkflowUpdateContextClearRule) Name() string {
	return "WorkflowUpdateContextClearRule"
}

func (m *WorkflowUpdateContextClearRule) CheckLiveness(c *umpire.LivenessContext) {
	// Build set of workflows that have a pending (non-polled) task.
	workflowsWithPendingTask := make(map[string]bool)
	for r := range umpire.ChangedEntities[entity.WorkflowTask](c) {
		wt := r.Entity
		if wt.WorkflowID == "" {
			continue
		}
		state := wt.FSM.Current()
		if state == "added" || state == "stored" {
			workflowsWithPendingTask[wt.WorkflowID] = true
		}
	}

	// Build set of completed workflows.
	completedWorkflows := make(map[string]bool)
	for r := range umpire.ChangedEntities[entity.Workflow](c) {
		wf := r.Entity
		if wf.WorkflowID == "" {
			continue
		}
		if wf.FSM.Current() == "completed" {
			completedWorkflows[wf.WorkflowID] = true
		}
	}

	for r := range umpire.ChangedEntities[entity.WorkflowUpdate](c) {
		wu := r.Entity
		if wu.WorkflowID == "" || wu.UpdateID == "" {
			continue
		}
		// Only care about non-terminal updates.
		state := wu.FSM.Current()
		if state != "admitted" && state != "accepted" {
			c.Resolve(r.Key)
			continue
		}
		// Skip if workflow is completed (closure rule handles that).
		if completedWorkflows[wu.WorkflowID] {
			continue
		}
		// Skip if there's a pending task that could drive this update.
		if workflowsWithPendingTask[wu.WorkflowID] {
			c.Resolve(r.Key)
			continue
		}
		c.Pending(r.Key, umpire.Violation{
			Message: fmt.Sprintf("update stranded in %q with no pending workflow task — possible context clear or stale token", state),
			Tags: map[string]string{
				"workflowID":  wu.WorkflowID,
				"updateID":    wu.UpdateID,
				"updateState": state,
			},
		})
	}
}
