package rulebook

import (
	"time"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/entity"
)

// WorkflowUpdateContinueAsNewRule detects updates that remain admitted
// on a completed workflow. In a continue-as-new scenario, the SDK should
// retry the update on the new run. If the update stays admitted on the old
// (completed) run, the retry mechanism may have failed.
type WorkflowUpdateContinueAsNewRule struct{}

func (m *WorkflowUpdateContinueAsNewRule) Name() string {
	return "WorkflowUpdateContinueAsNewRule"
}

func (m *WorkflowUpdateContinueAsNewRule) CheckLiveness(c *umpire.LivenessContext) {
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
		// Only care about updates still admitted on a completed workflow.
		if wu.FSM.Current() != "admitted" || wu.AdmittedAt.IsZero() {
			c.Resolve(r.Key)
			continue
		}
		if _, closed := completedWorkflows[wu.WorkflowID]; !closed {
			continue
		}
		c.Pending(r.Key, umpire.Violation{
			Message: "update admitted on completed workflow was not retried to new run — possible continue-as-new retry failure",
			Tags: map[string]string{
				"workflowID": wu.WorkflowID,
				"updateID":   wu.UpdateID,
			},
		})
	}
}
