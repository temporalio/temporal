package rulebook

import (
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/entity"
)

// WorkflowUpdateCompletionRule detects updates stuck in "accepted" state.
type WorkflowUpdateCompletionRule struct{}

func (m *WorkflowUpdateCompletionRule) Name() string {
	return "WorkflowUpdateCompletionRule"
}

func (m *WorkflowUpdateCompletionRule) CheckLiveness(c *umpire.LivenessContext) {
	for r := range umpire.ChangedEntities[entity.WorkflowUpdate](c) {
		wu := r.Entity
		if wu.UpdateID == "" {
			continue
		}
		if wu.FSM.Current() != "accepted" || wu.AcceptedAt.IsZero() {
			c.Resolve(r.Key)
			continue
		}
		c.Pending(r.Key, umpire.Violation{
			Message: "workflow update accepted but not completed within expected window",
			Tags: map[string]string{
				"workflowID": wu.WorkflowID,
				"updateID":   wu.UpdateID,
			},
		})
	}
}
