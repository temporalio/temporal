package rulebook

import (
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/entity"
)

// WorkflowUpdateLossPreventionRule detects updates stuck in "admitted" state.
type WorkflowUpdateLossPreventionRule struct{}

func (m *WorkflowUpdateLossPreventionRule) Name() string {
	return "WorkflowUpdateLossPreventionRule"
}

func (m *WorkflowUpdateLossPreventionRule) CheckLiveness(c *umpire.LivenessContext) {
	for r := range umpire.ChangedEntities[entity.WorkflowUpdate](c) {
		wu := r.Entity
		if wu.UpdateID == "" {
			continue
		}
		if wu.FSM.Current() != "admitted" || wu.AdmittedAt.IsZero() {
			c.Resolve(r.Key)
			continue
		}
		c.Pending(r.Key, umpire.Violation{
			Message: "workflow update admitted but never accepted — may be lost",
			Tags: map[string]string{
				"workflowID": wu.WorkflowID,
				"updateID":   wu.UpdateID,
			},
		})
	}
}
