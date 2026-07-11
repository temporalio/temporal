package rule

import (
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/entity"
)

// WorkflowUpdateLossPrevention detects updates stuck in "admitted" state.
type WorkflowUpdateLossPrevention struct{}

func (m *WorkflowUpdateLossPrevention) Name() string {
	return "WorkflowUpdateLossPreventionRule"
}

func (m *WorkflowUpdateLossPrevention) CheckLiveness(c *umpire.LivenessContext) {
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
