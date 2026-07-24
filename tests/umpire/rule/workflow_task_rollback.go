package rule

import (
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/model"
)

// SpeculativeTaskRollback detects updates accepted on speculative tasks but never completed.
type SpeculativeTaskRollback struct{}

func (m *SpeculativeTaskRollback) Name() string {
	return "SpeculativeTaskRollbackRule"
}

func (m *SpeculativeTaskRollback) CheckLiveness(c *umpire.LivenessContext) {
	speculativePolled := make(map[string]bool)
	for r := range c.Changed[model.WorkflowTask]() {
		wt := r.Entity
		if !wt.IsSpeculative || wt.WorkflowID == "" {
			continue
		}
		if wt.FSM.Current() == "polled" {
			speculativePolled[wt.WorkflowID] = true
		}
	}
	if len(speculativePolled) == 0 {
		return
	}

	for r := range c.Changed[model.WorkflowUpdate]() {
		wu := r.Entity
		if wu.UpdateID == "" || wu.WorkflowID == "" {
			continue
		}
		if wu.FSM.Current() != "accepted" || wu.AcceptedAt().IsZero() {
			c.Resolve(r.Key)
			continue
		}
		if !speculativePolled[wu.WorkflowID] {
			continue
		}
		c.Pending(r.Key, umpire.Violation{
			Message: "update accepted on speculative workflow task but never completed — possible silent rollback",
			Tags: map[string]string{
				"workflowID": wu.WorkflowID,
				"updateID":   wu.UpdateID,
			},
		})
	}
}
