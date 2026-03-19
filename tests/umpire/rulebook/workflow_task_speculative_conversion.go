package rulebook

import (
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/entity"
)

// SpeculativeConversionRule detects updates that remain stuck in admitted
// state after a speculative workflow task was converted to normal (stored).
// The conversion should not lose the update — it should still progress to
// accepted/completed/rejected.
type SpeculativeConversionRule struct{}

func (m *SpeculativeConversionRule) Name() string {
	return "SpeculativeConversionRule"
}

func (m *SpeculativeConversionRule) CheckLiveness(c *umpire.LivenessContext) {
	// Find workflows where a speculative task was converted (stored).
	converted := make(map[string]bool)
	for r := range umpire.ChangedEntities[entity.WorkflowTask](c) {
		wt := r.Entity
		if wt.WorkflowID == "" {
			continue
		}
		// A speculative task that reached "stored" state was converted to normal.
		if wt.IsSpeculative && wt.FSM.Current() == "stored" {
			converted[wt.WorkflowID] = true
		}
	}
	if len(converted) == 0 {
		return
	}

	for r := range umpire.ChangedEntities[entity.WorkflowUpdate](c) {
		wu := r.Entity
		if wu.WorkflowID == "" || wu.UpdateID == "" {
			continue
		}
		// Only care about updates stuck in admitted.
		if wu.FSM.Current() != "admitted" || wu.AdmittedAt.IsZero() {
			c.Resolve(r.Key)
			continue
		}
		if !converted[wu.WorkflowID] {
			continue
		}

		c.Pending(r.Key, umpire.Violation{
			Message: "update stuck in admitted after speculative task converted to normal — conversion may have lost the update",
			Tags: map[string]string{
				"workflowID": wu.WorkflowID,
				"updateID":   wu.UpdateID,
			},
		})
	}
}
