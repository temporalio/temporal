package rulebook

import (
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/entity"
)

// SpeculativeTaskCreationRule detects when a speculative workflow task is
// created while a normal (non-speculative) task already exists for the same
// workflow. The update should be delivered on the existing normal task instead.
type SpeculativeTaskCreationRule struct{}

func (m *SpeculativeTaskCreationRule) Name() string {
	return "SpeculativeTaskCreationRule"
}

func (m *SpeculativeTaskCreationRule) CheckSafety(c *umpire.SafetyContext) {
	// Group workflow tasks by (workflowID, runID).
	type taskPair struct {
		hasNormal      bool
		hasSpeculative bool
		normalState    string
		specState      string
	}
	byWorkflow := make(map[string]*taskPair)

	for r := range umpire.ChangedEntities[entity.WorkflowTask](c) {
		wt := r.Entity
		if wt.WorkflowID == "" {
			continue
		}
		// Only consider tasks that are still pending (added or stored, not yet polled).
		state := wt.FSM.Current()
		if state != "added" && state != "stored" {
			continue
		}
		// Group by workflow identity, not entity key — two tasks (normal + speculative)
		// for the same workflow have different entity keys.
		wfKey := wt.WorkflowID + ":" + wt.RunID
		pair, exists := byWorkflow[wfKey]
		if !exists {
			pair = &taskPair{}
			byWorkflow[wfKey] = pair
		}
		if wt.IsSpeculative {
			pair.hasSpeculative = true
			pair.specState = state
		} else {
			pair.hasNormal = true
			pair.normalState = state
		}
	}

	for key, pair := range byWorkflow {
		c.Eval(key, !pair.hasNormal || !pair.hasSpeculative, umpire.Violation{
			Message: "speculative workflow task created while a normal task already exists — update should use existing task",
			Tags: map[string]string{
				"workflowKey":     key,
				"normalTaskState": pair.normalState,
				"specTaskState":   pair.specState,
			},
		})
	}
}
