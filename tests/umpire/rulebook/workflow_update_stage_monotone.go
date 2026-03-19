package rulebook

import (
	"fmt"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/entity"
)

// WorkflowUpdateStageMonotoneRule checks that update FSM states only move
// forward (admitted → accepted → completed/rejected), never backwards.
type WorkflowUpdateStageMonotoneRule struct {
	// highWater tracks the highest observed FSM state per update.
	highWater map[string]int
}

// workflowUpdateStageOrder maps FSM state names to monotonically increasing integers.
var workflowUpdateStageOrder = map[string]int{
	"unspecified": 0,
	"admitted":    1,
	"accepted":    2,
	"completed":   3,
	"rejected":    3, // terminal, same level as completed
	"aborted":     3, // terminal, same level as completed
}

func (m *WorkflowUpdateStageMonotoneRule) Name() string {
	return "WorkflowUpdateStageMonotoneRule"
}

func (m *WorkflowUpdateStageMonotoneRule) CheckSafety(c *umpire.SafetyContext) {
	if m.highWater == nil {
		m.highWater = make(map[string]int)
	}

	for r := range umpire.ChangedEntities[entity.WorkflowUpdate](c) {
		wu := r.Entity
		if wu.UpdateID == "" {
			continue
		}

		currentStage := workflowUpdateStageOrder[wu.FSM.Current()]
		prev, seen := m.highWater[r.Key]

		if seen && currentStage < prev {
			c.Eval(r.Key+":regression", false, umpire.Violation{
				Message: fmt.Sprintf(
					"update stage regressed from %d to %d (state=%s)",
					prev, currentStage, wu.FSM.Current(),
				),
				Tags: map[string]string{
					"workflowID": wu.WorkflowID,
					"updateID":   wu.UpdateID,
					"state":      wu.FSM.Current(),
				},
			})
		} else {
			c.Pass(r.Key)
		}

		if currentStage > prev || !seen {
			m.highWater[r.Key] = currentStage
		}
	}
}
