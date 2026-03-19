package rulebook

import (
	"fmt"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/entity"
)

// WorkflowUpdateDeduplicationRule detects duplicate update requests not resolved.
type WorkflowUpdateDeduplicationRule struct{}

func (m *WorkflowUpdateDeduplicationRule) Name() string {
	return "WorkflowUpdateDeduplicationRule"
}

func (m *WorkflowUpdateDeduplicationRule) CheckLiveness(c *umpire.LivenessContext) {
	for r := range umpire.ChangedEntities[entity.WorkflowUpdate](c) {
		wu := r.Entity
		if wu.UpdateID == "" || wu.RequestCount <= 1 {
			continue
		}
		// Only flag updates that actually entered the lifecycle.
		// "unspecified" means only requested but never admitted — normal for retries.
		state := wu.FSM.Current()
		if state == "unspecified" || state == "completed" || state == "rejected" || state == "aborted" {
			c.Resolve(r.Key)
			continue
		}
		c.Pending(r.Key, umpire.Violation{
			Message: "duplicate update requests received but update not resolved",
			Tags: map[string]string{
				"workflowID":   wu.WorkflowID,
				"updateID":     wu.UpdateID,
				"requestCount": fmt.Sprintf("%d", wu.RequestCount),
				"currentState": state,
			},
		})
	}
}
