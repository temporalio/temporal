package rule

import (
	"fmt"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/entity"
)

// WorkflowUpdateDeduplication detects duplicate update requests not resolved.
type WorkflowUpdateDeduplication struct{}

func (m *WorkflowUpdateDeduplication) Name() string {
	return "WorkflowUpdateDeduplicationRule"
}

func (m *WorkflowUpdateDeduplication) CheckLiveness(c *umpire.LivenessContext) {
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
			Message: fmt.Sprintf("deduplicated update (requested %d times) stuck in %q — duplicate request not resolved, dedup may be blocking progress", wu.RequestCount, state),
			Tags: map[string]string{
				"workflowID":   wu.WorkflowID,
				"updateID":     wu.UpdateID,
				"requestCount": fmt.Sprintf("%d", wu.RequestCount),
				"currentState": state,
			},
		})
	}
}
