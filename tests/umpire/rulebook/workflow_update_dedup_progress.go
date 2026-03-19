package rulebook

import (
	"fmt"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/entity"
)

// WorkflowUpdateDedupProgressRule detects deduplicated updates that fail
// to make progress. When the same UpdateID is sent multiple times (RequestCount > 1),
// the duplicate should either return instantly with the known result or share
// the in-flight lifecycle. If a deduplicated update remains in a non-terminal
// state, deduplication logic may be blocking progress.
type WorkflowUpdateDedupProgressRule struct{}

func (m *WorkflowUpdateDedupProgressRule) Name() string {
	return "WorkflowUpdateDedupProgressRule"
}

func (m *WorkflowUpdateDedupProgressRule) CheckLiveness(c *umpire.LivenessContext) {
	for r := range umpire.ChangedEntities[entity.WorkflowUpdate](c) {
		wu := r.Entity
		if wu.WorkflowID == "" || wu.UpdateID == "" {
			continue
		}
		// Only care about deduplicated updates (multiple requests for same ID).
		if wu.RequestCount <= 1 {
			continue
		}
		// Only care about updates that progressed past initial state but stalled.
		// "unspecified" means the update was requested but never admitted — normal for retries.
		state := wu.FSM.Current()
		if state == "unspecified" || state == "completed" || state == "rejected" || state == "aborted" {
			c.Resolve(r.Key)
			continue
		}
		c.Pending(r.Key, umpire.Violation{
			Message: fmt.Sprintf("deduplicated update (requested %d times) stuck in %q — deduplication may be blocking progress", wu.RequestCount, state),
			Tags: map[string]string{
				"workflowID":   wu.WorkflowID,
				"updateID":     wu.UpdateID,
				"updateState":  state,
				"requestCount": fmt.Sprintf("%d", wu.RequestCount),
			},
		})
	}
}
