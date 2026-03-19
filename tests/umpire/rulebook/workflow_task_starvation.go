package rulebook

import (
	"time"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/entity"
)

// WorkflowTaskStarvationRule detects workflow tasks added but never delivered.
type WorkflowTaskStarvationRule struct{}

func (m *WorkflowTaskStarvationRule) Name() string {
	return "WorkflowTaskStarvationRule"
}

func (m *WorkflowTaskStarvationRule) CheckLiveness(c *umpire.LivenessContext) {
	for r := range umpire.ChangedEntities[entity.WorkflowTask](c) {
		wt := r.Entity
		if wt.WorkflowID == "" || wt.IsSpeculative {
			continue
		}

		// Detect starvation in either state:
		//   "added"  — dispatched to matching, never stored or polled (task loss bug)
		//   "stored" — spooled to persistence, never polled by any worker
		var since time.Time
		var stateLabel string
		switch wt.FSM.Current() {
		case "added":
			if wt.AddedAt.IsZero() {
				continue
			}
			since = wt.AddedAt
			stateLabel = "added"
		case "stored":
			if wt.StoredAt.IsZero() {
				continue
			}
			since = wt.StoredAt
			stateLabel = "stored"
		default:
			c.Resolve(r.Key)
			continue
		}

		c.Pending(r.Key, umpire.Violation{
			Message: "workflow task dispatched to matching but never polled — possible worker starvation",
			Tags: map[string]string{
				"taskQueue":  wt.TaskQueue,
				"workflowID": wt.WorkflowID,
				"runID":      wt.RunID,
				"state":      stateLabel,
				"since":      since.Format(time.RFC3339),
			},
		})
	}
}
