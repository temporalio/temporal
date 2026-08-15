package rule

import (
	"time"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/internal/model"
)

// WorkflowTaskStarvation detects workflow tasks added but never delivered.
type WorkflowTaskStarvation struct{}

func (m *WorkflowTaskStarvation) Name() string {
	return "WorkflowTaskStarvationRule"
}

func (m *WorkflowTaskStarvation) CheckLiveness(c *umpire.LivenessContext) {
	// Workflows observed to have closed. A task never polled before its workflow
	// closed was superseded by the close, not starved — so task progress is
	// judged relative to observed workflow close, not test-teardown timing.
	closed := make(map[string]bool)
	for r := range c.ChangedLifecycles() {
		if wf, ok := r.Entity.(*model.Workflow); ok && wf.WorkflowID != "" && wf.FSM.IsTerminal() {
			closed[wf.WorkflowID] = true
		}
	}

	for r := range c.ChangedLifecycles() {
		wt, ok := r.Entity.(*model.WorkflowTask)
		if !ok {
			continue
		}
		if wt.WorkflowID == "" || wt.IsSpeculative {
			continue
		}
		// A task delivered to a worker made progress.
		if wt.FSM.Reached("polled") {
			c.Resolve(r.Key)
			continue
		}
		// The task's workflow closed; the task was superseded, not starved.
		if closed[wt.WorkflowID] {
			c.Resolve(r.Key)
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
