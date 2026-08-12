package rule

import (
	"time"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/model"
)

// NexusOperationClosure checks that no Nexus operation transitions after its
// caller workflow has reached a terminal state. Workflow close is observed for
// only one path; *At timestamps are observation-time. Triage against the real
// Nexus suite before relying on it (see UMPIRE_NEXUS.md).
type NexusOperationClosure struct{}

func (m *NexusOperationClosure) Name() string { return "NexusOperationClosureRule" }

func (m *NexusOperationClosure) CheckSafety(c *umpire.SafetyContext) {
	// Build the set of completed caller workflows with their completion times.
	completedWorkflows := make(map[string]time.Time)
	for r := range c.ChangedLifecycles() {
		wf, ok := r.Entity.(*model.Workflow)
		if !ok {
			continue
		}
		if wf.WorkflowID == "" {
			continue
		}
		if wf.FSM.Current() == "completed" && !wf.CompletedAt.IsZero() {
			completedWorkflows[wf.WorkflowID] = wf.CompletedAt
		}
	}

	for r := range c.ChangedLifecycles() {
		op, ok := r.Entity.(*model.NexusOperation)
		if !ok {
			continue
		}
		if op.WorkflowID == "" {
			continue
		}
		closedAt, closed := completedWorkflows[op.WorkflowID]
		if !closed {
			continue
		}
		violated := false
		// An operation must not start after its caller workflow has closed.
		if !op.StartedAt().IsZero() && op.StartedAt().After(closedAt) {
			violated = true
			c.Eval(r.Key+":started-after-close", false, umpire.Violation{
				Message: "nexus operation started after caller workflow closed",
				Tags: map[string]string{
					"workflowID":       op.WorkflowID,
					"scheduledEventID": op.ScheduledEventID,
					"closedAt":         closedAt.Format(time.RFC3339),
					"startedAt":        op.StartedAt().Format(time.RFC3339),
				},
			})
		}
		// An operation must not reach a terminal state after its caller workflow closed.
		if settledAt, ok := op.SettledAt(); ok && settledAt.After(closedAt) {
			violated = true
			c.Eval(r.Key+":settled-after-close", false, umpire.Violation{
				Message: "nexus operation settled after caller workflow closed",
				Tags: map[string]string{
					"workflowID":       op.WorkflowID,
					"scheduledEventID": op.ScheduledEventID,
					"state":            op.FSM.Current(),
					"closedAt":         closedAt.Format(time.RFC3339),
					"settledAt":        settledAt.Format(time.RFC3339),
				},
			})
		}
		if !violated {
			c.Pass(r.Key)
		}
	}
}
