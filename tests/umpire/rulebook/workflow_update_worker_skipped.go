package rulebook

import (
	"time"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/entity"
)

// WorkflowUpdateWorkerSkippedRule detects updates that remain admitted
// while workflow tasks for the same workflow have been polled by workers.
// This indicates the worker is ignoring the update, and the server should
// eventually reject it.
type WorkflowUpdateWorkerSkippedRule struct{}

func (m *WorkflowUpdateWorkerSkippedRule) Name() string {
	return "WorkflowUpdateWorkerSkippedRule"
}

func (m *WorkflowUpdateWorkerSkippedRule) CheckLiveness(c *umpire.LivenessContext) {
	// Build map of workflow→latest polled time.
	latestPoll := make(map[string]time.Time)
	for r := range umpire.ChangedEntities[entity.WorkflowTask](c) {
		wt := r.Entity
		if wt.WorkflowID == "" {
			continue
		}
		if wt.FSM.Current() == "polled" && !wt.PolledAt.IsZero() {
			if existing, seen := latestPoll[wt.WorkflowID]; !seen || wt.PolledAt.After(existing) {
				latestPoll[wt.WorkflowID] = wt.PolledAt
			}
		}
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
		polledAt, hasPolls := latestPoll[wu.WorkflowID]
		if !hasPolls {
			continue
		}
		// Worker polled a task after the update was admitted but didn't process it.
		if polledAt.Before(wu.AdmittedAt) {
			continue
		}

		c.Pending(r.Key, umpire.Violation{
			Message: "update remains admitted after worker polled workflow task — worker may be ignoring the update",
			Tags: map[string]string{
				"workflowID": wu.WorkflowID,
				"updateID":   wu.UpdateID,
			},
		})
	}
}
