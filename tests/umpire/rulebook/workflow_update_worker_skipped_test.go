package rulebook

import (
	"testing"
)

func TestWorkflowUpdateWorkerSkippedRule_DetectsSkippedUpdate(t *testing.T) {
	reg := newTestRegistry()
	// Admit update, then poll a task (worker ignores the update).
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowTaskAdded("tq", "wf1", "run1"))
	routeFact(t, reg, makeWorkflowTaskPolled("tq", "wf1", "run1", true))

	violations := checkLivenessRule(reg, &WorkflowUpdateWorkerSkippedRule{})
	if len(violations) != 1 {
		t.Fatalf("expected 1 violation for skipped update, got %d", len(violations))
	}
}

func TestWorkflowUpdateWorkerSkippedRule_NoViolation_UpdateAccepted(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowTaskAdded("tq", "wf1", "run1"))
	routeFact(t, reg, makeWorkflowTaskPolled("tq", "wf1", "run1", true))

	violations := checkLivenessRule(reg, &WorkflowUpdateWorkerSkippedRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for accepted update, got %d", len(violations))
	}
}

func TestWorkflowUpdateWorkerSkippedRule_NoViolation_NoPolls(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))

	violations := checkLivenessRule(reg, &WorkflowUpdateWorkerSkippedRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations without polled tasks, got %d", len(violations))
	}
}
