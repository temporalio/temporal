package rulebook

import (
	"testing"
)

func TestWorkflowUpdateCompletionRule_DetectsStuckUpdate(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))

	violations := checkLivenessRule(reg, &WorkflowUpdateCompletionRule{})
	if len(violations) != 1 {
		t.Fatalf("expected 1 violation, got %d", len(violations))
	}
	if violations[0].Rule != "WorkflowUpdateCompletionRule" {
		t.Fatalf("wrong model: %s", violations[0].Rule)
	}
}

func TestWorkflowUpdateCompletionRule_NoViolation_UpdateCompleted(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateCompleted("wf1", "upd1"))

	violations := checkLivenessRule(reg, &WorkflowUpdateCompletionRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for completed update, got %d", len(violations))
	}
}
