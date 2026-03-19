package rulebook

import (
	"testing"
)

func TestWorkflowUpdateLossPreventionRule_DetectsStuckUpdate(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))

	violations := checkLivenessRule(reg, &WorkflowUpdateLossPreventionRule{})
	if len(violations) != 1 {
		t.Fatalf("expected 1 violation, got %d", len(violations))
	}
	if violations[0].Rule != "WorkflowUpdateLossPreventionRule" {
		t.Fatalf("wrong model: %s", violations[0].Rule)
	}
}

func TestWorkflowUpdateLossPreventionRule_NoViolation_UpdateAccepted(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))

	violations := checkLivenessRule(reg, &WorkflowUpdateLossPreventionRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for accepted update, got %d", len(violations))
	}
}
