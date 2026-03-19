package rulebook

import (
	"testing"
)

func TestWorkflowUpdateDedupProgressRule_DetectsStuckDedup(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowUpdateRequested("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateRequested("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))

	violations := checkLivenessRule(reg, &WorkflowUpdateDedupProgressRule{})
	if len(violations) != 1 {
		t.Fatalf("expected 1 violation for stuck dedup, got %d", len(violations))
	}
}

func TestWorkflowUpdateDedupProgressRule_NoViolation_SingleRequest(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowUpdateRequested("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))

	violations := checkLivenessRule(reg, &WorkflowUpdateDedupProgressRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for single request, got %d", len(violations))
	}
}

func TestWorkflowUpdateDedupProgressRule_NoViolation_Completed(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowUpdateRequested("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateRequested("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateCompleted("wf1", "upd1"))

	violations := checkLivenessRule(reg, &WorkflowUpdateDedupProgressRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for completed update, got %d", len(violations))
	}
}
