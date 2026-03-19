package rulebook

import (
	"testing"
)

func TestWorkflowUpdateHistoryOrderingRule_DetectsAcceptedAfterClose(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowStarted("wf1"))
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowCompleted("wf1"))

	violations := checkSafetyRule(reg, &WorkflowUpdateHistoryOrderingRule{})
	if len(violations) != 1 {
		t.Fatalf("expected 1 violation for accepted update after close, got %d", len(violations))
	}
}

func TestWorkflowUpdateHistoryOrderingRule_NoViolation_UpdateCompleted(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowStarted("wf1"))
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateCompleted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowCompleted("wf1"))

	violations := checkSafetyRule(reg, &WorkflowUpdateHistoryOrderingRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for completed update before close, got %d", len(violations))
	}
}

func TestWorkflowUpdateHistoryOrderingRule_NoViolation_WorkflowNotClosed(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowStarted("wf1"))
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))

	violations := checkSafetyRule(reg, &WorkflowUpdateHistoryOrderingRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations when workflow is not closed, got %d", len(violations))
	}
}
